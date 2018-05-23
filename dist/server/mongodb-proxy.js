var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var app = express();
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
var config = require('config');
var Stopwatch = require("statman-stopwatch");
var moment = require('moment')

app.use(bodyParser.json());

// Called by test
app.all('/', function(req, res, next) 
{
  logRequest(req.body, "/")
  setCORSHeaders(res);
  res.send('Test OK');
  next()
});

// Called by template functions and to look up variables
app.all('/search', function(req, res, next)
{
  logRequest(req.body, "/search")
  setCORSHeaders(res);

  // Generate an id to track requests
  const requestId = ++requestIdCounter                 
  // Add state for the queries in this request
  var queryStates = []
  requestsPending[requestId] = queryStates
  // Parse query string in target
  queryArgs = parseQuery(req.body.target, {})
  if (queryArgs.err != null)
  {
    queryError(requestId, queryArgs.err, next)
  }
  else
  {
    doTemplateQuery(requestId, queryArgs, req.body.db, res, next);
  }
});

// State for queries in flight. As results come it, acts as a semaphore and sends the results back
var requestIdCounter = 0
// Map of request id -> array of results. Results is
// { query, err, output }
var requestsPending = {}

// Called when a query finishes with an error
function queryError(requestId, err, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if ( requestId in requestsPending )
  {
    // Remove request
    delete requestsPending[requestId]
    // Send back error
    next(err)
  }
}

// Called when query finished
function queryFinished(requestId, queryId, results, res, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if ( requestId in requestsPending )
  {
    var queryStatus = requestsPending[requestId]
    // Mark this as finished
    queryStatus[queryId].pending = false
    queryStatus[queryId].results = results

    // See if we're all done
    var done = true
    for ( var i = 0; i < queryStatus.length; i++)
    {
      if (queryStatus[i].pending == true )
      {
        done = false
        break
      }
    }
  
    // If query done, send back results
    if (done)
    {
      // Concatenate results
      output = []    
      for ( var i = 0; i < queryStatus.length; i++)
      {
        if ( queryStatus[i].results.datapoints.length > 0)
        {
          output.push(queryStatus[i].results)
        }
      }
      res.json(output);
      next()
      // Remove request
      delete requestsPending[requestId]
    }
  }
}

// Called to get graph points
app.all('/query', function(req, res, next)
{
    logRequest(req.body, "/query")
    setCORSHeaders(res);

    // Parse query string in target
    substitutions = { "$from" : new Date(req.body.range.from),
                      "$to" : new Date(req.body.range.to),
                      "$dateBucketCount" : getBucketCount(req.body.range.from, req.body.range.to, req.body.intervalMs)
                     }

    // Generate an id to track requests
    const requestId = ++requestIdCounter                 
    // Add state for the queries in this request
    var queryStates = []
    requestsPending[requestId] = queryStates
    var error = false

    for ( var queryId = 0; queryId < req.body.targets.length && !error; queryId++)
    {
      tg = req.body.targets[queryId].target
      queryArgs = parseQuery(tg, substitutions)
      if (queryArgs.err != null)
      {
        queryError(requestId, queryArgs.err, next)
        error = true
      }
      else
      {
        // Add to the state
        queryStates.push( { pending : true } )

        // Run the query
        runAggregateQuery( requestId, queryId, req.body, queryArgs, res, next)
      }
    }
  }
);

app.use(function(error, req, res, next) 
{
  // Any request to this server will get here, and will send an HTTP
  // response with the error message
  res.status(500).json({ message: error.message });
});

// Get config from server/default.json
var serverConfig = config.get('server');

app.listen(serverConfig.port);

console.log("Server is listening on port " + serverConfig.port);

function setCORSHeaders(res) 
{
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST");
  res.setHeader("Access-Control-Allow-Headers", "accept, content-type");  
}

function forIn(obj, processFunc)
{
    var key;
    for (key in obj) 
    {
        var value = obj[key]
        processFunc(obj, key, value)
        if ( value != null && typeof(value) == "object")
        {
            forIn(value, processFunc)
        }
    }
}

function parseQuery(query, substitutions)
{
  query = query.trim() 
  if (query.substring(0,3) != "db.")
  {
    return null
  }
  doc = {}
  queryErrors = []

  // Query is of the form db.<collection>.aggregate or db.<collection>.find
  // Split on the first ( after db.
  var openBracketIndex = query.indexOf('(', 3)
  if (openBracketIndex == -1)
  {
    queryErrors.push("Can't find opening bracket")
  }
  else
  {
    // Split the first bit - it's the collection name and operation ( find or aggregate )
    var parts = query.substring(3, openBracketIndex).split('.')
    // Collection names can have .s so last part is operation, rest is the collection name
    if (parts.length >= 2)
    {
      doc.operation = parts.pop().trim()
      doc.collection = parts.join('.')       
    }
    else
    {
      queryErrors.push("Invalid collection and operation syntax")
    }
  
    // Args is the rest up to the last bracket
    var closeBracketIndex = query.indexOf(')', openBracketIndex)
    if (closeBracketIndex == -1)
    {
      queryErrors.push("Can't find last bracket")
    }
    else
    {
      var args = query.substring(openBracketIndex + 1, closeBracketIndex)
      if ( doc.operation == 'aggregate')
      {
        // Arg is pipeline
        doc.pipeline = JSON.parse(args)
        // Replace with substitutions
        for ( var i = 0; i < doc.pipeline.length; i++)
        {
            var stage = doc.pipeline[i]
            forIn(stage, function (obj, key, value)
                {
                    if ( typeof(value) == "string" )
                    {
                        if ( value in substitutions )
                        {
                            obj[key] = substitutions[value]
                        }
                    }
                })
          }
      }
      else
      {
        queryErrors.push("Unknown operation " + doc.operation + ", only aggregate supported")
      }
    }
  }
  
  if (queryErrors.length > 0 )
  {
    doc.err = new Error('Failed to parse query - ' + queryErrors.join(':'))
  }

  return doc
}

// Run an aggregate query. Must return documents of the form
// { value : 0.34334, ts : <epoch time in seconds> }

function runAggregateQuery( requestId, queryId, body, queryArgs, res, next )
{
  MongoClient.connect(body.db.url, function(err, client) 
  {
    if ( err != null )
    {
      queryError(requestId, err, next)
    }
    else
    {
      const db = client.db(body.db.db);
  
      // Get the documents collection
      const collection = db.collection(queryArgs.collection);
      logQuery(queryArgs.pipeline)
      var stopwatch = new Stopwatch(true)

      collection.aggregate(queryArgs.pipeline).toArray(function(err, docs) 
        {
          if ( err != null )
          {
            client.close();
            queryError(requestId, err, next)
          }
          else
          {
            try
            {
              datapoints = []
              for ( var i = 0; i < docs.length; i++)
              {
                var doc = docs[i]
                tg = doc.name
                datapoints.push([doc['value'], doc['ts'].getTime()])
              }
      
              client.close();
              var elapsedTimeMs = stopwatch.stop()
              var results = { 'target' : tg, 'datapoints' : datapoints }
              logTiming(body, elapsedTimeMs, datapoints)
              // Mark query as finished - will send back results when all queries finished
              queryFinished(requestId, queryId, results, res, next)
            }
            catch(err)
            {
              queryError(requestId, err, next)
            }
          }
        })
      }
    })
}

// Runs a query to support templates. Must returns documents of the form
// { _id : <id> }
function doTemplateQuery(requestId, queryArgs, db, res, next)
{
 if ( queryArgs.err == null)
  {
    // Database Name
    const dbName = db.db
    
    // Use connect method to connect to the server
    MongoClient.connect(db.url, function(err, client) 
    {
      if ( err != null )
      {
        queryError(requestId, err, next )
      }
      else
      {
        // Remove request from list
        if ( requestId in requestsPending )
        {
          delete requestsPending[requestId]
        }
        const db = client.db(dbName);
        // Get the documents collection
        const collection = db.collection(queryArgs.collection);
          
        collection.aggregate(queryArgs.pipeline).toArray(function(err, result) 
          {
            assert.equal(err, null)
    
            output = []
            for ( var i = 0; i < result.length; i++)
            {
              var doc = result[i]
              output.push(doc["_id"])
            }
            res.json(output);
            client.close()
            next()
          })
      }
    })
  }
  else
  {
    next(queryArgs.err)
  }
}

function logRequest(body, type)
{
  if (serverConfig.logRequests)
  {
    console.log("REQUEST: " + type + ":\n" + JSON.stringify(body,null,2))
  }
}

function logQuery(query, type)
{
  if (serverConfig.logQueries)
  {
    console.log(JSON.stringify(query,null,2))
  }
}

function logTiming(body, elapsedTimeMs, datapoints)
{
  if (serverConfig.logTimings)
  {
    var range = new Date(body.range.to) - new Date(body.range.from)
    var diff = moment.duration(range)
    
    console.log("Request: " + intervalCount(diff, body.interval, body.intervalMs) + " - Returned " + datapoints.length + " data points in " + elapsedTimeMs.toFixed(2) + "ms")
  }
}

// Take a range as a moment.duration and a grafana interval like 30s, 1m etc
// And return the number of intervals that represents
function intervalCount(range, intervalString, intervalMs) 
{
  // Convert everything to seconds
  var rangeSeconds = range.asSeconds()
  var intervalsInRange = rangeSeconds / (intervalMs / 1000)

  var output = intervalsInRange.toFixed(0) + ' ' + intervalString + ' intervals'
  return output
}

function getBucketCount(from, to, intervalMs)
{
  var boundaries = []
  var current = new Date(from).getTime()
  var toMs = new Date(to).getTime()
  var count = 0
  while ( current < toMs )
  {
    current += intervalMs
    count++
  }

  return count
}