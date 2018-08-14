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

  MongoClient.connect(req.body.db.url, function(err, client)
  {
    if ( err != null )
    {
      res.send({ status : "error", 
                 display_status : "Error", 
                 message : 'MongoDB Connection Error: ' + err.message });
    }
    else
    {
      res.send( { status : "success", 
                  display_status : "Success", 
                  message : 'MongoDB Connection test OK' });
    }
    next()
  })
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
        var queryResults = queryStatus[i].results
        var keys = Object.keys(queryResults)
        for (var k = 0; k < keys.length; k++)
        {
          var tg = keys[k]
          output.push(queryResults[tg])
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
      tg = req.body.targets[queryId]
      queryArgs = parseQuery(tg.target, substitutions)
      queryArgs.type = tg.type
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
  doc = {}
  queryErrors = []

  query = query.trim() 
  if (query.substring(0,3) != "db.")
  {
    queryErrors.push("Query must start with db.")
    return null
  }

  // Query is of the form db.<collection>.aggregate or db.<collection>.find
  // Split on the first ( after db.
  var openBracketIndex = query.indexOf('(', 3)
  if (openBracketIndex == -1)
  {
    queryErrors.push("Can't find opening bracket")
  }
  else
  {
    // Split the first bit - it's the collection name and operation ( must be aggregate )
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
        // Wrap args in array syntax so we can check for optional options arg
        args = '[' + args + ']'
        docs = JSON.parse(args)
        // First Arg is pipeline
        doc.pipeline = docs[0]
        // If we have 2 top level args, second is agg options
        if ( docs.length == 2 )
        {
          doc.agg_options = docs[1]
        }
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
      logQuery(queryArgs.pipeline, queryArgs.agg_options)
      var stopwatch = new Stopwatch(true)

      collection.aggregate(queryArgs.pipeline, queryArgs.agg_options).toArray(function(err, docs) 
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
              var results = {}
              if ( queryArgs.type == 'timeserie' )
              {
                results = getTimeseriesResults(docs)
              }
              else
              {
                results = getTableResults(docs)
              }
      
              client.close();
              var elapsedTimeMs = stopwatch.stop()
              logTiming(body, elapsedTimeMs)
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

function getTableResults(docs)
{
  var columns = {}
  
  // Build superset of columns
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    // Go through all properties
    for (var propName in doc )
    {
      // See if we need to add a new column
      if ( !(propName in columns) )
      {
        columns[propName] = 
        {
          text : propName,
          type : "text"
        }
      }
    }
  }
  
  // Build return rows
  rows = []
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    row = []
    // All cols
    for ( var colName in columns )
    {
      var col = columns[colName]
      if ( col.text in doc )
      {
        row.push(doc[col.text])
      }
      else
      {
        row.push(null)
      }
    }
    rows.push(row)
  }
  
  var results = {}
  results["table"] = {
    columns :  Object.values(columns),
    rows : rows,
    type : "table"
  }
  return results
}

function getTimeseriesResults(docs)
{
  var results = {}
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    var tg = doc.name
    var dp = null
    if (tg in results)
    {
      dp = results[tg]
    }
    else
    {
      dp = { 'target' : tg, 'datapoints' : [] }
      results[tg] = dp
    }
    
    results[tg].datapoints.push([doc['value'], doc['ts'].getTime()])
  }
  return results
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

function logQuery(query, options)
{
  if (serverConfig.logQueries)
  {
    console.log("Query:")
    console.log(JSON.stringify(query,null,2))
    if ( options != null )
    {
      console.log("Query Options:")
      console.log(JSON.stringify(options,null,2))
    }
  }
}

function logTiming(body, elapsedTimeMs)
{
  if (serverConfig.logTimings)
  {
    var range = new Date(body.range.to) - new Date(body.range.from)
    var diff = moment.duration(range)
    
    console.log("Request: " + intervalCount(diff, body.interval, body.intervalMs) + " - Returned in " + elapsedTimeMs.toFixed(2) + "ms")
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