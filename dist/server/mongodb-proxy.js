var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var app = express();
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

app.use(bodyParser.json());

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

// Called by test
app.all('/', function(req, res, next) 
{
  setCORSHeaders(res);
  res.send('Test OK');
  next()
});

// Called by templating functions and to look up variables
app.all('/search', function(req, res, next)
{
  setCORSHeaders(res);

  // Parse query string in target
  queryArgs = parseQuery(req.body.target, {})
  doTemplateQuery(queryArgs, req.body.db, res, next);
});

// Called to get graph points
app.all('/query', function(req, res, next)
{
    setCORSHeaders(res);

    // Parse query string in target
    substitutions = { "$from" : new Date(req.body.range.from),
                      "$to" : new Date(req.body.range.to) }
    tg = req.body.targets[0].target
    queryArgs = parseQuery(tg, substitutions)
    if (queryArgs.err != null)
    {
      next(queryArgs.err)
    }
    else
    {
      // Run the query
      runAggregateQuery(req.body.db.url, req.body.db.db, queryArgs, res, next)
    }
  }
);

app.use(function(error, req, res, next) 
{
  // Any request to this server will get here, and will send an HTTP
  // response with the error message
  res.status(500).json({ message: error.message });
});

app.listen(3333);

console.log("Server is listening to port 3333");

// Run an aggregate query. Must return documents of the form
// { value : 0.34334, ts : <epoch time in seconds> }

function runAggregateQuery(url, dbName, queryArgs, res, next )
{
  MongoClient.connect(url, function(err, client) 
  {
    if ( err != null )
    {
      next(err)
    }
    else
    {
      const db = client.db(dbName);
  
      // Get the documents collection
      const collection = db.collection(queryArgs.collection);
      collection.aggregate(queryArgs.pipeline).toArray(function(err, docs) 
        {
          if ( err != null )
          {
            client.close();
            next(err)
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
              output = []
              output.push({ 'target' : tg, 'datapoints' : datapoints })
              res.json(output);
              next()
            }
            catch(err)
            {
              next(err)
            }
          }
        })
      }
    })
}

// Runs a query to support templating. Must returns documents of the form
// { _id : <id> }
function doTemplateQuery(queryArgs, db, res, next)
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
        next(err)
      }
      else
      {
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
