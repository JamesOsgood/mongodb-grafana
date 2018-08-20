# MongoDB datasource for Grafana

## Features
Allows MongoDB to be used as a data source for Grafana by providing a proxy to convert the Grafana Data source [API](http://docs.grafana.org/plugins/developing/datasources/) into MongoDB aggregation queries

## Requirements

* **Grafana** > 3.x.x
* **MongoDB** > 3.4.x

## Installation

### Install the Grafana plugin components

* Copy the whole mongodb-grafana dir into the Grafana plugins dir ( /usr/local/var/lib/grafana/plugins )
* Restart the Grafana server. If installed via Homebrew, this will be `brew services restart grafana`

### Install and Start the MongoDB proxy server

* Open a command prompt in the mongodb-grafana directory
* Run `npm install` to install the node.js dependencies
* Run `npm run server` to start the REST API proxy to MongoDB. By default, the server listens on http://localhost:3333

## Examples

Create a new data source of type MongoDB as shown below. The MongoDB details are :

* **MongoDB URL** - `mongodb://rpiread:rpiread@rpi-sensor-data-shard-00-00-ifxxs.mongodb.net:27017,rpi-sensor-data-shard-00-01-ifxxs.mongodb.net:27017,rpi-sensor-data-shard-00-02-ifxxs.mongodb.net:27017/test?ssl=true&replicaSet=rpi-sensor-data-shard-0&authSource=admin`
* **MongoDB Database** - `rpi`

<img src="src/img/sample_datasource.png" alt="Sample Data Source" style="width: 500px;"/>

Then save the data source

#### Example 1 - Simple aggregate to rename fields

Import the dashboard in `examples\RPI MongoDB - Atlas.json`

This should show a graph of light sensor values from a Raspberry PI with an [EnviroPHAT](https://thepihut.com/products/enviro-phat) board feeding readings every minute into a MongoDB Atlas database.

<img src="src/img/sample_dashboard.png" alt="Sample Dashboard" style="width: 800px;"/>

Clicking on the title of the graph allows you to see the aggregation query being run against the 'RPI Atlas' data source

<img src="src/img/sample_query.png" alt="Sample Query" style="width: 800px;"/>

The query here is

```javascript
db.sensor_value.aggregate ( [ 
{ "$match" :    {   "sensor_type" : "$sensor",   "host_name" : "$host",   "ts" : { "$gte" : "$from", "$lte" : "$to" } }  },        
 {"$sort" : {"ts" : 1}},            
 {"$project" :   {  "name" : "value",   "value" : "$sensor_value",  "ts" : "$ts",  "_id" : 0} } ])
 ```

 The API is expecting back documents with the following fields

 * `name` - Name of the series ( will be displayed on the graph)
 * `value` - The float value of the point
 * `ts` - The time of the point as a BSON date

 These documents are then converted into the [Grafana API](http://docs.grafana.org/plugins/developing/datasources/)

`$from` and `$to` are expanded by the plugin as BSON dates based on the range settings on the UI.

## Template Variables

`$sensor` and `$host` are template variables that are filled in by Grafana based on the drop down. The sample template queries are shown below. They expect documents to be returned with a single `_id` field.


<img src="src/img/sample_template.png" alt="Sample Templates" style="width: 800px;"/>

#### Example 2 - Using $bucketAuto to push data point aggregation to the server

Grafana tells the backend server the date range along with the size of the buckets that should be used to calculate points. Therefore it's possible to use the MongoDB aggregation operator [$bucketAuto](https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/) to automatically bucket the data points into display points. To support this the backend provides the `$dateBucketCount` macro so that queries such as the one below can be written

```javascript
db.sensor_value.aggregate( [ 
{  "$match" :  {  "sensor_type" : "$sensor", "host_name" : "$host" , "ts" : { "$gte" : "$from", "$lt" : "$to" }}},
{  "$bucketAuto" :  { "groupBy" : "$ts",  
                           "buckets" : "$dateBucketCount", 
                            "output" :  {  "maxValue" : { "$max" : "$sensor_value" }  }   }   },  
{  "$project" :  {  "name" : "value",  "value" : "$maxValue",  "ts" : "$_id.min",  "_id" : 0  }  }  ]  )
```    
Note that ```_id``` field of the bucketAuto output contains the start and end of the bucket so we can use that as the ```ts``` value

The dashboard in `examples\RPI MongoDB Bucket - Atlas.json` shows this.

#### Example 3 - Using a Tabel Panel

<img src="src/img/table_panel.png" alt="Table Panel" style="width: 800px;"/>

Table panels are now supported with queries of the form

```javascript
db.sensor_value.aggregate(
[
    {  "$match" :  {  "ts" : { "$gte" : "$from", "$lt" : "$to"  }}},
    {  "$group":    {  "_id":  { "sensor_name" : "$sensor_name",  "sensor_type" : "$sensor_type"  }, "cnt" : { "$sum" : 1 },  "ts" : { "$max" : "$ts" }  } }, 
    { "$project": {  "name" : { "$concat" : ["$_id.sensor_name",":","$_id.sensor_type" ]},  "value" : "$cnt",  "ts" : 1, "_id" : 0} } 
])
```    

The dashboard in `examples\Sensor Values Count - Atlas.json` shows this.

## Running the proxy as a service on a Mac

* Install [forever-mac](https://www.npmjs.com/package/forever-mac)
* Copy server/mongodb-grafana-proxy.plist to ~/Library/LaunchAgents
* run `launchctl load mongodb-grafana-proxy` from ~/Library/LaunchAgents

This launch ctrl plist runs the node script via forever. To check it's running, use `forever list`. Logs go into /usr/local/var/lib/grafana/plugins/mongodb-grafana/dist/server

## Development

To run grafana against a dev version of the plugin on a mac using grafana installed via Homebrew
 
* Stop the grafana service `brew services stop grafana`
* Open a command prompt in /debugging
* Run ./start_grafana.sh
* Alter code
* npm run build to build the UI
* Developer tools -> empty cache and hard reload

Note

* Homebrew grafana versions in /usr/local/Cellar









