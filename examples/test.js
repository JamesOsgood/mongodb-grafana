conn = new Mongo();
db = conn.getDB("rpi");

var cursor = db.sensor_value.aggregate(
    [
        {
          "$match": {
            "sensor_type": "light",
            "host_name": "rp1"
          }
        },
        {
            "$bucketAuto": {
              "groupBy": "$ts",
              "buckets": 1299,
              "output": {
                "maxValue": {
                  "$max": "$sensor_value",
                }
              }
            }
          },
          {
            "$group": {
              "_id": "$sensor_type"
            }
        },
        {
            "$project": {
            "name": "$sensor_type",
            "value": "$maxValue",
            "ts": "$_id.min",
            "_id": 0
            }
        }
      ]
      )

 while ( cursor.hasNext() ) {
    printjson( cursor.next() );
 }
