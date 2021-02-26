## Introduction
 
I decided to use apache beam to create the posibility to use dataflow for scaling the process. I found the data zipped and apache beam with python doesn't accept zip files, so I make a bash script to unzip files and execute the pipeline. Another wall that I found was the correcto type for latitude and longitude I used FLOAT64 to achieve the load on bigquery.


## Quality checks (Ideas)

 * Latitude or longitude can't be 0 or null
 * When distance is 0 we should consider to filter them and when is very high aswell. (For example, we can create a rule that we need atleast 300 meters to believe that it was a real trip)
 * Pickup time and dropoff time should be more than 10 minutes. (I'm only making a rule to believe or not in the data)


## SQL Answers

 * 3.A
SELECT avg(fare_amount) as avg_fare
FROM `cartointerview.taxi_data.trips` 

 * 3.B
SELECT pickup_latitude, pickup_longitude, avg(tip_amount) as avg_tip
FROM `cartointerview.taxi_data.trips` 
GROUP BY pickup_latitude, pickup_longitude
order by avg_tip desc
limit 10

PD: My guts are telling me that I need to locate that latitude and longitude on a geometry and then group by with that zones. 
