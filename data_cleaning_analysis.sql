WITH combined_data AS
(
   SELECT *
   FROM (
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.jan_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.feb_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.mar_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.apr_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.may_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.jun_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.jul_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.aug_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.sep_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.oct_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.nov_tripdata`
     UNION ALL 
     SELECT * FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.dec_tripdata`
     )
),
-----------------------------------------------------------------------------------------------------
----------------------------------------CLEAN THE DATA-----------------------------------------------
-----------------------------------------------------------------------------------------------------

----------#1. Find classic/docked bike trips which do not begin or end at a docking station----------

null_station_names AS
(
   SELECT ride_id AS bad_ride_id
   FROM (SELECT ride_id, start_station_name, start_station_id, 
            end_station_name, end_station_id
         FROM combined_data 
         WHERE rideable_type = 'docked_bike' OR rideable_type = 'classic_bike'
         )
   WHERE start_station_name IS NULL AND start_station_id IS NULL OR
      end_station_name IS NULL AND end_station_id IS NULL
),
----------#2. Remove these rows as classic/docked bikes have to start/end at a docking station----------
----------------ALSO remove rows that do not have a starting/ending latitude and longitude--------------
null_station_names_cleaned AS
(
   SELECT *
   FROM combined_data cd
   LEFT JOIN null_station_names nsn
   ON cd.ride_id = nsn.bad_ride_id
   WHERE nsn.bad_ride_id IS NULL AND
      cd.start_lat IS NOT NULL AND
      cd.start_lng IS NOT NULL AND
      cd.end_lat IS NOT NULL AND
      cd.end_lng IS NOT NULL
),
----------#3. Replace occurrences of docked_bike with classic bike, clean up station names,----------
------------- fill null station names with 'On Bike Lock' as these are occurrences when--------------
------------- the customer used the bike lock on the electric bike instead of docking it,-------------
------------- and create date/ride-time columns ------------------------------------------------------
aggregated_tripdata AS
(
   SELECT ride_id,
      REPLACE(rideable_type, 'docked_bike', 'classic_bike') AS ride_type,
      started_at,
      ended_at,
      IFNULL(TRIM(REPLACE(start_station_name, '(Temp)', '')), 'On Bike Lock') AS starting_station_name,
      IFNULL(TRIM(REPLACE(end_station_name, '(Temp)', '')), 'On Bike Lock') AS ending_station_name,
      CASE
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 1 THEN 'Sun'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 2 THEN 'Mon'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 3 THEN 'Tues'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 4 THEN 'Wed'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 5 THEN 'Thur'
         WHEN EXTRACT(DAYOFWEEK FROM started_at) = 6 THEN 'Fri'
         ELSE'Sat' 
      END AS day_of_week,
      CASE
         WHEN EXTRACT(MONTH FROM started_at) = 1 THEN 'Jan'
         WHEN EXTRACT(MONTH FROM started_at) = 2 THEN 'Feb'
         WHEN EXTRACT(MONTH FROM started_at) = 3 THEN 'Mar'
         WHEN EXTRACT(MONTH FROM started_at) = 4 THEN 'Apr'
         WHEN EXTRACT(MONTH FROM started_at) = 5 THEN 'May'
         WHEN EXTRACT(MONTH FROM started_at) = 6 THEN 'Jun'
         WHEN EXTRACT(MONTH FROM started_at) = 7 THEN 'July'
         WHEN EXTRACT(MONTH FROM started_at) = 8 THEN 'Aug'
         WHEN EXTRACT(MONTH FROM started_at) = 9 THEN 'Sept'
         WHEN EXTRACT(MONTH FROM started_at) = 10 THEN 'Oct'
         WHEN EXTRACT(MONTH FROM started_at) = 11 THEN 'Nov'
         ELSE 'Dec'
      END AS month,
      EXTRACT(DAY FROM started_at) as day,
      EXTRACT(YEAR FROM started_at) AS year,
      TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_time_minutes,
      start_lat,
      start_lng,
      end_lat,
      end_lng,
      member_casual AS member_type
   FROM null_station_names_cleaned 
),
---------- Remove rows that contain bike maintainence and testing data----------
cleaned_combined_tripdata AS
(
   SELECT *
   FROM aggregated_tripdata 
   WHERE ride_time_minutes > 1 AND
      ride_time_minutes < 1440 AND
      starting_station_name <> 'DIVVY CASSETTE REPAIR MOBILE STATION' AND
      starting_station_name <> 'Lyft Driver Center Private Rack' AND 
      starting_station_name <> '351' AND 
      starting_station_name <> 'Base - 2132 W Hubbard Warehouse' AND 
      starting_station_name <> 'Hubbard Bike-checking (LBS-WH-TEST)' AND 
      starting_station_name <> 'WEST CHI-WATSON' AND 
      ending_station_name <> 'DIVVY CASSETTE REPAIR MOBILE STATION' AND
      ending_station_name <> 'Lyft Driver Center Private Rack' AND 
      ending_station_name <> '351' AND 
      ending_station_name <> 'Base - 2132 W Hubbard Warehouse' AND 
      ending_station_name <> 'Hubbard Bike-checking (LBS-WH-TEST)' AND 
      ending_station_name <> 'WEST CHI-WATSON'
),
------------------------------DATA IS CLEAN------------------------------

----------------NOW LETS ANALYZE AND PREPARE FOR VISUALIZATION--------------

-- type of ride:
type_of_ride AS
(
   SELECT ride_type, member_type, count(*) AS amount_of_rides
   FROM cleaned_combined_tripdata
   GROUP BY ride_type, member_type
   ORDER BY member_type, amount_of_rides DESC
),
-- amont of rides per month:
rides_per_month AS
(
   SELECT member_type, month, count(*) AS num_of_rides
   FROM cleaned_combined_tripdata
   GROUP BY member_type, month
),

-- amount of rides per day:
rides_per_day AS
(
   SELECT member_type, day_of_week, count(*) AS num_of_rides
   FROM cleaned_combined_tripdata
   GROUP BY member_type, day_of_week
),

-- amount of rides per hour:
rides_per_hour AS
(
   SELECT member_type, EXTRACT(HOUR FROM started_at) AS time_of_day, count(*) as num_of_rides
   FROM cleaned_combined_tripdata
   GROUP BY member_type, time_of_day
),

--average length of ride per day:
avg_trip_length AS
(
   SELECT member_type, 
      day_of_week, 
      ROUND(AVG(ride_time_minutes), 0) AS avg_ride_time_minutes,
      AVG(AVG(ride_time_minutes)) OVER(PARTITION BY member_type) AS combined_avg_ride_time
   FROM cleaned_combined_tripdata
   GROUP BY member_type, day_of_week
), 

-- starting docking station location for casuals:
start_station_casual AS
(
   SELECT starting_station_name, 
      ROUND(AVG(start_lat), 4) AS start_lat, 
      ROUND(AVG(start_lng), 4) AS start_lng,  
      count(*) AS num_of_rides
   FROM bike_tripdata_2021.cleaned_tripdata_2021
   WHERE member_type = 'casual' AND starting_station_name <> 'On Bike Lock' 
   GROUP BY starting_station_name
),

-- starting docking station location for members:
start_station_member AS
(
   SELECT starting_station_name,
      ROUND(AVG(start_lat), 4) AS start_lat, 
      ROUND(AVG(start_lng), 4) AS start_lng,  
      count(*) AS num_of_rides
   FROM bike_tripdata_2021.cleaned_tripdata_2021
   WHERE member_type = 'member' AND starting_station_name <> 'On Bike Lock' 
   GROUP BY starting_station_name
),

-- ending docking station name for casuals:
end_station_casual AS
(
   SELECT ending_station_name,
      ROUND(AVG(start_lat), 4) AS end_lat, 
      ROUND(AVG(start_lng), 4) AS end_lng,  
      count(*) AS num_of_rides
   FROM bike_tripdata_2021.cleaned_tripdata_2021
   WHERE member_type = 'casual' AND ending_station_name <> 'On Bike Lock'
   GROUP BY ending_station_name
),

-- ending bike station for members:
end_station_member AS
(
   SELECT ending_station_name,
      ROUND(AVG(start_lat), 4) AS end_lat, 
      ROUND(AVG(start_lng), 4) AS end_lng,  
      count(*) AS num_of_rides
   FROM bike_tripdata_2021.cleaned_tripdata_2021
   WHERE member_type = 'member' AND ending_station_name <> 'On Bike Lock'
   GROUP BY ending_station_name
),

-- ending bike lock location for casuals:
lock_location_casual AS
(
   SELECT end_lat, end_lng, count(*) AS num_of_rides
   FROM cleaned_combined_tripdata
   WHERE ending_station_name = 'On Bike Lock' AND member_type = 'casual'
   GROUP BY end_lat, end_lng
)

-- ending bike lock location for members:
SELECT end_lat, end_lng, count(*) AS num_of_rides
FROM cleaned_combined_tripdata
WHERE ending_station_name = 'On Bike Lock' AND member_type = 'member'
GROUP BY end_lat, end_lng




