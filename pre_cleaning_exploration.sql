/* 
First we will append/union the 12 monthly bike trip data tables into one table
consisting of all the biketrips from Jan 1, 2021 to Dec 31, 2021. 
*/

CREATE TABLE bike_tripdata_2021.combined_tripdata AS
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
     );

SELECT *
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`;
    
/* NOTES:
Above 'SELECT *' query returned 5,595,063 rows. 
The sum off all 12 table's rows is the same, thus we know the table was created correctly.
We should expect the rows from the 12 seperate tables to equal the appended table as we used a UNION ALL.
A UNION ALL keeps all the rows from the multiple tables specified in the UNION ALL OR appends them.
However, a UNION will remove all rows that have duplicate values in one of the table's you are unioning.

---------------Analyze all columns from left to right for cleaning----------------------------------------------

#1.ride_id:
- check length combinations for ride_id  
- and all values are unique as ride_id is a primary key
*/

SELECT LENGTH(ride_id), count(*)
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
GROUP BY LENGTH(ride_id);

SELECT COUNT (DISTINCT ride_id)
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`;

/* NOTES:
All ride_id strings are 16 characters long and they are all distinct. 
No cleaning neccesary on this column.
*/

--#2. check the allowable rideable_types

SELECT DISTINCT rideable_type
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`;

/* NOTES: 
As seen above, there are 3 types of 'rideable_type': 
electric_bike, classic_bike, docked_bike.
But docked_bikes is a naming error, should be changed to classic_bike,
*/

/* 
#3. Check started_at and ended_at columns.
We only want the rows where the time length of the ride was longer than one minute,
but shorter than one day.
*/

SELECT *
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
WHERE TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 1 OR
   TIMESTAMP_DIFF(ended_at, started_at, MINUTE) >= 1440;

/*
#4. Check the start/end station name/id columns for naming inconsistencies
*/

SELECT start_station_name, count(*)
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
GROUP BY start_station_name
ORDER BY start_station_name;

SELECT end_station_name, count(*)
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
GROUP BY end_station_name
ORDER BY end_station_name;

SELECT COUNT(DISTINCT(start_station_name)) AS unq_startname,
   COUNT(DISTINCT(end_station_name)) AS unq_endname,
   COUNT(DISTINCT(start_station_id)) AS unq_startid,
   COUNT(DISTINCT(end_station_id)) AS unq_endid
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`;

/*
Start and end station names need to be cleaned up:
 -Remove leading and traling spaces.
 -Remove substrings '(Temp)' as Cyclisitc uses these substrings when repairs
  are happening to a station. All station names should have the same naming conventions.
 -Found starting/end_names with "DIVVY CASSETTE REPAIR MOBILE STATION", "Lyft Driver Center Private Rack",
  "351", "Base - 2132 W Hubbard Warehouse", Hubbard Bike-checking (LBS-WH-TEST), "WEST CHI-WATSON".
   We will delete these as they are maintainence trips.
 -Start and end station id columns have many naming convention errors and different string lengths.
  As they do not offer any use to the analysis and there is no benefit to cleaning them, they will be ignored.
*/

#5. Check NULLS in start and end station name columns

SELECT rideable_type, count(*) as num_of_rides
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
WHERE start_station_name IS NULL AND start_station_id IS NULL OR
    end_station_name IS NULL AND end_station_id IS NULL 
GROUP BY rideable_type;

/* 
Classic_bikes/docked_bikes will always start and end their trip locked in a docking station,
but electric bikes have more versatility. Electric bikes can be locked up using their bike lock
in the general vicinity of a docking station; thus, trips do not have to start or end at a station.
As such we will do the following:
- remove classic/docked bike trips that do not have a start or end station name and have no start/end station id to use to fill in the null.
- change the null station names to 'On Bike Lock' for electric bikes
*/

--#6. Check rows were latitude and longitude are null

SELECT *
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`
WHERE start_lat IS NULL OR
 start_lng IS NULL OR
 end_lat IS NULL OR
 end_lng IS NULL;

-- NOTE: we will remove these rows as all rows should have location points

#7. Confirm that there are only 2 member types in the member_casual column:

SELECT DISTINCT member_casual
FROM `divvy-bike-sharing-app-data.bike_tripdata_2021.combined_tripdata`

--NOTE: Yes the only values in this field are 'member' or 'casual'

--Now we are ready to clean the data and then analyze it.
--Go to the data_cleaning_analysis.sql file to see that query.
