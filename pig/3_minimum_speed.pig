-- Pig Task 3: Calculate minimum traffic speed by sensor

traffic_data = LOAD '/metr-la/input/METR-LA_1million.csv'
    USING PigStorage(',')
    AS (Sensor_ID:chararray, Timestamp:chararray, Traffic_Speed:double);

-- Remove the CSV header
data = FILTER traffic_data BY Sensor_ID != 'Sensor_ID';

grouped_data = GROUP data BY Sensor_ID;

minimum_speed = FOREACH grouped_data
    GENERATE group AS Sensor_ID, MIN(data.Traffic_Speed) AS Minimum_Speed;

DUMP minimum_speed;