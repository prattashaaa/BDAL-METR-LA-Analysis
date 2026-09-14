-- Pig Task 4: Calculate maximum traffic speed by sensor

traffic_data = LOAD '/metr-la/input/METR-LA_1million.csv'
    USING PigStorage(',')
    AS (Sensor_ID:chararray, Timestamp:chararray, Traffic_Speed:double);

-- Remove the CSV header
data = FILTER traffic_data BY Sensor_ID != 'Sensor_ID';

grouped_data = GROUP data BY Sensor_ID;

maximum_speed = FOREACH grouped_data
    GENERATE group AS Sensor_ID, MAX(data.Traffic_Speed) AS Maximum_Speed;

DUMP maximum_speed;