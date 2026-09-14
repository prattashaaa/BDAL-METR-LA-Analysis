-- Pig Task 2: Calculate average traffic speed by sensor

traffic_data = LOAD '/metr-la/input/METR-LA_1million.csv'
    USING PigStorage(',')
    AS (Sensor_ID:chararray, Timestamp:chararray, Traffic_Speed:double);

-- Remove the CSV header
data = FILTER traffic_data BY Sensor_ID != 'Sensor_ID';

grouped_data = GROUP data BY Sensor_ID;

average_speed = FOREACH grouped_data
    GENERATE group AS Sensor_ID, AVG(data.Traffic_Speed) AS Average_Speed;

DUMP average_speed;