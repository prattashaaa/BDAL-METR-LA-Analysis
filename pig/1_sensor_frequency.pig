-- Pig Task 1: Count traffic records by sensor

traffic_data = LOAD '/metr-la/input/METR-LA_1million.csv'
    USING PigStorage(',')
    AS (Sensor_ID:chararray, Timestamp:chararray, Traffic_Speed:double);

-- Remove the CSV header
data = FILTER traffic_data BY Sensor_ID != 'Sensor_ID';

grouped_data = GROUP data BY Sensor_ID;

sensor_frequency = FOREACH grouped_data
    GENERATE group AS Sensor_ID, COUNT(data) AS Record_Count;

DUMP sensor_frequency;