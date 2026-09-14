-- Pig Task 5: Speed Distribution

traffic_data = LOAD '/metr-la/input/METR-LA_1million.csv'
USING PigStorage(',')
AS (Sensor_ID:chararray, Timestamp:chararray, Traffic_Speed:double);

-- Remove the CSV header
data = FILTER traffic_data BY Sensor_ID != 'Sensor_ID';

-- Create speed ranges
speed_buckets = FOREACH data GENERATE
    (int)(Traffic_Speed / 10) * 10 AS Speed_Range;

-- Group records by speed range
grouped_data = GROUP speed_buckets BY Speed_Range;

-- Count records in each speed range
speed_distribution = FOREACH grouped_data
    GENERATE group AS Speed_Range, COUNT(speed_buckets) AS Record_Count;

-- Display the distribution
DUMP speed_distribution;