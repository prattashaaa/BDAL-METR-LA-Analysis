package avgspeed;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgSpeed {

    public static class SpeedMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text sensorId = new Text();
        private DoubleWritable speed = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("Sensor_ID")) {
                return;
            }

            String[] fields = line.split(",");

            // Expected format:
            // Sensor_ID,Timestamp,Traffic_Speed
            if (fields.length >= 3) {

                try {
                    sensorId.set(fields[0].trim());

                    double trafficSpeed =
                            Double.parseDouble(fields[2].trim());

                    speed.set(trafficSpeed);

                    context.write(sensorId, speed);

                } catch (NumberFormatException e) {
                    // Ignore invalid speed values
                }
            }
        }
    }

    public static class SpeedReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                result.set(average);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: AvgSpeed <input path> <output path>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Average Traffic Speed by Sensor"
        );

        job.setJarByClass(AvgSpeed.class);

        job.setMapperClass(SpeedMapper.class);
        job.setReducerClass(SpeedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}