package maxspeed;

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

public class MaxSpeed {

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

            double maximum = Double.MIN_VALUE;

            for (DoubleWritable value : values) {
                if (value.get() > maximum) {
                    maximum = value.get();
                }
            }

            result.set(maximum);
            context.write(key, result);
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: MaxSpeed <input path> <output path>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Maximum Traffic Speed by Sensor"
        );

        job.setJarByClass(MaxSpeed.class);

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