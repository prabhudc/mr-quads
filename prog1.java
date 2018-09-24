import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class prog1 extends Configured implements Tool {


    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }


    public static class MapClass extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = ((Text) value).toString();

            String[] strList = line.split(",");
            if (strList.length == 2)
                context.write(new Text(strList[1]), new Text(strList[0]));

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> txtArr=new ArrayList<String>();
            Set<String> outArr=new HashSet<String>();
            String concatOutput = new String();

            int i = 0;

            for (Text v : values) {
                txtArr.add(v.toString());
            }


            Collections.sort(txtArr);


            for (int j = 0; j < java.lang.Math.floor(txtArr.size()/3) ; j++) {
                int loop_count  =   j;
                  while(loop_count < txtArr.size()-2) {
                    for (int k = loop_count   ; k < loop_count + 3 ; k++) {
//                        System.out.println("loop_count : " + loop_count + "\t|\tj : " + j + "\t|\tk : " + k + "\t|\t" + txtArr.get(k));
                        outArr.add(txtArr.get(k));
                    }
                    loop_count++;
                      outArr.add(key.toString());
//                      Collections.sort(outArr);
                      for( String entry : outArr){
                          concatOutput += "~"+entry;
                      }
                      outArr.clear();
                      context.write( new Text(concatOutput), key);
                      concatOutput = "";
                }
            }


        }
    }



    public static class MapClass2 extends Mapper<Text, Text, Text, Text> {
//Identity mapper

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                context.write(key , value);
        }
    }


    public static class Reduce2 extends Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> txtSet=new HashSet<String>();
            String concatOutput = new String();

            for (Text v : values) {
                txtSet.add(v.toString());
            }

            for( String entry : txtSet){
                concatOutput += "~"+entry;
            }

            if(key.toString().equals(concatOutput))
                context.write(key , NullWritable.get());
        }
    }



            public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new prog1(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mr-quads/quad1/");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "quad-1");
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);

        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);


        jobA.setMapperClass(MapClass.class);
        jobA.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(jobA, new Path("/mr-quads/input.txt"));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(prog1.class);
        jobA.setNumReduceTasks(1);

        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "quad-2");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(NullWritable.class);

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(MapClass2.class);
        jobB.setReducerClass(Reduce2.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path("/mr-quads/quad2/"));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(prog1.class);
        return jobB.waitForCompletion(true) ? 0 : 1;


    }

}