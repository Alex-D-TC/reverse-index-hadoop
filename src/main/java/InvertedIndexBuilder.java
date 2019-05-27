import java.io.*;
import java.util.Set;
import java.util.StringJoiner;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexBuilder {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

        private HashSet<String> stopWords = loadStopWords();

        private HashSet<String> loadStopWords(){
            HashSet<String> stopWords = new HashSet<>();

            try {
                // Get StopWords file from the dfs
                Path path = new Path("hdfs:/home/input-meta/StopWords.txt");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line;
                while((line = br.readLine()) != null) {
                    stopWords.add(line);
                }

            } catch(IOException e) {
                System.out.println(e.getMessage());
                System.exit(43);
            }

            return stopWords;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println(String.format("Processing line: %s", value.toString()));

            String[] lineWords = value.toString()
                                    .toLowerCase()
                                    .replaceAll("[^a-zA-Z0-9\\s]", "")
                                    .split(" ");

            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            String lineNum = lineWords[0];

            // Store which words we found on the line
            Set<String> wordsFound = new HashSet<>();

            for(String candidate : lineWords) {
                candidate = candidate.replaceAll("[0-9]", "");
                if (!stopWords.contains(candidate) && candidate.length() > 0) {
                    wordsFound.add(candidate);
                }
            }

            for(String word: wordsFound) {
                context.write(new Text(word), new Text(fileName + " " + lineNum));
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringJoiner docIDs = new StringJoiner(" ");
            StringJoiner lines = new StringJoiner(" ");

            for (Text val : values) {
                String[] IDLinePair = val.toString().split(" ");
                docIDs.add(IDLinePair[0]);
                lines.add(IDLinePair[1]);
            }

            context.write(key, new Text(docIDs.toString() + "|" + lines.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(InvertedIndexBuilder.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
