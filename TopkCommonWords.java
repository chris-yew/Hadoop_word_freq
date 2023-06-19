import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.*;
import static java.lang.Math.min;
import java.io.BufferedReader;
import java.util.Map.*;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.WritableComparator;

public class TopkCommonWords {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, MapWritable> {

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);
    public Set<String> stopwords = new HashSet<String>();

    // setup to read stopwords file
    protected void setup(Context context) throws IOException, InterruptedException {

      Configuration confStop = context.getConfiguration();
      String stop = confStop.get("stopwords");
      Path stopwordspath = new Path(stop);

      FileSystem fs = stopwordspath.getFileSystem(context.getConfiguration());
      FSDataInputStream fdsis = fs.open(stopwordspath);
      BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));
      String line;
      while ((line = br.readLine()) != null) {
        stopwords.add(line);
      }

      br.close();
      // System.out.println("STOPWORDS"+stopwords.size());

    }

    // mapper to output word,<filename,one>
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String filestring = ((FileSplit) context.getInputSplit()).getPath().getName();

      Configuration confMap = context.getConfiguration();
      Text filename = new Text(confMap.get(filestring));
      // System.out.println("CHECKKK:"+ filename.toString());

      String delimiters = " \t\n\r\f";
      StringTokenizer itr = new StringTokenizer(value.toString(), delimiters);
      MapWritable map = new MapWritable();
      while (itr.hasMoreTokens()) {

        String words = itr.nextToken();

        if (words.length() > 4 && !stopwords.contains(words)) {

          word.set(words);
          map.put(filename, one);
          // System.out.println(filename);
          context.write(word, map);
          map.clear();
        }
      }
    }
  }

  // combiner to ouput word,<filename,count in file>
  public static class WordCombiner2
      extends Reducer<Text, MapWritable, Text, MapWritable> {
    private Text filename1 = new Text("file1");
    private Text filename2 = new Text("file2");
    private IntWritable sum = new IntWritable();
    private MapWritable comb = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      int file1sum = 0;
      int file2sum = 0;
      for (MapWritable val : values) {
        if (val.containsKey(filename1)) {
          file1sum++;
        }
        if (val.containsKey(filename2)) {
          file2sum++;
        }
      }
      if (file1sum > 0) {
        sum.set(file1sum);
        comb.put(filename1, sum);
        context.write(key, comb);
        comb.clear();
      }
      if (file2sum > 0) {
        sum.set(file2sum);
        comb.put(filename2, sum);
        context.write(key, comb);
        comb.clear();
      }
    }
  }

  // reducer to choose the lowest count among 2 files and put in hashmap
  public static class WordReducer
      extends Reducer<Text, MapWritable, Integer, Text> {

    public HashMap<Text, Integer> resultmap = new HashMap<Text, Integer>();
    private Text filename1 = new Text("file1");
    private Text filename2 = new Text("file2");
    public int limit;

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      // occur to track whether word appears in both files, filetotal to count
      // appearances in each file
      int occur1 = 0;
      int occur2 = 0;
      int file1total = 0;
      int file2total = 0;
      Text outputkey = new Text(key.toString());
      Configuration conf3 = context.getConfiguration();
      int numwords = Integer.parseInt(conf3.get("numwords"));
      limit = numwords;
      for (MapWritable val : values) {
        if (val.containsKey(filename1)) {
          IntWritable file1Writable = (IntWritable) val.get(filename1);
          file1total = file1Writable.get();
          occur1 = 1;
        }
        if (val.containsKey(filename2)) {
          IntWritable file2Writable = (IntWritable) val.get(filename2);
          file2total = file2Writable.get();
          occur2 = 1;
        }
      }
      if (occur1 == 1 && occur2 == 1) {
        // choose the minimum occurence
        Integer minoccur = min(file1total, file2total);
        resultmap.put(outputkey, minoccur);

        file1total = 0;
        file2total = 0;
      }
    }

    // cleanup to sort the results with custom comparator
    public void cleanup(Context context) throws IOException, InterruptedException {

      // populate a list of entries<word,count>
      List<Entry<Text, Integer>> sortarr = new ArrayList<Entry<Text, Integer>>();
      for (Entry<Text, Integer> entry : resultmap.entrySet()) {
        // System.out.println("ENTRIES" + entry.getKey() + entry.getValue());
        sortarr.add(entry);
      }
      // System.out.println("SIZE" + sortarr.size());
      Comparator<Entry<Text, Integer>> customComparator = new Comparator<Entry<Text, Integer>>() {
        @Override
        public int compare(Entry<Text, Integer> entry1, Entry<Text, Integer> entry2) {
          // Compare the counts of the words, if equal then compare the first characters
          if ((-1) * entry1.getValue().compareTo(entry2.getValue()) == 0) {
            return entry1.getKey().compareTo(entry2.getKey());
          }
          return (-1) * entry1.getValue().compareTo(entry2.getValue());
        }
      };
      Collections.sort(sortarr, customComparator);
      // System.out.println("SIZE" + sortarr.size());
      int j = 0;
      // write out the results
      for (Entry maps : sortarr) {
        if (j < limit) {
          context.write((Integer) maps.getValue(), (Text) maps.getKey());
          j++;
        } else {
          break;
        }
      }
    }

  }

  public static void main(String[] args) throws Exception {

    String k = args[4];

    // get filenames of the inputs
    Path path1 = new Path(args[0]);
    String input1 = path1.getName();

    Path path2 = new Path(args[1]);
    String input2 = path2.getName();

    // set the configurations
    Configuration conf2 = new Configuration();
    conf2.set("numwords", k);
    conf2.set("stopwords", args[2]);
    conf2.set(input1, "file1");
    conf2.set(input2, "file2");

    // map reduce job
    Job job = Job.getInstance(conf2, "wordcount");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(WordCombiner2.class);
    job.setReducerClass(WordReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Integer.class);
    job.setOutputValueClass(Text.class);

    // add input and output files
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    boolean hasCompleted = job.waitForCompletion(true);
    System.exit(hasCompleted ? 0 : 1);

  }
}
