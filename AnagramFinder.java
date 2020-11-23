
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

public class AnagramFinder {

    static Collection<Text> anagrams = new HashSet<Text>();

    public static class AnagramMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
	    Scanner sc = new Scanner(new File(stopwords.txt));
	    List<String> lines = new ArrayList<String>();
	    while (sc.hasNextLine()) {
	    	lines.add(sc.nextLine());
	    }
	    String[] stopwords = lines.toArray(new String[0]);

            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
		
	    for (int i=0; i<=stopwords.length(); i++){
		itr = itr.replaceAll(stopwords[i]);
	    }
	
	    itr = itr.replaceAll("[^a-zA-Z ]", "");
		
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                char[] arr = word.toCharArray();
                Arrays.sort(arr);
                String wordKey = new String(arr);
                context.write(new Text(wordKey), new Text(word));
            }
        }
    }

    // Does not summarize, but simply collets the list of anagramming words for

    // a key

    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Collection<Text> anagrams = new HashSet<Text>();
			
            String anagram = null;
            
	    for (Text val : values) {
                if (anagram == null) {
                    anagram = val.toString();
                } else {
                    anagram = anagram + ',' + val.toString();
                }
		//could be where I could sort anagrams alphabetically
                // anagrams.add(val);
            }
            context.write(key, new Text(anagram));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(AnagramFinder.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
