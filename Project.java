import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project {



    
//    private static Map<String, Document> documentMap = new HashMap<>();
//    private static int DOC_ID_SEQ = 1;
//    private static Words words = new Words();

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // get filename
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path filePath = fileSplit.getPath();
            String fileName = filePath.getName();
            fileName = filePath.toString();
            fileName = fileName.replace("hdfs://ric-master-01.sci.pitt.edu:8020/user/sup47/Data/", "");

            // get document ID
            String docId = fileName.replace(".txt","");

            // get each line of document
            String line = value.toString().replaceAll("\\p{Punct}", "").toLowerCase();

            // divide lines into tokens
            StringTokenizer tokenizer = new StringTokenizer(line);
            
//            String docPathName = String.format("%s/%s", filePath.getParent().getName(), fileName);
//            Document doc = documentMap.get(docPathName);
//            if (doc == null) {
//            	doc = new Document(fileName, filePath.getParent().getName(), DOC_ID_SEQ++);
//            	documentMap.put(docPathName, doc);
//            }
            

            // map output is (word, docId)
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                // don't count the docId word
                if (docId.equals(nextToken)) {
                    continue;
                }
                word.set(nextToken);
                context.write(word, new Text(docId));
                
//                words.addDocument(nextToken, doc);
//                doc.addWordCount(nextToken);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
            Iterator<Text> vals = values.iterator();
            while (vals.hasNext()) {
                // value for each line is docId (key = word, value = docId)
                String docId = vals.next().toString();

                // get count for each docId from hashMap
                Integer currentCount = hashMap.get(docId);

                // update count for each docId
                if (currentCount == null) {
                    hashMap.put(docId, 1);
                } else {
                    currentCount = currentCount + 1;
                    hashMap.put(docId, currentCount);
                }
            }

            // set output format
            boolean isFirst = true;
            StringBuilder toReturn = new StringBuilder();
            for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                if (!isFirst) {
                    toReturn.append("\t");
                }
                isFirst = false;
                toReturn.append(entry.getKey()).append(":").append(entry.getValue());
            }

            context.write(key, new Text(toReturn.toString()));
        }
    }

    
    

    



    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

//        int input;
//        Scanner kb = new Scanner(System.in);
        //if (!indexExists()) {
            while (true) {
             
//                if (input > 2 || input < 1) {
//                    System.out.println("Invalid option. Please try again.\n");
//                } 
//                if (input == 1) {
                	Configuration conf = new Configuration();
                    Job job = Job.getInstance(conf, "TinyGoogle");
                    job.setJarByClass(Project.class);
                    job.setMapperClass(InvertedIndexMapper.class);
                    job.setReducerClass(InvertedIndexReducer.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    // job.setNumMapTasks(1);
                    // job.setNumReduceTasks(1);
                    //FileInputFormat.addInputPath(job, inPath);
                    //FileOutputFormat.setOutputPath(job, outPath);
                    FileInputFormat.setInputDirRecursive(job, true);
                    FileInputFormat.addInputPath(job, new Path(args[0]));
                    FileOutputFormat.setOutputPath(job, new Path(args[1]));
                    System.exit(job.waitForCompletion(true) ? 0 : 1);
                    break;
//                } 
//                else {
//                    //                    break;
//                }
            }
    }
}