import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ReviewAnalysis {
    public static class ReviewAnalysisMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");

            String productID = split[3];
            String title = split[5];
            String category = split[6];
            String rating = split[7];
            String helpful_votes = split[8];
            String total_votes = split[9];

            context.write(new Text(productID),
                    new Text(rating + "\t" + title + "\t" + category + "\t" + helpful_votes + "\t" + total_votes));
        }
    }

//    public static class RatingAverageCombiner
//            extends Reducer<Text, Text, Text, Text> {
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context)
//                throws IOException, InterruptedException {
//            List<Integer> ratings = new ArrayList<>();
//            for(Text text : values){
//                String value = text.toString();
//                ratings.add(Integer.parseInt(value));
//            }
//
//            int ratingNum = ratings.size();
//            float ratingSum = 0;
//            if(ratingNum > 0)
//                for(int rating : ratings)
//                    ratingSum += rating;
//
////            System.out.println(key + " " + ratingNum + " " + ratingSum);
//            context.write(key, new Text(ratingNum + "," + ratingSum));
//        }
//    }

    public static class ReviewAnalysisReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double ratingSum = 0.0;
            long ratingCount = 0L;
            double percentSum = 0.0;
            long percentCount = 0L;
            String title = "";
            boolean titleExist = false;
            String category = "";
            boolean categoryExist = false;

            try {
                for (Text text : values) {
                    String value = text.toString();
                    String[] split = value.split("\t");
                    String rating = split[0];
                    if(!titleExist && split[1].length() != 0 && split[1] != null){
                        title = split[1];
                        titleExist = true;
                    }
                    if(!categoryExist && split[2].length() != 0 && split[2] != null){
                        category = split[2];
                        categoryExist = true;
                    }
                    String helpful_votes = split[3];
                    String total_votes = split[4];

                    ratingSum += Double.parseDouble(rating);
                    ratingCount++;
                    percentSum += Double.parseDouble(helpful_votes) / Double.parseDouble(total_votes);
                    percentCount++;
                }
                context.write(key, new Text(title + "\t" + category + "\t" +
                        String.valueOf(ratingSum / ratingCount) + "\t" + String.valueOf(percentSum / percentCount)));
            }
            catch (Exception e)
            {
                context.write(key, new Text(title + "\t" + category + "\t" +
                        String.valueOf(ratingSum / ratingCount) + "\t" + String.valueOf(percentSum / percentCount)));
            }
//            System.out.println(String.valueOf(sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amazon review analysis");

        Path output = new Path("hdfs://localhost:9000/cs585/ratingAverage");

        FileSystem fileSystem = output.getFileSystem(conf);
        if (fileSystem.exists(output))
            fileSystem.delete(output, true);

        job.setJarByClass(ReviewAnalysis.class);
        job.setMapperClass(ReviewAnalysisMapper.class);
//        job.setCombinerClass(RatingAverageCombiner.class);
        job.setReducerClass(ReviewAnalysisReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/AmazonReviews/amazon_reviews_multilingual_US_v1_00.tsv"));
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/AmazonReviews/amazon_reviews_us_Apparel_v1_00.tsv"));
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/AmazonReviews/amazon_reviews_us_Automotive_v1_00.tsv"));
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/AmazonReviews/amazon_reviews_us_Beauty_v1_00.tsv"));
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/cs585/AmazonReviews/amazon_reviews_us_Books_v1_02.tsv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/cs585/reviewAnalysis"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}