import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{
    public int run(String[] args) throws Exception{
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "TweetsFirstJob");
        job.setJarByClass(Driver.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("Input dir: " + "/home/mauroliuzzo/IdeaProjects/Tweets-First-Job/input/");
        System.out.println("Output dir: " + "/home/mauroliuzzo/IdeaProjects/Tweets-First-Job/output/");
        FileInputFormat.addInputPath(job, new Path("/home/mauroliuzzo/IdeaProjects/Tweets-First-Job/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/home/mauroliuzzo/IdeaProjects/Tweets-First-Job/output/"));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
    }
}
