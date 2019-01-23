import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.ArrayList;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;


public class Map extends Mapper<Object, Text, Text, Text>{

    // the mapper check if a tweet contains one or more keywords
    // the control is done word by word
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        try(FileReader in = new FileReader("/home/mauroliuzzo/IdeaProjects/Tweets-First-Job/dataset/keywords.txt"); BufferedReader inFile = new BufferedReader(in)){
            // Keywords import
            ArrayList<String> keywords = new ArrayList<>();
            String str = null;
            do{
                str = inFile.readLine();
                if(str!= null){
                    keywords.add(str);
                }
            }while(str != null);

            // Tweets from input file
            String tweetfile = value.toString();
            String [] tweets = tweetfile.split("\\n");

            for(String tweet : tweets){
                for(String word : tweet.split(" ")){
                    for(int i = 0; i < keywords.size(); i++) {
                        if (word.replace("#","").equals(keywords.get(i))) {
                            context.write(new Text(keywords.get(i)), new Text(tweet));
                        }
                    }
                }
            }

        }
    }
}
