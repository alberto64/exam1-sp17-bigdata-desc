package exam1mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;


/**
 * Created by ubuntu on 2/6/17.
 */
public class Exam1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();
        Status status = null;
        try {
            status = TwitterObjectFactory.createStatus(tweet);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        if(status == null) { return; }
        String text = status.getText();
        if(text == null) { return; }
        text = text.toLowerCase();
        if(text.contains("flu")) {
            context.write(new Text("flu"), new Text(String.valueOf(status.getId())));
        }
        if(text.contains("zika")) {
            context.write(new Text("zika"), new Text(String.valueOf(status.getId())));
        }
        if(text.contains("diarrhea")) {
            context.write(new Text("diarrhea"), new Text(String.valueOf(status.getId())));
        }
        if(text.contains("ebola")) {
            context.write(new Text("ebola"), new Text(String.valueOf(status.getId())));
        }
        if(text.contains("swamp")) {
            context.write(new Text("swamp"), new Text(String.valueOf(status.getId())));
        }
        if(text.contains("change")) {
            context.write(new Text("change"), new Text(String.valueOf(status.getId())));
        }
    }
}
