package exam1mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Created by ubuntu on 2/6/17.
 */
public class Exam1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        // key is the abreviation of a state
        // values is a list of 1s, one for each school found for the state

        // setup a counter
        String list = "";
        // iterator over list of 1s, to count them (no size() or length() method available)
        for (Text value : values ){
            list = list.concat(", " + String.valueOf(value));
        }
        // emit key-pair: key, count
        // key is the abreviation for the state
        // count is the number of schools in the state
        Logger logger = LogManager.getRootLogger();
        //logger.trace("Red: " + key.toString());

        // DEBUG
        context.write(key, new Text(list));
    }
}
