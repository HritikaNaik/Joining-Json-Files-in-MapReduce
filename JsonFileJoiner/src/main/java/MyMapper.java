import java.util.*;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.json.JSONObject;
import org.json.JSONException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String id, featur;
        String line = value.toString();//Turn text into string
        try {
            String[] tuple = line.split("\\n");//Take one line at a time
            for(int i=0;i<tuple.length; i++) {
                JSONObject obj = new JSONObject(tuple[i]); //Turn string into json object
                id = obj.getString("product_id"); // Extract the id, this sets each apart
                //To ensure that the correct feature goes with the correct field, after being shuffled enroute to the reducer
                if (obj.has("brand"))
                    featur = "0"+obj.getString("brand");
                else if (obj.has("category"))
                    featur = "1"+ obj.getString("category");
                else if (obj.has("range"))
                    featur = "2"+ obj.getString("range");
                else
                    featur = "3"+ obj.getString("value");
                context.write(new Text(id), new Text(featur)); // links product id with associated field
            }
        } catch (JSONException e) {
            System.out.println("Mapper1 Exception");
            e.printStackTrace();
        }
    }
}