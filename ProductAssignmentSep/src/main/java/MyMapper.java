import java.util.*;
import java.io.IOException;

/*import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

//import org.json.*;
import org.json.JSONObject;
import org.json.JSONException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String id, featur;
        String line = value.toString();
        try {
            String[] tuple = line.split("\\n");
            //String[] tuple = line.split(",");
            for(int i=0;i<tuple.length; i++) {
                JSONObject obj = new JSONObject(tuple[i]);
                id = obj.getString("product_id");
                if (obj.has("brand"))
                    featur = "0"+obj.getString("brand");
                else if (obj.has("category"))
                    featur = "1"+ obj.getString("category");
                else if (obj.has("category_name"))
                    featur = "2"+ obj.getString("category_name");
                else
                    featur = "3"+ obj.getString("brand_name");
                /*if (id.charAt(0)=='a')
                    context.getCounter(ProductCombiner.trackNum.id_stat_a).increment(1);8*/
                context.write(new Text(id), new Text(featur));
            }
        } catch (JSONException e) {
            System.out.println("Mapper1 Exception");
            e.printStackTrace();
        }
    }
}
