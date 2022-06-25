import java.util.*;
import java.io.IOException;

/*import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;*/

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

//import org.json.*;
import org.json.JSONObject;
import org.json.JSONException;

public class MyReducer extends Reducer<Text,Text,NullWritable,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        try{
            JSONObject obj = new JSONObject();
            //JSONArray ja = new JSONArray();
            obj.put("product_id", key.toString());
            //ja.put(obj);
            Iterator<Text> it = values.iterator();
            String str;
            int i = 0;
            while(it.hasNext()) {
                i++;
                str = it.next().toString();
                switch(str.charAt(0)){
                    case '0':
                        obj.put("brand", "100"+str.substring(2));
                        //if (i==1) context.getCounter(trackNum.brand_first).increment(1);
                        break;
                    case '1':
                        obj.put("category", str.substring(2));
                        break;
                    case '2':
                        obj.put("category_name", str.substring(1));
                        break;
                    default:
                        obj.put("brand_name", str.substring(1));
                        break;
                }
            }
            /*if (i<4)
                context.getCounter(ProductCombiner.trackNum.fields_num_n).increment(1);
            else
                context.getCounter(ProductCombiner.trackNum3.fields_num_y).increment(1);*/

            //ja.put(obj);

            //ja.put(obj);
            context.write(NullWritable.get(), new Text(obj.toString()));
        }catch(JSONException e){
            System.out.println("Mapper Exception");
            e.printStackTrace();
        }
    }
}
