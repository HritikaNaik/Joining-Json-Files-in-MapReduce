import java.util.*;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import org.json.JSONObject;
import org.json.JSONException;

public class MyReducer extends Reducer<Text,Text,NullWritable,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        try{
            JSONObject obj = new JSONObject();
            //Here mapreduce has combined all those with the same id into an iterable
            obj.put("id", key.toString());

            Iterator<Text> it = values.iterator(); //to iterate over the iterables
            String str;
            while(it.hasNext()) { //iterating over the field names
                str = it.next().toString();
                switch(str.charAt(0)){ //To ensure that the correct feature goes with the correct field
                    case '0':
                        obj.put("brand", str.substring(1));
                        break;
                    case '1':
                        obj.put("category", str.substring(1));
                        break;
                    case '2':
                        obj.put("range", str.substring(1));
                        break;
                    default:
                        obj.put("value", str.substring(1));
                        break;
                }
            }
            context.write(NullWritable.get(), new Text(obj.toString()));
        }catch(JSONException e){
            e.printStackTrace();
        }
    }
}