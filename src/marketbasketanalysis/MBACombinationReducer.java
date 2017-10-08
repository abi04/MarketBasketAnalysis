/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketbasketanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import static marketbasketanalysis.MBAAlgorithms.ProductHashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Abi
 */
public class MBACombinationReducer extends Reducer<Text, IntWritable, NullWritable, Text> {

    TreeMap<String, String> top25 = new TreeMap<String, String>();
    HashMap<String, String> map = new HashMap<String, String>();
    String key,value;
    String key1,key2;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int total = 0;
        for (IntWritable sum : values) {
            total += Integer.valueOf(sum.toString());

        }

        top25.put(Integer.toString(total) + key.toString(), key.toString() + " " + Integer.toString(total));

        if (top25.size() > 25) {
            top25.remove(top25.firstKey());
        }
        // context.write(key, new IntWritable(total));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        map = ProductHashMap("C:\\Users\\Abi\\Desktop\\abi\\Product.csv");
        for (String t : top25.descendingMap().values()) {
            key = t.substring(0, t.length() - 2);
            String[] keys = key.split(",");
                 key1= map.get(keys[0]);
                 key2= map.get(keys[1]);
                 value=t.substring(t.length()-1);
                 
            context.write(NullWritable.get(), new Text(key1+","+key2+" "+value));
        }
    }
}
