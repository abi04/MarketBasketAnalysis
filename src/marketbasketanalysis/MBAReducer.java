/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketbasketanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Abi
 */
public class MBAReducer extends Reducer<Text, Text, Text, Text> {

    // List<String> valArray = new ArrayList<String>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder valArray = new StringBuilder();

        String a;

        for (Text val : values) {
            a = val.toString();
            a = a.trim();
            valArray.append(a).append(",");

        }
        String newString = valArray.toString();

        newString = newString.substring(0, newString.length() - 1);
        context.write(new Text(key.toString().trim()), new Text(newString));
    }

}
