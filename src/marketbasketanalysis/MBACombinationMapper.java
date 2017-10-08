/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketbasketanalysis;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import marketbasketanalysis.MBAAlgorithms;

/**
 *
 * @author Abi
 */
public class MBACombinationMapper extends Mapper<Text, Text, Text, IntWritable> {

    IntWritable one = new IntWritable(1);
    String newString;

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString();
        List<String> splittedval = MBAAlgorithms.StringtoList(val);
        List<List<String>> combinations = MBAAlgorithms.SortedCombinations(splittedval, 2);
        for (List<String> abc : combinations) {
            StringBuilder valArray = new StringBuilder();
            for (String text : abc) {
                valArray.append(text).append(",");
            }
            newString = valArray.toString();
            newString = newString.substring(0, newString.length() - 1);

            context.write(new Text(newString.trim()), one);
        }

    }

}
