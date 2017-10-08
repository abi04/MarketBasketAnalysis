/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketbasketanalysis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author Abi
 */
public class MBAAlgorithms {

    public static List<String> StringtoList(String value) {
        String[] splitedString = value.split(",");
        List<String> list = new ArrayList<String>();
        for (String c : splitedString) {
            list.add(c);
        }

        return list;
    }

    public static List<List<String>> SortedCombinations(Collection<String> stringstringelements, int i) {

        List<List<String>> result = new ArrayList<List<String>>();

        if (i == 0) {
            result.add(new ArrayList<String>());
            return result;
        }

        List<List<String>> combinations = SortedCombinations(stringstringelements, i - 1);
        for (List<String> combination : combinations) {
            for (String stringelement : stringstringelements) {
                if (combination.contains(stringelement)) {
                    continue;
                }

                List<String> newlist = new ArrayList<String>();
                newlist.addAll(combination);

                if (newlist.contains(stringelement)) {
                    continue;
                }

                newlist.add(stringelement);

                Collections.sort(newlist);

                if (result.contains(newlist)) {
                    continue;
                }
                result.add(newlist);
            }
        }
        return result;

    }

    public static HashMap<String, String> ProductHashMap(String csv) throws FileNotFoundException, IOException {

        HashMap<String, String> map = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(csv));
        String line = null;

        while ((line = br.readLine()) != null) {
            String str[] = line.split(",");
             map.put(str[1], str[3]);
        }

        return map;

    }
}
