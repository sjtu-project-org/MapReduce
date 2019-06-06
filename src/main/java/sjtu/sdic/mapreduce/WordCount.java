package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/21.
 */
public class WordCount {
    private static boolean Valid(char c){
        return  (c >= 'a' && c <= 'z')||
                (c >= 'A' && c <= 'Z');
    }
    /*
     * mapFunc for WordCount instead of MRTest
     */
    public static List<KeyValue> mapFunc(String file, String value) {
        List<KeyValue> keyValues = new ArrayList<>();
        String RegExp = "[a-zA-Z0-9]+";
        Pattern p = Pattern.compile(RegExp);
        Matcher m = p.matcher(value);
        while(m.find()){
            String curString = m.group();
            keyValues.add(new KeyValue(curString, "1"));
        }
        return keyValues;
        /*
        //regular expression
        String RegExp = "[a-zA-Z0-9]+";
        int Len = value.length(), Start = 0, End = 0;
        String last = "";
        Pattern p = Pattern.compile(RegExp);
        for(int i = 0; i < Len; ++i){
            //find substring start
            while(i < Len){
                if(Valid(value.charAt(i)))
                    break;
                else
                    i++;
            }
            Start = i;
            //find substring last
            while(i < Len){
                if(Valid(value.charAt(i)))
                    i++;
                else
                    break;
            }
            End = i;
            //get substring and update Start
            String tmp = value.substring(Start, End);
            Start = i + 1;
            //compile regular expression and judge whether pattern
            Matcher m = p.matcher(tmp);
            //if matches, add this substring to KeyValue set
            //just map it with '1'
            if(m.matches()){
                keyValues.add(new KeyValue(tmp, "1"));
            }
        }
        return keyValues;*/
    }
    /*
     * reduceFunc for WordCount instead of MRTest
     */
    public static String reduceFunc(String key, String[] values) {
        String ret;
        Integer tmp = 0;
        for (String s : values) {
            tmp = tmp + Integer.valueOf(s);
        }
        ret = String.valueOf(tmp);
        return ret;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("error: see usage comments in file");
        } else if (args[0].equals("master")) {
            Master mr;

            String src = args[2];
            File file = new File(".");
            String[] files = file.list(new WildcardFileFilter(src));
            if (args[1].equals("sequential")) {
                mr = Master.sequential("wcseq", files, 3, WordCount::mapFunc, WordCount::reduceFunc);
            } else {
                mr = Master.distributed("wcseq", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], WordCount::mapFunc, WordCount::reduceFunc, 100, null);
        }
    }
}
