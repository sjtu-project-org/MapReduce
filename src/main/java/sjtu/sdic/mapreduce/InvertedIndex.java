package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/24.
 */
public class InvertedIndex {
    /*
     * mapFunc for invertedIndex instead of MRTest
     */
    public static List<KeyValue> mapFunc(String file, String value) {
        List<KeyValue> ret = new ArrayList<>();
        //Utils.debug("file:" + file + "value:"+value);
        int len = value.length();
        String RegExp = "[a-zA-Z0-9]+";
        Pattern p = Pattern.compile(RegExp);
        Matcher m = p.matcher(value);
        while(m.find()){
            String curString = m.group();
            ret.add(new KeyValue(curString, file));
        }
        //why this does not work? I DON'T KNOW
        /*int count = m.groupCount();
        for(int i = 0; i < count; ++i){
            String curString = m.group(i);
            ret.add(new KeyValue(curString, file));
        }*/
        return ret;
    }
    /*
     * reduceFunc for invertIndex instead of MRTest
     */
    public static String reduceFunc(String key, String[] values) {
        String ret = "";
        Set<String> set = new HashSet<>();
        int len = values.length;
        for(int i = 0; i < len; ++i){
            if(!set.contains(values[i])){
                set.add(values[i]);
            }else{
                //do nothing
            }
        }
        int count = set.size();
        ret = ret + String.valueOf(count) + " ";
        //
        Set<String> sortSet = new TreeSet<String>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);//降序排列
            }
        });
        sortSet.addAll(set);
        for(String s:sortSet){
            ret = ret + s + ",";
        }
        //delete last ','
        ret = ret.substring(0, ret.length() - 1);
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
                mr = Master.sequential("iiseq", files, 3, InvertedIndex::mapFunc, InvertedIndex::reduceFunc);
            } else {
                mr = Master.distributed("wcdis", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], InvertedIndex::mapFunc, InvertedIndex::reduceFunc, 100, null);
        }
    }
}
