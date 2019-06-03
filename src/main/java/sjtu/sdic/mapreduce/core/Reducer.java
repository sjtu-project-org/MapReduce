package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * 
     * 	doReduce manages one reduce task: it should read the intermediate
     * 	files for the task, sort the intermediate key/value pairs by key,
     * 	call the user-defined reduce function {@code reduceFunc} for each key,
     * 	and write reduceFunc's output to disk.
     * 	
     * 	You'll need to read one intermediate file from each map task;
     * 	{@code reduceName(jobName, m, reduceTask)} yields the file
     * 	name from map task m.
     *
     * 	Your {@code doMap()} encoded the key/value pairs in the intermediate
     * 	files, so you will need to decode them. If you used JSON, you can refer
     * 	to related docs to know how to decode.
     * 	
     *  In the original paper, sorting is optional but helpful. Here you are
     *  also required to do sorting. Lib is allowed.
     * 	
     * 	{@code reduceFunc()} is the application's reduce function. You should
     * 	call it once per distinct key, with a slice of all the values
     * 	for that key. {@code reduceFunc()} returns the reduced value for that
     * 	key.
     * 	
     * 	You should write the reduce output as JSON encoded KeyValue
     * 	objects to the file named outFile. We require you to use JSON
     * 	because that is what the merger than combines the output
     * 	from all the reduce tasks expects. There is nothing special about
     * 	JSON -- it is just the marshalling format we chose to use.
     * 	
     * 	Your code here (Part I).
     * 	
     *
     * @param jobName the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile write the output here
     * @param nMap the number of map tasks that were run ("M" in the paper)
     * @param reduceFunc user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceFunc) {
        Utils.debug("doRduce:jobName:" + jobName + "    reduceTask:"+String.valueOf(reduceTask) + "   outFile:"+outFile + " nMap:"+String.valueOf(nMap));
        /*
        FOR EACH REDUCE TASK
        it should read the intermediate
        files for the task, sort the intermediate key/value pairs by key,
        call the user-defined reduce function {@code reduceFunc} for each key,
        and write reduceFunc's output to disk.
        */
        JSONObject res = new JSONObject(new LinkedHashMap<>());
        Boolean Inverted = jobName.equals("iiseq")?true:false;
        //add reducer's elements in res
        for(int i = 0; i < nMap; ++i){
            String filename = Utils.reduceName(jobName, i, reduceTask);
            JSONObject curJson = new JSONObject();
            //read content
            try{
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(filename));
                String content = "";
                int r = -1;
                byte[] bytes = new byte[2048];
                while((r = in.read(bytes, 0, bytes.length)) != -1){
                    String str = new String(bytes,0,r,"UTF-8");
                    content = content + str;
                }
                in.close();
                //parse content to JSON
                curJson = JSONObject.parseObject(content);
            }catch (IOException e){
                Utils.debug("read immediate file error");
            }
            //add it to res
            for(Map.Entry<String, Object> e: curJson.entrySet()){
                String key = e.getKey();
                String value = (String)e.getValue();
                if(res.containsKey(key)){
                    //json pair exists, update its value
                    JSONArray ValueArray = JSONArray.parseArray(value);
                    String OldValue = (String)res.get(key);
                    JSONArray OldValueArray = JSONArray.parseArray(OldValue);
                    OldValueArray.addAll(ValueArray);
                    res.replace(key, OldValueArray.toString());
                }else{
                    //new json pair
                    res.put(e.getKey(), e.getValue());
                }
            }
        }
        //for each json pair in res, reduce its value
        for(Map.Entry<String, Object> e:res.entrySet()){
            String Value = (String)e.getValue();
            JSONArray ValueArray = JSONArray.parseArray(Value);
            //transform JSONArray to Object[]
            Object[] Objs = ValueArray.toArray();
            //transform Object[] to String[]
            //TODO: how to make it easier instead of constructing a new String Array ?
            int size = Objs.length;
            String[] Values = new String[size];
            for(int q = 0; q < size; ++q){
                Values[q] = (String)Objs[q];
            }
            String ReducedValue = reduceFunc.reduce(e.getKey(), Values);
            res.replace(e.getKey(), ReducedValue);
        }
        //write back reduced json pair in dst file
        try{
            //clear dst file, in case some previous FAILED TEST's impact
            File file = new File(outFile);
            if(!file.exists()){
                file.createNewFile();
            }else{
                file.delete();
                file.createNewFile();
            }
            //write content
            FileWriter fw = new FileWriter(outFile, false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(res.toJSONString());
            bw.close();
            fw.close();
        }catch (IOException e){
            Utils.debug("reducer write back error");
        }
    }
}
