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
        for(int i = 0; i < nMap; ++i){
            //eg. mrtmp.test-perMapTask-ReduceNum
            //eg. mrtmp.test.0-0
            //eg. mrtmp.test.0-1
            //eg. mrtmp.test.0-2
            //eg. mrtmp.test.x-0
            //eg. mrtmp.test.x-1
            //...
            //eg. mrtmp.test.7-2
            String filename = Utils.reduceName(jobName, i, reduceTask);
            Utils.debug("reduce task's filename:" + filename);
            //read
            try {
                //read immediate file
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(filename));
                String content = "";
                int r = -1;
                byte[] bytes = new byte[2048];
                while((r = in.read(bytes, 0, bytes.length)) != -1){
                    String str = new String(bytes,0,r,"UTF-8");
                    content = content + str;
                }
                in.close();
                //parse current content to JSON
                JSONObject curJson = JSONObject.parseObject(content);
                //for each json obj, call reduceFun to add String[] values to a single String
                //then put the ReducedValue in res json --- store in relative reducer-res-file
                //update res's key-value pair if there are same word existing
                for(Map.Entry<String, Object> e: curJson.entrySet()){
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
                    if(!res.containsKey(e.getKey())){
                        //not exists, construct new pair
                        res.put(e.getKey(), ReducedValue);
                    }else{
                        //exists, update value
                        String OldValue = (String)res.getString(e.getKey());
                        Integer oldValue = Integer.valueOf(OldValue);
                        //String CurValue = (String)e.getValue();
                        Integer curValue = Integer.valueOf(ReducedValue);
                        String NewValue = String.valueOf(oldValue+curValue);
                        res.replace(e.getKey(), NewValue);
                    }

                }

            } catch (IOException e) {
                e.printStackTrace();
                Utils.debug("file read/write error\n");
            }
        }
        //write final JSON into File: *-res-reduceNum
        //before write JSON data, check whether this file is empty.
        //if not, PARSE old value and add this json(res) in its, then store new JSON in it
        try {
            //test file exists
            File file = new File(outFile);
            if(!file.exists()){
                file.createNewFile();
            }else{
                //clear this file, in order to avoid *FAILED TEST* impact
                file.delete();
                file.createNewFile();
            }
            //read OldJson
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(outFile));
            String content = "";
            int r = -1;
            byte[] bytes = new byte[2048];
            while((r = in.read(bytes, 0, bytes.length)) != -1){
                String str = new String(bytes,0,r,"UTF-8");
                content = content + str;
            }
            //update OldJson.
            JSONObject OldJson = JSONObject.parseObject(content);
            Boolean EmptyFile = false;
            if(OldJson == null){
                EmptyFile = true;
                OldJson = new JSONObject();
            }
            //loop OldJson, merge res into OldJson
            for(Map.Entry<String, Object> e: OldJson.entrySet()){
                //res.containsKey(...) is *TRUE*
                //this word exists already, add res' value to OldValue
                if(res.containsKey(e.getKey())){
                    String OldValue = OldJson.getString(e.getKey());
                    Integer oldValue = Integer.valueOf(OldValue);
                    String CurValue = (String)res.getString(e.getKey());
                    Integer curValue = Integer.valueOf(CurValue);

                    String NewValue = String.valueOf(oldValue + curValue);
                    OldJson.replace(e.getKey(), NewValue);
                }
            }
            //add res' remained value to OldJson
            for(Map.Entry<String, Object> e: res.entrySet()){
                if(!OldJson.containsKey(e.getKey())){
                    OldJson.put(e.getKey(), e.getValue());
                }
            }
            //write
            FileWriter fw = new FileWriter(outFile, false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(OldJson.toJSONString());
            bw.close();
            fw.close();

        }catch (IOException e){
            e.printStackTrace();
            Utils.debug("reduce writing error\n");
        }
    }
}
