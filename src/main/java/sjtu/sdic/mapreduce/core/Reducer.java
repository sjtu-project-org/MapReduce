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
                Utils.debug("curJson's size:" + curJson.size());
                Utils.debug("curJson:" + curJson);
                //loop curJson, check whether exists in res JSON. if not exists, put new element; else value++
                for(Map.Entry<String, Object> e: curJson.entrySet()){
                    String key = e.getKey();
                    //key exists already
                    if(res.containsKey(key)){
                        //reduceFunc.reduce(e.getKey(), new String[1](res.getString(key)));
                        String value = res.getString(key);
                        value = String.valueOf(Integer.parseInt(value) + 1);
                        res.replace(key,value);//add 1
                    }else{
                        //new element
                        res.put(e.getKey(), e.getValue());
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                Utils.debug("file read/write error\n");
            }
        }
        //write final JSON into File: *-res-reduceNum
        try {
            FileWriter fw = new FileWriter(outFile, false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(res.toJSONString());
            bw.close();
            fw.close();
        }catch (IOException e){
            e.printStackTrace();
            Utils.debug("reduce writing error\n");
        }
    }
}
