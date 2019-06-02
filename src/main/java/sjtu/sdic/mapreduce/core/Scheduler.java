package sjtu.sdic.mapreduce.core;

import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import sjtu.sdic.mapreduce.common.Channel;
import sjtu.sdic.mapreduce.common.DoTaskArgs;
import sjtu.sdic.mapreduce.common.JobPhase;
import sjtu.sdic.mapreduce.common.Utils;
import sjtu.sdic.mapreduce.rpc.Call;
import sjtu.sdic.mapreduce.rpc.WorkerRpcService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Cachhe on 2019/4/22.
 */
public class Scheduler {

    /**
     * schedule() starts and waits for all tasks in the given phase (mapPhase
     * or reducePhase). the mapFiles argument holds the names of the files that
     * are the inputs to the map phase, one per map task. nReduce is the
     * number of reduce tasks. the registerChan argument yields a stream
     * of registered workers; each item is the worker's RPC address,
     * suitable for passing to {@link Call}. registerChan will yield all
     * existing registered workers (if any) and new ones as they register.
     *
     * @param jobName job name
     * @param mapFiles files' name (if in same dir, it's also the files' path)
     * @param nReduce the number of reduce task that will be run ("R" in the paper)
     * @param phase MAP or REDUCE
     * @param registerChan register info channel
     */
    public static void schedule(String jobName, String[] mapFiles, int nReduce, JobPhase phase, Channel<String> registerChan) {
        Utils.debug("jobName:" + jobName + "nReduce:" + String.valueOf(nReduce) + "jobPhase:" + phase);
        Utils.debug("mapFile:");
        for(String s: mapFiles){
            Utils.debug(s);
        }

        int nTasks = -1; // number of map or reduce tasks
        int nOther = -1; // number of inputs (for reduce) or outputs (for map)
        switch (phase) {
            case MAP_PHASE:
                nTasks = mapFiles.length;
                nOther = nReduce;
                break;
            case REDUCE_PHASE:
                nTasks = nReduce;
                nOther = mapFiles.length;
                break;
        }

        System.out.println(String.format("Schedule: %d %s tasks (%d I/Os)", nTasks, phase, nOther));

        /**
        // All ntasks tasks have to be scheduled on workers. Once all tasks
        // have completed successfully, schedule() should return.
        //
        // Your code here (Part III, Part IV).
        //
        */
        /*
         * for MapPhase, i means the number of MapFile
         * **Call.getWorkerRpcService(worker).doTask(arg)** to hand out a task to a worker
         * nMapTask: 20  nReducerTask: 10
         */
        CountDownLatch downLatch = new CountDownLatch(nTasks);
        for(int i = 0; i < nTasks; ++i){
            DoTaskArgs doTask = new DoTaskArgs(jobName, mapFiles[i], phase, i, nOther);
            //new thread
            int NumOfSuccess = 0;
            Thread t = new Thread(new Runnable(){
                public void run(){
                    String curWorker = null;
                    while(true){
                        try {
                            curWorker = (String) registerChan.read();
                        }catch (InterruptedException e){
                            Utils.debug("registerChan.read() error");
                        }
                        Utils.debug("curWorker:" + curWorker);
                        WorkerRpcService wRpcs = Call.getWorkerRpcService(curWorker);
                        try {
                            wRpcs.doTask(doTask);
                            Utils.debug("Worker finish its task, then countDown...");
                            downLatch.countDown();
                            try {
                                registerChan.write(curWorker);
                                break;//break when success
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }catch (SofaRpcException e){
                            //re-assign worker to continue
                            Utils.debug(curWorker+"failed--------------------------------------------------------------------");
                        }
                    }
                }
            });
            t.start();
        }
        try {
            Utils.debug("Master is waiting Workers to finish all task...");
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Schedule: %s done", phase));
    }
}
