package redis.clients.util;

import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Debugger {
    private final static AtomicInteger seq = new AtomicInteger(0);
    private static long counter = 0;
    private final static ScheduledExecutorService schedulerExecutorService = Executors.newScheduledThreadPool(1,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName("Jedis-Debugger-pool-" + seq.getAndIncrement());
                    return t;
                }
            });
    static Map<Jedis, ConnInfo> connInfos = new ConcurrentHashMap<Jedis, ConnInfo>(128);

    public static void addConn(Jedis jedis, String hostInfo) {
        Thread thread = currentThread();
        connInfos.put(jedis, new ConnInfo(System.currentTimeMillis(), thread,
                new RuntimeException("connDebugger"), hostInfo));
//        if (connInfos.containsKey(thread)) {
//            log("Jedis-Debugger-Error:Thread:" + thread + " not return the conn before acquire a new one");
//            simpleLog(connInfos.get(thread).toString());
//        } else {
//            connInfos.put(thread, new ConnInfo(System.currentTimeMillis(), thread,
//                    new RuntimeException("connDebugger"), hostInfo));
//        }
    }

    public static void removeConn(Jedis jedis) {
        if (connInfos.remove(jedis) == null) {
            log("Jedis-Debugger-Error:Thread:" + currentThread() + " has nothing to return");
        }
    }

    static void checkConn() {
        long now = System.currentTimeMillis();
        int numOfHeightCost = 0;
        if (counter % 1000 == 0) {
            System.out.println("Jedis-Debugger checking at:" + now + ", sizeOfconnInfos:" + connInfos.size());
        }
        for (ConnInfo connInfo : connInfos.values()) {
            if ((now-connInfo.ts) > 30*1000) {
                numOfHeightCost++;
                simpleLog("!! Possible Jedsi Leak: " + connInfo.toString());
            }
        }
        if (numOfHeightCost > 0 || counter % 1000 ==0) {
            System.out.println("Jedis-Debugger-Error:Jedis-Debugger checked at:" + System.currentTimeMillis() + ", numOfHeightCost:" + numOfHeightCost);
        }
        counter++;
    }

    static void initCheck() {
        schedulerExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                checkConn();
            }
        },0, 5, TimeUnit.SECONDS);
    }

    static {
        initCheck();
    }

    static Thread currentThread() {
        return Thread.currentThread();
    }

    public static void log(Throwable e) {
        log(exception2String(e));
    }

    public static void log(String msg) {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss:SSS");
        System.out.println("[" +sdf.format(new Date()) + "]Jedis-Debugger:Thread:[" + currentThread() + "], currentTs:"
                + System.currentTimeMillis() + ", detail:" + msg);
    }

    public static void simpleLog(String msg) {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss:SSS");
        System.out.println("[" +sdf.format(new Date()) + "]Jedis-Debugger-simpleLog:" + msg);
    }

    public static void log(String msg, Throwable e) {
        log(msg);
        log(e);
    }

    static String exception2String(Throwable ex) {
        StringBuilder sb = new StringBuilder(ex.toString());
        sb.append("\n");
        for (StackTraceElement element : ex.getStackTrace()) {
            sb.append("\t" + element + "\n");
        }

        return sb.toString();
    }

    static class ConnInfo {
        final long ts;
        final Exception ex;
        final Thread owner;
        final String hostInfo;


        ConnInfo(long ts, Thread thread, Exception ex, String hostInfo) {
            this.ts = ts;
            this.owner = thread;
            this.ex = ex;
            this.hostInfo = hostInfo;
        }

        @Override
        public String toString() {

            return "Jedis-Debugger-Error:ConnInfo[" + hostInfo + "] borrowed by " + owner + " at:" + ts
                    + "\n" + exception2String(ex);
        }
    }
}
