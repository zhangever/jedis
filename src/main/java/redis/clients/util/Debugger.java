package redis.clients.util;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Debugger {
    private final static AtomicInteger seq = new AtomicInteger(0);
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
    static Map<Thread, ConnInfo> connInfos = new ConcurrentHashMap<Thread, ConnInfo>(128);

    public static void addConn(String hostInfo) {
        Thread thread = currentThread();
        connInfos.put(thread, new ConnInfo(System.currentTimeMillis(), thread,
                new RuntimeException("connDebugger"), hostInfo));
    }

    public static void removeConn() {
        connInfos.remove(currentThread());
    }

    static void checkConn() {
        long now = System.currentTimeMillis();
        System.out.println("Jedis-Debugger checking at:" + now + ", sizeOfconnInfos:" + connInfos.size());
        for (ConnInfo connInfo : connInfos.values()) {
            if ((now-connInfo.ts) > 30*1000) {
                simpleLog(connInfo.toString());
            }
        }
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

    public static void log(Exception e) {
        new MyException(e, currentThread()).printStackTrace();
    }

    public static void log(String msg) {
        System.out.println("Thread:[" + currentThread() + "], currentTs:" + System.currentTimeMillis() + ", detail:" + msg);
    }

    public static void simpleLog(String msg) {
        System.out.println("msg");
    }

    public static void log(String msg, Exception e) {
        log(msg);
        log(e);
    }

    static class MyException extends Exception {
        final Thread owner;
        MyException(Exception e, final Thread owner) {
            super(e);
            this.owner = (owner==null)?currentThread():owner;
        }

        @Override
        public String toString() {
            return owner + ":" + super.toString();
        }
    }

    static class ConnInfo {
        final long ts;
        final Exception ex;
        final Thread owner;
        final String hostInfo;


        ConnInfo(long ts, Thread thread, Exception ex, String hostInfo) {
            this.ts = ts;
            this.owner = thread;
            this.ex = new MyException(ex, thread);
            this.hostInfo = hostInfo;
        }

        @Override
        public String toString() {
            log(ex);
            return "ConnInfo[" + hostInfo + "] borrowed by " + owner + " at:" + ts;
        }
    }
}
