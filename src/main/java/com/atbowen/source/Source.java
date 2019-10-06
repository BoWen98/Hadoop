package com.atbowen.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: hdfs
 * @Package: com.atbowen.source
 * @ClassName: Source
 * @Author: Bowen
 * @Description: 自定义Source
 * @Date: 2019/10/6 16:23
 * @Version: 1.0.0
 */
public class Source extends AbstractSource implements Configurable, PollableSource {

    //定义配置文件将来要读取的字段
    private Long delay;
    private String field;


    public Status process() throws EventDeliveryException {
        try {
            Map<String, String> header = new HashMap<String, String>();
            SimpleEvent event = new SimpleEvent();

            for (int i = 0; i < 5; i++) {
                event.setHeaders(header);
                event.setBody((field + i).getBytes());
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {

        delay = context.getLong("delay", 2000l);
        field = context.getString("field", "hadoop");
    }
}
