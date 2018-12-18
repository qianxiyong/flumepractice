package com.practice.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import sun.java2d.pipe.SpanShapeRenderer;

import java.util.HashMap;

/**
 * @author Altshuler
 * 自定义flume source 采集数据
 */
public class MySource extends AbstractSource implements Configurable,PollableSource {

    private String preText;
    private Long waitTime;

    @Override
    public Status process() throws EventDeliveryException {
        try {
            int times = 5;
            for (int i = 0; i < times; i++) {
                SimpleEvent event = new SimpleEvent();
                event.setBody((preText + ">>" + i + " hello world").getBytes());
                event.setHeaders(new HashMap<String, String>(0));
                getChannelProcessor().processEvent(event);
                Thread.sleep(waitTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {

        preText = context.getString("preText");
        waitTime = context.getLong("waitTime",3000L);

    }
}
