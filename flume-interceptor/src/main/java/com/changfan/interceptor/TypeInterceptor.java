package com.changfan.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    private List<Event> interceptors = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //获取数据的header和body
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        //根据不同的日志类型添加头信息
        if (body.contains("start")) {
            headers.put("topic", "topic_start");
        } else {
            headers.put("topic", "topic_event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //清空集合
        interceptors.clear();

        //遍历events添加头信息
        for (Event event : events) {

            interceptors.add(intercept(event));
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Build implements Interceptor.Builder {

        @Override
        public Interceptor build() {

            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
