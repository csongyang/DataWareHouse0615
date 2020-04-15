package com.changfan.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    private List<Event> intercepts = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //获取数据
        String body = new String(event.getBody());

        //定义一个标记
        boolean flag;

        //根据不同的日志类型进行过滤
        if (body.contains("start")) {

            //对启动日志进行过滤
            flag = ETLUtil.valiStart(body);

        } else {

            //对事件日志进行过滤
            flag = ETLUtil.valiEvent(body);

        }

        if (flag) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //清空集合
        intercepts.clear();

        //遍历数据并过滤
        for (Event event : events) {

            //调用上一个方法
            Event intercept = intercept(event);

            if (intercept != null) {
                intercepts.add(intercept);
            }
        }

        return intercepts;
    }

    @Override
    public void close() {

    }

    //这个bulid名字可以随便起
    public static class Build implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
