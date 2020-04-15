package com.changfan.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class ETLUtil {

    //判断启动日志是否合法
    public static boolean valiStart(String body) {
        return body.startsWith("{") && body.endsWith("}");
    }

    //判断事件日志是否合法
    public static boolean valiEvent(String body) {

        //切割   |需要用两个\\来转义
        String[] splits = body.split("\\|");

        //判断是否是两个字段
        if (splits.length != 2) {
            return false;
        }

        //判断第一部分是否为纯数字
        if (!NumberUtils.isDigits(splits[0])) {
            return false;
        }

        //判断第二部分是否为JSON格式
        return splits[1].startsWith("{") && splits[1].endsWith("}");
    }
}
