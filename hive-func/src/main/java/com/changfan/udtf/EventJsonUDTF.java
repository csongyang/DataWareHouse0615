package com.changfan.udtf;

import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override  //初始化方法定义输出的字段名称和类型
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");

        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //[{},{},{}...]:String
    @Override
    public void process(Object[] args) throws HiveException {

        //1.取数据[{},{},{}...]
        String input = args[0].toString();

        //2.判空
        if (StringUtil.isBlank(input)) {
            return;
        }

        try {
            //3.构建JSON数组
            JSONArray jsonArray = new JSONArray(input);

            //4.遍历数组
            for (int i = 0; i < jsonArray.length(); i++) {

                //5.获取每一个JSON对象
                JSONObject jsonObject = jsonArray.getJSONObject(i);

                //6.获取事件名称及事件整体
                String event_name = jsonObject.getString("en");
                String event_json = jsonObject.toString();

                //7.定义输出数组
                String[] outPuts = new String[2];

                //8.给数组赋值
                outPuts[0] = event_name;
                outPuts[1] = event_json;

                //9.写出
                forward(outPuts);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
