package com.changfan.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    //line：13456|{"cm":{"mid":"a1"},"app":"gmall","et":[{1},{2},{3}]}，key:ln or et...st(系统时间)
    public static String evaluate(String line, String key) {

        //1.分割字符串
        String[] splits = line.split("\\|");

        //2.判断传入获取数据的字段是否为获取系统时间
        if ("st".equals(key)) {
            return splits[0];
        }

        //3.判断传入获取数据的字段是否为获取事件数组
        if ("et".equals(key)) {
            try {
                //4.将第二个字段转换为JSON对象
                JSONObject jsonObject = new JSONObject(splits[1]);

                //5.判断et字段是否存在
                if (jsonObject.has(key)) {
                    return jsonObject.getString(key);
                } else {
                    return "";
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        //6.获取的是公共字段
        try {
            JSONObject jsonObject = new JSONObject(splits[1]);

            //7.获取公共字段的JSON对象
            JSONObject cm = jsonObject.getJSONObject("cm");

            //8.获取指定的Key对应的Value

            if (cm.has(key)) {
                return cm.getString(key);
            } else {
                return "";
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) {

        System.out.println(evaluate("1549728167699|{\"cm\":{\"ln\":\"-66.3\",\"sv\":\"V2.5.5\",\"os\":\"8.2.5\",\"g\":\"U7311A07@gmail.com\",\"mid\":\"998\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"10\",\"hw\":\"640*960\",\"ar\":\"MX\",\"uid\":\"998\",\"t\":\"1549671623365\",\"la\":\"24.2\",\"md\":\"Huawei-5\",\"vn\":\"1.2.6\",\"ba\":\"Huawei\",\"sr\":\"V\"},\"ap\":\"gmall\",\"et\":[{\"ett\":\"1549728158667\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"goodsid\":\"251\",\"news_staytime\":\"0\",\"loading_time\":\"3\",\"action\":\"1\",\"showtype\":\"0\",\"category\":\"59\",\"type1\":\"\"}},{\"ett\":\"1549682952493\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1549663015331\",\"action\":\"3\",\"type\":\"2\",\"content\":\"\"}},{\"ett\":\"1549655489363\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\\\n at java.lang.reflect.Method.invoke(Method.java:606)\\\\n\",\"errorBrief\":\"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\"}},{\"ett\":\"1549637201865\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1549688947867\",\"praise_count\":823,\"other_id\":3,\"comment_id\":2,\"reply_count\":174,\"userid\":7,\"content\":\"谭称须逊奉\"}},{\"ett\":\"1549708754411\",\"en\":\"praise\",\"kv\":{\"target_id\":4,\"id\":0,\"type\":4,\"add_time\":\"1549720306667\",\"userid\":3}}]}", "et"));

    }

}
