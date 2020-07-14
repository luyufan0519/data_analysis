package com.luyufan.data_analysis;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @auther luyufan
 * @create 2020-06-27-16:03
 */

/**
 * 输入的数据格式【{"a"："a1"},{"b"："b1"},{"c"："c1"}】
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 给生成的列定义列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("action");
        // 列类型校验器集合
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
// 返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 遍历每一行数据，做炸开操作【多次写出操作】
//    数据 [{"a":"a1"},{"b":"b1"},{"c":"c1"}]
    public void process(Object[] objects) throws HiveException {

        if (objects.length <= 0) {
            return;
        }
        // 1.获取UDTF函数的输入数据
        if (objects[0] == null) {
            return;
        }
        String input = objects[0].toString();
        // 2. 对输入数据创建JSON数组
        JSONArray jsonArray = new JSONArray(input);
        //3.遍历JSON数组，将每一个元素写出
        for (int i = 0; i < jsonArray.length(); i++) {
            ArrayList<Object> list = new ArrayList<Object>();
            list.add(jsonArray.getString(i));
            forward(list);
        }

    }

    public void close() throws HiveException {

    }
}
