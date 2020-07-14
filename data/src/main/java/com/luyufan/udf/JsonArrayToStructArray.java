package com.luyufan.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @auther luyufan
 * @create 2020-06-29-18:05
 */
public class JsonArrayToStructArray extends GenericUDF {
    private List<List<Object>> result = new ArrayList<>();

    //用于定义字段的名称以及类型
    //UDF("[{},{},{}]",     a_id,item,item_type,ts,    a_id:String,item:String...)
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length < 3) {
            throw new UDFArgumentException("json_array_to_struct_array需要至少3个参数");
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!"string".equals(arguments[i].getTypeName())) {
                throw new UDFArgumentException("json_array_to_struct_array的第" + (i + 1) + "个参数应为string类型");
            }
        }
        System.out.println("git test");
        //创建默认列名的集合
        List<String> fieldNames = new ArrayList<>();
        //创建输出类型的集合
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        //遍历数据,取出类型参数
        for (int i = 1 + (arguments.length - 1) / 2; i < arguments.length; i++) {
            if (!(arguments[i] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("参数错误");
            }

            //a_id:String
            String field = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();

            //切分
            String[] split = field.split(":");

            //添加默认列名
            fieldNames.add(split[0]);

            //添加列类型
            switch (split[1]) {
                case "string":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                case "boolean":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
                    break;
                case "tinyint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
                    break;
                case "smallint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
                    break;
                case "int":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "bigint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;
                case "float":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
                    break;
                case "double":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                    break;
                default:
                    throw new UDFArgumentException("json_array_to_struct_array 不支持" + split[1] + "类型");
            }
        }

        //返回
        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs));
    }

    //对每一行数据进行处理
    //UDF("[{},{},{}]",     a_id,item,item_type,ts,    a_id:String,item:String...)
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        //1.获取输入数据
        DeferredObject data = arguments[0];

        //2.判断数据是否为空
        if (data.get() == null) {
            return null;
        }

        //3.提取真正的数据信息==>"[{},{},{}]"
        String line = data.get().toString();

        //清空集合
        result.clear();

        //4.将数据转换为JSON数组
        JSONArray jsonArray = new JSONArray(line);

        //5.遍历jsonArray
        for (int i = 0; i < jsonArray.length(); i++) {

            JSONObject json = jsonArray.getJSONObject(i);

            List<Object> struct = new ArrayList<>();

            //("[{},{},{}]",     a_id,item,item_type,ts,    a_id:String,item:String...)
            for (int j = 1; j < 1 + (arguments.length - 1) / 2; j++) {
                String key = arguments[j].get().toString();
                if (json.has(key)) {
                    struct.add(json.getString(key));
                } else {
                    struct.add(null);
                }
            }
            result.add(struct);
        }

        return result;
    }

    //执行计划
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("JsonArrayToStructArray", children, ",");
    }
}
