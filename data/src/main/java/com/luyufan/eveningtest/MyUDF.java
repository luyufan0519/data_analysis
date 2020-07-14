package com.luyufan.eveningtest;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * @auther luyufan
 * @create 2020-06-28-14:29
 */
public class MyUDF extends GenericUDF {

    /**
     * 对输入参数的判断处理和返回值类型的一个约定
     * @param arguments  传入到函数的参数的类型对应的ObjectInspector
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
       if(arguments == null || arguments.length != 1){
           throw new UDFArgumentLengthException("input Args Length Error");
       }
       if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
           throw new UDFArgumentTypeException(0,"input Args Type Error");
       }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的处理逻辑
     * @param arguments 传入到函数的参数
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if( o == null){
            return 0;
        }
        return o.toString().length();
    }

    public String getDisplayString(String[] children) {
        return "";
    }
}
