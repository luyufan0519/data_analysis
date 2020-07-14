package com.luyufan.eveningtest;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @auther luyufan
 * @create 2020-06-28-14:59
 */
public class MyUDTF1 extends GenericUDTF {
    private List<String> outList = new ArrayList<String>();
    /**
     * 将指定的字符串通过指定的分隔符，进行拆分，返回多行数据
     * 约定函数输出的列的名字和列的类型
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

// 约定函数输出的列的名字
        List<String> names = new ArrayList<String>();
        names.add("words");
// 约定函数输出的列的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory .getStandardStructObjectInspector(names, fieldOIs);
    }

    /**
     * 函数的处理逻辑
     * @param args
     * @throws HiveException
     */
    public void process(Object[] args) throws HiveException {
        // 我要传入的参数为一个需要被分割的字符串，然后第二个参数是分割符
   if(args == null || args.length <2 ){
       throw new HiveException("Input Args Length Error");
   }
   // 获取待处理的数据
        String inputData = args[0].toString();
   // 获取分隔符
        String inputSplit = args[1].toString();
        String[] words = inputData.split(inputSplit);
        // 迭代写出
        for (String word : words) {
            outList.clear();
          outList.add(word);
            forward(outList);
        }

    }

    public void close() throws HiveException {

    }
}
