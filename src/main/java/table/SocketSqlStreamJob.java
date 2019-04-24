package table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author yangpf
 * @date 2019/4/24
 * 用SQL处理socket传入的流式数据
 * 启动此程序前必须先启动SocketServer程序
 */
public class SocketSqlStreamJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);

        String sqlquery=null;

        try {
            // 读取文件中的SQL语句
            BufferedReader queryFile = new BufferedReader(
                    new FileReader("E://workspace//flinkdemo//src//main//resources//queryIn"));
            sqlquery = queryFile.readLine();
            queryFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 接收来自socket text流的包含6个元素的输入数据
        DataStream<Tuple6<Long,Integer,String,String,Long,Double>> inputStr = env.socketTextStream("127.0.0.1", 12341)
                .map(new MapFunction<String, Tuple6<Long,Integer,String,String,Long,Double>>() {
                    @Override
                    public Tuple6<Long,Integer,String,String,Long,Double> map(String values) throws Exception {

                        String [] items = values.split(",");
                        Long timestamp = Long.parseLong(items[0]);
                        Integer id = Integer.parseInt(items[1]);
                        String	 user = items[2];
                        String	 note = items[3];
                        Long specificNumber = Long.parseLong(items[4]);
                        Double amount = Double.parseDouble(items[5]);

                        Tuple6<Long,Integer,String,String,Long,Double> inputTuple = new
                                Tuple6<>(timestamp,id,user,note,specificNumber,amount);

                        return inputTuple;
                    }
                });

        // 把流注册为一个动态表, register the DataStream under the name "inputStream"
        tableEnvironment.registerDataStream(
                "inputStream",
                inputStr, "timeevent,id,name,note,specificnumber,amount");
        // run a SQL，产生一个查询结果的动态表
        Table result = tableEnvironment.sqlQuery(sqlquery);

        // 将结果动态表转换为流输出
        // SQL查询结果包括两个字段，we expect that the output will be 2 fields timestamp and double, eg: select timeevent, amount from inputStream
//        TypeInformation<Tuple2<Long, Double>> tpinf = new TypeHint<Tuple2<Long, Double>>() {
//        }.getTypeInfo();
//        DataStream<Tuple2<Long, Double>> saa = tableEnvironment.toAppendStream(result, tpinf);

        // 将动态表转换为仅追加流
        // SQL查询结果可包含多个字段, eg: select timeevent,id,name,note,specificnumber,amount from inputStream
//        DataStream<Row> saa = tableEnvironment.toAppendStream(result, Row.class);

        // SQL查询可以是：select name,sum(amount) from inputStream group by name
        // 将动态表转换为撤销流, the Boolean field indicates the type of the change
        // True is INSERT , False is DELETE
        DataStream<Tuple2<Boolean, Row>> saa = tableEnvironment.toRetractStream(result, Row.class);

        // 把结果打印出来
        saa.print();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // execute program
        env.execute("sql on custom schema");

    }
}
