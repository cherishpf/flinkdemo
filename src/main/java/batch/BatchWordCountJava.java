package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author yangpf
 * @date 2019/4/24
 */
public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        String inputPath = "E:\\data\\file";
        String outPath = "E:\\data\\result";

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 加载/创建初始化数据, 获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);
        // 说明在数据上要做的转换
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);
        // 说明将计算的结果保存在哪
        counts.writeAsCsv(outPath, "\n", " ").setParallelism(1);
        // 执行程序
        env.execute("batch word count");

    }

    // 以实现FlatMapFunction接口的方式定义一个转换函数
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 匹配特殊字符，即非字母、非数字、非汉字、非_
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}