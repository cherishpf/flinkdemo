package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author yangpf
 * @date 2019/4/24
 * 通过socket模拟产生单词数据
 * Flink对数据进行统计计算
 * 规则：每隔1秒对最近两秒内的数据进行统计计算
 * 滑动窗口计算
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {
        //1、获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、获取socket数据流
        String hostname = "localhost";
        int port;
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        port = parameterTool.getInt("port", 9000);
        String delimiter = "\n";
        long maxRetry = 3;
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter, maxRetry);

        // a a c
        // a 1
        // a 1
        // c 1
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            // 以匿名类的方式定义一个转换函数
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                // 匹配空白，即 空格，tab键
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                //指定时间窗口大小为2秒，指定时间间隔为1秒
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //在这里使用sum或者reduce都可以
                .sum("count");
                /*.reduce(new ReduceFunction<WordWithCount>() {
                                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                                        return new WordWithCount(a.word,a.count+b.count);
                                    }
                                })*/
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
