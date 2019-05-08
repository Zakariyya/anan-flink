package flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 *
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现：每隔1秒对最近2秒内的数据进行计算
 */

public class SocketWindowWordCount {

  public static void main(String[] args) throws Exception {

    int port;
    try{
      ParameterTool parameterTool = ParameterTool.fromArgs(args);
      port = parameterTool.getInt("port");

    }catch (Exception e){
      System.out.println("No port set. use default port 9000");
      port = 9000;
    }

    //获取flink的运行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    String hostname = "kafka2";
    String delimiter = "\n";

    //连接socket获取输入的数据
    DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
    SingleOutputStreamOperator<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
      public void flatMap(String s, Collector<WordWithCount> out) throws Exception {
        String[] split = s.split("\\s");
        for (String word : split) {
          out.collect(new WordWithCount(word, 1L));
        }
      }
    }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1))
            .sum("count");

    //把数据打印到控制台，并设置并行度
    windowCount.print().setParallelism(1);

    //执行触发
    env.execute("Socket window count: ");

  }

  public static class WordWithCount{
    public String word;
    public long count;
    public WordWithCount(){}

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
