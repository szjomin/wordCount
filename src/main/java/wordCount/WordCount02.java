package wordCount;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/**
 * @Description: get data from zooKeeper
 * @Author: Jomin
 * @Date: 23:11 2020/2/7
 */
public class WordCount02 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaReceiverSpark");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 使用KafkaUtils.createStream()方法，创建 Kafka 的输入数据流
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("test", 1);

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jsc,
                "127.0.0.1:2181",
                "test",
                topicThreadMap);

        // wordcount code
        JavaDStream<String> words = lines.flatMap(

                new FlatMapFunction<Tuple2<String, String>, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                        return Arrays.asList(tuple._2.split(" ")).iterator();
                    }
                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        wordCounts.print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (Exception e) {

        }
        jsc.close();
    }


}
