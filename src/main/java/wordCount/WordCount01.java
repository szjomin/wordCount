package wordCount;

import java.util.*;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * @Description: get data from kafka
 * @Author: Jomin
 * @Date: 22:58 2020/2/7
 */
public class WordCount01{

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaDirectSpark");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(6));

        // 创建map，添加参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                "127.0.0.1:9092");


        // 创建一个集合set，添加读取的topic

        Set<String> topics = new HashSet<String>();
        topics.add("test");

        // 创建输入DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        // 单词统计
        /*JavaPairDStream<String, Integer> wordCounts = lines
                .flatMap(s -> Arrays.asList(s._2.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);*/

         JavaDStream<String> words = lines.flatMap(

                new FlatMapFunction<Tuple2<String, String>, String>() {

                    public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                        return Arrays.asList(tuple._2.split(" ")).iterator();
                    }

                    private static final long serialVersionUID = 1L;


                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );


        wordCounts.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {

        }
        jssc.close();
    }

}