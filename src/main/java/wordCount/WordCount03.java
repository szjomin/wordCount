package wordCount;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;
import wordCount.common.HBaseClient;
import wordCount.common.MyProperties;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
/**
 * @Description: $description$
 * @Author: Jomin
 * @Date: $time$ $date$
 */
public class WordCount03 {

    public static Logger log = Logger.getLogger(WordCount03.class);

    public static void main(String[] args) throws Exception {
        String configFile = "test.properties";
        MyProperties myProperties = new MyProperties();
        final Properties serverProps = myProperties.getProperties(configFile);

        JavaStreamingContext javaStreamingContext = createContext(serverProps);

        javaStreamingContext.start();
        try
        {
            javaStreamingContext.awaitTermination();
        }
        catch (Exception localException) {}
        javaStreamingContext.close();


    }

    public static JavaStreamingContext createContext(final Properties serverProps) {

        final String topic = serverProps.getProperty("kafka.topic");
        Set<String> topicSet = new HashSet();
        topicSet.add(topic);

        final String groupId = serverProps.getProperty("kafka.groupId");
        //获取批次的时间间隔，比如5s
        final Long streamingInterval = Long.parseLong(serverProps.getProperty("streaming.interval"));
        //获取kafka broker列表
        final String brokerList = serverProps.getProperty("bootstrap.servers");

        //从hbase中获取每个分区的消费到的offset位置
        //Map<TopicPartition, Long> consumerOffsetsLong = getConsumerOffsets(serverProps, topic, groupId);
        //printOffset(consumerOffsetsLong);
        //组合kafka参数
        final Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", brokerList);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        //创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("UserBehaviorStreaming");
        //本地测试
        sparkConf.setMaster("local[2]");
        //sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //sparkConf.set("spark.kryo.registrator", "com.djt.stream.registrator.MyKryoRegistrator");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000");

        Map<TopicPartition, Long> consumerOffsetsLong = getConsumerOffsets(serverProps, topic, groupId);

        //需要把每个批次的offset保存到此变量
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();

        //streamingInterval指每隔多长时间执行一个批次
        JavaStreamingContext javaStreamingContext =
                new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));

        //注： 这里的 KafkaUtils 的版本是 spark-streaming-kafka-0-10_2.11 的2.3.0 在 POM 的版本里面看
        JavaDStream<ConsumerRecord<String, String>> kafkaMessageDstream = KafkaUtils.createDirectStream(javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams, consumerOffsetsLong)
        );

        JavaDStream<ConsumerRecord<String, String>> kafkaMessageTransfrom = kafkaMessageDstream.transform(
                new Function2<JavaRDD<ConsumerRecord<String, String>>, Time, JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD, Time time) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        offsetToHbase(serverProps, offsetRanges, topic, groupId);
                        return consumerRecordJavaRDD;
                    }
                }
        );

        JavaDStream<String> words = kafkaMessageTransfrom.flatMap(
                new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                    @Override
                    public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                        return Arrays.asList(stringStringConsumerRecord.value().toString().split(" ")).iterator();
                    }
                }

        );


        JavaPairDStream<String, Integer> count = words.mapToPair(

                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return  new Tuple2(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer.intValue() + integer2.intValue();
                    }
                }
        );

        count.print();

        return javaStreamingContext;

    }

    /**
     * 将offset写入hbase
     */
    public static void offsetToHbase(Properties props, final AtomicReference<OffsetRange[]> offsetRanges, final String topic, String groupId) {

        String tableName = "test_offset";
        Table table = HBaseClient.getInstance(props).getTable(tableName);
        String rowKey = topic + ":" + groupId;

        for (OffsetRange or : offsetRanges.get()) {
            try {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes(String.valueOf(or.partition())), Bytes.toBytes(String.valueOf(or.untilOffset())));
                table.put(put);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                HBaseClient.closeTable(table);
            }
        }
    }

    /*
     * 从hbase中获取kafka每个分区消费到的offset,以便继续消费
     * */
    public static Map<TopicPartition, Long> getConsumerOffsets(Properties props, String topic, String groupId) {

        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        String tableName = "test_offset";
        Table table = HBaseClient.getInstance(props).getTable(tableName);
        String rowKey = topic + ":" + groupId;

        Map<TopicPartition, Long> map = new HashMap();

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = table.get(get);
            if (result.isEmpty()) {
                return map;
            } else {
                for (Cell cell : result.rawCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));

                    if (!"offset".equals(family)) {
                        continue;
                    }

                    int partition = Integer.parseInt(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    long offset = Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell)));
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    map.put(topicPartition, offset);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
