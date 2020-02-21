package wordCount;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Description: consume data
 * @Author: Jomin
 * @Date: 23:11 2020/2/7
 */
public class ConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "CDH01:9092");
        props.put("group.id", "test");//指定消费者属于哪个组
        props.put("enable.auto.commit", "true");//开启kafka的offset自动提交功能，可以保证消费者数据不丢失
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        String topic = "test";
        TopicPartition partition0 = new TopicPartition(topic, 0);//主题，分区
        TopicPartition partition1 = new TopicPartition(topic, 1);//主题，分区
        //consumer.subscribe(Arrays.asList(partition1));//指定消费的topic
        consumer.assign(Arrays.asList(partition1));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

}
