package wordCount;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import java.util.Properties;
import org.apache.kafka.clients.producer.RecordMetadata;
/**
 * @Description: kafka produce data
 * @Author: Jomin
 * @Date: 23:11 2020/2/7
 */
public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "CDH01:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义分区
        props.put("partitioner.class", "wordCount.common.CustomPartitioner");


        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        //ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "bye go hello hello bye bye bye bye bye bye bye");//topic，value

        //producer.send(record);

        producer.send(new ProducerRecord<String, String>("test", "three one one one two two two"), new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if (metadata != null) {

                    System.out.println(metadata.partition() + "---" + metadata.offset());
                }else{
                    System.out.println("send Fail");
                }
            }
        });

        /*for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "hello"+ i);//topic，value
            producer.send(record);
            //producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i))); //topic, key, value
        }*/

        producer.close();//不要忘记close
    }

}
