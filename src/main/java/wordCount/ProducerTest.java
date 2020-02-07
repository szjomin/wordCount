package wordCount;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/** 
* @Description: kafka produce data
* @Author: Jomin
* @Date: 23:11 2020/2/7 
*/ 
public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "hello hello hello hello bye bye bye bye bye bye bye");//topic，value
        producer.send(record);

        /*for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "hello"+ i);//topic，value
            producer.send(record);
            //producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i))); //topic, key, value
        }*/

        producer.close();//不要忘记close
    }

}
