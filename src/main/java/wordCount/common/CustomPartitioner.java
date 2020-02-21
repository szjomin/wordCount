package wordCount.common;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;


/**
 * @Description: $description$
 * @Author: Jomin
 * @Date: $time$ $date$
 */
public class CustomPartitioner implements Partitioner {


    public void configure(Map<String, ?> configs) {

    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("parition : "+ 0);
        // 控制分区
        return 0;
    }


    public void close() {

    }



}
