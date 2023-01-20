package example.producer.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * 根据指定分区发送数据
 */
public class SendRecordByPartition {
    public static void main(String[] args) {
        // 添加配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092");
        // 指定 k v 的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10; i++) {
                // fixme: 这里 ProducerRecord 中的 key 先随便写一个好了~
                kafkaProducer.send(new ProducerRecord<>("serverMorePartition", new Random().nextInt(5), "", "sdf" + i), (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("主题：" + metadata.topic() + "，分区：" + metadata.partition());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
