package example.producer.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 使用自定义分区器发送数据
 */
public class SendRecordByMyPartitioner {
    public static void main(String[] args) {
        // 添加配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 指定 k v 的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        // 创建kafka producer 对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 异步发送数据 - callback
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> metadataFuture = kafkaProducer.send(new ProducerRecord<>("topicA", "aa hello Kafka " + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("主题：" + metadata.topic() + "，分区：" + metadata.partition());
                } else {
                    exception.printStackTrace();
                }
            });
            Future<RecordMetadata> metadataFuture2 = kafkaProducer.send(new ProducerRecord<>("topicA", "cc hello Kafka " + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("主题：" + metadata.topic() + "，分区：" + metadata.partition());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // 关闭资源
        kafkaProducer.close();
    }
}
// 【可能出现的异常】
// org.apache.kafka.common.errors.TimeoutException: Expiring 10 record(s) for topicA-1:120006 ms has passed since batch creation
// 不存在 topicA 主题的 1 分区
//
// org.apache.kafka.common.errors.TimeoutException: Topic topicA not present in metadata after 60000 ms.
// 可能是 192.168.1.100:9092 的防火墙没关。。。