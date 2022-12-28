package example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 异步发送
 * <p>
 *  1. 异步发送、异步发送带回调方法
 *  2. 同步发送
 */
public class SendRecord {
    public static void main(String[] args) {
        // 添加配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 指定 k v 的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建kafka producer 对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 异步发送数据
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> metadataFuture = kafkaProducer.send(new ProducerRecord<>("topicA", "hello Kafka " + i));
        }
        // 异步发送数据 - callback
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> metadataFuture = kafkaProducer.send(new ProducerRecord<>("topicA", "hello Kafka " + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("主题：" + metadata.topic() + "，分区：" + metadata.partition());
                }
            });
        }
        // 同步发送数据
        for (int i = 0; i < 10; i++) {
            try {
                RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>("topicA", "hello Kafka " + i)).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // 关闭资源
        kafkaProducer.close();
    }
}
