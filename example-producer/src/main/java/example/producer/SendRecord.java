package example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 发送消息
 * <p>
 *  1. 异步发送、异步发送带回调方法
 *  2. 同步发送
 */
public class SendRecord {
    public static void main(String[] args) {
        // 添加配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092");
        // 指定 k v 的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // 数据压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        // 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 重新发送次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);

        // 创建kafka producer 对象
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
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
        }
    }
}
