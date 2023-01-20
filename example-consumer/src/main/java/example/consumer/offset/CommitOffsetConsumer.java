package example.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 手动提交 Offset
 * <p>
 *     1. commitSync（同步提交）
 *     2. commitAsync（异步提交）
 */
public class CommitOffsetConsumer {
    public static void main(String[] args) {
        // 填充配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 指定 k v 的反序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定 Consumer Group ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "t");
        // 关闭自动提交 Offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 拉取数据
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // 订阅主题
            kafkaConsumer.subscribe(Collections.singletonList("topicA"));
            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord);
                }
                // 同步提交 offset
                kafkaConsumer.commitSync();
                // 异步提交 offset
                // kafkaConsumer.commitAsync();
            }
        }
    }
}
