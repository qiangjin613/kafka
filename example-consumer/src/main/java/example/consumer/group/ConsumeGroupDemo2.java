package example.consumer.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 消费者组示例
 */
public class ConsumeGroupDemo2 {
    public static void main(String[] args) {
        // 填充配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 指定 k v 的反序列化类型
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定 Consumer Group ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupA");

        // 创建 Kafka Consumer 对象
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // 订阅主题
            kafkaConsumer.subscribe(Collections.singletonList("serverMorePartition"));
            // 拉取数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // 简单地打印对象
                    System.out.println(consumerRecord);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
