package example.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * 指定 Offset 消费
 */
public class OffsetSeekConsumer {
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

        // 拉取数据
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // 订阅主题
            kafkaConsumer.subscribe(Collections.singletonList("serverMorePartition"));

            // 指定位置进行消费
            // 1. 获取 Topic 对应的分区信息
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            // 2. 为了确保主题分区分配方案已经制定完毕
            while (assignment.size() == 0) {
                // todo: 暂时搞不懂为什么拉取数据会加速分区分配方案的定制进度
                kafkaConsumer.poll(Duration.ofSeconds(1));
                // 更新分区信息
                assignment = kafkaConsumer.assignment();
            }
            // 3. 便利分区信息，指定分区和该分区的 offset
            for (TopicPartition topicPartition : assignment) {
                kafkaConsumer.seek(topicPartition, 10);
            }

            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord);
                }
            }
        }
    }
}
