package example.consumer.offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 按时间消费数据
 */
public class OffsetSeekConsumerByTime {
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

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            // 订阅主题
            kafkaConsumer.subscribe(Collections.singletonList("serverMorePartition"));

            // 获取分区信息
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            while (assignment.isEmpty()) {
                // todo: 暂时搞不懂为什么拉取数据会加速分区分配方案的定制进度
                kafkaConsumer.poll(Duration.ofSeconds(1));
                assignment = kafkaConsumer.assignment();
            }
            // 填充分区对应的时间信息
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>(assignment.size());
            for (TopicPartition topicPartition : assignment) {
                // 填充一天前的时间
                timestampsToSearch.put(topicPartition, System.currentTimeMillis() - 24 * 60 * 60 * 1000);
            }
            // 根据分区对应的时间获取分区对应的 offset
            Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                    = kafkaConsumer.offsetsForTimes(timestampsToSearch);
            topicPartitionOffsetAndTimestampMap.forEach((topicPartition, offsetAndTimestamp) -> {
                if (offsetAndTimestamp == null) {
                    return;
                }
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            });

            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
