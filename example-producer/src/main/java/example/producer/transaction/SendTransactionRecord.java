package example.producer.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者事务代码示例
 */
public class SendTransactionRecord {
    public static void main(String[] args) {
        // 添加配置信息
        Properties properties = new Properties();
        // 连接 kafka server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 指定 k v 的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定事务ID：确保全局唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "TransactionID_001");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 初始化事务
        kafkaProducer.initTransactions();
        // 启动事务
        kafkaProducer.beginTransaction();
        try {
            // 发送数据
            for (int i = 0; i < 100; i++) {
                kafkaProducer.send(new ProducerRecord<>("topicA", "TransactionRecord" + i));
                if (i == 80) {
                    throw new RuntimeException("ao~");
                }
            }
            // 提交事务
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            // 终止/回滚事务
            kafkaProducer.abortTransaction();
            e.printStackTrace();
        } finally {
            // 关闭 kafka 连接
            kafkaProducer.close();
        }
    }
}
