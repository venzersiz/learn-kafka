package learn.kafka.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class ExactlyOnceConsumerTest {

    private static Properties props = new Properties();

    @BeforeAll
    static void setUpOnce() {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "some-consumer-01");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 트랜잭션 소비자로 동작
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    }

    @Test
    void exactlyOnce() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("some-topic-9"));

            while (true) {
                // Timeout 1초 동안 Block
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                             record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

                // 추가 메시지를 폴링하기 전 현재의 오프셋을 비동기 커밋
                consumer.commitAsync();
            }
        } catch (Exception e) {
            log.error("", e);
        }

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
