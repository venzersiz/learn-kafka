package learn.kafka.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class BasicConsumerTest {

    private Properties props = new Properties();

    @BeforeEach
    void setUp() {
        props.put("bootstrap.servers", "localhost:19092, localhost:29092, localhost:39092");
        props.put("group.id", "some-consumer-group-1");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Test
    void autoCommit() {
        props.put("enable.auto.commit", "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("some-topic-3"));

            while (true) {
                // Timeout 1초 동안 Block
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                             record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
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

    @Test
    void sync() {
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("some-topic-3"));

            while (true) {
                // Timeout 1초 동안 Block
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                             record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

                // 추가 메시지를 폴링하기 전 현재의 오프셋을 동기 커밋
                consumer.commitSync();
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

    @Test
    void async() {
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("some-topic-3"));

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
