package learn.kafka.producer;

import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class BasicProducerTest {

    private static Properties props = new Properties();

    @BeforeAll
    static void setUpOnce() {
        props.put("bootstrap.servers", "localhost:19092, localhost:29092, localhost:39092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Test
    void fireAndForget() {
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int n = 1; n <= 3; n++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("some-topic-3", "value - " + n);
                producer.send(record);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Test
    void sync() {
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int n = 1; n <= 3; n++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("some-topic-3", "value - " + n);
                RecordMetadata metadata = producer.send(record).get();

                log.info("Topics: {}, Partition: {}, Offset: {}, Key: {}, Message: {}",
                         metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Test
    void async() {
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int n = 1; n <= 3; n++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("some-topic-3", "value - " + n);
                producer.send(record, new AsyncCallback(record));
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @RequiredArgsConstructor
    private class AsyncCallback implements Callback {

        private final ProducerRecord<String, String> record;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {

            if (e == null) {
                log.info("Topics: {}, Partition: {}, Offset: {}, Key: {}, Message: {}",
                         metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
            } else {
                log.error("", e);
            }
        }
    }
}
