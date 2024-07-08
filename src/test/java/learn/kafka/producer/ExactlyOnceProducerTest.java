package learn.kafka.producer;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class ExactlyOnceProducerTest {

    private static Properties props = new Properties();

    @BeforeAll
    static void setUpOnce() {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092, localhost:29092, localhost:39092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.RETRIES_CONFIG, "5");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-transaction-01");
    }

    @Test
    void exactlyOnce() {
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            producer.initTransactions();
            producer.beginTransaction();

            ProducerRecord<String, String> record = new ProducerRecord<>("some-topic-9", "Hello");
            producer.send(record);
            producer.flush();

            System.out.println("Message sent successfully");
        } catch (Exception e) {
            producer.abortTransaction();
            log.error("", e);
        } finally {
            producer.commitTransaction();
            producer.close();
        }
    }
}
