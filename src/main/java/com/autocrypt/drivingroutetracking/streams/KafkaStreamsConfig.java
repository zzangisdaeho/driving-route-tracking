package com.autocrypt.drivingroutetracking.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    public static final String SESSION_STORE_NAME = "in-memory-driving-route-store";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "vehicle-tracking-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream("gps-topic");

        KGroupedStream<String, String> groupedStream = stream.groupByKey();


        // 상태 저장소를 설정합니다. (예: In-Memory 상태 저장소)
        Materialized<String, String, SessionStore<Bytes, byte[]>> materialized =
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as(SESSION_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String());


        SessionWindows sessionWindows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10));

        KTable<Windowed<String>, String> aggregatedTable = groupedStream
                .windowedBy(sessionWindows)
                .aggregate(
                        () -> "", // 초기 값
                        (vehicleId, record, aggregate) -> {
                            log.info("record income : {}", record);
                            return parsingRecord(vehicleId, record, aggregate);
                        },
                        (aggKey, aggOne, aggTwo) -> aggOne + aggTwo, // 세션 병합 로직
                        materialized
                );

        aggregatedTable.toStream()
//                .process(VehicleDataProcessor::new, SESSION_STORE_NAME);
                .transform(() -> new Transformer<Windowed<String>, String, KeyValue<Windowed<String>, String>>() {
                    private ProcessorContext context;
                    private SessionStore<String, String> sessionStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.sessionStore = context.getStateStore(SESSION_STORE_NAME);
                    }

                    @Override
                    public KeyValue<Windowed<String>, String> transform(Windowed<String> key, String value) {
                        if (value != null && value.contains("STOPPED")) {
                            processSessionData(key.key(), value);
                            sessionStore.remove(key);
                        }
                        return new KeyValue<>(key, value);
                    }

                    @Override
                    public void close() {}
                }, SESSION_STORE_NAME)
                .foreach((windowedKey, aggregatedValue) -> {
                    if (aggregatedValue != null && !aggregatedValue.contains("STOPPED")) {
                        processSessionData(windowedKey.key(), aggregatedValue);
                    }
                });

        return stream;

    }

    private static String parsingRecord(String vehicleId, String record, String aggregate) {
        try {
            JsonNode jsonNode = objectMapper.readTree(record);

            if (jsonNode != null) {
                String timestamp = jsonNode.hasNonNull("timestamp") ? jsonNode.get("timestamp").asText() : "";
                double latitude = jsonNode.hasNonNull("latitude") ? jsonNode.get("latitude").asDouble() : 0.0;
                double longitude = jsonNode.hasNonNull("longitude") ? jsonNode.get("longitude").asDouble() : 0.0;
                return aggregate + vehicleId + "," + timestamp + "," + latitude + "," + longitude + "\n";
            } else {
                return aggregate;
            }
        } catch (Exception e) {
            log.error("record parsing fail : {}", record, e);
            return aggregate;
        }
    }

    private void processSessionData(String vehicleId, String aggregatedValue) {

        System.out.printf("Vehicle ID: %s, aggregatedValue : %s%n", vehicleId, aggregatedValue);

        // 세션 종료 후 데이터를 처리하는 로직을 구현
        String[] records = aggregatedValue.split("\n");
        long currentTime = System.currentTimeMillis();
        long oneMinuteAgo = currentTime - Duration.ofMinutes(1).toMillis();
        StringBuilder recentData = new StringBuilder();

        for (String record : records) {
            String[] parts = record.split(",");
            long timestamp = Long.parseLong(parts[1]); // assume timestamp is in the second field
            if (timestamp >= oneMinuteAgo) {
                recentData.append(record).append("\n");
            }
        }

        System.out.printf("Vehicle ID: %s, Recent Data: %s%n", vehicleId, recentData.toString());
        // 예: 다른 메소드로 전달
    }
}
