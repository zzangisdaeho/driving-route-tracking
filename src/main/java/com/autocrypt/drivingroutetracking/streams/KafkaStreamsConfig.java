package com.autocrypt.drivingroutetracking.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
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
        KStream<String, String> stream = streamsBuilder.stream("gps-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> log.info("record income = \n key : {}, value : {}", key, value))
                .mapValues(KafkaStreamsConfig::parsingRecord);
//                .map((key, value) -> {
//                    String newValue = parsingRecord(value);
//                    return new KeyValue<>(key, newValue);
//                });


        KGroupedStream<String, String> groupedStream = stream.groupByKey();

        // 상태 저장소를 설정
        Materialized<String, String, SessionStore<Bytes, byte[]>> materialized =
                Materialized.<String, String, SessionStore<Bytes, byte[]>>as(SESSION_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String());

        SessionWindows sessionWindows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10));

        KTable<Windowed<String>, String> aggregatedTable = groupedStream
                .windowedBy(sessionWindows)
                .reduce(
                        (aggValue, newValue) -> aggValue.concat("\n").concat(newValue),
                        materialized
                )
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                ;


        aggregatedTable.toStream()
                .filter((windowedKey, aggregatedValue) -> aggregatedValue!=null && !aggregatedValue.isBlank())
                .filter((windowedKey, aggregatedValue) -> aggregatedValue.contains("STOP"))
                .process(VehicleDataProcessor.supplier(), SESSION_STORE_NAME)
                .foreach((windowedKey, aggregatedValue) -> {
                    log.info("Session start: {}, Session end: {}", Instant.ofEpochMilli(windowedKey.window().start()).atZone(ZoneId.of("Asia/Seoul")), Instant.ofEpochMilli(windowedKey.window().end()).atZone(ZoneId.of("Asia/Seoul")));
                    log.info("aggregatedValue = \n{}", aggregatedValue);
                });

        return stream;
    }

    private static String parsingRecord(String value) {

        if(value.equalsIgnoreCase("\"STOP\"")) {
            return "STOP";
        }

        try {
            JsonNode jsonNode = objectMapper.readTree(value);
            if (jsonNode != null) {
                String vehicleId = jsonNode.hasNonNull("vehicleId") ? jsonNode.get("vehicleId").asText() : "";
                String timestamp = jsonNode.hasNonNull("timestamp") ? jsonNode.get("timestamp").asText() : "";
                double latitude = jsonNode.hasNonNull("latitude") ? jsonNode.get("latitude").asDouble() : 0.0;
                double longitude = jsonNode.hasNonNull("longitude") ? jsonNode.get("longitude").asDouble() : 0.0;
                return vehicleId + "," + timestamp + "," + latitude + "," + longitude;
            }
        } catch (Exception e) {
            log.error("Error parsing record: {}", value, e);
        }
        return "";
    }


}
