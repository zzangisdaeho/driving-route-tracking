package com.autocrypt.drivingroutetracking.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;

import static com.autocrypt.drivingroutetracking.streams.KafkaStreamsConfig.SESSION_STORE_NAME;

@Slf4j
public class VehicleDataProcessor implements Processor<Windowed<String>, String, Windowed<String>, String> {

    private ProcessorContext<Windowed<String>, String> context;
    private SessionStore<String, String> sessionStore;

    @Override
    public void init(ProcessorContext<Windowed<String>, String> context) {
        this.context = context;
        this.sessionStore = context.getStateStore(SESSION_STORE_NAME);
    }

    @Override
    public void process(Record<Windowed<String>, String> record) {
        log.info("processor received record : {}", record);
        if (record.value() == null) {
            log.warn("Received null value for record: {}", record);
            return;
        }
        if (record.value().contains("STOPPED")) {
            processSessionData(record.key().key(), record.value());
            context.forward(record.withKey(record.key()).withValue(record.value()));
            sessionStore.remove(record.key());
            context.commit();
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

    @Override
    public void close() {}
}
