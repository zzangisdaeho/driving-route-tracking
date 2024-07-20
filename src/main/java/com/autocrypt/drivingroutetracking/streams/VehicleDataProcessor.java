package com.autocrypt.drivingroutetracking.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.SessionStore;

@Slf4j
public class VehicleDataProcessor implements Processor<Windowed<String>, String, Windowed<String>, String> {

    private ProcessorContext<Windowed<String>, String> context;
    private SessionStore<String, String> store;

    @Override
    public void init(ProcessorContext<Windowed<String>, String> context) {
        this.context = context;
        this.store = context.getStateStore(KafkaStreamsConfig.SESSION_STORE_NAME);
    }

    @Override
    public void process(Record<Windowed<String>, String> record) {
        String value = record.value();
//        log.info("value processor receive : \n{}", value);
        Windowed<String> key = record.key();
        store.remove(key);
        context.commit();
        context.forward(record);
    }

    @Override
    public void close() {
        // 리소스 정리 작업
    }

    public static ProcessorSupplier<Windowed<String>, String, Windowed<String>, String> supplier() {
        return VehicleDataProcessor::new;
    }
}
