package com.pulsar.monitor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
public class BondMonitor {

    private double currentYield = 0.0;
    private double previousYield = 0.0; // New variable

    public BondMonitor(MeterRegistry registry) {
        Gauge.builder("bond.yield.current", () -> currentYield)
                .tag("maturity", "10Y")
                .register(registry);

        Gauge.builder("bond.yield.previous", () -> previousYield)
                .tag("maturity", "10Y")
                .register(registry);
    }

    @KafkaListener(topics = "uk-bond-data")
    public void consume(Map<String, Object> message) {
        try {
            List<Map<String, String>> data = (List<Map<String, String>>) message.get("data");

            if (data != null && data.size() >= 2) {
                // Index 0 = Today, Index 1 = Yesterday/Previous
                this.currentYield = Double.parseDouble(data.get(0).get("value"));
                this.previousYield = Double.parseDouble(data.get(1).get("value"));

                System.out.println("Gilt Update - Current: " + currentYield + " | Previous: " + previousYield);
            }
        } catch (Exception e) {
            System.err.println("Error parsing historical bond data: " + e.getMessage());
        }
    }
}