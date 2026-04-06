package com.pulsar.monitor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MarketMonitor {
    private final Map<String, Double> prices = new ConcurrentHashMap<>();

    public MarketMonitor(MeterRegistry registry) {
        // MATCH THESE TAGS TO YOUR KAFKA KEYS
        Gauge.builder("market.price", prices, map -> map.getOrDefault("FX:GBPUSD", 0.0))
                .tag("asset", "GBP_USD").register(registry);

        Gauge.builder("market.price", prices, map -> map.getOrDefault("LQD", 0.0))
                .tag("asset", "CORP_BOND").register(registry);

        Gauge.builder("market.price", prices, map -> map.getOrDefault("AAPL", 0.0))
                .tag("asset", "STOCK").register(registry);

        // For Alpha Vantage Oil
        Gauge.builder("market.price", prices, map -> map.getOrDefault("WTI_OIL", 0.0))
                .tag("asset", "OIL").register(registry);
    }


    @KafkaListener(topics = "market-updates")
    public void consume(ConsumerRecord<String, Map<String, Object>> record) {
        String key = record.key();
        Map<String, Object> val = record.value();

        try {
            if (val.containsKey("c")) { // Finnhub (Stocks/FX)
                prices.put(key, Double.valueOf(val.get("c").toString()));
            } else if (val.containsKey("data")) { // Alpha Vantage (Oil/Bonds)
                List<Map<String, String>> dataList = (List<Map<String, String>>) val.get("data");
                String latestPrice = dataList.get(0).get("value");
                prices.put(key, Double.valueOf(latestPrice));
            }
        } catch (Exception e) {
            System.err.println("Parsing error for " + key + ": " + e.getMessage());
        }
    }
}