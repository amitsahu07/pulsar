package com.pulsar.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class MarketDataProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${pulsar.alpha-key}") private String alphaKey;
    @Value("${pulsar.finnhub-key}") private String finnhubKey;

    public MarketDataProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 60000)
    public void fetchFastAssets() {
        // 1. AAPL (Stock), 2. LQD (Corp Bonds), 3. FX:GBPUSD (Forex)
        String[] symbols = {"AAPL", "LQD", "FX:GBPUSD"};

        for (String s : symbols) {
            try {
                String url = String.format("https://finnhub.io/api/v1/quote?symbol=%s&token=%s", s, finnhubKey);
                Map<String, Object> response = restTemplate.getForObject(url, Map.class);

                // Check if 'c' (current price) exists and is not null
                if (response != null && response.get("c") != null) {
                    double price = Double.parseDouble(response.get("c").toString());
                    if (price > 0) {
                        kafkaTemplate.send("market-updates", s, response);
                        System.out.println("✅ " + s + " is LIVE at: " + price);
                    }
                } else {
                    System.err.println("⚠️ " + s + " returned no price data. Might be restricted.");
                }
                Thread.sleep(1200); // Essential for free tier
            } catch (Exception e) {
                System.err.println("❌ Critical Error for " + s + ": " + e.getMessage());
            }
        }
    }

    // 2. Commodities via Alpha Vantage (1h interval - Safe for 25 calls/day limit)
    @Scheduled(fixedRate = 3600000)
    public void fetchSlowAssets() {
        String url = "https://www.alphavantage.co/query?function=WTI&apikey=" + alphaKey;
        Map response = restTemplate.getForObject(url, Map.class);
        kafkaTemplate.send("market-updates", "WTI_OIL", response);
    }
}