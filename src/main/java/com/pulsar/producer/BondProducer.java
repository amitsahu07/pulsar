package com.pulsar.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.Map;

@Service
public class BondProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${pulsar.alpha-key}")
    private String apiKey;

    @Value("${pulsar.bond-symbol}")
    private String maturity;

    public BondProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Runs once an hour to stay under free tier limits (25 calls/day)
    @Scheduled(fixedRate = 3600000)
    public void fetchBondData() {
        String url = String.format(
                "https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=%s&apikey=%s",
                maturity, apiKey
        );

        try {
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            if (response != null && response.containsKey("data")) {
                // We send the whole payload to Kafka; let the consumer handle it
                kafkaTemplate.send("uk-bond-data", maturity, response);
                System.out.println("Pushed Gilt update to Kafka for: " + maturity);
            }
        } catch (Exception e) {
            System.err.println("Error fetching bond data: " + e.getMessage());
        }
    }
}