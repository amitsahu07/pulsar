package com.pulsar;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PulsarApplication {
    public static void main(String[] args) {
        SpringApplication.run(PulsarApplication.class, args);
    }
}