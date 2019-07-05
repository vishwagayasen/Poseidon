package com.flipkart.poseidon.serviceclients;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.ext.web.client.WebClient;

/**
 * Created by vishwa.gayasen on 03/07/19.
 */
public class VertxHttpClient {
    private WebClient client;
    private CircuitBreaker circuitBreaker;
    private String host;
    private int port;
    private boolean clientCreated;

    public VertxHttpClient() {
    }

    public VertxHttpClient(WebClient client, CircuitBreaker circuitBreaker, String host, int port) {
        this.client = client;
        this.circuitBreaker = circuitBreaker;
        this.host = host;
        this.port = port;
        this.clientCreated = true;
    }

    public WebClient getClient() {
        return client;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isClientCreated() {
        return clientCreated;
    }
}
