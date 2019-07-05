package com.flipkart.poseidon.serviceclients;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;

/**
 * Created by vishwa.gayasen on 24/06/19.
 */
public class VertxManager {
    private static Vertx vertx;

    public static void init(Vertx vrtx) {
        vertx = vrtx;
    }

    public static Vertx getVertx() {
        if (vertx == null) {
            vertx = Vertx.vertx(new VertxOptions()
                    .setFileSystemOptions(new FileSystemOptions().setClassPathResolvingEnabled(false))
                    .setEventLoopPoolSize(400)
                    .setMetricsOptions(new DropwizardMetricsOptions().setJmxEnabled(true)));
        }
        return vertx;
    }
}
