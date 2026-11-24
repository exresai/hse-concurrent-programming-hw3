
package edu.hse.concurrency.metrics;

import java.util.Map;

public class MetricsRegistryImpl implements MetricsRegistry {

    private final MetricsEngine engine = new MetricsEngine();

    @Override
    public Counter counter(String name, Map<String, String> tags) {
        return engine.counter(name, tags);
    }

    @Override
    public Gauge gauge(String name, Map<String, String> tags) {
        return engine.gauge(name, tags);
    }

    @Override
    public Timer timer(String name, Map<String, String> tags, long... upperBoundsNanos) {
        return engine.timer(name, tags, upperBoundsNanos);
    }

    @Override
    public String scrape() {
        return engine.scrape();
    }

    @Override
    public Registration onRegistration(RegistrationListener listener) {
        return engine.onRegistration(listener);
    }
}

