package edu.hse.concurrency.metrics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

final class MetricsEngine {

    private final Object registryLock = new Object();
    private final Map<MetricId, MetricEntry> metrics = new LinkedHashMap<>();
    private final Map<String, long[]> timerBounds = new HashMap<>();
    private final List<Subscription> subscriptions = new ArrayList<>();

    Counter counter(String name, Map<String, String> tags) {
        MetricId id = new MetricId(name, tags);
        if (!name.endsWith("_total")) {
            throw new IllegalArgumentException("Counter name must end with _total");
        }
        return (Counter) register(id, MetricType.COUNTER, null);
    }

    Gauge gauge(String name, Map<String, String> tags) {
        MetricId id = new MetricId(name, tags);
        if (name.endsWith("_total") || name.endsWith("_seconds")
                || name.endsWith("_bucket") || name.endsWith("_count")
                || name.endsWith("_sum")) {
            throw new IllegalArgumentException("Reserved metric name suffix");
        }
        return (Gauge) register(id, MetricType.GAUGE, null);
    }

    Timer timer(String name, Map<String, String> tags, long... upperBoundsNanos) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(tags, "tags");
        Objects.requireNonNull(upperBoundsNanos, "upperBoundsNanos");

        MetricId id = new MetricId(name, tags);

        if (!name.endsWith("_seconds")) {
            throw new IllegalArgumentException("Timer name must end with _seconds");
        }

        long[] bounds = upperBoundsNanos.clone();

        if (bounds.length < 1 || bounds.length > 20) {
            throw new IllegalArgumentException("Timer must have 1 to 20 bounds");
        }

        for (int i = 0; i < bounds.length; i++) {
            if (bounds[i] < 0 || (i > 0 && bounds[i] <= bounds[i - 1])) {
                throw new IllegalArgumentException(
                        "Bounds must be non-negative and strictly increasing"
                );
            }
        }

        return (Timer) register(id, MetricType.TIMER, bounds);
    }

    Registration onRegistration(RegistrationListener listener) {
        Objects.requireNonNull(listener, "listener");
        Subscription subscription = new Subscription(listener);

        synchronized (registryLock) {
            subscriptions.add(subscription);
        }

        return subscription;
    }

    String scrape() {
        List<MetricEntry> snapshot;

        synchronized (registryLock) {
            snapshot = new ArrayList<>(metrics.values());
        }

        if (snapshot.isEmpty()) {
            return "";
        }

        Map<String, List<MetricEntry>> byName = new LinkedHashMap<>();

        for (MetricEntry entry : snapshot) {
            byName.computeIfAbsent(
                    entry.id().name(),
                    key -> new ArrayList<>()
            ).add(entry);
        }

        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, List<MetricEntry>> group : byName.entrySet()) {
            String name = group.getKey();
            MetricType type = group.getValue().getFirst().type();

            String prometheusType = switch (type) {
                case COUNTER -> "counter";
                case GAUGE -> "gauge";
                case TIMER -> "histogram";
            };

            result.append("# TYPE ")
                    .append(name)
                    .append(' ')
                    .append(prometheusType)
                    .append('\n');

            for (MetricEntry entry : group.getValue()) {
                MetricId id = entry.id();

                switch (entry.type()) {
                    case COUNTER -> appendSample(
                            result,
                            name,
                            id.tags(),
                            null,
                            Long.toString(((Counter) entry.metric()).count())
                    );

                    case GAUGE -> appendSample(
                            result,
                            name,
                            id.tags(),
                            null,
                            Long.toString(((Gauge) entry.metric()).get())
                    );

                    case TIMER -> appendTimer(
                            result,
                            name,
                            id.tags(),
                            (TimerMetric) entry.metric()
                    );
                }
            }
        }

        return result.toString();
    }

    private Object register(MetricId id, MetricType type, long[] bounds) {
        Object metric;
        List<RegistrationListener> listeners;

        synchronized (registryLock) {
            if (type == MetricType.TIMER) {
                long[] previous = timerBounds.get(id.name());

                if (previous != null && !Arrays.equals(previous, bounds)) {
                    throw new IllegalArgumentException(
                            "Timer bounds conflict for " + id.name()
                    );
                }
            }

            MetricEntry existing = metrics.get(id);

            if (existing != null) {
                if (existing.type() != type) {
                    throw new IllegalArgumentException(
                            "Metric type conflict for " + id.name()
                    );
                }

                return existing.metric();
            }

            metric = switch (type) {
                case COUNTER -> new CounterMetric();
                case GAUGE -> new GaugeMetric();
                case TIMER -> new TimerMetric(bounds);
            };

            if (type == MetricType.TIMER) {
                timerBounds.putIfAbsent(id.name(), bounds.clone());
            }

            metrics.put(id, new MetricEntry(id, type, metric));

            listeners = new ArrayList<>(subscriptions.size());

            for (Subscription subscription : subscriptions) {
                listeners.add(subscription.listener);
            }
        }

        for (RegistrationListener listener : listeners) {
            try {
                listener.onRegistration(id, type);
            } catch (RuntimeException ignored) {
            }
        }

        return metric;
    }

    private static void appendTimer(
            StringBuilder output,
            String name,
            Map<String, String> tags,
            TimerMetric timer
    ) {
        TimerSnapshot snapshot = timer.snapshot();
        long[] bounds = timer.bounds;
        List<Long> buckets = snapshot.cumulativeBuckets();

        for (int i = 0; i < bounds.length; i++) {
            appendSample(
                    output,
                    name + "_bucket",
                    tags,
                    decimalSeconds(bounds[i]),
                    Long.toString(buckets.get(i))
            );
        }

        appendSample(
                output,
                name + "_bucket",
                tags,
                "+Inf",
                Long.toString(buckets.getLast())
        );

        appendSample(
                output,
                name + "_count",
                tags,
                null,
                Long.toString(snapshot.count())
        );

        appendSample(
                output,
                name + "_sum",
                tags,
                null,
                decimalSeconds(snapshot.totalNanos())
        );
    }

    private static void appendSample(
            StringBuilder output,
            String name,
            Map<String, String> tags,
            String le,
            String value
    ) {
        output.append(name);

        if (!tags.isEmpty() || le != null) {
            output.append('{');

            boolean first = true;

            for (Map.Entry<String, String> tag : tags.entrySet()) {
                if (!first) {
                    output.append(',');
                }

                first = false;

                output.append(tag.getKey()).append("=\"");
                appendEscaped(output, tag.getValue());
                output.append('"');
            }

            if (le != null) {
                if (!first) {
                    output.append(',');
                }

                output.append("le=\"").append(le).append('"');
            }

            output.append('}');
        }

        output.append(' ').append(value).append('\n');
    }

    private static void appendEscaped(StringBuilder output, String value) {
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);

            switch (character) {
                case '\\' -> output.append("\\\\");
                case '"' -> output.append("\\\"");
                case '\n' -> output.append("\\n");
                default -> output.append(character);
            }
        }
    }

    private static String decimalSeconds(long nanos) {
        return BigDecimal.valueOf(nanos, 9)
                .stripTrailingZeros()
                .toPlainString();
    }

    private record MetricEntry(
            MetricId id,
            MetricType type,
            Object metric
    ) {
    }

    private final class Subscription implements Registration {

        private final RegistrationListener listener;
        private boolean closed;

        private Subscription(RegistrationListener listener) {
            this.listener = listener;
        }

        @Override
        public void close() {
            synchronized (registryLock) {
                if (!closed) {
                    closed = true;
                    subscriptions.remove(this);
                }
            }
        }
    }

    private static final class CounterMetric implements Counter {

        private final LongAdder value = new LongAdder();

        @Override
        public void increment() {
            value.increment();
        }

        @Override
        public long count() {
            return value.sum();
        }
    }

    private static final class GaugeMetric implements Gauge {

        private final AtomicLong value = new AtomicLong();

        @Override
        public void set(long newValue) {
            value.set(newValue);
        }

        @Override
        public long get() {
            return value.get();
        }
    }

    private static final class TimerMetric implements Timer {

        private final long[] bounds;
        private final AtomicReference<TimerValues> state;

        private TimerMetric(long[] bounds) {
            this.bounds = bounds.clone();

            state = new AtomicReference<>(
                    new TimerValues(
                            0,
                            0,
                            0,
                            new long[bounds.length + 1]
                    )
            );
        }

        @Override
        public void record(long nanos) {
            if (nanos < 0) {
                throw new IllegalArgumentException(
                        "Timer measurement must be non-negative"
                );
            }

            while (true) {
                TimerValues previous = state.get();
                long[] buckets = previous.buckets.clone();

                for (int i = 0; i < bounds.length; i++) {
                    if (nanos <= bounds[i]) {
                        buckets[i]++;
                    }
                }

                buckets[bounds.length]++;

                TimerValues next = new TimerValues(
                        previous.count + 1,
                        previous.sum + nanos,
                        Math.max(previous.max, nanos),
                        buckets
                );

                if (state.compareAndSet(previous, next)) {
                    return;
                }
            }
        }

        @Override
        public TimerSnapshot snapshot() {
            TimerValues current = state.get();

            List<Long> buckets = new ArrayList<>(
                    current.buckets.length
            );

            for (long count : current.buckets) {
                buckets.add(count);
            }

            return new TimerSnapshot(
                    current.count,
                    current.sum,
                    current.max,
                    buckets
            );
        }
    }

    private static final class TimerValues {

        private final long count;
        private final long sum;
        private final long max;
        private final long[] buckets;

        private TimerValues(
                long count,
                long sum,
                long max,
                long[] buckets
        ) {
            this.count = count;
            this.sum = sum;
            this.max = max;
            this.buckets = buckets;
        }
    }
}

