package info.jerrinot.experiements.hrtests;

import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMonitor {
    private final AtomicLong opsCounter = new AtomicLong();
    private final TimeSeriesVisualizer visualizer;
    private long lastValue;

    private static final int UPDATE_INTERVAL_SECONDS = 1;

    public PerformanceMonitor(TimeSeriesVisualizer visualizer) {
        this.visualizer = visualizer;
    }

    public void stepFinished() {
        opsCounter.incrementAndGet();
    }

    public void start() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (;;) {
                    sleepMs(UPDATE_INTERVAL_SECONDS * 1000);
                    update();
                }

            }
        };
        thread.setDaemon(true);
        thread.start();
    }

    private void update() {
        long currentValue = opsCounter.get();
        long delta = currentValue - lastValue;
        lastValue = currentValue;

        double throughput = ((double)delta) / UPDATE_INTERVAL_SECONDS;
        visualizer.currentValue(throughput);
    }

    private static void sleepMs(int sleepingTimeMs) {
        try {
            Thread.sleep(sleepingTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
