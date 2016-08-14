package info.jerrinot.experiements.hrtests;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.File;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheHRTest {

    @Rule
    public TestName testName = new TestName();

    private File folder;
    private String cacheName;

    @Before
    public final void setup() throws UnknownHostException {
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
        cacheName = "test-cache";
    }

    @After
    public final void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();

        if (folder != null) {
            delete(folder);
        }
    }

    @Test(timeout = Long.MAX_VALUE)
    public void test() throws InterruptedException {
        final Config config = makeConfig();
        int clusterSize = 3;
        int clientCount = 5;
        int workerThreadsPerClient = 5;
        int iterationCount = 20;
        int iterationDurationSeconds = 30;
        int keySpace = 1000000;


        HazelcastInstance instances[] = startCluster(config, clusterSize);

        PerformanceMonitor performanceMonitor = createPerformanceMonitor();
        for (int i = 0; i < clientCount; i++) {
            startClient(performanceMonitor, workerThreadsPerClient, keySpace);
        }

        for (int i = 0; i < iterationCount; i++) {
            System.out.println("Starting an iteration no. " + i);
            Thread.sleep(SECONDS.toMillis(iterationDurationSeconds));
            System.out.println("About to terminate the cluster");
            stopCluster(instances);
            System.out.println("The cluster is terminated. Starting a new one.");
            instances = startCluster(config, clusterSize);
            System.out.println("Cluster is running");
        }

    }

    private PerformanceMonitor createPerformanceMonitor() {
        TimeSeriesChart chart = new TimeSeriesChart("Throughput");
        PerformanceMonitor performanceMonitor = new PerformanceMonitor(chart);
        performanceMonitor.start();
        return performanceMonitor;
    }

    private void stopCluster(HazelcastInstance[] instances) {
        HazelcastInstance instance = instances[0];
        instance.getCluster().shutdown();
    }

    private void startClient(final PerformanceMonitor performanceMonitor, int workerCount, final int keySpace) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        CacheManager cacheManager = HazelcastClientCachingProvider.createCachingProvider(client).getCacheManager();
        CacheConfig cacheConfig = new CacheConfig(cacheName);
        cacheConfig.getHotRestartConfig().setEnabled(true);
        final Cache cache = cacheManager.createCache(cacheName, cacheConfig);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                System.out.println("Client state changed. Current state: " + event.getState());
            }
        });

        for (int i = 0; i < workerCount; i++) {
            Thread thread = new Thread() {
                public void run() {
                    int key = 0;
                    while (true) {
                        try {
                            cache.put(key, key);
                            performanceMonitor.stepFinished();
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        key = ++key % keySpace;
                    }
                }
            };
            thread.start();
        }
    }

    private HazelcastInstance[] startCluster(final Config config, int clusterSize) throws InterruptedException {
        HazelcastInstance[] instances = createNewInstances(config, clusterSize);
        HazelcastInstance instance = instances[0];
        moveToACTIVEifNeeded(instance);
        return instances;
    }

    private HazelcastInstance[] toArray(AtomicReferenceArray<HazelcastInstance> instances) {
        int size = instances.length();
        HazelcastInstance[] hz = new HazelcastInstance[size];
        for (int i = 0; i < size; i++) {
            hz[i] = instances.get(i);
        }
        return hz;
    }

    private HazelcastInstance[] createNewInstances(final Config config, int clusterSize) throws InterruptedException {
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(clusterSize);
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            final int index = i;
            new Thread() {
                @Override
                public void run() {
                    HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
                    instances.set(index, instance);
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
        return toArray(instances);
    }

    private void moveToACTIVEifNeeded(HazelcastInstance instance) {
        Cluster cluster = instance.getCluster();
        ClusterState clusterState = cluster.getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            System.out.println("Cluster state is " + clusterState + ", moving to ACTIVE");
            cluster.changeClusterState(ClusterState.ACTIVE);
            System.out.println("Done. Cluster is now in ACTIVE mode");
        } else {
            System.out.println("Cluster is already in ACTIVE mode");
        }
    }

    private Config makeConfig() {
        Config config = new Config();
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(folder);
        return config;
    }
}