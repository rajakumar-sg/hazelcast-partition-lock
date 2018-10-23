package com.haztest;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AppTest {

    HazelcastInstance hazelcastInstance;

    public static class SlowMapLoader implements MapLoader<String, String> {
        @Override
        public String load(String key) {
            System.out.println("Loading key " + key);
            return valueOf(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            System.out.println("Loading keys " + keys);
            return keys.stream()
                    .collect(HashMap::new, (map, key) -> map.put(key, valueOf(key)), HashMap::putAll);
        }

        @Override
        public Iterable<String> loadAllKeys() {
            System.out.println("loadAllKeys() invoked");
            return Collections.emptySet();
        }

        private String valueOf(String key) {
            slowBy(10);
            return "Value For " + key;
        }

        private void slowBy(int secs) {
            try {
                Thread.sleep(secs * 1000L);
            } catch (InterruptedException e) {
                //ignore
            }
        }
    }

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty("hazelcast.map.partition.count", "1");

        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setClassName(SlowMapLoader.class.getName());
        storeConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);

        MapConfig slowMapConfig = new MapConfig();
        slowMapConfig.setName("SlowMap");
        slowMapConfig.setMapStoreConfig(storeConfig);
        config.addMapConfig(slowMapConfig);

        MapConfig normalMapConfig = new MapConfig();
        normalMapConfig.setName("NormalMap");
        config.addMapConfig(normalMapConfig);

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        IMap<String, String> normalMap = hazelcastInstance.getMap("NormalMap");
        IntStream.range(1, 10).boxed()
                .forEach(k -> normalMap.put(String.valueOf(k), "Normal Value " + k));
    }

    @Test
    public void shouldAnswerWithTrue() throws Exception {
        IMap<String, String> slowMap = hazelcastInstance.getMap("SlowMap");
        IMap<String, String> normalMap = hazelcastInstance.getMap("NormalMap");

        CompletableFuture<String> slowValue = CompletableFuture.supplyAsync(() -> slowMap.get("1"));

        System.out.println(now() + " Invoking Normal Map ");
        System.out.println("Value from normal map :" + normalMap.get("1"));
        System.out.println(now() + " Invoking Normal Map completed.");

        assertEquals("Value For 1", slowValue.get());
    }

    private String now() {
        return new SimpleDateFormat("hh:mm:ss").format(new Date());
    }
}
