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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
            return IntStream.range(1, 100).boxed()
                    .map(String::valueOf)
                    .collect(Collectors.toSet());
        }

        private String valueOf(String key) {
            return "Value For " + key;
        }
    }

    @Before
    public void setup() {
        Config config = new Config();

        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setClassName(SlowMapLoader.class.getName());
        storeConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("TestMap");
        mapConfig.setMapStoreConfig(storeConfig);
        config.addMapConfig(mapConfig);

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void shouldAnswerWithTrue() throws Exception {
        IMap<String, String> testMap = hazelcastInstance.getMap("TestMap");
        assertEquals("Value For 1", testMap.get("1"));
    }
}
