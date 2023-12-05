package belajar.springredis;

import io.lettuce.core.RedisException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.data.redis.support.collections.RedisZSet;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ProdustRepository produstRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ProductService productService;

    @Test
    void redisTemplate() {
        assertNotNull(stringRedisTemplate);
    }

    @Test
    void string() throws InterruptedException {
        ValueOperations<String, String> operations = stringRedisTemplate.opsForValue();
        operations.set("name", "Nidhom", Duration.ofSeconds(2));
        assertEquals("Nidhom", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        assertNull(operations.get("name"));
    }

    @Test
    void list() {
        ListOperations<String, String> opsForList = stringRedisTemplate.opsForList();

        opsForList.rightPush("names", "Chusnun");
        opsForList.rightPush("names", "Nidhom");

        assertEquals("Chusnun", opsForList.leftPop("names"));
        assertEquals("Nidhom", opsForList.leftPop("names"));

    }

    @Test
    void set() {
        SetOperations<String, String> opsForSet = stringRedisTemplate.opsForSet();

        opsForSet.add("students", "Chusnun");
        opsForSet.add("students", "Chusnun");
        opsForSet.add("students", "Nidhom");
        opsForSet.add("students", "Nidhom");

        Set<String> members = opsForSet.members("students");
        assertEquals(3, members.size());
        assertThat(members, hasItems("Chusnun", "Nidhom"));
    }

    @Test
    void zSet() {
        ZSetOperations<String, String> opsForZSet = stringRedisTemplate.opsForZSet();

        opsForZSet.add("score", "Chusnun", 100);
        opsForZSet.add("score", "Nidhom", 85);

        assertEquals("Chusnun", opsForZSet.popMax("score").getValue());
        assertEquals("Nidhom", opsForZSet.popMax("score").getValue());
    }

    @Test
    void hash() {
        HashOperations<String, Object, Object> opsForHash = stringRedisTemplate.opsForHash();
//        opsForHash.put("user:1", "id", "1");
//        opsForHash.put("user:1", "name", "Nidhom");
//        opsForHash.put("user:1", "email", "nidhom@example.com");

        Map<Object, Object> map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "Nidhom");
        map.put("email", "nidhom@example.com");
        opsForHash.putAll("user:1", map);

        assertEquals("1", opsForHash.get("user:1", "id"));
        assertEquals("Nidhom", opsForHash.get("user:1", "name"));
        assertEquals("nidhom@example.com", opsForHash.get("user:1", "email"));

        stringRedisTemplate.delete("user:1");
    }

    @Test
    void geo() {
        GeoOperations<String, String> opsForGeo = stringRedisTemplate.opsForGeo();
        opsForGeo.add("sellers", new Point(112.663420,-7.256266), "Toko A");
        opsForGeo.add("sellers", new Point(112.655524, -7.264014), "Toko B");

        Distance distance = opsForGeo.distance("sellers", "Toko A", "Toko B");
        assertEquals(1225.3074, distance.getValue());

        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers = opsForGeo.search("sellers", new Circle(
                new Point(112.663678, -7.261034),
                new Distance(5, Metrics.KILOMETERS)
        ));

        assertEquals(2, sellers.getContent().size());
        assertEquals("Toko B", sellers.getContent().get(0).getContent().getName());
        assertEquals("Toko A", sellers.getContent().get(1).getContent().getName());

    }

    @Test
    void hyperLogLog() {
        HyperLogLogOperations<String, String> operations = stringRedisTemplate.opsForHyperLogLog();

        operations.add("traffics", "chusnun", "nidhom", "yudi");
        operations.add("traffics", "budi", "anto", "jordan");
        operations.add("traffics", "yanto", "nidhom", "yudi");

        assertEquals(7, operations.size("traffics"));
    }

    @Test
    void transasction() {
        stringRedisTemplate.execute(new SessionCallback<Object>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("test1", "Chusnun", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Nidhom", Duration.ofSeconds(2));
                operations.exec();
                return null;
            }
        });

        assertEquals("Chusnun", stringRedisTemplate.opsForValue().get("test1"));
        assertEquals("Nidhom", stringRedisTemplate.opsForValue().get("test2"));
    }

    @Test
    void pipeLine() {
        List<Object> statuses = stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Nidhom", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Nidhom", Duration.ofSeconds(2));
                operations.opsForValue().set("test3", "Nidhom", Duration.ofSeconds(2));
                return null;
            }
        });

        assertThat(statuses, hasSize(3));
        assertThat(statuses, hasItem(true));
        assertThat(statuses, not(hasItem(false)));
    }

    @Test
    void publishStream() {
        StreamOperations<String, Object, Object> operations = stringRedisTemplate.opsForStream();

        MapRecord<String, Object, Object> stream = MapRecord.create("stream", Map.of(
                "name", "Chusnun Nidhom",
                "address", "Surabaya"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(stream);
        }
    }

    @Test
    void subscribeStream() {
        StreamOperations<String, Object, Object> operations = stringRedisTemplate.opsForStream();

        try {
            operations.createGroup("stream", "sample-group");
        }catch (RedisException redisException){
            // group already exist
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records){
            System.out.println(record);
        }
    }

    @Test
    void pubSub() {
        stringRedisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());
                System.out.println("Receive message : " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            stringRedisTemplate.convertAndSend("my-channel", "Hello World : " + i);
        }
    }

    @Test
    void redisList() {
        List<String> list = RedisList.create("names", stringRedisTemplate);
        list.add("Chusnun");
        list.add("Nidhom");
        assertThat(list, hasItems("Chusnun", "Nidhom"));

        List<String> result = stringRedisTemplate.opsForList().range("names",0, -1);
        assertThat(result, hasItems("Chusnun", "Nidhom"));
    }

    @Test
    void redisSet() {
        RedisSet<String> redisSet = RedisSet.create("traffic", stringRedisTemplate);
        redisSet.addAll(Set.of("chusnun", "nidhom"));
        redisSet.addAll(Set.of("joko", "budi"));
        redisSet.addAll(Set.of("eko", "budi"));
        assertThat(redisSet, hasItems("chusnun", "nidhom", "joko", "budi", "eko"));

        Set<String> members = stringRedisTemplate.opsForSet().members("traffic");
        assertThat(members, hasItems("chusnun", "nidhom", "joko", "budi", "eko"));
    }

    @Test
    void redisZSet() {
        RedisZSet<String> set = RedisZSet.create("winner", stringRedisTemplate);
        set.add("Chusnun", 100);
        set.add("Nidhom", 90);
        set.add("Budi", 50);
        assertThat(set, hasItems("Chusnun", "Nidhom", "Budi"));

        Set<String> wiinner = stringRedisTemplate.opsForZSet().range("winner", 0, -1);
        assertThat(wiinner, hasItems("Chusnun", "Nidhom", "Budi"));

        assertEquals("Chusnun", set.popLast());
        assertEquals("Nidhom", set.popLast());
        assertEquals("Budi", set.popLast());
    }

    @Test
    void redisMap() {
        Map<String, String> map = new DefaultRedisMap<>("user:1", stringRedisTemplate);
        map.put("name", "Nidhom");
        map.put("addres", "Surabaya");
        assertThat(map, hasEntry("name", "Nidhom"));
        assertThat(map, hasEntry("addres", "Surabaya"));

        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries("user:1");
        assertThat(map, hasEntry("name", "Nidhom"));
        assertThat(map, hasEntry("addres", "Surabaya"));
    }

    @Test
    void repository() {
        Product product = Product.builder()
                .id("1")
                .name("Tahu Isi")
                .price(20_000L)
                .build();

        produstRepository.save(product);

        Product product2 = produstRepository.findById("1").get();
        assertEquals(product, product2);

        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries("products:1");
        assertEquals(product.getId(), entries.get("id"));
        assertEquals(product.getName(), entries.get("name"));
        assertEquals(product.getPrice().toString(), entries.get("price"));

    }

    @Test
    void ttl() throws InterruptedException {
        Product product = Product.builder()
                .id("1")
                .name("Tahu Isi")
                .price(20_000L)
                .ttl(3L)
                .build();

        produstRepository.save(product);

        assertTrue(produstRepository.findById("1").isPresent());

        Thread.sleep(Duration.ofSeconds(5));

        assertFalse(produstRepository.findById("1").isPresent());
    }

    @Test
    void cache() {
        Cache cache = cacheManager.getCache("scores");
        cache.put("Chusnun", 100);
        cache.put("Nidhom", 90);

        assertEquals(100, cache.get("Chusnun", Integer.class));
        assertEquals(90, cache.get("Nidhom", Integer.class));

        cache.evict("Chusnun");
        cache.evict("Nidhom");

        assertNull(cache.get("Chusnun"));
        assertNull(cache.get("Nidhom"));

    }

    @Test
    void cacheable() {
        Product product = productService.getProduct("001");
        assertEquals("001",product.getId());

        Product product2 = productService.getProduct("001");
        assertEquals(product,product2);

        Product product3 = productService.getProduct("002");
        assertEquals(product,product2);
    }

    @Test
    void cachePut() {
        Product product = Product.builder().id("P003").name("asal").price(100L).build();
        productService.save(product);

        Product product2 = productService.getProduct("P003");
        assertEquals(product, product2);
    }

    @Test
    void cacheEvict() {
        Product product = productService.getProduct("004");
        assertEquals("004",product.getId());

        productService.remove("004");

        Product product2 = productService.getProduct("004");
        assertEquals(product,product2);
    }
}
