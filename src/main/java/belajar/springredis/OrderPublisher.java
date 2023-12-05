package belajar.springredis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class OrderPublisher {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.SECONDS)
    public void publish(){
        Order order =new Order(UUID.randomUUID().toString(), 1000L);

        Record<String, Order> record = ObjectRecord.create("orders", order);
        stringRedisTemplate.opsForStream().add(record);
    }
}
