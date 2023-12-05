package belajar.springredis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProductService {

    @Cacheable(value = "products", key = "#id")
    public Product getProduct(String id){
        log.info("Get Product : {} ", id);
        return Product.builder().id(id).name("example").price(1000L).build();
    }

    @CachePut(value = "products", key = "#product.id")
    public Product save(Product product){
        log.info("Save product {}", product);
        return product;
    }

    @CacheEvict(value = "products", key = "#id")
    public void remove(String id){
        log.info("Remove product {}", id);
    }


}
