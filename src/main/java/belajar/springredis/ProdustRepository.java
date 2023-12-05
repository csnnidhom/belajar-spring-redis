package belajar.springredis;

import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProdustRepository extends KeyValueRepository<Product, String> {
}
