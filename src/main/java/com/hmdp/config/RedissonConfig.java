package com.hmdp.config;

import io.lettuce.core.RedisClient;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author hy
 * @version 1.0
 * @Desctiption
 * @date 2022/3/25 16:39
 */

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redisClient(){
        //配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.127.128:6379").setPassword("1111");
        return Redisson.create(config);
    }

}
