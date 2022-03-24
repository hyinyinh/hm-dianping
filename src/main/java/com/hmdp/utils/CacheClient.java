package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author hy
 * @version 1.0
 * @Desctiption
 * @date 2022/3/23 23:49
 */

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogin(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id,
            Class<R> type,
            Function<ID,R> dbFallback,
            Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.存在 直接返回
            return JSONUtil.toBean(json,type);
        }

        if(json != null){
            return null;
        }

        //4.不存在 根据id查询数据库
        R r = dbFallback.apply(id);
        //5.不存在，返回错误
        if(r == null){
            //写入空值到redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在 写入redis
        this.set(key,r,time,unit);
        return r;
    }

    //加锁
    private boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //解锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //逻辑过期解决缓存击穿
    public <R,ID> R queryWithLogincalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit timeUnit){
        String key =keyPrefix + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //3.不存在 直接返回
            return null;
        }

        //4.命中 需要把接送反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期 直接返回店铺信息
            return r;
        }

        //5.2已过期 获取缓存重建
        //6.缓存重建
        //6.1 获取互斥锁
        String lockkey = LOCK_SHOP_KEY + id;
        boolean isLock = trylock(lockkey);
        //6.2 判断是否获取锁成功
        if (isLock) {
            //6.3 成功 开启线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                //重建缓存
                try {
                    //先查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogin(key,r1,time,timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockkey);
                }
            });
        }
        //6.4 返回过期商铺信息
        return r;
    }

}
