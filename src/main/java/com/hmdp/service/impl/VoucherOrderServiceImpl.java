package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisWorker redisWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }

    private BlockingQueue<VoucherOrder> orderTacks = new ArrayBlockingQueue<>(1024*1024);
    //线程池(单个线程即可)
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    //内部类 要确保在用户抢购之前就完成 所以在该类初始化时就实现
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run(){
            String queueName = "stream.orders";
            while(true){
                //1.获取消息队列中的订单信息 XREADGROUTP GROUP g1 c1 COUNT 1BLOCK 2000 STREAMS s1 >
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断订单信息是否为空
                    if(list == null || list.isEmpty()){
                        //如果为null 说明没有消息 继续下一层循环
                        continue;
                    }
                    //解析信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    //3.创建订单
                    createVoucherOrder(voucherOrder);
                    //4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        String queueName = "stream.orders";
        while(true){
            //1.获取消息队列中的订单信息 XREADGROUTP GROUP g1 c1 COUNT 1BLOCK 2000 STREAMS s1 >
            try {
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                //2.判断订单信息是否为空
                if(list == null || list.isEmpty()){
                    //如果为null 说明没有消息 继续下一层循环
                    break;
                }
                //解析信息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                //3.创建订单
                createVoucherOrder(voucherOrder);
                //4.确认消息 XACK
                stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
            } catch (Exception e) {
                log.error("处理订单异常",e);
            }
        }
    }


   /* private BlockingQueue<VoucherOrder> orderTacks = new ArrayBlockingQueue<>(1024*1024);
    //线程池(单个线程即可)
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    //内部类 要确保在用户抢购之前就完成 所以在该类初始化时就实现
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run(){
            while(true){
                //1.获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTacks.take();
                    //2.创建订单
                    createVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //6.一人一单
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //非阻塞式 默认值等待时间-1 不等待 超时时间30s 后释放
        boolean islock = lock.tryLock();
        if (!islock) {
            return;
        }
        try {
            //6.1 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            //6.2 判断是否存在
            if (count > 0) {
                return;
            }
            save(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /*@Override
    //通过往阻塞队列里面添加订单
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        //2.2.1 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        //2.2.2 订单id
        long orderId = redisWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.2.3 用户id
        voucherOrder.setUserId(userId);
        //2.2.4 代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.2.6 放入阻塞队列
        orderTacks.add(voucherOrder);
        // 3.返回订单id
        return Result.ok(orderId);
    }*/

   /* @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始！");
        }
        //3.判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        //4.判断库存是否充足
        if (voucher.getStock()<1) {
            return Result.fail("库存不足");
        }
        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock",0)
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }
        return createVoucherOrder(voucherId);
    }*/


    //lua脚本
    /*@Transactional
    public Result createVoucherOrder(Long voucherId){
        //6.一人一单
        Long userId = UserHolder.getUser().getId();
        SimpleRedisLock redisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        boolean trylock = redisLock.trylock(1200);
        if(!trylock){
            return Result.fail("请不要重复提交订单");
        }
        try {
            //synchronized(userId.toString().intern()){
            //intern从常量池中找一样的字符串 确保每次的id都是同一个
            //6.1 查询订单
            int count = query().eq("user_id",userId).eq("voucher_id",voucherId).count();
            //6.2 判断是否存在
            if(count>0){
                return Result.fail("该商品限购一份");
            }
            //7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setVoucherId(voucherId);
            //7.1订单id
            long orderId = redisWorker.nextId("order");
            voucherOrder.setId(orderId);
            //7.2用户id
            voucherOrder.setUserId(userId);
            //7.3保存用户id
            save(voucherOrder);
            //8.返回订单id
            return Result.ok(orderId);
            //}
        } finally {
            redisLock.unlock();
            return Result.fail("该用户已下单，不可重复下单");
        }
    }


    //可重入锁 redisson
    @Transactional
    public Result createVoucherOrder(Long voucherId){
        //6.一人一单
        Long userId = UserHolder.getUser().getId();
        RLock lock = redissonClient.getLock("lock:order:"+userId);
        //非阻塞式 默认值等待时间-1 不等待 超时时间30s 后释放
        boolean islock = lock.tryLock();
        if(!islock){
            return Result.fail("请不要重复提交订单");
        }
        try {
            //synchronized(userId.toString().intern()){
            //intern从常量池中找一样的字符串 确保每次的id都是同一个
            //6.1 查询订单
            int count = query().eq("user_id",userId).eq("voucher_id",voucherId).count();
            //6.2 判断是否存在
            if(count>0){
                return Result.fail("该商品限购一份");
            }
            //7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setVoucherId(voucherId);
            //7.1订单id
            long orderId = redisWorker.nextId("order");
            voucherOrder.setId(orderId);
            //7.2用户id
            voucherOrder.setUserId(userId);
            //7.3保存用户id
            save(voucherOrder);
            //8.返回订单id
            return Result.ok(orderId);
            //}
        } finally {
            lock.unlock();
        }*/
}

