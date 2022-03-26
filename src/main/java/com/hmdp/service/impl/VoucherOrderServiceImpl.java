package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.val;
import org.apache.tomcat.jni.Lock;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisWorker redisWorker;

    @Override
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
    }

    /*@Resource
    private StringRedisTemplate stringRedisTemplate;*/
    @Resource
    private RedissonClient redissonClient;

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
    }*/


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
        }
    }
}

