package com.hmdp.utils;

/**
 * @author hy
 * @version 1.0
 * @Desctiption
 * @date 2022/3/25 14:00
 */
public interface ILock {
    boolean trylock(long timeoutSec);
    void unlock();
}
