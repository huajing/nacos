package com.alibaba.nacos.example;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * @Description TODO
 * @Author chenck
 * @Date 2022/5/3 19:15
 * @Version 1.0
 **/

public class MyTest {
    public static void main(String[] args) throws InterruptedException {
        final LinkedBlockingDeque<Integer> deque = new LinkedBlockingDeque<>(1);

        new Thread("MyTestThread"){
            @Override
            public void run() {
                for (;;){
                    try {
                        Integer take = deque.take();
                        System.out.println("take " + take);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        Thread.sleep(3000);
        deque.add(1);

        Thread.sleep(3000);
        deque.add(1);

        Thread.sleep(3000);
        deque.add(1);
    }
}
