package ru.andrewkir.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Фабрика потоков с уникальными именами и логированием создания.
 * Имена потоков имеют вид {@code <poolName>-worker-<N>}.
 * Завершение потока логирует сам {@link Worker} в блоке finally.
 */
public class CustomThreadFactory implements ThreadFactory {

    private final String poolName;
    private final AtomicInteger threadCounter = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = poolName + "-worker-" + threadCounter.getAndIncrement();
        System.out.printf("[ThreadFactory] Creating new thread: %s%n", name);
        Thread thread = new Thread(r, name);
        thread.setDaemon(false);
        return thread;
    }
}
