package ru.andrewkir.executor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Рабочий поток пула. Непрерывно опрашивает закреплённую за ним очередь задач.
 *
 * <p>Логика работы:
 * <ol>
 *   <li>Ожидать задачу не дольше {@code keepAliveTime}.</li>
 *   <li>Получив задачу — проверить флаг немедленного завершения и выполнить её.</li>
 *   <li>Если задача не поступила за {@code keepAliveTime}: завершиться, если
 *       текущее число потоков больше {@code corePoolSize} и свободных потоков
 *       достаточно (>= minSpareThreads), чтобы без нас хватало резерва.</li>
 *   <li>При мягком завершении — дочитать оставшиеся задачи и выйти.</li>
 * </ol>
 */
public class Worker implements Runnable {

    private final BlockingQueue<Runnable> queue;
    private final CustomThreadPool pool;
    private final long keepAliveTimeMillis;

    public Worker(BlockingQueue<Runnable> queue, CustomThreadPool pool, long keepAliveTime, TimeUnit timeUnit) {
        this.queue = queue;
        this.pool = pool;
        this.keepAliveTimeMillis = timeUnit.toMillis(keepAliveTime);
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        try {
            while (true) {
                // shutdownNow — немедленный выход, очередь не дочитываем
                if (pool.isTerminated()) {
                    break;
                }

                // Мягкое завершение — выходим только когда очередь опустела
                if (pool.isShutdown() && queue.isEmpty()) {
                    break;
                }

                // Переходим в idle — сообщаем пулу
                pool.onWorkerIdle();
                Runnable task = null;
                boolean interrupted = false;
                try {
                    task = queue.poll(keepAliveTimeMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                } finally {
                    // В любом случае больше не простаиваем
                    pool.onWorkerActive();
                }

                if (interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (task != null) {
                    // Не запускаем задачи после shutdownNow
                    if (pool.isTerminated()) {
                        break;
                    }
                    System.out.printf("[Worker] %s executes '%s'%n", name, task);
                    try {
                        task.run();
                    } catch (Throwable t) {
                        System.out.printf("[Worker] %s caught exception: %s%n", name, t.getMessage());
                    }
                } else {
                    if (pool.canWorkerExit()) {
                        System.out.printf("[Worker] %s idle timeout, stopping.%n", name);
                        break;
                    }
                }
            }
        } finally {
            System.out.printf("[Worker] %s terminated.%n", name);
            pool.onWorkerExit(this);
        }
    }

    /** Возвращает очередь задач этого воркера. */
    public BlockingQueue<Runnable> getQueue() {
        return queue;
    }
}
