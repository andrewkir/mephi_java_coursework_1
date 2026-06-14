package ru.andrewkir.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Пул потоков с индивидуальными очередями для каждого воркера и балансировкой Round Robin.
 *
 * <p><b>Архитектура:</b> каждый {@link Worker} владеет одной {@link LinkedBlockingQueue}.
 * При постановке задачи пул перебирает очереди по кругу (Round Robin) и кладёт задачу
 * в первую незаполненную. Если все очереди заполнены — пробует создать дополнительный
 * поток (до {@code maxPoolSize}), иначе срабатывает политика отказа CallerRuns.
 *
 * <p><b>minSpareThreads:</b> перед каждой постановкой задачи пул проверяет количество
 * свободных потоков и при необходимости доводит его до {@code minSpareThreads}.
 */
public class CustomThreadPool implements CustomExecutor {

    // Параметры конфигурации
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    private final CustomThreadFactory threadFactory;
    private final RejectedTaskHandler rejectionHandler;

    /**
     * Списки воркеров и их потоков синхронизированы по индексу.
     * Доступ защищён {@code lock}.
     */
    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> workerThreads = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();

    /** Индекс Round Robin: следующая очередь, которую попробуем первой. */
    private final AtomicInteger rrIndex = new AtomicInteger(0);

    /** Количество воркеров, находящихся в ожидании задачи. */
    private final AtomicInteger idleCount = new AtomicInteger(0);

    /** Общее число запущенных воркеров (включая занятых). */
    private final AtomicInteger totalThreads = new AtomicInteger(0);

    /** true после вызова {@link #shutdown()} — новые задачи не принимаются. */
    private volatile boolean shutdown = false;

    /** true после вызова {@link #shutdownNow()} — воркеры прерываются немедленно. */
    private volatile boolean terminated = false;

    private CustomThreadPool(Builder builder) {
        // Валидация параметров
        if (builder.corePoolSize < 0 || builder.maxPoolSize <= 0
                || builder.maxPoolSize < builder.corePoolSize
                || builder.queueSize <= 0 || builder.minSpareThreads < 0) {
            throw new IllegalArgumentException("Некорректные параметры пула");
        }

        this.corePoolSize    = builder.corePoolSize;
        this.maxPoolSize     = builder.maxPoolSize;
        this.keepAliveTime   = builder.keepAliveTime;
        this.timeUnit        = builder.timeUnit;
        this.queueSize       = builder.queueSize;
        this.minSpareThreads = builder.minSpareThreads;
        this.threadFactory   = new CustomThreadFactory(builder.poolName);
        this.rejectionHandler = new RejectedTaskHandler();

        System.out.printf("[Pool] Initializing '%s': core=%d, max=%d, " +
                        "keepAlive=%d %s, queueSize=%d, minSpare=%d%n",
                builder.poolName, corePoolSize, maxPoolSize,
                keepAliveTime, timeUnit, queueSize, minSpareThreads);

        for (int i = 0; i < corePoolSize; i++) {
            spawnWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("command == null");
        execute(command, command.toString());
    }

    private void execute(Runnable command, String description) {
        if (shutdown || terminated) {
            rejectionHandler.reject(command, description);
            return;
        }

        ensureMinSpareThreads();

        if (tryEnqueue(command, description)) {
            return;
        }

        // Все очереди заполнены — пробуем создать дополнительный поток
        lock.lock();
        try {
            if (totalThreads.get() < maxPoolSize) {
                Worker newWorker = spawnWorker();
                if (newWorker.getQueue().offer(command)) {
                    System.out.printf("[Pool] Task accepted into new worker's queue: '%s'%n", description);
                    return;
                }
            }
        } finally {
            lock.unlock();
        }

        // Мест нет — отказ
        rejectionHandler.reject(command, description);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("callable == null");
        FutureTask<T> future = new FutureTask<>(callable) {
            @Override
            public String toString() {
                return "Callable@" + Integer.toHexString(callable.hashCode());
            }
        };
        execute(future, future.toString());
        return future;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        System.out.println("[Pool] Shutdown initiated — no new tasks will be accepted.");
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        terminated = true;
        System.out.println("[Pool] ShutdownNow — interrupting all workers.");

        List<Runnable> pending = new ArrayList<>();
        lock.lock();
        try {
            // Собираем все задачи из очередей и очищаем их
            for (Worker w : workers) {
                w.getQueue().drainTo(pending);
                w.getQueue().clear();
            }
            // Прерываем все потоки
            for (Thread t : workerThreads) {
                t.interrupt();
            }
        } finally {
            lock.unlock();
        }
        return pending;
    }

    /**
     * Пробует поставить задачу в одну из очередей по алгоритму Round Robin.
     *
     * @return {@code true}, если задача успешно поставлена в очередь
     */
    private boolean tryEnqueue(Runnable command, String description) {
        lock.lock();
        try {
            int n = workers.size();
            if (n == 0) return false;

            int start = rrIndex.get() % n;
            for (int i = 0; i < n; i++) {
                int idx = (start + i) % n;
                BlockingQueue<Runnable> q = workers.get(idx).getQueue();
                if (q.offer(command)) {
                    // Сдвигаем индекс, чтобы следующая задача попала в другую очередь
                    rrIndex.set((idx + 1) % n);
                    System.out.printf("[Pool] Task accepted into queue #%d: '%s'%n", idx, description);
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Создаёт нового воркера и запускает его поток.
     * Вызывается под {@code lock} (либо из конструктора).
     */
    private Worker spawnWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        Worker worker = new Worker(queue, this, keepAliveTime, timeUnit);
        Thread thread = threadFactory.newThread(worker);
        workers.add(worker);
        workerThreads.add(thread);
        totalThreads.incrementAndGet();
        thread.start();
        return worker;
    }

    /**
     * Проверяет, достаточно ли свободных потоков. Если нет — создаёт новые
     * (до {@code maxPoolSize}).
     */
    private void ensureMinSpareThreads() {
        if (idleCount.get() >= minSpareThreads) return;

        lock.lock();
        try {
            while (idleCount.get() < minSpareThreads && totalThreads.get() < maxPoolSize) {
                System.out.printf("[Pool] Spare threads below minimum (%d < %d) — spawning extra worker.%n",
                        idleCount.get(), minSpareThreads);
                spawnWorker();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Вызывается воркером перед тем, как он уходит в ожидание задачи. */
    public void onWorkerIdle() {
        idleCount.incrementAndGet();
    }

    /** Вызывается воркером, как только он получил задачу (или был прерван). */
    public void onWorkerActive() {
        idleCount.decrementAndGet();
    }

    /**
     * Вызывается при завершении {@link Worker#run()}.
     * Удаляет воркера из списка активных.
     */
    public void onWorkerExit(Worker worker) {
        lock.lock();
        try {
            int idx = workers.indexOf(worker);
            if (idx >= 0) {
                workers.remove(idx);
                workerThreads.remove(idx);
            }
            totalThreads.decrementAndGet();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Возвращает {@code true}, если воркер может завершиться по idle timeout.
     * Условие: потоков больше чем corePoolSize И свободных потоков (без учёта
     * данного воркера, уже вышедшего из idle) достаточно >= minSpareThreads.
     */
    public boolean canWorkerExit() {
        return totalThreads.get() > corePoolSize && idleCount.get() >= minSpareThreads;
    }

    public boolean isShutdown()   { return shutdown; }
    public boolean isTerminated() { return terminated; }

    /**
     * Ожидает завершения всех рабочих потоков.
     * Вызывать после {@link #shutdown()} или {@link #shutdownNow()}.
     */
    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        List<Thread> snapshot;
        lock.lock();
        try {
            snapshot = new ArrayList<>(workerThreads);
        } finally {
            lock.unlock();
        }
        for (Thread t : snapshot) {
            t.join(unit.toMillis(timeout));
        }
    }

    // ---- Builder -------------------------------------------------------------

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private int      corePoolSize    = 2;
        private int      maxPoolSize     = 4;
        private long     keepAliveTime   = 60;
        private TimeUnit timeUnit        = TimeUnit.SECONDS;
        private int      queueSize       = 10;
        private int      minSpareThreads = 1;
        private String   poolName        = "MyPool";

        public Builder corePoolSize(int v)    { corePoolSize = v;    return this; }
        public Builder maxPoolSize(int v)     { maxPoolSize = v;     return this; }
        public Builder keepAliveTime(long v)  { keepAliveTime = v;   return this; }
        public Builder timeUnit(TimeUnit v)   { timeUnit = v;        return this; }
        public Builder queueSize(int v)       { queueSize = v;       return this; }
        public Builder minSpareThreads(int v) { minSpareThreads = v; return this; }
        public Builder poolName(String v)     { poolName = v;        return this; }

        public CustomThreadPool build() { return new CustomThreadPool(this); }
    }
}
