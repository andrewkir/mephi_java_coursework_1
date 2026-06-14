package ru.andrewkir;

import ru.andrewkir.executor.CustomThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Демонстрационная программа для {@link CustomThreadPool}.
 * Сценарии:
 *  1. Нормальная нагрузка — задачи выполняются core-потоками.
 *  2. Всплеск нагрузки — пул расширяется до maxPoolSize.
 *  3. Перегрузка — все очереди заполнены, срабатывает CallerRuns.
 *  4. Мягкое завершение — уже поставленные задачи выполняются, новые отклоняются.
 *  5. shutdownNow — возврат невыполненных задач.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        CustomThreadPool pool = CustomThreadPool.builder()
                .poolName("MyPool")
                .corePoolSize(2)
                .maxPoolSize(4)
                .keepAliveTime(5)
                .timeUnit(TimeUnit.SECONDS)
                .queueSize(5)
                .minSpareThreads(1)
                .build();

        // --- Сценарий 1: нормальная нагрузка ---
        System.out.println("\n=== Scenario 1: Normal load ===");

        List<Future<String>> futures = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final int id = i;
            futures.add(pool.submit(() -> {
                String threadName = Thread.currentThread().getName();
                System.out.printf("[Task-%d] START on %s%n", id, threadName);
                Thread.sleep(500);
                System.out.printf("[Task-%d] END on %s%n", id, threadName);
                return "result-" + id;
            }));
        }

        for (int i = 0; i < futures.size(); i++) {
            try {
                System.out.println("[Main] Got result: " + futures.get(i).get());
            } catch (Exception e) {
                System.out.println("[Main] Task " + (i + 1) + " failed: " + e.getMessage());
            }
        }

        // --- Сценарий 2: всплеск нагрузки ---
        System.out.println("\n=== Scenario 2: Burst load (extra workers expected) ===");

        for (int i = 1; i <= 8; i++) {
            pool.execute(namedTask("BurstTask-" + i, 300));
        }

        Thread.sleep(2000);

        // --- Сценарий 3: перегрузка → политика отказа ---
        System.out.println("\n=== Scenario 3: Overload → CallerRuns ===");

        // Вместимость пула: maxPoolSize(4) × queueSize(5) = 20 слотов.
        // 25 задач → задачи 21-25 гарантированно выполнятся в потоке main (CallerRuns).
        for (int i = 1; i <= 25; i++) {
            pool.execute(namedTask("HeavyTask-" + i, 800));
        }

        Thread.sleep(3000);

        // --- Сценарий 4: мягкое завершение ---
        System.out.println("\n=== Scenario 4: Graceful shutdown ===");

        pool.execute(namedTask("PreShutdown-1", 200));
        pool.execute(namedTask("PreShutdown-2", 200));

        pool.shutdown();

        System.out.println("[Main] Submitting task after shutdown (should be rejected):");
        pool.execute(namedTask("PostShutdown-1", 100));

        pool.awaitTermination(15, TimeUnit.SECONDS);
        System.out.println("\n[Main] All workers finished. Pool is stopped.");

        // --- Сценарий 5: shutdownNow с возвратом незавершённых задач ---
        System.out.println("\n=== Scenario 5: shutdownNow — returns pending tasks ===");

        CustomThreadPool pool2 = CustomThreadPool.builder()
                .poolName("MyPool2")
                .corePoolSize(1)
                .maxPoolSize(2)
                .keepAliveTime(5)
                .timeUnit(TimeUnit.SECONDS)
                .queueSize(10)
                .minSpareThreads(0)
                .build();

        for (int i = 1; i <= 8; i++) {
            pool2.execute(namedTask("QuickTask-" + i, 1000));
        }

        Thread.sleep(200); // дадим паре задач стартовать
        List<Runnable> notExecuted = pool2.shutdownNow();
        System.out.printf("[Main] shutdownNow returned %d pending tasks.%n", notExecuted.size());
    }

    /**
     * Создаёт {@link Runnable}, который «работает» заданное время и логирует старт/конец.
     */
    private static Runnable namedTask(String name, long millis) {
        return new Runnable() {
            @Override
            public void run() {
                String thread = Thread.currentThread().getName();
                System.out.printf("[%s] START on %s%n", name, thread);
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.printf("[%s] INTERRUPTED on %s%n", name, thread);
                    return;
                }
                System.out.printf("[%s] END on %s%n", name, thread);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
