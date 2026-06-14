package ru.andrewkir.executor;

/**
 * Политика отказа, применяемая когда все очереди заполнены и пул достиг {@code maxPoolSize}.
 *
 * <p><b>Выбранная политика — CallerRuns:</b> задача выполняется непосредственно
 * в потоке вызывающего, а не отбрасывается.
 */
public class RejectedTaskHandler {

    /**
     * Обрабатывает задачу, которую не удалось поставить в очередь.
     *
     * @param task        отклонённая задача
     * @param description описание задачи для лога
     */
    public void reject(Runnable task, String description) {
        String callerName = Thread.currentThread().getName();
        System.out.printf("[Rejected] Task '%s' was rejected due to overload! " +
                "Running in caller thread: %s%n", description, callerName);
        task.run();
    }
}
