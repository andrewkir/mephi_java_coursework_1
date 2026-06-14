package ru.andrewkir.executor;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Интерфейс пула потоков.
 * Расширяет {@link Executor}, добавляя поддержку задач с возвращаемым значением
 * и методы управления жизненным циклом пула.
 */
public interface CustomExecutor extends Executor {

    /**
     * Передаёт задачу на выполнение.
     * Если пул перегружен, срабатывает политика отказа.
     */
    @Override
    void execute(Runnable command);

    /**
     * Передаёт задачу на выполнение и возвращает {@link Future} для получения результата.
     */
    <T> Future<T> submit(Callable<T> callable);

    /**
     * Инициирует мягкое завершение: новые задачи не принимаются,
     * уже поставленные в очередь — выполняются до конца.
     */
    void shutdown();

    /**
     * Немедленное завершение: прерывает активные потоки, очищает очереди.
     * Возвращает список задач, которые не успели выполниться.
     */
    List<Runnable> shutdownNow();
}
