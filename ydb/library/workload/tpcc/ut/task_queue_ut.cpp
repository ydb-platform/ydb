#include <ydb/library/workload/tpcc/task_queue.h>
#include <ydb/library/workload/tpcc/terminal.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/coroutine_traits.h>

#include <thread>

using namespace NThreading;
using namespace NYdb::NTPCC;

Y_UNIT_TEST_SUITE(TTaskQueueTest) {

    TFuture<void> MakeTransactionTask(int& counter) {
        counter++;
        co_return;
    }

    TFuture<void> MakeTerminalTask(ITaskQueue& queue, int& transactionCounter, int& sleepCounter, size_t terminalId) {
        co_await TTaskReady(queue, terminalId);

        // First sleep
        co_await TSuspend(queue, terminalId, std::chrono::milliseconds(10));
        sleepCounter++;

        // First transaction
        co_await MakeTransactionTask(transactionCounter);

        // Second sleep
        co_await TSuspend(queue, terminalId, std::chrono::milliseconds(20));
        sleepCounter++;

        // Second transaction
        co_await MakeTransactionTask(transactionCounter);

        // Final sleep
        co_await TSuspend(queue, terminalId, std::chrono::milliseconds(30));
        sleepCounter++;

        co_return;
    }

    Y_UNIT_TEST(ShouldExecuteTerminalTaskWithSleepsAndTransactions) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        auto queue = CreateTaskQueue(1, 0, 10, 10, log);

        int transactionCounter = 0;
        int sleepCounter = 0;
        const size_t terminalId = 1;

        queue->Run();

        auto taskFuture = MakeTerminalTask(*queue, transactionCounter, sleepCounter, terminalId);
        taskFuture.GetValueSync(); // should not throw


        UNIT_ASSERT_VALUES_EQUAL(transactionCounter, 2);
        UNIT_ASSERT_VALUES_EQUAL(sleepCounter, 3);

        queue->Join();
    }

    TFuture<void> MakeTerminalTaskWithMultipleTransactions(
        ITaskQueue& queue, int& transactionCounter, int& sleepCounter, size_t terminalId, int numTransactions)
    {
        co_await TTaskReady(queue, terminalId);

        for (int i = 0; i < numTransactions; ++i) {
            // Sleep before each transaction
            co_await TSuspend(queue, terminalId, std::chrono::milliseconds(10));
            sleepCounter++;

            // Execute transaction
            co_await MakeTransactionTask(transactionCounter);
        }

        co_return;
    }

    Y_UNIT_TEST(ShouldExecuteMultipleTransactionsWithSleeps) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        auto queue = CreateTaskQueue(1, 0, 10, 10, log);

        int transactionCounter = 0;
        int sleepCounter = 0;
        const size_t terminalId = 1;
        const int numTransactions = 5;

        queue->Run();

        auto taskFuture = MakeTerminalTaskWithMultipleTransactions(
            *queue, transactionCounter, sleepCounter, terminalId, numTransactions);
        taskFuture.GetValueSync(); // should not throw

        UNIT_ASSERT_VALUES_EQUAL(transactionCounter, numTransactions);
        UNIT_ASSERT_VALUES_EQUAL(sleepCounter, numTransactions);

        queue->Join();
    }

    TFuture<void> MakeFailingTransactionTask() {
        throw std::runtime_error("Transaction failed");
        co_return;
    }

    TFuture<void> MakeTerminalTaskWithFailingTransaction(ITaskQueue& queue, size_t terminalId) {
        co_await TTaskReady(queue, terminalId);

        co_await TSuspend(queue, terminalId, std::chrono::milliseconds(10));
        co_await MakeFailingTransactionTask();
        co_return;
    }

    Y_UNIT_TEST(ShouldPropagateTransactionFailure) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        auto queue = CreateTaskQueue(1, 0, 10, 10, log);

        const size_t terminalId = 1;

        queue->Run();

        auto taskFuture = MakeTerminalTaskWithFailingTransaction(*queue, terminalId);
        UNIT_ASSERT_EXCEPTION_CONTAINS(taskFuture.GetValueSync(), std::runtime_error, "Transaction failed");

        queue->Join();
    }

    Y_UNIT_TEST(ShouldHandleMultipleTerminals) {
        const int numTerminals = 147;

        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        auto queue = CreateTaskQueue(4, 0, numTerminals, numTerminals, log);

        std::vector<int> transactionCounters(numTerminals, 0);
        std::vector<int> sleepCounters(numTerminals, 0);
        std::vector<TFuture<void>> taskFutures;

        queue->Run();

        // Create and start multiple terminals
        for (int i = 0; i < numTerminals; ++i) {
            taskFutures.push_back(MakeTerminalTaskWithMultipleTransactions(
                *queue, transactionCounters[i], sleepCounters[i], i, 2));
        }

        // Wait for all tasks to complete
        for (size_t i = 0; i < taskFutures.size(); ++i) {
            taskFutures[i].GetValueSync(); // should not throw
        }

        // Verify all terminals completed their work
        for (int i = 0; i < numTerminals; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(transactionCounters[i], 2);
            UNIT_ASSERT_VALUES_EQUAL(sleepCounters[i], 2);
        }

        queue->Join();
    }

    Y_UNIT_TEST(ShouldHandleQueueLimits) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        const size_t maxTerminals = 2;
        const size_t maxTransactions = 2;
        auto queue = CreateTaskQueue(1, 0, maxTerminals, maxTransactions,log);

        int transactionCounter = 0;
        int sleepCounter = 0;
        std::vector<TFuture<void>> taskFutures;

        // Create more tasks than queue limits
        for (size_t i = 0; i < maxTerminals + 1; ++i) {
            taskFutures.push_back(MakeTerminalTask(*queue, transactionCounter, sleepCounter, i));
        }

        queue->Run();

        // Wait for all tasks to complete
        for (size_t i = 0; i < taskFutures.size(); ++i) {
            taskFutures[i].Wait();
        }

        size_t exceptionCount = 0;
        for (size_t i = 0; i < taskFutures.size(); ++i) {
            if (taskFutures[i].HasException()) {
                ++exceptionCount;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(exceptionCount, 1);

        queue->Join();
    }

    TFuture<void> MakeTransactionTaskWithInflight(ITaskQueue& queue, int& counter, size_t terminalId) {
        // Use the new inflight control mechanism
        co_await TTaskHasInflight(queue, terminalId);
        counter++;
        // Simulate some work with a small delay
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        queue.DecInflight();
        co_return;
    }

    TFuture<void> MakeTerminalTaskWithInflightControl(
        ITaskQueue& queue, int& transactionCounter, size_t terminalId, int numTransactions)
    {
        co_await TTaskReady(queue, terminalId);

        for (int i = 0; i < numTransactions; ++i) {
            // Sleep before transaction
            co_await TSuspend(queue, terminalId, std::chrono::milliseconds(5));

            // Execute transaction with inflight control
            co_await MakeTransactionTaskWithInflight(queue, transactionCounter, terminalId);
        }
        co_return;
    }

    Y_UNIT_TEST(ShouldSupportUnlimitedInflight) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        const size_t maxRunningTerminals = 0; // unlimited
        const int numTerminals = 5;
        const int transactionsPerTerminal = 2;
        auto queue = CreateTaskQueue(4, maxRunningTerminals, numTerminals, numTerminals, log);

        std::vector<int> transactionCounters(numTerminals, 0);
        std::vector<TFuture<void>> taskFutures;

        queue->Run();

        // Create terminals with inflight control
        for (int i = 0; i < numTerminals; ++i) {
            taskFutures.push_back(MakeTerminalTaskWithInflightControl(
                *queue, transactionCounters[i], i, transactionsPerTerminal));
        }

        // Wait for all tasks to complete
        for (size_t i = 0; i < taskFutures.size(); ++i) {
            taskFutures[i].GetValueSync(); // should not throw
        }

        // Verify all terminals completed their transactions
        for (int i = 0; i < numTerminals; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(transactionCounters[i], transactionsPerTerminal);
        }

        queue->Join();
    }

    Y_UNIT_TEST(ShouldLimitInflightTerminals) {
        auto log = std::make_shared<TLog>(CreateLogBackend("cerr", TLOG_ERR));
        const size_t maxRunningTerminals = 2; // limited to 2 concurrent
        const int numTerminals = 5;
        const int transactionsPerTerminal = 3;
        auto queue = CreateTaskQueue(4, maxRunningTerminals, numTerminals, numTerminals, log);

        std::vector<int> transactionCounters(numTerminals, 0);
        std::vector<TFuture<void>> taskFutures;

        queue->Run();

        // Create more terminals than the inflight limit
        for (int i = 0; i < numTerminals; ++i) {
            taskFutures.push_back(MakeTerminalTaskWithInflightControl(
                *queue, transactionCounters[i], i, transactionsPerTerminal));
        }

        // Wait for all tasks to complete
        for (size_t i = 0; i < taskFutures.size(); ++i) {
            taskFutures[i].GetValueSync(); // should not throw
        }

        // Verify all terminals completed their transactions despite the limit
        for (int i = 0; i < numTerminals; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(transactionCounters[i], transactionsPerTerminal);
        }

        queue->Join();
    }
}
