#include "actorsystem.h"
#include "debug.h"
#include "executor_pool_basic.h"
#include "executor_pool_shared.h"
#include "hfunc.h"
#include "scheduler_basic.h"
#include "thread_context.h"

#include <ydb/library/actors/util/should_continue.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;


namespace {


constexpr EDebugLevel TestLogLevel = EDebugLevel::None;


#define TEST_LOG(...) ACTORLIB_DEBUG(TestLogLevel, __VA_ARGS__)


struct TOverriddenThreadContext {
    std::optional<bool> IsNeededToWaitNextActivation;
};

class TThreadContextGuard {
private:
    TThreadContext* OriginalContext;
    std::optional<TOverriddenThreadContext> PreviousChangedContext;

public:
    TThreadContextGuard(TThreadContext* newContext, std::optional<TOverriddenThreadContext> overridenContext = std::nullopt)
        : OriginalContext(TlsThreadContext)
    {
        TlsThreadContext = newContext;
        if (overridenContext) {
            PreviousChangedContext = OverrideContext(newContext, overridenContext.value());
        }
    }

    TOverriddenThreadContext OverrideContext(TThreadContext *context, TOverriddenThreadContext overridenContext) {
        TOverriddenThreadContext previousContext;
        if (overridenContext.IsNeededToWaitNextActivation) {
            previousContext.IsNeededToWaitNextActivation = std::exchange(context->ExecutionContext.IsNeededToWaitNextActivation, overridenContext.IsNeededToWaitNextActivation.value());
        }
        return previousContext;
    }

    ~TThreadContextGuard() {
        if (PreviousChangedContext) {
            OverrideContext(TlsThreadContext, PreviousChangedContext.value());
        }
        TlsThreadContext = OriginalContext;
    }
};

struct TWorkerIdentity {
    IExecutorPool* Pool;
    TWorkerId WorkerId;
};

// Фабрика для создания контекстов потоков
class TThreadContexts {
private:
    using TContextKey = std::pair<IExecutorPool*, TWorkerId>;

    std::map<TContextKey, std::unique_ptr<TThreadContext>> Contexts;
    std::vector<IExecutorPool*> Pools;
    std::map<TContextKey, TExecutionStats> ExecutionStats;
    std::vector<TExecutorThreadStats> ExecutorThreadStats;
    TSharedExecutorPool* SharedPool;

public:
    TThreadContexts(std::vector<IExecutorPool*> pools, TSharedExecutorPool* sharedPool)
        : Pools(pools)
        , SharedPool(sharedPool)
    {
        ExecutorThreadStats.resize(SumThreadCount());
        InitContexts();
    }

private:
    TExecutionStats* MakeExecutionStats(TWorkerIdentity workerIdentity) {
        auto it = ExecutionStats.find(TContextKey(workerIdentity.Pool, workerIdentity.WorkerId));
        if (it == ExecutionStats.end()) {
            ui32 nextIdx = ExecutionStats.size();
            it = ExecutionStats.emplace(TContextKey(workerIdentity.Pool, workerIdentity.WorkerId), TExecutionStats()).first;
            it->second.Stats = &ExecutorThreadStats[nextIdx];
            return &it->second;
        }
        return &it->second;
    }

    ui32 SumThreadCount() {
        ui32 threadCount = 0;
        for (auto pool : Pools) {
            threadCount += pool->GetMaxFullThreadCount();
        }
        if (SharedPool) {
            threadCount += SharedPool->GetThreads();
        }
        return threadCount;
    }

    void InitContexts() {
        for (auto pool : Pools) {
            for (TWorkerId workerId = 0; workerId < pool->GetMaxFullThreadCount(); ++workerId) {
                Contexts[TContextKey(pool, workerId)] = std::make_unique<TThreadContext>(workerId, pool, nullptr);
                Contexts[TContextKey(pool, workerId)]->ExecutionStats = MakeExecutionStats(TWorkerIdentity{pool, workerId});
            }
        }
        if (!SharedPool) {
            return;
        }
        TPoolManager poolManager = SharedPool->GetPoolManager();
        for (auto poolId : poolManager.PriorityOrder) {
            for (TWorkerId workerId = poolManager.PoolThreadRanges[poolId].Begin; workerId < poolManager.PoolThreadRanges[poolId].End; ++workerId) {
                Contexts[TContextKey(SharedPool, workerId)] = std::make_unique<TThreadContext>(workerId, Pools[poolId], SharedPool);
                Contexts[TContextKey(SharedPool, workerId)]->ExecutionStats = MakeExecutionStats(TWorkerIdentity{SharedPool, workerId});
            }
        }
    }

public:
    TThreadContext* GetContext(TWorkerIdentity workerIdentity) {
        auto it = Contexts.find(TContextKey(workerIdentity.Pool, workerIdentity.WorkerId));
        UNIT_ASSERT(it != Contexts.end());
        return it->second.get();
    }
};


// Класс для эмуляции вызовов методов от лица разных потоков
class TThreadEmulator {
private:
    TThreadContexts Contexts;

public:
    TThreadEmulator(std::vector<IExecutorPool*> pools, TSharedExecutorPool* sharedPool)
        : Contexts(pools, sharedPool)
    {
    }

    // Получить активацию от лица потока из определенного пула
    TMailbox* GetReadyActivation(TWorkerIdentity workerIdentity, ui64 revolvingReadCounter, std::optional<TOverriddenThreadContext> overridenContext = std::nullopt) {
        UNIT_ASSERT(workerIdentity.Pool);
        UNIT_ASSERT(workerIdentity.WorkerId < workerIdentity.Pool->GetMaxFullThreadCount());

        TThreadContext* context = GetContext(workerIdentity);
        UNIT_ASSERT(context);

        TThreadContextGuard guard(context, overridenContext);
        if (context->IsShared()) {
            return context->SharedPool()->GetReadyActivation(revolvingReadCounter);
        }
        return context->Pool()->GetReadyActivation(revolvingReadCounter);
    }

    // Запланировать активацию от лица потока из определенного пула
    void ScheduleActivation(std::optional<TWorkerIdentity> workerIdentity, IExecutorPool* pool, TMailbox* mailbox, ui64 revolvingWriteCounter, std::optional<TOverriddenThreadContext> overridenContext = std::nullopt) {
        UNIT_ASSERT(pool);
        UNIT_ASSERT(mailbox);

        TThreadContext* context = nullptr;
        if (workerIdentity) {
            context = GetContext(workerIdentity.value());
        }
        UNIT_ASSERT(context);

        TThreadContextGuard guard(context, overridenContext);
        pool->ScheduleActivationEx(mailbox, revolvingWriteCounter);
    }

    // Получить контекст потока для определенного пула и workerId
    TThreadContext* GetContext(TWorkerIdentity workerIdentity) {
        return Contexts.GetContext(workerIdentity);
    }

    IExecutorPool* GetActualPool(TWorkerIdentity workerIdentity) {
        return Contexts.GetContext(workerIdentity)->Pool();
    }
};

void PreparePool(IExecutorPool* pool) {
    NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
    ui32 scheduleSz = 0;
    pool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
}

void PreparePools(std::vector<IExecutorPool*> pools) {
    for (auto pool : pools) {
        PreparePool(pool);
    }
}

template <typename TPoolPtr>
void PreparePools(std::vector<TPoolPtr>& pools) {
    for (auto &pool : pools) {
        PreparePool(pool.get());
    }
}

void TieBasicPoolsAndSharedPool(std::initializer_list<TBasicExecutorPool*> pools, TSharedExecutorPool* sharedPool) {
    for (auto pool : pools) {
        pool->SetSharedPool(sharedPool);
        sharedPool->SetBasicPool(pool);
    }
}

void TieBasicPoolsAndSharedPool(const std::vector<std::unique_ptr<TBasicExecutorPool>>& pools, TSharedExecutorPool* sharedPool) {
    for (auto &pool : pools) {
        pool->SetSharedPool(sharedPool);
        sharedPool->SetBasicPool(pool.get());
    }
}

} // namespace



////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ExecutorPoolsTests) {

    Y_UNIT_TEST(ReceiveActivationForEachRevolvingCounter) {
        std::unique_ptr<IExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .UseRingQueue = false,
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0
        }, nullptr);
        PreparePool(pool.get());

        TThreadEmulator emulator({pool.get()}, nullptr);

        TMailbox* mailbox = pool->GetMailboxTable()->Allocate();
        TWorkerIdentity workerIdentity{pool.get(), 0};

        for (ui64 readCounter = 0; readCounter < 10; ++readCounter) {
            for (ui64 writeCounter = 0; writeCounter < 10; ++writeCounter) {
                emulator.ScheduleActivation(workerIdentity, pool.get(), mailbox, writeCounter);
                TMailbox* activation = emulator.GetReadyActivation(workerIdentity, readCounter);

                UNIT_ASSERT(activation);
                UNIT_ASSERT_EQUAL(activation->Hint, mailbox->Hint);
                UNIT_ASSERT_EQUAL(activation, mailbox);
            }
        }
    }

    TString ToString(std::vector<TMailbox*> mailboxes) {
        TStringStream ss;
        ss << "[";
        bool first = true;
        for (auto mailbox : mailboxes) {
            if (!first) {
                ss << ", ";
            }
            ss << mailbox->Hint;
            first = false;
        }
        ss << "]";
        return ss.Str();
    }

    TString ToString(std::vector<ui64> counters) {
        TStringStream ss;
        ss << "[";
        bool first = true;
        for (auto counter : counters) {
            if (!first) {
                ss << ", ";
            }
            ss << counter;
            first = false;
        }
        ss << "]";
        return ss.Str();
    }

    template <typename T>
    TString ToString(const T& value) {
        TStringStream ss;
        ss << value;
        return ss.Str();
    }

    Y_UNIT_TEST(ReorderingOfCommonQueue) {
        std::unique_ptr<IExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .UseRingQueue = false,
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0
        }, nullptr);

        PreparePool(pool.get());

        TThreadEmulator emulator({pool.get()}, nullptr);

        std::vector<TMailbox*> mailboxes;
        for (ui32 i = 0; i < 4; ++i) {
            mailboxes.push_back(pool->GetMailboxTable()->Allocate());
        }

        std::vector<ui64> readCounters {0, 1, 2, 3};
        std::vector<ui64> writeCounters {0, 1, 2, 3};
        std::vector<TMailbox*> activations {nullptr, nullptr, nullptr, nullptr};
        TWorkerIdentity workerIdentity{pool.get(), 0};

        while (std::next_permutation(readCounters.begin(), readCounters.end())) {
            std::sort(writeCounters.begin(), writeCounters.end());
            while (std::next_permutation(writeCounters.begin(), writeCounters.end())) {
                for (ui64 idx = 0; idx < 4; ++idx) {
                    emulator.ScheduleActivation(workerIdentity, pool.get(), mailboxes[idx], writeCounters[idx]);
                    activations[writeCounters[idx]] = mailboxes[idx];
                }
                for (ui64 idx = 0; idx < 4; ++idx) {
                    TMailbox* activation = emulator.GetReadyActivation(workerIdentity, readCounters[idx]);
                    UNIT_ASSERT(activation);
                    UNIT_ASSERT_VALUES_EQUAL_C(activation->Hint, activations[readCounters[idx]]->Hint,
                        "idx: " << idx
                        << ", readCounters: " << ToString(readCounters)
                        << ", writeCounters: " << ToString(writeCounters)
                        << ", activations: " << ToString(activations));
                    UNIT_ASSERT_EQUAL_C(activation, activations[readCounters[idx]],
                        "idx: " << idx
                        << ", readCounters: " << ToString(readCounters)
                        << ", writeCounters: " << ToString(writeCounters)
                        << ", activations: " << ToString(activations));
                }
            }
        }
    }

    Y_UNIT_TEST(OrderingOfRingQueue) {
        std::unique_ptr<IExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .UseRingQueue = true,
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0
        }, nullptr);

        PreparePool(pool.get());

        TThreadEmulator emulator({pool.get()}, nullptr);

        std::vector<TMailbox*> mailboxes;
        for (ui32 i = 0; i < 4; ++i) {
            mailboxes.push_back(pool->GetMailboxTable()->Allocate());
        }

        std::vector<ui64> readCounters {0, 1, 2, 3};
        std::vector<ui64> writeCounters {0, 1, 2, 3};
        TWorkerIdentity workerIdentity{pool.get(), 0};

        while (std::next_permutation(readCounters.begin(), readCounters.end())) {
            std::sort(writeCounters.begin(), writeCounters.end());
            while (std::next_permutation(writeCounters.begin(), writeCounters.end())) {
                for (ui64 idx = 0; idx < 4; ++idx) {
                    emulator.ScheduleActivation(workerIdentity, pool.get(), mailboxes[idx], writeCounters[idx]);
                }
                for (ui64 idx = 0; idx < 4; ++idx) {
                    TMailbox* activation = emulator.GetReadyActivation(workerIdentity, readCounters[idx]);
                    UNIT_ASSERT(activation);
                    UNIT_ASSERT_VALUES_EQUAL_C(activation->Hint, mailboxes[idx]->Hint,
                        "idx: " << idx
                        << ", readCounters: " << ToString(readCounters)
                        << ", writeCounters: " << ToString(writeCounters));
                    UNIT_ASSERT_EQUAL_C(activation, mailboxes[idx],
                        "idx: " << idx
                        << ", readCounters: " << ToString(readCounters)
                        << ", writeCounters: " << ToString(writeCounters));
                }
            }
        }
    }

    Y_UNIT_TEST(CorrectnessOfLocalQueue) {
        std::unique_ptr<IExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .UseRingQueue = false,
            .MinLocalQueueSize = 8,
            .MaxLocalQueueSize = 8
        }, nullptr);

        PreparePool(pool.get());

        TThreadEmulator emulator({pool.get()}, nullptr);
        TOverriddenThreadContext turnOffWaiting{
            .IsNeededToWaitNextActivation = false
        };

        TMailbox* mailbox = pool->GetMailboxTable()->Allocate();
        TWorkerIdentity workerIdentity{pool.get(), 0};

        UNIT_ASSERT(!emulator.GetReadyActivation(workerIdentity, 0, turnOffWaiting));
        for (ui64 i = 0; i < 8; ++i) {
            emulator.ScheduleActivation(workerIdentity, pool.get(), mailbox, i);
        }

        TMailbox* outerMailbox = pool->GetMailboxTable()->Allocate();
        emulator.ScheduleActivation(workerIdentity, pool.get(), outerMailbox, 8);

        for (ui64 i = 0; i < 8; ++i) {
            TMailbox* activation = emulator.GetReadyActivation(workerIdentity, i);
            UNIT_ASSERT(activation);
            UNIT_ASSERT_EQUAL(activation, mailbox);
        }

        {
            TMailbox* activation = emulator.GetReadyActivation(workerIdentity, 8);
            UNIT_ASSERT(activation);
            UNIT_ASSERT_EQUAL(activation, outerMailbox);
        }
    }

    Y_UNIT_TEST(SharedPoolWith1Thread1Pool) {
        std::unique_ptr<TSharedExecutorPool> sharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
            .Threads = 1
        }, std::vector<TPoolShortInfo>{
            TPoolShortInfo{
                .PoolId = 0,
                .SharedThreadCount = 1,
                .InPriorityOrder = true,
                .PoolName = "SharedPool",
            }
        });

        std::unique_ptr<TBasicExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .HasSharedThread = true,
            .UseRingQueue = false,
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0,
        }, nullptr);

        TieBasicPoolsAndSharedPool({pool.get()}, sharedPool.get());
        PreparePools({pool.get(), sharedPool.get()});
        
        TThreadEmulator emulator({pool.get()}, sharedPool.get());
        TWorkerIdentity workerIdentity{sharedPool.get(), 0};

        std::vector<TMailbox*> mailboxes;   
        for (ui32 i = 0; i < 4; ++i) {
            mailboxes.push_back(pool->GetMailboxTable()->Allocate());
        }

        for (ui64 i = 0; i < 8; ++i) {
            emulator.ScheduleActivation(workerIdentity, pool.get(), mailboxes[i % 4], i);
        }

        for (ui64 i = 0; i < 8; ++i) {
            TMailbox* activation = emulator.GetReadyActivation(workerIdentity, i);
            UNIT_ASSERT(activation);
            UNIT_ASSERT_VALUES_EQUAL_C(activation->Hint, mailboxes[i % 4]->Hint,
                "i: " << i);
            UNIT_ASSERT_EQUAL_C(activation, mailboxes[i % 4],
                "i: " << i);
        }
    }

    Y_UNIT_TEST(SharedPoolWithMultiplePools) {
        // Создание shared пула с несколькими базовыми пулами разных приоритетов
        std::unique_ptr<TSharedExecutorPool> sharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
            .Threads = 3
        }, std::vector<TPoolShortInfo>{
            TPoolShortInfo{
                .PoolId = 0,
                .SharedThreadCount = 1,
                .ForeignSlots = 1,
                .InPriorityOrder = true,
                .PoolName = "Pool0",
            },
            TPoolShortInfo{
                .PoolId = 1,
                .SharedThreadCount = 1,
                .ForeignSlots = 1,
                .InPriorityOrder = true,
                .PoolName = "Pool1",
            },
            TPoolShortInfo{
                .PoolId = 2,
                .SharedThreadCount = 1,
                .ForeignSlots = 1,
                .InPriorityOrder = true,
                .PoolName = "Pool2",
            }
        });

        std::vector<std::unique_ptr<TBasicExecutorPool>> pools;
        for (ui32 i = 0; i < 3; ++i) {
            pools.push_back(std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
                .PoolId = i,
                .PoolName = "Pool" + ToString(i),
                .Threads = 1,
                .HasSharedThread = true,
                .UseRingQueue = false,
                .MinLocalQueueSize = 0,
                .MaxLocalQueueSize = 0,
            }, nullptr));
        }

        TieBasicPoolsAndSharedPool(pools, sharedPool.get());
        PreparePools(pools);
        PreparePool(sharedPool.get());

        std::vector<IExecutorPool*> poolsForEmulator = {pools[0].get(), pools[1].get(), pools[2].get()};
        TThreadEmulator emulator(poolsForEmulator, sharedPool.get());
        
        std::vector<TWorkerIdentity> workers {
            {sharedPool.get(), 0},
            {sharedPool.get(), 1},
            {sharedPool.get(), 2},
        };

        TEST_LOG("Allocate mailboxes");
        std::vector<TMailbox*> mailboxes(3);
        for (auto identity : workers) {
            IExecutorPool* pool = emulator.GetActualPool(identity);
            mailboxes[pool->PoolId] = pool->GetMailboxTable()->Allocate();
        }
        UNIT_ASSERT(std::all_of(mailboxes.begin(), mailboxes.end(), [](TMailbox* mailbox) { return mailbox != nullptr; }));

        auto scheduleActivation = [&](TWorkerIdentity workerIdentity, IExecutorPool* pool) {
            TEST_LOG("Schedule activation ", mailboxes[pool->PoolId]->Hint, " for pool ", pool->PoolId, " (", pool->GetName(), ") with worker [", workerIdentity.Pool->GetName(), ", ", workerIdentity.WorkerId, "]");
            emulator.ScheduleActivation(workerIdentity, pool, mailboxes[pool->PoolId], 0);
        };

        auto initialScheduleActivations = [&]() {
            for (ui32 i = 0; i < 3; ++i) {
                scheduleActivation(workers[i], pools[i].get());
            }
        };

        auto getReadyActivation = [&](TWorkerIdentity workerIdentity) {
            TEST_LOG("Get ready activation for pool ", workerIdentity.Pool->GetName(), " with worker [", workerIdentity.Pool->GetName(), ", ", workerIdentity.WorkerId, "]");
            return emulator.GetReadyActivation(workerIdentity, 0);
        };

        TEST_LOG("Check order of pools for high priority pool worker");
        initialScheduleActivations();
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[0]), mailboxes[0]);
        scheduleActivation(workers[0], pools[0].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[0]), mailboxes[0]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[0]), mailboxes[1]);
        scheduleActivation(workers[0], pools[0].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[0]), mailboxes[0]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[0]), mailboxes[2]);

        TEST_LOG("Check order of pools for mid priority pool worker");
        initialScheduleActivations();
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[1]), mailboxes[1]);
        scheduleActivation(workers[1], pools[1].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[1]), mailboxes[1]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[1]), mailboxes[0]);
        scheduleActivation(workers[1], pools[1].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[1]), mailboxes[1]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[1]), mailboxes[2]);

        TEST_LOG("Check order of pools for low priority pool worker");
        initialScheduleActivations();
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[2]), mailboxes[2]);
        scheduleActivation(workers[2], pools[2].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[2]), mailboxes[2]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[2]), mailboxes[0]);
        scheduleActivation(workers[2], pools[2].get());
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[2]), mailboxes[2]);
        UNIT_ASSERT_EQUAL(getReadyActivation(workers[2]), mailboxes[1]);
    }

    Y_UNIT_TEST(LocalQueueWithSharedPool) {
        std::unique_ptr<TSharedExecutorPool> sharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
            .Threads = 1
        }, std::vector<TPoolShortInfo>{
            TPoolShortInfo{
                .PoolId = 0,
                .SharedThreadCount = 1,
                .InPriorityOrder = true,
                .PoolName = "SharedPool",
            }
        });

        std::unique_ptr<TBasicExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .HasSharedThread = true,
            .UseRingQueue = false,
            .MinLocalQueueSize = 4,  // Локальная очередь размером 4
            .MaxLocalQueueSize = 4,
        }, nullptr);

        pool->SetSharedPool(sharedPool.get());

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            pool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            sharedPool->SetBasicPool(static_cast<TBasicExecutorPool*>(pool.get()));
            sharedPool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }
        
        TThreadEmulator emulator({pool.get()}, sharedPool.get());
        TWorkerIdentity workerIdentity{sharedPool.get(), 0};

        TMailbox* mailbox = pool->GetMailboxTable()->Allocate();
        TMailbox* anotherMailbox = pool->GetMailboxTable()->Allocate();

        // Заполняем локальную очередь
        for (ui64 i = 0; i < 4; ++i) {
            emulator.ScheduleActivation(workerIdentity, pool.get(), mailbox, i);
        }
        
        // Добавляем еще одну активацию, которая должна пойти в общую очередь
        emulator.ScheduleActivation(workerIdentity, pool.get(), anotherMailbox, 4);

        // Проверяем что все активации доступны в правильном порядке
        for (ui64 i = 0; i < 4; ++i) {
            TMailbox* activation = emulator.GetReadyActivation(workerIdentity, i);
            UNIT_ASSERT(activation);
            UNIT_ASSERT_EQUAL(activation, mailbox);
        }

        // Последняя активация из общей очереди
        TMailbox* activation = emulator.GetReadyActivation(workerIdentity, 4);
        UNIT_ASSERT(activation);
        UNIT_ASSERT_EQUAL(activation, anotherMailbox);
    }

    Y_UNIT_TEST(RingQueueWithSharedPool) {
        std::unique_ptr<TSharedExecutorPool> sharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
            .Threads = 1
        }, std::vector<TPoolShortInfo>{
            TPoolShortInfo{
                .PoolId = 0, 
                .SharedThreadCount = 1,
                .InPriorityOrder = true,
                .PoolName = "SharedPool",
            }
        });

        std::unique_ptr<TBasicExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .HasSharedThread = true,
            .UseRingQueue = true,  // Используем кольцевую очередь
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0,
        }, nullptr);

        pool->SetSharedPool(sharedPool.get());

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            pool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            sharedPool->SetBasicPool(static_cast<TBasicExecutorPool*>(pool.get()));
            sharedPool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }
        
        TThreadEmulator emulator({pool.get()}, sharedPool.get());
        TWorkerIdentity workerIdentity{sharedPool.get(), 0};

        std::vector<TMailbox*> mailboxes;
        for (ui32 i = 0; i < 4; ++i) {
            mailboxes.push_back(pool->GetMailboxTable()->Allocate());
        }

        // Планируем активации в разном порядке
        for (ui64 i = 0; i < 4; ++i) {
            emulator.ScheduleActivation(workerIdentity, pool.get(), mailboxes[i], i);
        }

        // Проверяем, что активации возвращаются в правильном порядке (RingQueue сохраняет FIFO порядок)
        for (ui64 i = 0; i < 4; ++i) {
            TMailbox* activation = emulator.GetReadyActivation(workerIdentity, i);
            UNIT_ASSERT(activation);
            UNIT_ASSERT_EQUAL(activation, mailboxes[i]);
        }
    }

    Y_UNIT_TEST(MultipleWorkersSharedPool) {
        // Тест на распределение нагрузки между несколькими потоками в shared пуле
        std::unique_ptr<TSharedExecutorPool> sharedPool = std::make_unique<TSharedExecutorPool>(TSharedExecutorPoolConfig{
            .Threads = 2
        }, std::vector<TPoolShortInfo>{
            TPoolShortInfo{
                .PoolId = 0,
                .SharedThreadCount = 2,
                .InPriorityOrder = true,
                .PoolName = "SharedPool",
            }
        });

        std::unique_ptr<TBasicExecutorPool> pool = std::make_unique<TBasicExecutorPool>(TBasicExecutorPoolConfig{
            .Threads = 1,
            .HasSharedThread = true,
            .UseRingQueue = false,
            .MinLocalQueueSize = 0,
            .MaxLocalQueueSize = 0,
        }, nullptr);

        pool->SetSharedPool(sharedPool.get());

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            pool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }

        {
            NActors::NSchedulerQueue::TReader* scheduleReaders = nullptr;
            ui32 scheduleSz = 0;
            sharedPool->SetBasicPool(static_cast<TBasicExecutorPool*>(pool.get()));
            sharedPool->Prepare(nullptr, &scheduleReaders, &scheduleSz);
        }
        
        TThreadEmulator emulator({pool.get()}, sharedPool.get());
        TWorkerIdentity worker0{sharedPool.get(), 0};
        TWorkerIdentity worker1{sharedPool.get(), 1};

        TMailbox* mailbox1 = pool->GetMailboxTable()->Allocate();
        TMailbox* mailbox2 = pool->GetMailboxTable()->Allocate();
        
        // Планируем активации для двух разных потоков
        emulator.ScheduleActivation(worker0, pool.get(), mailbox1, 0);
        emulator.ScheduleActivation(worker1, pool.get(), mailbox2, 1);

        // Проверяем, что каждый поток получает свою активацию
        TMailbox* activation1 = emulator.GetReadyActivation(worker0, 0);
        UNIT_ASSERT(activation1);
        UNIT_ASSERT_EQUAL(activation1, mailbox1);

        TMailbox* activation2 = emulator.GetReadyActivation(worker1, 0);
        UNIT_ASSERT(activation2);
        UNIT_ASSERT_EQUAL(activation2, mailbox2);
    }
}
