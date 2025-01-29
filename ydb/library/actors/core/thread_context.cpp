#include "thread_context.h"

#include "executor_pool.h"

namespace NActors {

    TNewWorkerContext::TNewWorkerContext(TWorkerId workerId, IExecutorPool* pool, IExecutorPool* sharedPool)
        : WorkerId(workerId)
        , Pool(pool)
        , OwnerPool(pool)
        , SharedPool(sharedPool)
    {
        AssignPool(pool);
    }

    TThreadContext::TThreadContext(TWorkerId workerId, IExecutorPool* pool, IExecutorPool* sharedPool)
        : WorkerContext(workerId, pool, sharedPool)
    {
        i64 now = GetCycleCountFast();
        ActivityContext.StartOfProcessingEventTS = now;
        ActivityContext.ActivationStartTS = now;
    }

    ui32 TNewWorkerContext::PoolId() const {
        return Pool ? Pool->PoolId : Max<ui32>();
    }

    TString TNewWorkerContext::PoolName() const {
        return Pool ? Pool->GetName() : TString();
    }

    ui32 TNewWorkerContext::OwnerPoolId() const {
        return OwnerPool ? OwnerPool->PoolId : Max<ui32>();
    }

    bool TNewWorkerContext::IsShared() const {
        return SharedPool != nullptr;
    }

    void TNewWorkerContext::AssignPool(IExecutorPool* pool, ui64 softDeadlineTs) {
        Pool = pool;
        TimePerMailboxTs = pool->TimePerMailboxTs();
        EventsPerMailbox = pool->EventsPerMailbox();
        SoftDeadlineTs = softDeadlineTs;
        MailboxTable = pool->GetMailboxTable();
        MailboxCache.Switch(MailboxTable);
    }

    void TNewWorkerContext::FreeMailbox(TMailbox* mailbox) {
        MailboxCache.Free(mailbox);
    }

    ui32 TThreadContext::PoolId() const {
        return WorkerContext.PoolId();
    }

    TString TThreadContext::PoolName() const {
        return WorkerContext.PoolName();
    }

    ui32 TThreadContext::OwnerPoolId() const {
        return WorkerContext.OwnerPoolId();
    }

    TWorkerId TThreadContext::WorkerId() const {
        return WorkerContext.WorkerId;
    }

    IExecutorPool* TThreadContext::Pool() const {
        return WorkerContext.Pool;
    }

    IExecutorPool* TThreadContext::SharedPool() const {
        return WorkerContext.SharedPool;
    }

    bool TThreadContext::IsShared() const {
        return WorkerContext.IsShared();
    }

    ui64 TThreadContext::TimePerMailboxTs() const {
        return WorkerContext.TimePerMailboxTs;
    }

    ui32 TThreadContext::EventsPerMailbox() const {
        return WorkerContext.EventsPerMailbox;
    }

    ui64 TThreadContext::SoftDeadlineTs() const {
        return WorkerContext.SoftDeadlineTs;
    }

    void TThreadContext::AssignPool(IExecutorPool* pool, ui64 softDeadlineTs) {
        WorkerContext.AssignPool(pool, softDeadlineTs);
    }

    void TThreadContext::FreeMailbox(TMailbox* mailbox) {
        WorkerContext.FreeMailbox(mailbox);
    }
}
