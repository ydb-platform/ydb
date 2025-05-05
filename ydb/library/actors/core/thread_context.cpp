#include "thread_context.h"

#include "executor_pool.h"
#include "executor_pool_base.h"

namespace NActors {

    bool UseRingQueue(IExecutorPool* pool) {
        if (auto* basePool = dynamic_cast<TExecutorPoolBase*>(pool)) {
            return basePool->UseRingQueue();
        }
        return false;
    }

    TWorkerContext::TWorkerContext(TWorkerId workerId, IExecutorPool* pool, IExecutorPool* sharedPool)
        : WorkerId(workerId)
        , Pool(pool)
        , OwnerPool(pool)
        , SharedPool(sharedPool)
        , UseRingQueueValue(::NActors::UseRingQueue(pool))
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

    ui32 TWorkerContext::PoolId() const {
        return Pool ? Pool->PoolId : Max<ui32>();
    }

    TString TWorkerContext::PoolName() const {
        return Pool ? Pool->GetName() : TString();
    }

    ui32 TWorkerContext::OwnerPoolId() const {
        return OwnerPool ? OwnerPool->PoolId : Max<ui32>();
    }

    bool TWorkerContext::IsShared() const {
        return SharedPool != nullptr;
    }

    bool TWorkerContext::UseRingQueue() const {
        return UseRingQueueValue;
    }

    void TWorkerContext::AssignPool(IExecutorPool* pool, ui64 softDeadlineTs) {
        Pool = pool;
        TimePerMailboxTs = pool ? pool->TimePerMailboxTs() : TBasicExecutorPoolConfig::DEFAULT_TIME_PER_MAILBOX.SecondsFloat() * NHPTimer::GetClockRate();
        EventsPerMailbox = pool ? pool->EventsPerMailbox() : TBasicExecutorPoolConfig::DEFAULT_EVENTS_PER_MAILBOX;
        SoftDeadlineTs = softDeadlineTs;
        MailboxTable = pool ? pool->GetMailboxTable() : nullptr;
        MailboxCache.Switch(MailboxTable);
    }

    void TWorkerContext::FreeMailbox(TMailbox* mailbox) {
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

    bool TThreadContext::UseRingQueue() const {
        return WorkerContext.UseRingQueue();
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

    bool TExecutionContext::CheckSendingType(ESendingType type) const {
        return SendingType == type;
    }

    bool TExecutionContext::CheckCapturedSendingType(ESendingType type) const {
        return CapturedActivation.SendingType == type;
    }

    bool TThreadContext::CheckSendingType(ESendingType type) const {
        return ExecutionContext.CheckSendingType(type);
    }

    bool TThreadContext::CheckCapturedSendingType(ESendingType type) const {
        return ExecutionContext.CheckCapturedSendingType(type);
    }

    void TThreadContext::SetSendingType(ESendingType type) {
        ExecutionContext.SendingType = type;
    }

    ESendingType TThreadContext::ExchangeSendingType(ESendingType type) {
        return std::exchange(ExecutionContext.SendingType, type);
    }

    TMailbox* TThreadContext::CaptureMailbox(TMailbox* mailbox) {
        ExecutionContext.CapturedActivation.SendingType = ExecutionContext.SendingType;
        return std::exchange(ExecutionContext.CapturedActivation.Mailbox, mailbox);
    }

    ESendingType TThreadContext::SendingType() const {
        return ExecutionContext.SendingType;
    }

    void TThreadContext::ChangeCapturedSendingType(ESendingType type) {
        ExecutionContext.CapturedActivation.SendingType = type;
    }

    ui32 TThreadContext::OverwrittenEventsPerMailbox() const {
        return ExecutionContext.OverwrittenEventsPerMailbox;
    }

    void TThreadContext::SetOverwrittenEventsPerMailbox(ui32 value) {
        ExecutionContext.OverwrittenEventsPerMailbox = value;
    }

    ui64 TThreadContext::OverwrittenTimePerMailboxTs() const {
        return ExecutionContext.OverwrittenTimePerMailboxTs;
    }

    void TThreadContext::SetOverwrittenTimePerMailboxTs(ui64 value) {
        ExecutionContext.OverwrittenTimePerMailboxTs = value;
    }

    void TThreadContext::ResetOverwrittenEventsPerMailbox() {
        ExecutionContext.OverwrittenEventsPerMailbox = EventsPerMailbox();
    }

    void TThreadContext::ResetOverwrittenTimePerMailboxTs() {
        ExecutionContext.OverwrittenTimePerMailboxTs = TimePerMailboxTs();
    }
}
