#pragma once

#include "actor.h"
#include "executor_pool_basic.h"

namespace NActors {

// Non-preemptive High-first polling between mailbox activations. A transient
// High queue miss may select Normal even while High work remains queued.
// Ordinary mailboxes are Normal. RegisterWithSameMailbox inherits the existing
// mailbox's class; adding a High actor does not promote that mailbox.
class TPriorityExecutorPool : public TBasicExecutorPool {
public:
    explicit TPriorityExecutorPool(const TBasicExecutorPoolConfig& config,
        IHarmonizer* harmonizer = nullptr, TExecutorPoolJail* jail = nullptr);
    ~TPriorityExecutorPool() override;

    using TBasicExecutorPool::Register;
    TActorId Register(IActor* actor, TMailboxCache& cache, ui64 revolvingCounter, const TActorId& parentId) override;
    TMailbox* GetReadyActivation(ui64 revolvingCounter) override;
    void ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) override;
    void SpecificScheduleActivation(TMailbox* mailbox) override;
    void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;

private:
    class TActivationQueueAdapter;
    class TMailboxQueueState;

    alignas(64) TRingActivationQueueV4 HighActivations;
    const std::unique_ptr<TMailboxQueueState> QueueState;
};

} // namespace NActors
