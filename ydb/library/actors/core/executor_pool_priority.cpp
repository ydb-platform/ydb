#include "executor_pool_priority.h"
#include "executor_pool_base_impl.h"
#include "executor_pool_basic_queue.h"
#include "executor_pool_priority_queue.h"

#include <array>

namespace NActors {

// Each mailbox has at most one queued activation. The slot keeps its priority
// and enqueue timestamp together so monitoring can read both consistently even
// when a mailbox is reused. Pages live until pool teardown.
class TPriorityExecutorPool::TMailboxQueueState {
    static constexpr ui64 HighBit = ui64(1) << 63;
    static constexpr ui64 TimestampMask = ~HighBit;

    struct TLine {
        std::array<std::atomic<ui64>, TMailboxTable::MailboxesPerLine> Slots{};
    };
    std::array<std::atomic<TLine*>, TMailboxTable::LinesCount> Lines{};

    std::atomic<ui64>& Slot(ui32 hint) const {
        auto* line = Lines[(hint >> TMailboxTable::LineIndexShift) & TMailboxTable::LineIndexMask]
            .load(std::memory_order_acquire);
        return line->Slots[hint & TMailboxTable::MailboxIndexMask];
    }

public:
    ~TMailboxQueueState() {
        for (auto& line : Lines) {
            delete line.load(std::memory_order_relaxed);
        }
    }

    void Initialize(ui32 hint, bool high) {
        auto& slot = Lines[(hint >> TMailboxTable::LineIndexShift) & TMailboxTable::LineIndexMask];
        auto* line = slot.load(std::memory_order_acquire);
        if (!line) {
            auto candidate = std::make_unique<TLine>();
            if (slot.compare_exchange_strong(line, candidate.get(),
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                line = candidate.release();
            }
        }
        // Registration owns this mailbox; there is no pending activation yet.
        line->Slots[hint & TMailboxTable::MailboxIndexMask].store(high ? HighBit : 0, std::memory_order_relaxed);
    }

    bool Enqueued(ui32 hint) {
        auto& slot = Slot(hint);
        const ui64 priority = slot.load(std::memory_order_relaxed) & HighBit;
        // Publish before pushing to the queue: a consumer may pop immediately afterwards.
        slot.store(priority | (GetCycleCountFast() & TimestampMask), std::memory_order_relaxed);
        return priority != 0;
    }

    void Dequeued(ui32 hint, bool high) {
        // Clear before returning the mailbox to its worker. It cannot requeue
        // or be reused until then, so neither update needs a shared atomic RMW.
        Slot(hint).store(high ? HighBit : 0, std::memory_order_relaxed);
    }

    void GetCurrentStats(TExecutorPoolStats& stats) const {
        ui64 normal = 0;
        ui64 high = 0;
        for (const auto& slot : Lines) {
            if (const auto* line = slot.load(std::memory_order_acquire)) {
                for (const auto& entry : line->Slots) {
                    const ui64 state = entry.load(std::memory_order_relaxed);
                    if (const ui64 timestamp = state & TimestampMask) {
                        auto& oldest = state & HighBit ? high : normal;
                        if (!oldest || timestamp < oldest) {
                            oldest = timestamp;
                        }
                    }
                }
            }
        }
        stats.HasPriorityActivationQueues = true;
        stats.OldestNormalActivationTs = normal;
        stats.OldestHighActivationTs = high;
    }
};

class TPriorityExecutorPool::TActivationQueueAdapter {
    TPriorityExecutorPool& Pool;

public:
    explicit TActivationQueueAdapter(TPriorityExecutorPool& pool)
        : Pool(pool)
    {}

    void Push(ui32 hint, ui64 counter) {
        auto& queue = Pool.QueueState->Enqueued(hint) ? Pool.HighActivations : Pool.Activations;
        queue.Push(hint, counter);
    }

    ui32 Pop(ui64 counter) {
        bool high;
        const ui32 hint = NPrivate::PopPriorityActivation(Pool.HighActivations, Pool.Activations, counter, high);
        if (hint) {
            Pool.QueueState->Dequeued(hint, high);
        }
        return hint;
    }
};

TPriorityExecutorPool::TPriorityExecutorPool(const TBasicExecutorPoolConfig& config,
        IHarmonizer* harmonizer, TExecutorPoolJail* jail)
    : TBasicExecutorPool(config, harmonizer, jail)
    , HighActivations(config.Threads)
    , QueueState(std::make_unique<TMailboxQueueState>())
{}

TPriorityExecutorPool::~TPriorityExecutorPool() {
    // Match the base pool's cleanup; it drains Normal (Activations) afterwards.
    while (HighActivations.Pop(0))
        ;
}

TActorId TPriorityExecutorPool::Register(IActor* actor, TMailboxCache& cache, ui64 revolvingCounter,
        const TActorId& parentId) {
    return RegisterWithInitializer(actor, cache, revolvingCounter, parentId,
        [this](TMailbox* mailbox, IActor* actor) {
            QueueState->Initialize(mailbox->Hint, actor->GetMailboxPriority() == EMailboxPriority::High);
        });
}

TMailbox* TPriorityExecutorPool::GetReadyActivation(ui64 revolvingCounter) {
    TActivationQueueAdapter queue(*this);
    return GetReadyActivationWithQueue(queue, revolvingCounter);
}

void TPriorityExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) {
    TActivationQueueAdapter queue(*this);
    ScheduleActivationWithQueue(queue, mailbox, revolvingCounter);
}

void TPriorityExecutorPool::SpecificScheduleActivation(TMailbox* mailbox) {
    // Captured Common/Lazy/Tail activations would bypass the priority choice.
    // This pool always publishes them to the shared ready queues.
    ScheduleActivationEx(mailbox, 0);
}

void TPriorityExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats,
        TVector<TExecutorThreadStats>& statsCopy) const {
    TBasicExecutorPool::GetCurrentStats(poolStats, statsCopy);
    QueueState->GetCurrentStats(poolStats);
}

} // namespace NActors
