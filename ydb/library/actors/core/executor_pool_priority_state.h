#pragma once

#include "executor_pool_base.h"
#include "mailbox.h"
#include <ydb/library/actors/util/datetime.h>

#include <array>

namespace NActors {

// Allocated only for Basic pools with UsePriority enabled. Each mailbox has at
// most one queued activation; a slot holds its priority and enqueue timestamp.
class TExecutorPoolBaseMailboxed::TPriorityState {
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
    TPriorityState() = default;
    ~TPriorityState();

    void Initialize(ui32 hint, bool high);
    void GetCurrentStats(TExecutorPoolStats& stats) const;

    bool Enqueued(ui32 hint) {
        auto& slot = Slot(hint);
        const ui64 priority = slot.load(std::memory_order_relaxed) & HighBit;
        // Publish before pushing: a consumer may pop immediately afterwards.
        slot.store(priority | (GetCycleCountFast() & TimestampMask), std::memory_order_relaxed);
        return priority != 0;
    }

    void Dequeued(ui32 hint, bool high) {
        // Clear before returning the mailbox to its worker. It cannot requeue
        // or be reused until then, so neither update needs a shared atomic RMW.
        Slot(hint).store(high ? HighBit : 0, std::memory_order_relaxed);
    }
};

} // namespace NActors
