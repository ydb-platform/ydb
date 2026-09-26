#include "executor_pool_priority_state.h"

namespace NActors {

TExecutorPoolBaseMailboxed::TPriorityState::~TPriorityState() {
    for (auto& line : Lines) {
        delete line.load(std::memory_order_relaxed);
    }
}

void TExecutorPoolBaseMailboxed::TPriorityState::Initialize(ui32 hint, bool high) {
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
    // Pages live until pool teardown, including when mailbox hints are reused.
    line->Slots[hint & TMailboxTable::MailboxIndexMask].store(high ? HighBit : 0, std::memory_order_relaxed);
}

void TExecutorPoolBaseMailboxed::TPriorityState::GetCurrentStats(TExecutorPoolStats& stats) const {
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
    stats.OldestNormalActivationTs = normal;
    stats.OldestHighActivationTs = high;
}

} // namespace NActors
