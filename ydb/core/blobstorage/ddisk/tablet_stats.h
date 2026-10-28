#pragma once

#include <util/datetime/base.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/system/yassert.h>
#include <util/generic/hash.h>

#include <array>
#include <deque>
#include <optional>
#include <vector>

namespace NKikimr::NDDisk {

enum class ETabletOperation : size_t { Read, Write, Sync, Count };

inline constexpr size_t TabletOperationCount = static_cast<size_t>(ETabletOperation::Count);

struct TTabletIoCounters {
    ui64 Requests = 0;
    ui64 Bytes = 0;
    bool operator==(const TTabletIoCounters&) const = default;
};

struct TTabletStatsSample {
    ui64 TabletId = 0;
    ui64 Chunks = 0;
    std::array<TTabletIoCounters, TabletOperationCount> Previous;
    std::array<TTabletIoCounters, TabletOperationCount> Current;
    TDuration Elapsed;
    bool Retired = false;
};

struct TTabletStatsEntry {
    ui64 Chunks = 0;
    ui64 Sessions = 0;
    std::array<TTabletIoCounters, TabletOperationCount> Current = {};
    std::array<TTabletIoCounters, TabletOperationCount> Previous = {};
    TMonotonic CollectedAt;
    ui8 Unchanged = 0;
    bool Changed = false;
    bool Queued = false;
};

struct TTabletStatsLimits {
    static constexpr size_t MaxBatch = 100;
    static constexpr TDuration Period = TDuration::Seconds(1);
};

// References DDisk-owned tablet states. Queue order is deadline order: a new or
// sampled entry is appended with now + Period; an already queued entry never moves.
template<typename TTabletState>
class TTabletStatsTracker : public TTabletStatsLimits {
public:
    explicit TTabletStatsTracker(THashMap<ui64, TTabletState>* tablets)
        : Tablets(tablets)
    {}

    void AddIo(ui64 tabletId, ETabletOperation operation, ui64 requests, ui64 bytes, TMonotonic now) {
        AddIo(tabletId, &(*Tablets)[tabletId].Stats, operation, requests, bytes, now);
    }

    void AddIo(ui64 tabletId, TTabletStatsEntry* entry, ETabletOperation operation,
            ui64 requests, ui64 bytes, TMonotonic now) {
        Touch(tabletId, entry, now);
        auto& counters = entry->Current[static_cast<size_t>(operation)];
        counters.Requests += requests;
        counters.Bytes += bytes;
        entry->Changed = true;
    }

    void AddChunks(ui64 tabletId, i64 delta, TMonotonic now) {
        auto& entry = Touch(tabletId, now);
        Y_ABORT_UNLESS(delta >= 0 || entry.Chunks >= ui64(-delta));
        entry.Chunks += delta;
        entry.Changed = true;
    }

    void AddSessions(ui64 tabletId, i64 delta, TMonotonic now) {
        auto& entry = Touch(tabletId, now);
        Y_ABORT_UNLESS(delta >= 0 || entry.Sessions >= ui64(-delta));
        entry.Sessions += delta;
        entry.Changed = true;
    }

    std::optional<TMonotonic> NextDeadline() const {
        return Queue.empty() ? std::nullopt : std::optional(Tablets->at(Queue.front()).Stats.CollectedAt + Period);
    }

    std::vector<TTabletStatsSample> Collect(TMonotonic now) {
        std::vector<TTabletStatsSample> result;
        result.reserve(MaxBatch);
        for (size_t examined = 0; examined < MaxBatch && !Queue.empty(); ++examined) {
            const ui64 tabletId = Queue.front();
            auto& tablet = Tablets->at(tabletId);
            auto& entry = tablet.Stats;
            if (now < entry.CollectedAt + Period) {
                break;
            }
            Queue.pop_front();
            entry.Unchanged = entry.Changed ? 0 : entry.Unchanged + 1;
            const bool sleeping = entry.Unchanged == 2;
            const bool retired = sleeping && !entry.Chunks && !entry.Sessions && tablet.CanRetire();
            result.push_back({tabletId, entry.Chunks, entry.Previous, entry.Current,
                now - entry.CollectedAt, retired});
            entry.Previous = entry.Current;
            entry.CollectedAt = now;
            entry.Changed = false;
            entry.Queued = !sleeping;
            if (!sleeping) {
                Queue.push_back(tabletId);
            } else if (retired) {
                // No allocation and no activity: neither actor retains an ever-growing
                // history of transient tablet IDs. This monitoring is not billing.
                Tablets->erase(tabletId);
            }
        }
        return result;
    }

    size_t Size() const { return Tablets->size(); }

private:
    TTabletStatsEntry& Touch(ui64 tabletId, TMonotonic now) {
        auto& entry = (*Tablets)[tabletId].Stats;
        Touch(tabletId, &entry, now);
        return entry;
    }

    void Touch(ui64 tabletId, TTabletStatsEntry* entry, TMonotonic now) {
        if (!entry->Queued) {
            // Start the baseline before the first mutation, excluding idle time.
            entry->Previous = entry->Current;
            entry->CollectedAt = now;
            entry->Unchanged = 0;
            entry->Queued = true;
            Queue.push_back(tabletId);
        }
    }

    THashMap<ui64, TTabletState>* const Tablets;
    std::deque<ui64> Queue;
};

} // namespace NKikimr::NDDisk
