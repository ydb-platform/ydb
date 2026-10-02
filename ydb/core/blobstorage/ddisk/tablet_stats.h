#pragma once

#include <util/datetime/base.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/system/yassert.h>

#include <array>
#include <deque>
#include <optional>
#include <unordered_map>
#include <vector>

namespace NKikimr::NDDisk {

enum class ETabletOperation : size_t { Read, Write, Sync };

struct TTabletIoCounters {
    ui64 Requests = 0;
    ui64 Bytes = 0;
    bool operator==(const TTabletIoCounters&) const = default;
};

struct TTabletStatsSample {
    ui64 TabletId = 0;
    ui64 Chunks = 0;
    std::array<TTabletIoCounters, 3> Previous;
    std::array<TTabletIoCounters, 3> Current;
    TDuration Elapsed;
    bool Retired = false;
};

// Owned exclusively by DDisk. Queue order is deadline order: a new or sampled
// entry is appended with now + Period, and an already queued entry never moves.
class TTabletStatsTracker {
public:
    static constexpr size_t MaxBatch = 100;
    static constexpr TDuration Period = TDuration::Seconds(1);

    void AddIo(ui64 tabletId, ETabletOperation operation, ui64 requests, ui64 bytes, TMonotonic now) {
        auto& entry = Touch(tabletId, now);
        auto& counters = entry.Current[static_cast<size_t>(operation)];
        counters.Requests += requests;
        counters.Bytes += bytes;
        entry.Changed = true;
    }

    void AddChunks(ui64 tabletId, i64 delta, TMonotonic now) {
        if (!delta) {
            return;
        }
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
        return Queue.empty() ? std::nullopt : std::optional(Entries.at(Queue.front()).CollectedAt + Period);
    }

    std::vector<TTabletStatsSample> Collect(TMonotonic now) {
        std::vector<TTabletStatsSample> result;
        result.reserve(MaxBatch);
        for (size_t examined = 0; examined < MaxBatch && !Queue.empty(); ++examined) {
            const ui64 tabletId = Queue.front();
            auto& entry = Entries.at(tabletId);
            if (now < entry.CollectedAt + Period) {
                break;
            }
            Queue.pop_front();
            entry.Unchanged = entry.Changed ? 0 : entry.Unchanged + 1;
            const bool sleeping = entry.Unchanged == 2;
            result.push_back({tabletId, entry.Chunks, entry.Previous, entry.Current,
                now - entry.CollectedAt, sleeping && !entry.Chunks && !entry.Sessions});
            entry.Previous = entry.Current;
            entry.CollectedAt = now;
            entry.Changed = false;
            entry.Queued = !sleeping;
            if (!sleeping) {
                Queue.push_back(tabletId);
            } else if (!entry.Chunks && !entry.Sessions) {
                // No allocation and no activity: neither actor retains an ever-growing
                // history of transient tablet IDs. This monitoring is not billing.
                Entries.erase(tabletId);
            }
        }
        return result;
    }

    size_t Size() const { return Entries.size(); }

private:
    struct TEntry {
        ui64 Chunks = 0;
        ui64 Sessions = 0;
        std::array<TTabletIoCounters, 3> Current = {};
        std::array<TTabletIoCounters, 3> Previous = {};
        TMonotonic CollectedAt;
        ui8 Unchanged = 0;
        bool Changed = false;
        bool Queued = false;
    };

    TEntry& Touch(ui64 tabletId, TMonotonic now) {
        auto& entry = Entries[tabletId];
        if (!entry.Queued) {
            // Start the baseline before the first mutation, excluding idle time.
            entry.Previous = entry.Current;
            entry.CollectedAt = now;
            entry.Unchanged = 0;
            entry.Queued = true;
            Queue.push_back(tabletId);
        }
        return entry;
    }

    std::unordered_map<ui64, TEntry> Entries;
    std::deque<ui64> Queue;
};

} // namespace NKikimr::NDDisk
