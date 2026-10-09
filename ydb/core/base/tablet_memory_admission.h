#pragma once

#include "memory_controller_iface.h"

#include <list>
#include <util/generic/hash.h>

namespace NKikimr::NMemory {

struct TMemoryAdmissionStats {
    ui64 RunningBytes = 0;
    ui64 RunningCount = 0;
    ui64 PostponedBytes = 0;
    ui64 PostponedCount = 0;

    ui64 HeldBytes() const {
        return RunningBytes + PostponedBytes;
    }
};

// Tells the owner whether the item is starting on the Admit call or after a wait in the queue
enum class EAdmitSource {
    Immediate,
    FromQueue,
};

enum class EAdmitResult {
    Started,
    Queued,
    Duplicate,
};

// Gates the start of memory-charged items by the node zone: FIFO, no growth in Yellow, one at a time in Red
// One admission belongs to one tablet and runs only in that tablet's actor context, so it takes no locks by design
template <class TItem, class TOwner>
class TMemoryAdmission {
public:
    explicit TMemoryAdmission(TOwner& owner)
        : Owner(owner)
    {
    }

    // Starts the item now or queues it behind the items already waiting
    EAdmitResult Admit(ui64 uid, TItem&& item, ui64 charge) {
        if (Charges.contains(uid) || Waiting.contains(uid)) {
            return EAdmitResult::Duplicate;
        }
        if (Queue.empty() && Admits(charge)) {
            Run(uid, std::move(item), charge, EAdmitSource::Immediate);
            return EAdmitResult::Started;
        }
        PostponedBytes += charge;
        Queue.push_back({uid, charge, std::move(item)});
        Waiting.emplace(uid, std::prev(Queue.end()));
        return EAdmitResult::Queued;
    }

    // Takes the charge of a completed item back and drains the queue
    void Release(ui64 uid) {
        const auto it = Charges.find(uid);
        if (it == Charges.end()) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(RunningCount && RunningBytes >= it->second);
        RunningBytes -= it->second;
        --RunningCount;
        Charges.erase(it);
        Drain();
    }

    // Withdraws a waiting item without scanning unrelated or running requests.
    std::optional<TItem> CancelQueued(ui64 uid) {
        const auto it = Waiting.find(uid);
        if (it == Waiting.end()) {
            return std::nullopt;
        }
        auto entry = it->second;
        std::optional<TItem> item(std::move(entry->Item));
        PostponedBytes -= entry->Charge;
        Queue.erase(entry);
        Waiting.erase(it);
        Drain();
        return item;
    }

    // Called by the tablet from ITablet::OnMemoryZone with the zone the executor delivered
    void OnZoneChanged(EMemoryZone zone) {
        if (zone == Zone) {
            return;
        }
        Zone = zone;
        // The watermark is the running bytes seen on entering the zone, so nothing may grow past it
        Watermark = RunningBytes;
        Drain();
    }

    TMemoryAdmissionStats GetStats() const {
        return {
            .RunningBytes = RunningBytes,
            .RunningCount = RunningCount,
            .PostponedBytes = PostponedBytes,
            .PostponedCount = Queue.size(),
        };
    }

private:
    struct TEntry {
        ui64 Uid;
        ui64 Charge;
        TItem Item;
    };

    bool Admits(ui64 charge) const {
        switch (Zone) {
            case EMemoryZone::Green:
                return true;
            case EMemoryZone::Yellow:
                return RunningCount == 0 || (charge <= Watermark && RunningBytes <= Watermark - charge);
            case EMemoryZone::Red:
                return RunningCount == 0;
        }
        return true;
    }

    void Run(ui64 uid, TItem&& item, ui64 charge, EAdmitSource source) {
        // An idle progress request establishes a reusable ceiling for this Yellow interval.
        if (Zone == EMemoryZone::Yellow && RunningCount == 0) {
            Watermark = Max(Watermark, charge);
        }
        RunningBytes += charge;
        ++RunningCount;
        Charges[uid] = charge;
        Owner.StartAdmitted(std::move(item), source);
    }

    void Drain() {
        if (Draining) {
            return;
        }
        Draining = true;
        while (!Queue.empty() && Admits(Queue.front().Charge)) {
            TEntry entry = std::move(Queue.front());
            Queue.pop_front();
            Waiting.erase(entry.Uid);
            PostponedBytes -= entry.Charge;
            Run(entry.Uid, std::move(entry.Item), entry.Charge, EAdmitSource::FromQueue);
        }
        Draining = false;
    }

private:
    TOwner& Owner; // starts what the gate lets through: StartAdmitted(TItem&&, EAdmitSource)

    EMemoryZone Zone = EMemoryZone::Green;
    ui64 Watermark = 0;
    ui64 RunningBytes = 0;
    ui64 RunningCount = 0;
    ui64 PostponedBytes = 0;
    THashMap<ui64, ui64> Charges;
    using TQueue = std::list<TEntry>;
    TQueue Queue;
    THashMap<ui64, typename TQueue::iterator> Waiting;
    bool Draining = false;
};

}
