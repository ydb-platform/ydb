#pragma once

#include <ydb/core/base/memory_controller_iface.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <atomic>
#include <limits>
#include <optional>

namespace NKikimr::NMemory {

// One registrant's own consumer object; SetReport is safe to call from any thread.
class TRegistrantConsumer : public IMemoryConsumer {
public:
    void SetReport(TConsumerReport report) override;

    // Snapshot with the read-side clamp applied, so a torn triple cannot break the invariants.
    TConsumerReport GetReport() const;

private:
    std::atomic<ui64> Used{0};
    std::atomic<ui64> Demand{0};
    std::atomic<ui64> Reclaimable{0};
};

// One registrant's slice of a distributed limit or of a release request.
struct TConsumerShare {
    TActorId Registrant;
    ui64 Bytes = 0;
};

// One read of every live report, reused for totals and per-registrant allocation.
struct TConsumerCollectionSnapshot {
    struct TEntry {
        TActorId Registrant;
        TConsumerReport Report;
        std::optional<ui64> Desired;
        ui64 PlannedLimit = 0; // explicit target admitted by MC, or the retained grant while waiting
    };

    TVector<TEntry> Entries;

    TConsumerReport GetTotal() const;
    TVector<TConsumerShare> ComputeLimitShares(ui64 limitBytes) const;
    ui64 GetDesiredLimit(ui64 defaultLimitBytes) const;
};

// Per-kind set of registrants owned by the memory controller; every method runs on its thread.
class TConsumerCollection {
public:
    // Returns the sender's own consumer; a re-register by the same actor replaces it with a fresh one.
    TIntrusivePtr<TRegistrantConsumer> Register(TActorId registrant);

    // Drops the registrant's entry; false for an actor that holds none.
    bool Unregister(TActorId registrant);

    // A request belongs to this registration, independently of its usage report.
    bool SetDesired(TActorId registrant, ui64 desired);

    bool IsEmpty() const {
        return Registrants.empty();
    }

    size_t GetRegistrantsCount() const {
        return Registrants.size();
    }

    // Sum of the clamped reports over live entries.
    TConsumerReport GetTotal() const;
    TConsumerCollectionSnapshot GetSnapshot() const;

    // Default shares use reported demand; an explicit Desired replaces only its
    // registrant's share. Their sum may exceed the default kind limit.
    TVector<TConsumerShare> ComputeLimitShares(ui64 limitBytes) const;

    // Release request: bytes each registrant is asked to free, proportional to and capped by its Reclaimable; zero requests are omitted.
    TVector<TConsumerShare> ComputeReleaseRequests(ui64 limitBytes) const;

private:
    struct TRegistrant {
        TIntrusivePtr<TRegistrantConsumer> Consumer;
        std::optional<ui64> Desired;
    };
    TMap<TActorId, TRegistrant> Registrants;
};

}
