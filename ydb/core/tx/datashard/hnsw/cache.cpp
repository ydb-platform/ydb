#include "cache.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NDataShard {
namespace {

using namespace NActors;

class THnswCache : public TActorBootstrapped<THnswCache> {
    struct TEntry {
        // Metadata and immutable storage have independent reservations. Removing
        // the cache entry never releases storage still pinned by a tablet view.
        std::shared_ptr<void> Metadata;
        std::shared_ptr<THnswIndex> Graph;
        std::shared_ptr<const THnswJournalSnapshot> Journal;
        Ydb::Table::VectorIndexSettings Settings;
        TActorId Owner;
        TMonotonic LastAccess;
        ui64 MemoryBytes = 0;
    };

public:
    void Bootstrap() {
        Counters = AppData()->Counters->GetSubgroup("subsystem", "hnsw_cache");
        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

private:
    bool Enabled() const {
        return AppData()->FeatureFlags.GetEnableHnswIndex();
    }

    void UpdateCounters() {
        ui64 graphs = 0, bytes = 0, references = 0, reclaimable = 0;
        for (const auto& [_, entry] : Entries) {
            if (entry.Graph) {
                ++graphs;
                bytes += entry.Graph->EstimatedMemoryBytes();
                references += entry.Graph->GraphUseCount() - 1 + entry.Graph.use_count() - 1;
                if (entry.Graph->GraphUseCount() == 1 && entry.Graph.use_count() == 1) {
                    reclaimable += entry.MemoryBytes;
                }
            } else if (entry.Journal.use_count() == 1) {
                reclaimable += entry.MemoryBytes;
            }
        }
        Tracker->SetReclaimable(reclaimable);
        Counters->GetCounter("Graphs")->Set(graphs);
        Counters->GetCounter("GraphBytesEstimate")->Set(bytes);
        Counters->GetCounter("GraphReferences")->Set(references);
        Counters->GetCounter("UsedBytes")->Set(Tracker->GetUsed());
    }

    void Handle(TEvHnswCache::TEvRegister::TPtr& ev) {
        auto response = MakeHolder<TEvHnswCache::TEvRegistered>();
        if (Enabled()) {
            Clients.insert(ev->Sender);
            response->Tracker = Tracker;
            if (!Registered) {
                Registered = true;
                Send(NMemory::MakeMemoryControllerId(),
                    new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::SharedCache),
                    IEventHandle::FlagTrackDelivery);
            }
        } else {
            response->Tracker = std::make_shared<THnswCacheMemoryTracker>();
            response->Tracker->SetLimit(0);
        }
        Send(ev->Sender, response.Release());
        if (Enabled() && Tracker->HasLimit()) {
            Send(ev->Sender, new NMemory::TEvConsumerLimit(Tracker->GetLimit()));
        }
    }

    void Handle(TEvHnswCache::TEvUnregister::TPtr& ev) {
        Clients.erase(ev->Sender);
        for (auto it = Entries.begin(); it != Entries.end();) {
            if (it->second.Owner == ev->Sender) {
                Entries.erase(it++);
            } else {
                ++it;
            }
        }
        UpdateCounters();
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr& ev) {
        Tracker->SetConsumer(std::move(ev->Get()->Consumer));
    }

    static bool IsUnused(const TEntry& entry) {
        return (!entry.Graph || (entry.Graph->GraphUseCount() == 1 && entry.Graph.use_count() == 1))
            && (!entry.Journal || entry.Journal.use_count() == 1);
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr& ev) {
        Tracker->SetLimit(ev->Get()->LimitBytes);
        if (Tracker->GetUsed() > Tracker->GetLimit()) {
            // Keep live generations discoverable while their owner still
            // advertises them. Clients invalidate private views and forget the
            // publication if pressure persists; borrowed storage stays charged.
            for (auto it = Entries.begin(); it != Entries.end();) {
                if (IsUnused(it->second)) {
                    Entries.erase(it++);
                } else {
                    ++it;
                }
            }
        }
        for (const auto& client : Clients) {
            Send(client, new NMemory::TEvConsumerLimit(Tracker->GetLimit()));
        }
        UpdateCounters();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NMemory::TEvConsumerRegister::EventType) {
            Tracker->SetLimit(0);
            for (const auto& client : Clients) {
                Send(client, new NMemory::TEvConsumerLimit(0));
            }
        }
    }

    void Handle(TEvHnswCache::TEvPublish::TPtr& ev) {
        auto& message = *ev->Get();
        if (!Enabled() || !Clients.contains(ev->Sender) || message.Key.empty()
                || bool(message.Graph) == bool(message.Journal)
                || (message.Graph && !message.Graph->HasGraphReservation())
                || (message.Journal && !message.Journal->Reservation)) {
            return;
        }
        if (auto it = Entries.find(message.Key); it != Entries.end()) {
            if (it->second.Owner != ev->Sender
                    || it->second.Graph != message.Graph || it->second.Journal != message.Journal
                    || !AreHnswIndexSettingsCompatible(it->second.Settings, message.Settings)) {
                Counters->GetCounter("PublishErrors", true)->Inc();
                return;
            }
            it->second.LastAccess = TActivationContext::Monotonic();
            return;
        }
        auto metadata = TryReserveHnswMemory(Tracker, 256 + 2 * message.Key.size());
        if (!metadata) {
            Counters->GetCounter("AdmissionErrors", true)->Inc();
            return;
        }
        TEntry entry;
        entry.Metadata = std::move(metadata);
        entry.Owner = ev->Sender;
        entry.LastAccess = TActivationContext::Monotonic();
        entry.Settings = std::move(message.Settings);
        entry.Graph = std::move(message.Graph);
        entry.Journal = std::move(message.Journal);
        entry.MemoryBytes = entry.Graph ? entry.Graph->EstimatedMemoryBytes() : 0;
        if (entry.Journal) {
            for (const auto& change : entry.Journal->Changes) {
                entry.MemoryBytes += THnswIndexChanges::EstimateBytes(change.Key,
                    change.Vector ? change.Vector->size() : 0);
            }
        }
        Entries.emplace(std::move(message.Key), std::move(entry));
        UpdateCounters();
    }

    void Handle(TEvHnswCache::TEvAcquire::TPtr& ev) {
        const auto& request = *ev->Get();
        auto response = MakeHolder<TEvHnswCache::TEvAcquired>();
        response->Key = request.Key;
        response->RequestId = request.RequestId;
        auto it = Entries.find(request.Key);
        if (!Enabled()) {
            response->Error = "HNSW index support is disabled (EnableHnswIndex)";
        } else if (!Clients.contains(ev->Sender)) {
            response->Error = "Unregistered HNSW cache client";
        } else if (it == Entries.end()) {
            response->Error = "HNSW generation is no longer available";
        } else if (bool(it->second.Journal) != request.Journal
                || (!request.Journal && !AreHnswIndexSettingsCompatible(it->second.Settings, request.Settings))) {
            response->Error = "Incompatible HNSW cache generation";
        } else {
            it->second.LastAccess = TActivationContext::Monotonic();
            response->Graph = it->second.Graph;
            response->Journal = it->second.Journal;
            Counters->GetCounter("SharedAcquires", true)->Inc();
        }
        if (!response->Error.empty()) {
            Counters->GetCounter("AcquireErrors", true)->Inc();
        }
        Send(ev->Sender, response.Release());
        UpdateCounters();
    }

    void Handle(TEvHnswCache::TEvForget::TPtr& ev) {
        const auto it = Entries.find(ev->Get()->Key);
        if (it != Entries.end() && it->second.Owner == ev->Sender) {
            Entries.erase(it);
            UpdateCounters();
        }
    }

    void HandleWakeup() {
        const auto now = TActivationContext::Monotonic();
        for (auto it = Entries.begin(); it != Entries.end();) {
            if (!Enabled() || (IsUnused(it->second)
                    && (now - it->second.LastAccess > TDuration::Minutes(5)
                        || Tracker->HasPendingPressure()))) {
                Entries.erase(it++);
            } else {
                ++it;
            }
        }
        UpdateCounters();
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        Entries.clear();
        if (Registered) {
            Send(NMemory::MakeMemoryControllerId(),
                new NMemory::TEvConsumerUnregister(NMemory::EMemoryConsumerKind::SharedCache));
        }
        TActorBootstrapped::PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHnswCache::TEvRegister, Handle);
            hFunc(TEvHnswCache::TEvUnregister, Handle);
            hFunc(TEvHnswCache::TEvPublish, Handle);
            hFunc(TEvHnswCache::TEvAcquire, Handle);
            hFunc(TEvHnswCache::TEvForget, Handle);
            hFunc(NMemory::TEvConsumerRegistered, Handle);
            hFunc(NMemory::TEvConsumerLimit, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    bool Registered = false;
    std::shared_ptr<THnswCacheMemoryTracker> Tracker = std::make_shared<THnswCacheMemoryTracker>();
    THashSet<TActorId> Clients;
    THashMap<TString, TEntry> Entries;
};

} // namespace

NActors::TActorId MakeHnswCacheId(ui32 nodeId) {
    return NActors::TActorId(nodeId, TStringBuf("hnswcache"));
}

NActors::IActor* CreateHnswCache() {
    return new THnswCache;
}

} // namespace NKikimr::NDataShard
