#include "cache.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/system/tempfile.h>
#include <fstream>
#include <algorithm>
#include <functional>

#include <util/generic/hash_set.h>

namespace NKikimr::NDataShard {
namespace {

using namespace NActors;

constexpr ui64 ChunkBytes = 256_KB;
constexpr size_t MaxWaiters = 1024;

class TWorker : public TActorBootstrapped<TWorker> {
public:
    explicit TWorker(std::function<void()> work) : Work(std::move(work)) {}
    void Bootstrap() {
        Work();
        PassAway();
    }
private:
    std::function<void()> Work;
};

struct TEvFileReady : TEventLocal<TEvFileReady, TEvHnswCache::EvFileReady> {
    TString Key;
    ui64 ExportId = 0;
    std::shared_ptr<TTempFileHandle> File;
    TString Error;
};

struct TEvLoaded : TEventLocal<TEvLoaded, TEvHnswCache::EvLoaded> {
    TString Key;
    ui64 TransferId = 0;
    std::shared_ptr<THnswIndex> Graph;
    std::shared_ptr<const THnswJournalSnapshot> Journal;
    TString Error;
};

class THnswCache : public TActorBootstrapped<THnswCache> {
    struct TWaiter {
        TActorId Actor;
        ui64 RequestId;
        std::shared_ptr<void> Reservation;
    };
    struct TChunkWaiter {
        TEvHnswCache::TEvChunkRequest::TPtr Request;
        std::shared_ptr<void> Reservation;
    };
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
        std::shared_ptr<TTempFileHandle> File;
        std::shared_ptr<void> Reservation;
        std::shared_ptr<void> TransferReservation;
        TVector<TWaiter> Waiters;
        TVector<TChunkWaiter> ChunkRequests;
        ui64 Offset = 0;
        ui64 TotalBytes = 0;
        ui64 TransferId = 0;
        ui64 ExportId = 0;
        ui32 SourceNodeId = 0;
        bool IsJournal = false;
        bool Exporting = false;
        bool Loading = false;
    };
    using TEntries = THashMap<TString, TEntry>;

public:
    void Bootstrap() {
        TActivationContext::ActorSystem()->RegisterLocalService(MakeHnswCacheId(SelfId().NodeId()), SelfId());
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
            response->Active = true;
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
                Erase(it++, "HNSW generation is no longer available");
            } else {
                auto& waiters = it->second.Waiters;
                waiters.erase(std::remove_if(waiters.begin(), waiters.end(),
                    [&](const auto& waiter) { return waiter.Actor == ev->Sender; }), waiters.end());
                if (it->second.TransferId && waiters.empty()) {
                    Erase(it++, "HNSW transfer requester stopped");
                } else {
                    ++it;
                }
            }
        }
        UpdateCounters();
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr& ev) {
        Tracker->SetConsumer(std::move(ev->Get()->Consumer));
    }

    static bool IsUnused(const TEntry& entry) {
        return !entry.TransferId && !entry.Exporting && !entry.Loading && entry.Waiters.empty()
            && (!entry.Graph || (entry.Graph->GraphUseCount() == 1 && entry.Graph.use_count() == 1))
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
                    Erase(it++, "HNSW generation is no longer available");
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
        } else if (ev->Get()->SourceType == TEvHnswCache::TEvChunkRequest::EventType) {
            for (auto it = Entries.begin(); it != Entries.end(); ++it) {
                if (it->second.TransferId == ev->Cookie) {
                    Erase(it, "HNSW snapshot source is unavailable");
                    break;
                }
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
                    || (it->second.Graph != message.Graph && (!it->second.Graph || !message.Graph
                        || !it->second.Graph->SharesGraphWith(*message.Graph)))
                    || it->second.Journal != message.Journal
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
        entry.IsJournal = bool(message.Journal);
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
            if (request.SourceNodeId && request.SourceNodeId != SelfId().NodeId() && request.MemoryBytes) {
                StartAcquire(ev);
                return;
            }
            response->Error = "HNSW generation is no longer available";
        } else if (it->second.IsJournal != request.Journal
                || (!request.Journal && !AreHnswIndexSettingsCompatible(it->second.Settings, request.Settings))) {
            response->Error = "Incompatible HNSW cache generation";
        } else if (it->second.TransferId) {
            if ((request.SourceNodeId && request.SourceNodeId != it->second.SourceNodeId)
                    || (request.MemoryBytes && request.MemoryBytes != it->second.MemoryBytes)) {
                response->Error = "Incompatible HNSW transfer generation";
            } else if (!AddWaiter(it->second, ev)) {
                response->Error = "HNSW cache waiter admission pending";
            } else {
                return;
            }
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

    void StartWorker(std::function<void()> work) {
        Register(new TWorker(std::move(work)), TMailboxType::HTSwap, AppData()->BatchPoolId);
    }

    void Reply(TEntry& entry, const TString& key, const TString& error = {}) {
        for (const auto& waiter : entry.Waiters) {
            auto response = MakeHolder<TEvHnswCache::TEvAcquired>();
            response->Key = key;
            response->RequestId = waiter.RequestId;
            response->Error = error;
            if (error.empty()) {
                response->Graph = entry.Graph;
                response->Journal = entry.Journal;
            } else {
                Counters->GetCounter("AcquireErrors", true)->Inc();
            }
            Send(waiter.Actor, response.Release());
        }
        entry.Waiters.clear();
    }

    bool AddWaiter(TEntry& entry, const TEvHnswCache::TEvAcquire::TPtr& ev) {
        if (entry.Waiters.size() >= MaxWaiters) {
            return false;
        }
        auto reservation = TryReserveHnswMemory(Tracker, 256);
        if (!reservation) {
            return false;
        }
        entry.Waiters.push_back({ev->Sender, ev->Get()->RequestId, std::move(reservation)});
        return true;
    }

    void Erase(TEntries::iterator it, const TString& error) {
        Reply(it->second, it->first, error);
        for (auto& request : it->second.ChunkRequests) {
            ReplyChunk(request.Request, nullptr, error);
        }
        Entries.erase(it);
    }

    void RequestChunk(const TString& key, TEntry& entry) {
        auto request = MakeHolder<TEvHnswCache::TEvChunkRequest>();
        request->Record.SetKey(key);
        request->Record.SetOffset(entry.Offset);
        Send(MakeHnswCacheId(entry.SourceNodeId), request.Release(),
            IEventHandle::FlagTrackDelivery, entry.TransferId);
    }

    void StartAcquire(TEvHnswCache::TEvAcquire::TPtr& ev) {
        const auto& request = *ev->Get();
        TEntry entry;
        entry.LastAccess = TActivationContext::Monotonic();
        entry.SourceNodeId = request.SourceNodeId;
        entry.MemoryBytes = request.MemoryBytes;
        entry.Settings = request.Settings;
        entry.IsJournal = request.Journal;
        entry.Metadata = TryReserveHnswMemory(Tracker, 256 + 2 * request.Key.size());
        entry.Reservation = TryReserveHnswMemory(Tracker, entry.MemoryBytes);
        entry.TransferReservation = TryReserveHnswMemory(Tracker, 2 * ChunkBytes + 8192);
        if (!entry.Metadata || !entry.Reservation || !entry.TransferReservation || !AddWaiter(entry, ev)) {
            auto response = MakeHolder<TEvHnswCache::TEvAcquired>();
            response->Key = request.Key;
            response->RequestId = request.RequestId;
            response->Error = "HNSW cache memory admission pending";
            Send(ev->Sender, response.Release());
            Counters->GetCounter("AcquireErrors", true)->Inc();
            return;
        }
        try {
            entry.File = std::make_shared<TTempFileHandle>();
        } catch (const std::exception& error) {
            Reply(entry, request.Key, error.what());
            return;
        }
        entry.TransferId = ++NextTransferId;
        auto [it, inserted] = Entries.emplace(request.Key, std::move(entry));
        Y_ENSURE(inserted);
        Counters->GetCounter("Downloads", true)->Inc();
        RequestChunk(it->first, it->second);
    }

    void StartExport(const TString& key, TEntry& entry) {
        entry.Exporting = true;
        entry.ExportId = ++NextExportId;
        StartWorker([replyTo = SelfId(), key, exportId = entry.ExportId, graph = entry.Graph, journal = entry.Journal] {
            auto result = MakeHolder<TEvFileReady>();
            result->Key = key;
            result->ExportId = exportId;
            try {
                result->File = std::make_shared<TTempFileHandle>();
                std::ofstream output(result->File->Name().c_str(), std::ios::binary | std::ios::trunc);
                output.exceptions(std::ios::badbit | std::ios::failbit);
                if (graph)
                    graph->SaveGraph(output);
                else
                    journal->Save(output);
                output.close();
            } catch (const std::exception& ex) {
                result->Error = ex.what();
            }
            TActivationContext::Send(new IEventHandle(replyTo, {}, result.Release()));
        });
    }

    void ReplyChunk(TEvHnswCache::TEvChunkRequest::TPtr& ev, TEntry* entry, TString error = {}) {
        auto response = MakeHolder<TEvHnswCache::TEvChunkResponse>();
        const auto& request = ev->Get()->Record;
        if (!Enabled()) {
            error = "HNSW index support is disabled (EnableHnswIndex)";
        }
        response->Record.SetKey(request.GetKey());
        response->Record.SetOffset(request.GetOffset());
        try {
            if (entry && entry->File && error.empty()) {
                const ui64 size = entry->File->GetLength();
                response->Record.SetTotalBytes(size);
                if (request.GetOffset() > size)
                    error = "Invalid HNSW transfer offset";
                else {
                    const ui64 bytes = Min(ChunkBytes, size - request.GetOffset());
                    response->BufferReservation = TryReserveHnswMemory(Tracker, 2 * bytes + 4096);
                    if (!response->BufferReservation)
                        ythrow yexception() << "HNSW transfer memory admission pending";
                    TString data = TString::Uninitialized(bytes);
                    if (!data.empty())
                        entry->File->Pload(data.Detach(), data.size(), request.GetOffset());
                    response->Record.SetData(std::move(data));
                }
            } else if (error.empty()) {
                error = "HNSW generation is no longer available";
            }
        } catch (const std::exception& ex) {
            error = ex.what();
        }
        response->Record.SetError(std::move(error));
        Counters->GetCounter("BytesSent", true)->Add(response->Record.GetData().size());
        Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    void Handle(TEvHnswCache::TEvChunkRequest::TPtr& ev) {
        auto it = Entries.find(ev->Get()->Record.GetKey());
        if (!Enabled() || it == Entries.end()) {
            ReplyChunk(ev, nullptr);
            return;
        }
        auto& entry = it->second;
        entry.LastAccess = TActivationContext::Monotonic();
        if (entry.File && !entry.Exporting && !entry.TransferId) {
            ReplyChunk(ev, &entry);
        } else if (entry.Graph || entry.Journal) {
            auto reservation = TryReserveHnswMemory(Tracker, 512);
            if (!reservation || entry.ChunkRequests.size() >= MaxWaiters) {
                ReplyChunk(ev, nullptr, "HNSW transfer queue admission pending");
                return;
            }
            entry.ChunkRequests.push_back({ev, std::move(reservation)});
            if (!entry.Exporting)
                StartExport(it->first, entry);
        } else {
            ReplyChunk(ev, nullptr);
        }
    }

    void Handle(TEvFileReady::TPtr& ev) {
        auto& result = *ev->Get();
        auto it = Entries.find(result.Key);
        if (it == Entries.end() || it->second.ExportId != result.ExportId)
            return;
        auto& entry = it->second;
        entry.Exporting = false;
        entry.File = std::move(result.File);
        for (auto& request : entry.ChunkRequests)
            ReplyChunk(request.Request, &entry, result.Error);
        entry.ChunkRequests.clear();
        if (!result.Error.empty())
            entry.File.reset();
    }

    void Handle(TEvHnswCache::TEvChunkResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        auto it = Entries.find(response.GetKey());
        if (it == Entries.end() || it->second.TransferId != ev->Cookie || it->second.Loading)
            return;
        auto& entry = it->second;
        if (ev->Sender.NodeId() != entry.SourceNodeId)
            return;
        TString error = response.GetError();
        if (error.empty() && (!Enabled() || !response.GetTotalBytes() || response.GetOffset() != entry.Offset || response.GetData().size() > ChunkBytes
                || response.GetTotalBytes() < entry.Offset
                || response.GetData().size() > response.GetTotalBytes() - entry.Offset
                || entry.MemoryBytes > (Max<ui64>() - 1_MB) / 2
                || response.GetTotalBytes() > entry.MemoryBytes * 2 + 1_MB
                || (entry.TotalBytes && entry.TotalBytes != response.GetTotalBytes())
                || (response.GetData().empty() && entry.Offset != response.GetTotalBytes())))
            error = "Invalid HNSW transfer chunk";
        if (!error.empty()) {
            Erase(it, error);
            return;
        }
        try {
            entry.File->Pwrite(response.GetData().data(), response.GetData().size(), entry.Offset);
        } catch (const std::exception& ex) {
            Erase(it, ex.what());
            return;
        }
        entry.LastAccess = TActivationContext::Monotonic();
        entry.TotalBytes = response.GetTotalBytes();
        entry.Offset += response.GetData().size();
        Counters->GetCounter("BytesReceived", true)->Add(response.GetData().size());
        if (entry.Offset < entry.TotalBytes) {
            RequestChunk(it->first, entry);
            return;
        }
        entry.Loading = true;
        StartWorker([replyTo = SelfId(), key = it->first, transferId = entry.TransferId,
                file = entry.File, reservation = entry.Reservation, isJournal = entry.IsJournal,
                settings = entry.Settings, budget = entry.MemoryBytes] {
            auto result = MakeHolder<TEvLoaded>();
            result->Key = key;
            result->TransferId = transferId;
            try {
                std::ifstream input(file->Name().c_str(), std::ios::binary);
                if (isJournal) {
                    auto journal = THnswJournalSnapshot::Load(input, budget);
                    journal->Reservation = reservation;
                    result->Journal = std::move(journal);
                } else {
                    result->Graph = THnswIndex::LoadGraph(input, settings, budget, result->Error);
                    if (result->Graph)
                        result->Graph->SetGraphReservation(reservation);
                }
                if (result->Error.empty() && input.peek() != std::char_traits<char>::eof())
                    result->Error = "Trailing data in HNSW snapshot";
            } catch (const std::exception& ex) {
                result->Error = ex.what();
            }
            TActivationContext::Send(new IEventHandle(replyTo, {}, result.Release()));
        });
    }

    void Handle(TEvLoaded::TPtr& ev) {
        auto& result = *ev->Get();
        auto it = Entries.find(result.Key);
        if (it == Entries.end() || it->second.TransferId != result.TransferId)
            return;
        auto& entry = it->second;
        entry.Loading = false;
        entry.TransferId = 0;
        if (!Enabled() || Tracker->GetUsed() > Tracker->GetLimit()) {
            result.Error = "HNSW cache memory admission pending or feature disabled";
            result.Graph.reset();
            result.Journal.reset();
        }
        if (!result.Error.empty()) {
            Erase(it, result.Error);
            UpdateCounters();
            return;
        }
        Counters->GetCounter("Loads", true)->Inc();
        entry.Graph = std::move(result.Graph);
        entry.Journal = std::move(result.Journal);
        entry.Reservation.reset();
        entry.TransferReservation.reset();
        Reply(entry, it->first, result.Error);
        UpdateCounters();
    }

    void Handle(TEvHnswCache::TEvForget::TPtr& ev) {
        const auto it = Entries.find(ev->Get()->Key);
        if (it != Entries.end() && it->second.Owner == ev->Sender) {
            Erase(it, "HNSW generation is no longer available");
            UpdateCounters();
        }
    }

    void HandleWakeup() {
        const auto now = TActivationContext::Monotonic();
        for (auto it = Entries.begin(); it != Entries.end();) {
            if (!Enabled() || (it->second.TransferId && now - it->second.LastAccess > TDuration::Minutes(5))
                    || (IsUnused(it->second)
                    && (now - it->second.LastAccess > TDuration::Minutes(5)
                        || Tracker->HasPendingPressure()))) {
                Erase(it++, "HNSW generation is no longer available");
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
            hFunc(TEvHnswCache::TEvChunkRequest, Handle);
            hFunc(TEvHnswCache::TEvChunkResponse, Handle);
            hFunc(TEvFileReady, Handle);
            hFunc(TEvLoaded, Handle);
            hFunc(NMemory::TEvConsumerRegistered, Handle);
            hFunc(NMemory::TEvConsumerLimit, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    bool Registered = false;
    ui64 NextTransferId = 0;
    ui64 NextExportId = 0;
    std::shared_ptr<THnswCacheMemoryTracker> Tracker = std::make_shared<THnswCacheMemoryTracker>();
    THashSet<TActorId> Clients;
    TEntries Entries;
};

} // namespace

NActors::TActorId MakeHnswCacheId(ui32 nodeId) {
    return NActors::TActorId(nodeId, TStringBuf("hnswcache"));
}

NActors::IActor* CreateHnswCache() {
    return new THnswCache;
}

} // namespace NKikimr::NDataShard
