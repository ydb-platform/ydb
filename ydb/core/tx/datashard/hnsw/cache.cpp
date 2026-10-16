#include "cache.h"
#include "serialization.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/system/tempfile.h>

#include <fstream>
#include <functional>
#include <istream>
#include <ostream>

namespace NKikimr::NDataShard {
namespace {

using namespace NActors;
constexpr ui64 ChunkBytes = 256_KB;

void WriteWord(std::ostream& out, ui64 value) {
    char data[8];
    for (unsigned i = 0; i < 8; ++i)
        data[i] = static_cast<char>(value >> (8 * i));
    out.write(data, sizeof(data));
}

ui64 ReadWord(std::istream& in) {
    unsigned char data[8];
    if (!in.read(reinterpret_cast<char*>(data), sizeof(data)))
        ythrow yexception() << "Truncated HNSW journal";
    ui64 value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= ui64(data[i]) << (8 * i);
    return value;
}

void WriteBytes(std::ostream& out, TStringBuf value) {
    WriteWord(out, value.size());
    out.write(value.data(), value.size());
}

TString ReadBytes(std::istream& in, ui64& budget) {
    const ui64 size = ReadWord(in);
    if (size > budget)
        ythrow yexception() << "HNSW journal exceeds memory budget";
    budget -= size;
    TString value = TString::Uninitialized(size);
    if (!in.read(value.Detach(), size))
        ythrow yexception() << "Truncated HNSW journal value";
    return value;
}

std::shared_ptr<void> Reserve(const std::shared_ptr<THnswCacheMemoryTracker>& tracker, ui64 bytes) {
    struct TReservation {
        std::shared_ptr<THnswCacheMemoryTracker> Tracker;
        ui64 Bytes = 0;
        ~TReservation() { Tracker->Release(Bytes); }
    };
    if (!tracker->TryAcquire(bytes))
        return {};
    auto reservation = std::make_shared<TReservation>();
    reservation->Tracker = tracker;
    reservation->Bytes = bytes;
    return reservation;
}

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
    };
    struct TEntry {
        std::shared_ptr<THnswIndex> Graph;
        std::shared_ptr<const THnswJournalSnapshot> Journal;
        Ydb::Table::VectorIndexSettings Settings;
        std::shared_ptr<TTempFileHandle> File;
        std::shared_ptr<void> Reservation;
        std::shared_ptr<void> TransferReservation;
        TVector<TWaiter> Waiters;
        TVector<TEvHnswCache::TEvChunkRequest::TPtr> ChunkRequests;
        TMonotonic LastAccess;
        ui64 MemoryBytes = 0;
        ui64 Offset = 0;
        ui64 TotalBytes = 0;
        ui64 TransferId = 0;
        ui32 SourceNodeId = 0;
        bool IsJournal = false;
        bool Exporting = false;
        bool Loading = false;
    };

public:
    void Bootstrap() {
        // Local clients use node zero, while interconnect requests carry the
        // destination node id. Publish both names for this same cache actor.
        TActivationContext::ActorSystem()->RegisterLocalService(MakeHnswCacheId(SelfId().NodeId()), SelfId());
        Counters = AppData()->Counters->GetSubgroup("subsystem", "hnsw_cache");
        Become(&TThis::StateWork);
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

private:
    void UpdateCounters() {
        ui64 graphs = 0, bytes = 0, references = 0, downloading = 0, reclaimable = 0;
        ui64 received = 0, total = 0;
        for (const auto& [_, entry] : Entries) {
            if (entry.Graph) {
                ++graphs;
                bytes += entry.Graph->EstimatedMemoryBytes();
                references += entry.Graph->GraphUseCount() - 1 + entry.Graph.use_count() - 1;
                if (entry.Graph->GraphUseCount() == 1 && entry.Graph.use_count() == 1 && !entry.Exporting)
                    reclaimable += entry.Graph->EstimatedMemoryBytes();
            }
            if (entry.Journal && entry.Journal.use_count() == 1 && !entry.Exporting)
                reclaimable += entry.MemoryBytes;
            downloading += bool(entry.TransferId);
            if (entry.TransferId) {
                received += entry.Offset;
                total += entry.TotalBytes;
            }
        }
        Counters->GetCounter("Graphs")->Set(graphs);
        Counters->GetCounter("GraphBytesEstimate")->Set(bytes);
        Counters->GetCounter("GraphReferences")->Set(references);
        Counters->GetCounter("DownloadsInFlight")->Set(downloading);
        Counters->GetCounter("DownloadReceivedBytes")->Set(received);
        Counters->GetCounter("DownloadTotalBytes")->Set(total);
        Counters->GetCounter("UsedBytes")->Set(Tracker->GetUsed());
        Tracker->SetReclaimable(reclaimable);
    }

    void StartWorker(std::function<void()> work) {
        Register(new TWorker(std::move(work)), TMailboxType::HTSwap, AppData()->BatchPoolId);
    }

    void Handle(TEvHnswCache::TEvRegister::TPtr& ev) {
        Clients.insert(ev->Sender);
        if (!Registered) {
            Registered = true;
            Send(NMemory::MakeMemoryControllerId(),
                new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::SharedCache),
                IEventHandle::FlagTrackDelivery);
        }
        auto response = MakeHolder<TEvHnswCache::TEvRegistered>();
        response->Tracker = Tracker;
        Send(ev->Sender, response.Release());
        if (Tracker->HasLimit())
            Send(ev->Sender, new NMemory::TEvConsumerLimit(Tracker->GetLimit()));
    }

    void Handle(TEvHnswCache::TEvUnregister::TPtr& ev) {
        Clients.erase(ev->Sender);
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr& ev) {
        Tracker->SetConsumer(std::move(ev->Get()->Consumer));
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr& ev) {
        const auto limit = ev->Get()->LimitBytes;
        if (Tracker->GetUsed() > limit) {
            for (auto it = Entries.begin(); it != Entries.end();) {
                if (it->second.Waiters.empty() && !it->second.Exporting && !it->second.Loading
                        && (!it->second.Graph || (it->second.Graph->GraphUseCount() == 1
                            && it->second.Graph.use_count() == 1)))
                    Entries.erase(it++);
                else
                    ++it;
            }
        }
        Tracker->SetLimit(limit);
        for (const auto& client : Clients)
            Send(client, new NMemory::TEvConsumerLimit(limit));
        UpdateCounters();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NMemory::TEvConsumerRegister::EventType) {
            Tracker->SetLimit(0);
            for (const auto& client : Clients)
                Send(client, new NMemory::TEvConsumerLimit(0));
        }
        // Transfer loss is retried by the timeout; no partial object is visible.
    }

    void Reply(TEntry& entry, const TString& key, TString error = {}) {
        if (!error.empty())
            Counters->GetCounter("AcquireErrors", true)->Add(entry.Waiters.size());
        for (const auto& waiter : entry.Waiters) {
            auto result = MakeHolder<TEvHnswCache::TEvAcquired>();
            result->Key = key;
            result->RequestId = waiter.RequestId;
            result->Error = error;
            result->Graph = entry.Graph;
            result->Journal = entry.Journal;
            Send(waiter.Actor, result.Release());
        }
        entry.Waiters.clear();
    }

    void Handle(TEvHnswCache::TEvPublish::TPtr& ev) {
        auto& message = *ev->Get();
        if (message.Key.empty() || (!message.Graph && !message.Journal))
            return;
        auto& entry = Entries[message.Key];
        entry.LastAccess = TActivationContext::Monotonic();
        entry.Settings = std::move(message.Settings);
        entry.IsJournal = bool(message.Journal);
        entry.Graph = std::move(message.Graph);
        entry.Journal = std::move(message.Journal);
        entry.MemoryBytes = entry.Graph ? entry.Graph->EstimatedMemoryBytes() : 4096;
        if (entry.Journal) {
            for (const auto& change : entry.Journal->Changes)
                entry.MemoryBytes += THnswIndexChanges::EstimateBytes(change.Key,
                    change.Vector ? change.Vector->size() : 0);
        }
        Reply(entry, message.Key);
        UpdateCounters();
    }

    void RequestChunk(const TString& key, TEntry& entry) {
        auto request = MakeHolder<TEvHnswCache::TEvChunkRequest>();
        request->Record.SetKey(key);
        request->Record.SetOffset(entry.Offset);
        Send(MakeHnswCacheId(entry.SourceNodeId), request.Release(),
            IEventHandle::FlagTrackDelivery, entry.TransferId);
    }

    void Handle(TEvHnswCache::TEvAcquire::TPtr& ev) {
        const auto& request = *ev->Get();
        auto& entry = Entries[request.Key];
        entry.LastAccess = TActivationContext::Monotonic();
        if ((entry.Graph || entry.Journal || entry.TransferId)
                && (entry.IsJournal != request.Journal
                    || (!request.Journal && !AreHnswIndexSettingsCompatible(entry.Settings, request.Settings)))) {
            auto response = MakeHolder<TEvHnswCache::TEvAcquired>();
            response->Key = request.Key;
            response->RequestId = request.RequestId;
            response->Error = "Incompatible HNSW cache generation";
            Send(ev->Sender, response.Release());
            Counters->GetCounter("AcquireErrors", true)->Inc();
            return;
        }
        entry.Waiters.push_back({ev->Sender, request.RequestId});
        if (entry.Graph || entry.Journal) {
            Counters->GetCounter("SharedAcquires", true)->Inc();
            Reply(entry, request.Key);
            return;
        }
        if (entry.TransferId)
            return;
        entry.MemoryBytes = request.MemoryBytes;
        entry.IsJournal = request.Journal;
        entry.SourceNodeId = request.SourceNodeId;
        entry.Settings = request.Settings;
        if (!entry.MemoryBytes || !entry.SourceNodeId || entry.SourceNodeId == SelfId().NodeId()) {
            Reply(entry, request.Key, "HNSW generation is no longer available");
            Entries.erase(request.Key);
            return;
        }
        entry.Reservation = Reserve(Tracker, entry.MemoryBytes);
        entry.TransferReservation = Reserve(Tracker, 2 * ChunkBytes + 8192);
        if (!entry.Reservation || !entry.TransferReservation) {
            Reply(entry, request.Key, "HNSW cache memory admission pending");
            Entries.erase(request.Key);
            return;
        }
        entry.TransferId = ++NextTransferId;
        Counters->GetCounter("Downloads", true)->Inc();
        entry.File = std::make_shared<TTempFileHandle>();
        RequestChunk(request.Key, entry);
    }

    void StartExport(const TString& key, TEntry& entry) {
        entry.Exporting = true;
        StartWorker([replyTo = SelfId(), key, graph = entry.Graph, journal = entry.Journal] {
            auto result = MakeHolder<TEvFileReady>();
            result->Key = key;
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
                    response->BufferReservation = Reserve(Tracker, 2 * bytes + 4096);
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
        if (it == Entries.end()) {
            ReplyChunk(ev, nullptr);
            return;
        }
        auto& entry = it->second;
        entry.LastAccess = TActivationContext::Monotonic();
        if (entry.File && !entry.Exporting && !entry.TransferId) {
            ReplyChunk(ev, &entry);
        } else if (entry.Graph || entry.Journal) {
            entry.ChunkRequests.push_back(ev);
            if (!entry.Exporting)
                StartExport(it->first, entry);
        } else {
            ReplyChunk(ev, nullptr);
        }
    }

    void Handle(TEvFileReady::TPtr& ev) {
        auto& result = *ev->Get();
        auto it = Entries.find(result.Key);
        if (it == Entries.end())
            return;
        auto& entry = it->second;
        entry.Exporting = false;
        entry.File = std::move(result.File);
        for (auto& request : entry.ChunkRequests)
            ReplyChunk(request, &entry, result.Error);
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
        if (response.GetOffset() != entry.Offset || response.GetData().size() > ChunkBytes
                || response.GetTotalBytes() < entry.Offset
                || response.GetData().size() > response.GetTotalBytes() - entry.Offset
                || entry.MemoryBytes > (Max<ui64>() - 1_MB) / 2
                || response.GetTotalBytes() > entry.MemoryBytes * 2 + 1_MB
                || (entry.TotalBytes && entry.TotalBytes != response.GetTotalBytes())
                || (response.GetData().empty() && entry.Offset != response.GetTotalBytes()))
            error = "Invalid HNSW transfer chunk";
        if (!error.empty()) {
            Reply(entry, it->first, error);
            Entries.erase(it);
            return;
        }
        try {
            entry.File->Pwrite(response.GetData().data(), response.GetData().size(), entry.Offset);
        } catch (const std::exception& ex) {
            Reply(entry, it->first, ex.what());
            Entries.erase(it);
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
                settings = entry.Settings, budget = entry.MemoryBytes, tracker = Tracker] {
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
                        result->Graph->SetGraphReservation(reservation, tracker);
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
        if (result.Error.empty())
            Counters->GetCounter("Loads", true)->Inc();
        entry.Graph = std::move(result.Graph);
        entry.Journal = std::move(result.Journal);
        entry.Reservation.reset();
        entry.TransferReservation.reset();
        Reply(entry, it->first, result.Error);
        if (!result.Error.empty())
            Entries.erase(it);
        UpdateCounters();
    }

    void HandleWakeup() {
        const auto now = TActivationContext::Monotonic();
        for (auto it = Entries.begin(); it != Entries.end();) {
            auto& entry = it->second;
            const bool expired = now - entry.LastAccess > TDuration::Minutes(5);
            const bool pressure = Tracker->HasPendingPressure() && entry.Waiters.empty();
            if ((expired || pressure) && !entry.Exporting && !entry.Loading
                    && (!entry.Graph || (entry.Graph->GraphUseCount() == 1 && entry.Graph.use_count() == 1))) {
                Reply(entry, it->first, "HNSW transfer expired");
                Entries.erase(it++);
            } else {
                ++it;
            }
        }
        UpdateCounters();
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHnswCache::TEvRegister, Handle);
            hFunc(TEvHnswCache::TEvUnregister, Handle);
            hFunc(TEvHnswCache::TEvPublish, Handle);
            hFunc(TEvHnswCache::TEvAcquire, Handle);
            hFunc(TEvHnswCache::TEvChunkRequest, Handle);
            hFunc(TEvHnswCache::TEvChunkResponse, Handle);
            hFunc(TEvFileReady, Handle);
            hFunc(TEvLoaded, Handle);
            hFunc(NMemory::TEvConsumerRegistered, Handle);
            hFunc(NMemory::TEvConsumerLimit, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
        }
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    bool Registered = false;
    ui64 NextTransferId = 0;
    std::shared_ptr<THnswCacheMemoryTracker> Tracker = std::make_shared<THnswCacheMemoryTracker>();
    THashSet<TActorId> Clients;
    THashMap<TString, TEntry> Entries;
};

} // namespace

void THnswJournalSnapshot::Save(std::ostream& destination) const {
    NHnswSerialization::TSnapshotStreamBuf buffer(destination.rdbuf());
    std::ostream output(&buffer);
    output.exceptions(std::ios::badbit | std::ios::failbit);
    WriteWord(output, 1);
    WriteWord(output, Changes.size());
    for (const auto& change : Changes) {
        WriteBytes(output, change.Key);
        WriteWord(output, change.Version.Step);
        WriteWord(output, change.Version.TxId);
        WriteWord(output, bool(change.Vector));
        if (change.Vector)
            WriteBytes(output, *change.Vector);
    }
    WriteWord(destination, buffer.Hash);
}

std::shared_ptr<THnswJournalSnapshot> THnswJournalSnapshot::Load(std::istream& source, ui64 budget) {
    NHnswSerialization::TSnapshotStreamBuf buffer(source.rdbuf());
    std::istream input(&buffer);
    if (ReadWord(input) != 1)
        ythrow yexception() << "Unsupported HNSW journal format";
    const auto count = ReadWord(input);
    if (count > budget / sizeof(TChange))
        ythrow yexception() << "HNSW journal exceeds memory budget";
    budget -= count * sizeof(TChange);
    auto result = std::make_shared<THnswJournalSnapshot>();
    result->Changes.reserve(count);
    for (ui64 i = 0; i < count; ++i) {
        TChange change;
        change.Key = ReadBytes(input, budget);
        change.Version.Step = ReadWord(input);
        change.Version.TxId = ReadWord(input);
        const auto present = ReadWord(input);
        if (present > 1)
            ythrow yexception() << "Invalid HNSW journal entry";
        if (present)
            change.Vector = ReadBytes(input, budget);
        result->Changes.push_back(std::move(change));
    }
    if (ReadWord(source) != buffer.Hash)
        ythrow yexception() << "HNSW journal checksum mismatch";
    return result;
}

NActors::TActorId MakeHnswCacheId(ui32 nodeId) {
    return NActors::TActorId(nodeId, TStringBuf("hnswcache"));
}

NActors::IActor* CreateHnswCache() {
    return new THnswCache;
}

} // namespace NKikimr::NDataShard
