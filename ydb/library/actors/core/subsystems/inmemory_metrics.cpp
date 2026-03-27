#include "inmemory_metrics.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/ymath.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <queue>

namespace NActors {

    struct TTimeAnchor {
        NHPTimer::STime BaseCycles = 0;
        TInstant BaseWallClock;
    };

    enum class ELineState : ui8 {
        Open,
        Closed,
    };

    enum class EChunkState : ui8 {
        Free,
        Writable,
        Sealed,
        Retiring,
    };

    struct TChunk {
        explicit TChunk(ui32 chunkId, ui32 chunkSize)
            : ChunkId(chunkId)
            , Payload(chunkSize)
        {
        }

        ui32 ChunkId = 0;
        std::atomic<EChunkState> State = EChunkState::Free;
        std::atomic<i32> Readers = 0;
        std::atomic<ui32> CommittedBytes = 0;
        std::atomic<ui64> Generation = 0;
        std::atomic<NHPTimer::STime> FirstTs = 0;
        std::atomic<NHPTimer::STime> LastTs = 0;

        std::atomic<TLine*> Owner = nullptr;
        std::atomic<ui32> OwnerLineId = 0;
        TVector<char> Payload;
    };

    class TLine {
    public:
        ui32 LineId = 0;
        TLineKey Key;
        std::atomic<ELineState> State = ELineState::Open;

        mutable TMutex Lock;
        std::atomic<TChunk*> Writable = nullptr;
        TVector<TChunk*> Chunks;
    };

    struct TSnapshotPinnedChunk {
        TInMemoryMetricsRegistry* Registry = nullptr;
        TChunk* Chunk = nullptr;
        TChunkView View;
    };

    struct TSnapshotData {
        ~TSnapshotData();

        TTimeAnchor Anchor;
        TVector<TSnapshotPinnedChunk> Chunks;
    };

    struct TVictimKey {
        NHPTimer::STime LastTs = 0;
        bool Closed = false;
        ui32 ChunkId = 0;
        ui64 Generation = 0;
        TChunk* Chunk = nullptr;
    };

    struct TVictimCompare {
        bool operator()(const TVictimKey& lhs, const TVictimKey& rhs) const noexcept {
            if (lhs.LastTs != rhs.LastTs) {
                return lhs.LastTs > rhs.LastTs;
            }
            if (lhs.Closed != rhs.Closed) {
                return lhs.Closed < rhs.Closed;
            }
            return lhs.ChunkId > rhs.ChunkId;
        }
    };

    namespace {
        constexpr i32 RetiringBias = std::numeric_limits<i32>::min();

        TInstant DecodeTs(const TTimeAnchor& anchor, NHPTimer::STime ts) noexcept {
            return anchor.BaseWallClock + TDuration::MicroSeconds(Ts2Us(ts - anchor.BaseCycles));
        }

        bool TryPinChunk(TChunk* chunk) noexcept {
            i32 readers = chunk->Readers.load(std::memory_order_acquire);
            while (readers >= 0) {
                if (chunk->Readers.compare_exchange_weak(readers, readers + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return true;
                }
            }
            return false;
        }

    } // namespace

    class TInMemoryMetricsRegistry::TImpl {
    public:
        explicit TImpl(TInMemoryMetricsConfig cfg)
            : Config(std::move(cfg))
            , ChunkCount(Config.ChunkSizeBytes ? Config.MemoryBytes / Config.ChunkSizeBytes : 0)
            , MaxLines(Config.MaxLines ? Config.MaxLines : ChunkCount / 2)
        {
            TimeAnchor.BaseCycles = GetCycleCountFast();
            TimeAnchor.BaseWallClock = TInstant::Now();

            Storage.reserve(ChunkCount);
            FreeList.reserve(ChunkCount);
            for (ui32 i = 0; i < ChunkCount; ++i) {
                auto chunk = std::make_unique<TChunk>(i, Config.ChunkSizeBytes);
                FreeList.push_back(chunk.get());
                Storage.push_back(std::move(chunk));
            }
        }

        TInMemoryMetricsConfig Config;
        ui32 ChunkCount = 0;
        ui32 MaxLines = 0;

        std::atomic<ui32> NextLineId = 1;
        std::atomic<ui64> ReuseWatermark = 0;
        std::atomic<bool> ShuttingDown = false;

        TTimeAnchor TimeAnchor;

        mutable TMutex RegistryLock;
        THashMap<TLineKey, std::unique_ptr<TLine>, TLineKeyHash> LinesByKey;
        THashMap<ui32, TLine*> LinesById;

        mutable TMutex VictimLock;
        TVector<std::unique_ptr<TChunk>> Storage;
        TVector<TChunk*> FreeList;
        std::priority_queue<TVictimKey, TVector<TVictimKey>, TVictimCompare> VictimHeap;
    };

    TSnapshotData::~TSnapshotData() {
        for (const auto& chunk : Chunks) {
            if (chunk.Registry && chunk.Chunk) {
                chunk.Registry->ReleasePinnedChunk(chunk.Chunk);
            }
        }
    }

    bool TLabel::operator==(const TLabel& rhs) const noexcept {
        return Name == rhs.Name && Value == rhs.Value;
    }

    bool TLineKey::operator==(const TLineKey& rhs) const noexcept {
        return Name == rhs.Name && Labels == rhs.Labels;
    }

    size_t TLineKeyHash::operator()(const TLineKey& key) const noexcept {
        size_t hash = THash<TString>()(key.Name);
        for (const auto& label : key.Labels) {
            hash = CombineHashes(hash, THash<TString>()(label.Name));
            hash = CombineHashes(hash, THash<TString>()(label.Value));
        }
        return hash;
    }

    TLineWriter::TLineWriter(TInMemoryMetricsRegistry* registry, TLine* line) noexcept
        : Registry(registry)
        , Line(line)
    {
    }

    TLineWriter::TLineWriter(TLineWriter&& rhs) noexcept
        : Registry(rhs.Registry)
        , Line(rhs.Line)
    {
        rhs.Registry = nullptr;
        rhs.Line = nullptr;
    }

    TLineWriter& TLineWriter::operator=(TLineWriter&& rhs) noexcept {
        if (this != &rhs) {
            Close();
            Registry = rhs.Registry;
            Line = rhs.Line;
            rhs.Registry = nullptr;
            rhs.Line = nullptr;
        }
        return *this;
    }

    TLineWriter::~TLineWriter() {
        Close();
    }

    TLineWriter::operator bool() const noexcept {
        return Registry && Line;
    }

    bool TLineWriter::Append(ui64 value) noexcept {
        return Registry && Line ? Registry->Append(Line, value) : false;
    }

    void TLineWriter::Close() noexcept {
        if (Registry && Line) {
            Registry->CloseLine(Line);
            Registry = nullptr;
            Line = nullptr;
        }
    }

    ui32 TLineWriter::GetLineId() const noexcept {
        return Line ? Line->LineId : 0;
    }

    TLineSnapshot::TLineSnapshot() = default;
    TLineSnapshot::TLineSnapshot(const TLineSnapshot&) = default;
    TLineSnapshot::TLineSnapshot(TLineSnapshot&&) noexcept = default;
    TLineSnapshot& TLineSnapshot::operator=(const TLineSnapshot&) = default;
    TLineSnapshot& TLineSnapshot::operator=(TLineSnapshot&&) noexcept = default;
    TLineSnapshot::~TLineSnapshot() = default;

    void TLineSnapshot::ForEachRecord(const std::function<void(const TRecordView&)>& cb) const {
        if (!Data) {
            return;
        }
        for (size_t chunkIndex : ChunkIndexes) {
            const auto& pinned = Data->Chunks[chunkIndex];
            const auto* records = reinterpret_cast<const TStoredRecord*>(pinned.Chunk->Payload.data());
            const ui32 recordsCount = pinned.View.CommittedBytes / sizeof(TStoredRecord);
            for (ui32 i = 0; i < recordsCount; ++i) {
                cb(TRecordView{
                    .Timestamp = DecodeTs(Data->Anchor, records[i].TimestampTs),
                    .Value = records[i].Value,
                });
            }
        }
    }

    TSnapshot::TSnapshot() = default;
    TSnapshot::TSnapshot(const TSnapshot&) = default;
    TSnapshot::TSnapshot(TSnapshot&&) noexcept = default;
    TSnapshot& TSnapshot::operator=(const TSnapshot&) = default;
    TSnapshot& TSnapshot::operator=(TSnapshot&&) noexcept = default;
    TSnapshot::~TSnapshot() = default;

    TVector<TLineSnapshot> TSnapshot::Lines() const {
        return SnapshotLines;
    }

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config) {
        return std::make_unique<TInMemoryMetricsRegistry>(std::move(config));
    }

    TInMemoryMetricsRegistry::TInMemoryMetricsRegistry(TInMemoryMetricsConfig config)
        : Impl(std::make_unique<TImpl>(std::move(config)))
    {
    }

    TInMemoryMetricsRegistry::~TInMemoryMetricsRegistry() = default;

    namespace {
        TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> commonLabels, std::span<const TLabel> labels) {
            TLineKey key;
            key.Name = TString(name);
            key.Labels.reserve(commonLabels.size() + labels.size());
            for (const auto& label : commonLabels) {
                key.Labels.push_back(label);
            }
            for (const auto& label : labels) {
                key.Labels.push_back(label);
            }
            return key;
        }

        void ResetChunkForWrite(TChunk* chunk, TLine* line) {
            chunk->Owner.store(line, std::memory_order_release);
            chunk->OwnerLineId.store(line->LineId, std::memory_order_release);
            chunk->CommittedBytes.store(0, std::memory_order_release);
            chunk->Readers.store(0, std::memory_order_release);
            chunk->FirstTs.store(0, std::memory_order_release);
            chunk->LastTs.store(0, std::memory_order_release);
            chunk->Generation.fetch_add(1, std::memory_order_acq_rel);
            chunk->State.store(EChunkState::Writable, std::memory_order_release);
        }

        void FinalizeReturnedChunk(TChunk* chunk) {
            chunk->Owner.store(nullptr, std::memory_order_release);
            chunk->OwnerLineId.store(0, std::memory_order_release);
            chunk->CommittedBytes.store(0, std::memory_order_release);
            chunk->FirstTs.store(0, std::memory_order_release);
            chunk->LastTs.store(0, std::memory_order_release);
            chunk->Readers.store(0, std::memory_order_release);
            chunk->Generation.fetch_add(1, std::memory_order_acq_rel);
            chunk->State.store(EChunkState::Free, std::memory_order_release);
        }
    }

    TLineWriter TInMemoryMetricsRegistry::CreateLine(TStringBuf name, std::span<const TLabel> labels) {
        auto key = MakeLineKey(name, Impl->Config.CommonLabels, labels);

        TGuard<TMutex> guard(Impl->RegistryLock);
        if (Impl->LinesByKey.contains(key)) {
            return {};
        }

        if (Impl->LinesByKey.size() >= Impl->MaxLines) {
            return {};
        }

        auto line = std::make_unique<TLine>();
        line->LineId = Impl->NextLineId.fetch_add(1, std::memory_order_relaxed);
        line->Key = std::move(key);

        TLine* linePtr = line.get();
        Impl->LinesById.emplace(linePtr->LineId, linePtr);
        Impl->LinesByKey.emplace(linePtr->Key, std::move(line));
        return TLineWriter(this, linePtr);
    }

    namespace {
        bool RemoveChunkFromLineLocked(TLine* line, TChunk* chunk) {
            if (line->Writable.load(std::memory_order_acquire) == chunk) {
                line->Writable.store(nullptr, std::memory_order_release);
            }
            auto it = Find(line->Chunks, chunk);
            if (it != line->Chunks.end()) {
                line->Chunks.erase(it);
            }
            return line->State.load(std::memory_order_acquire) == ELineState::Closed && line->Chunks.empty();
        }
    }

    void TInMemoryMetricsRegistry::MaybeDropClosedLine(TLine* line) {
        TGuard<TMutex> guard(Impl->RegistryLock);
        auto it = Impl->LinesById.find(line->LineId);
        if (it == Impl->LinesById.end()) {
            return;
        }

        TLineKey key;
        TGuard<TMutex> lineGuard(line->Lock);
        if (line->State.load(std::memory_order_acquire) != ELineState::Closed || !line->Chunks.empty()) {
            return;
        }
        key = line->Key;
        lineGuard.Release();

        Impl->LinesById.erase(it);
        Impl->LinesByKey.erase(key);
    }

    TChunk* TInMemoryMetricsRegistry::TryAcquireFreeChunk() {
        TGuard<TMutex> guard(Impl->VictimLock);
        if (Impl->FreeList.empty()) {
            return nullptr;
        }

        TChunk* chunk = Impl->FreeList.back();
        Impl->FreeList.pop_back();
        return chunk;
    }

    void TInMemoryMetricsRegistry::PublishSealedChunk(TChunk* chunk) {
        TGuard<TMutex> guard(Impl->VictimLock);
        chunk->Generation.fetch_add(1, std::memory_order_acq_rel);
        TLine* owner = chunk->Owner.load(std::memory_order_acquire);
        Impl->VictimHeap.push(TVictimKey{
            .LastTs = chunk->LastTs.load(std::memory_order_acquire),
            .Closed = owner && owner->State.load(std::memory_order_acquire) == ELineState::Closed,
            .ChunkId = chunk->ChunkId,
            .Generation = chunk->Generation.load(std::memory_order_acquire),
            .Chunk = chunk,
        });
    }

    void TInMemoryMetricsRegistry::ReturnChunkToFree(TChunk* chunk) {
        TGuard<TMutex> guard(Impl->VictimLock);
        FinalizeReturnedChunk(chunk);
        Impl->FreeList.push_back(chunk);
    }

    void TInMemoryMetricsRegistry::ReleasePinnedChunk(TChunk* chunk) {
        const i32 prev = chunk->Readers.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == RetiringBias + 1) {
            ReturnChunkToFree(chunk);
        }
    }

    void TInMemoryMetricsRegistry::RetireChunk(TChunk* chunk) {
        i32 readers = chunk->Readers.load(std::memory_order_acquire);
        while (true) {
            Y_ABORT_UNLESS(readers >= 0);
            if (chunk->Readers.compare_exchange_weak(readers, RetiringBias + readers, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        chunk->State.store(EChunkState::Retiring, std::memory_order_release);
        if (readers == 0) {
            ReturnChunkToFree(chunk);
        }
    }

    TChunk* TInMemoryMetricsRegistry::TryStealOldestChunk() {
        while (true) {
            TChunk* victim = nullptr;
            ui64 generation = 0;
            {
                TGuard<TMutex> guard(Impl->VictimLock);
                while (!Impl->VictimHeap.empty()) {
                    const auto top = Impl->VictimHeap.top();
                    Impl->VictimHeap.pop();
                    victim = top.Chunk;
                    generation = top.Generation;
                    if (victim
                        && victim->State.load(std::memory_order_acquire) == EChunkState::Sealed
                        && victim->Generation.load(std::memory_order_acquire) == generation) {
                        break;
                    }
                    victim = nullptr;
                }
            }

            if (!victim) {
                return nullptr;
            }

            bool shouldDropLine = false;
            TLine* owner = victim->Owner.load(std::memory_order_acquire);
            NHPTimer::STime lastTs = 0;
            {
                TGuard<TMutex> lineGuard(owner->Lock);
                if (victim->State.load(std::memory_order_acquire) != EChunkState::Sealed
                    || victim->Generation.load(std::memory_order_acquire) != generation) {
                    continue;
                }

                shouldDropLine = RemoveChunkFromLineLocked(owner, victim);
                lastTs = victim->LastTs.load(std::memory_order_acquire);
            }

            ui64 watermark = Impl->ReuseWatermark.load(std::memory_order_relaxed);
            while (watermark < static_cast<ui64>(lastTs)
                   && !Impl->ReuseWatermark.compare_exchange_weak(watermark, static_cast<ui64>(lastTs), std::memory_order_acq_rel, std::memory_order_relaxed)) {
            }

            RetireChunk(victim);

            if (shouldDropLine) {
                MaybeDropClosedLine(owner);
            }

            if (victim->State.load(std::memory_order_acquire) == EChunkState::Free) {
                TGuard<TMutex> guard(Impl->VictimLock);
                if (!Impl->FreeList.empty() && Impl->FreeList.back() == victim) {
                    Impl->FreeList.pop_back();
                    return victim;
                }
                auto it = Find(Impl->FreeList, victim);
                if (it != Impl->FreeList.end()) {
                    Impl->FreeList.erase(it);
                    return victim;
                }
            }
        }
    }

    namespace {
        bool TryAppendToChunk(TChunk* chunk, const TStoredRecord& record) {
            const ui32 offset = chunk->CommittedBytes.load(std::memory_order_relaxed);
            if (offset + sizeof(TStoredRecord) > chunk->Payload.size()) {
                return false;
            }
            memcpy(chunk->Payload.data() + offset, &record, sizeof(TStoredRecord));
            if (offset == 0) {
                chunk->FirstTs.store(record.TimestampTs, std::memory_order_relaxed);
            }
            chunk->LastTs.store(record.TimestampTs, std::memory_order_relaxed);
            chunk->CommittedBytes.store(offset + sizeof(TStoredRecord), std::memory_order_release);
            return true;
        }

        bool CanFitRecord(const TChunk* chunk) {
            return sizeof(TStoredRecord) <= chunk->Payload.size();
        }
    }

    bool TInMemoryMetricsRegistry::Append(TLine* line, ui64 value) noexcept {
        if (!line || Impl->ShuttingDown.load(std::memory_order_acquire)) {
            return false;
        }
        if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
            return false;
        }

        const TStoredRecord record{
            .TimestampTs = static_cast<NHPTimer::STime>(GetCycleCountFast()),
            .Value = value,
        };

        while (true) {
            TChunk* chunk = line->Writable.load(std::memory_order_acquire);
            if (chunk && TryAppendToChunk(chunk, record)) {
                return true;
            }

            TChunk* sealedChunk = nullptr;
            {
                TGuard<TMutex> lineGuard(line->Lock);
                if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
                    return false;
                }

                chunk = line->Writable.load(std::memory_order_acquire);
                if (chunk && TryAppendToChunk(chunk, record)) {
                    return true;
                }

                if (chunk && chunk->CommittedBytes.load(std::memory_order_acquire) == 0 && !CanFitRecord(chunk)) {
                    return false;
                }

                if (chunk) {
                    chunk->State.store(EChunkState::Sealed, std::memory_order_release);
                    line->Writable.store(nullptr, std::memory_order_release);
                    sealedChunk = chunk;
                }
            }

            if (sealedChunk) {
                PublishSealedChunk(sealedChunk);
            }

            TChunk* newChunk = TryAcquireFreeChunk();
            if (!newChunk) {
                newChunk = TryStealOldestChunk();
            }
            if (!newChunk) {
                return false;
            }
            if (!CanFitRecord(newChunk)) {
                ReturnChunkToFree(newChunk);
                return false;
            }

            TGuard<TMutex> lineGuard(line->Lock);
            if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
                ReturnChunkToFree(newChunk);
                return false;
            }
            if (line->Writable.load(std::memory_order_acquire)) {
                ReturnChunkToFree(newChunk);
                continue;
            }

            ResetChunkForWrite(newChunk, line);
            line->Writable.store(newChunk, std::memory_order_release);
            line->Chunks.push_back(newChunk);
        }
    }

    void TInMemoryMetricsRegistry::CloseLine(TLine* line) noexcept {
        if (!line) {
            return;
        }

        bool shouldDrop = false;
        TChunk* sealedChunk = nullptr;
        TChunk* freeChunk = nullptr;
        {
            TGuard<TMutex> guard(line->Lock);
            line->State.store(ELineState::Closed, std::memory_order_release);
            if (TChunk* writable = line->Writable.exchange(nullptr, std::memory_order_acq_rel)) {
                if (writable->CommittedBytes.load(std::memory_order_acquire)) {
                    writable->State.store(EChunkState::Sealed, std::memory_order_release);
                    sealedChunk = writable;
                } else {
                    RemoveChunkFromLineLocked(line, writable);
                    freeChunk = writable;
                }
            }
            shouldDrop = line->Chunks.empty();
        }

        if (sealedChunk) {
            PublishSealedChunk(sealedChunk);
        }
        if (freeChunk) {
            ReturnChunkToFree(freeChunk);
        }
        if (shouldDrop) {
            MaybeDropClosedLine(line);
        }
    }

    TSnapshot TInMemoryMetricsRegistry::Snapshot() const {
        TSnapshot snapshot;
        auto data = std::make_shared<TSnapshotData>();
        data->Anchor = Impl->TimeAnchor;

        TGuard<TMutex> registryGuard(Impl->RegistryLock);
        snapshot.SnapshotLines.reserve(Impl->LinesById.size());

        for (const auto& [lineId, line] : Impl->LinesById) {
            Y_UNUSED(lineId);
            TLineSnapshot lineSnapshot;
            lineSnapshot.Data = data;
            lineSnapshot.LineId = line->LineId;
            lineSnapshot.Name = line->Key.Name;
            lineSnapshot.Labels = line->Key.Labels;

            TGuard<TMutex> lineGuard(line->Lock);
            lineSnapshot.Closed = line->State.load(std::memory_order_acquire) == ELineState::Closed;
            lineSnapshot.Chunks.reserve(line->Chunks.size());
            lineSnapshot.ChunkIndexes.reserve(line->Chunks.size());

            for (TChunk* chunk : line->Chunks) {
                if (!TryPinChunk(chunk)) {
                    continue;
                }

                lineSnapshot.ChunkIndexes.push_back(data->Chunks.size());
                lineSnapshot.Chunks.push_back(TChunkView{
                    .ChunkId = chunk->ChunkId,
                    .FirstTs = DecodeTs(data->Anchor, chunk->FirstTs.load(std::memory_order_acquire)),
                    .LastTs = DecodeTs(data->Anchor, chunk->LastTs.load(std::memory_order_acquire)),
                    .CommittedBytes = chunk->CommittedBytes.load(std::memory_order_acquire),
                });
                data->Chunks.push_back(TSnapshotPinnedChunk{
                    .Registry = const_cast<TInMemoryMetricsRegistry*>(this),
                    .Chunk = chunk,
                    .View = lineSnapshot.Chunks.back(),
                });
            }

            if (!lineSnapshot.Chunks.empty() || lineSnapshot.Closed) {
                snapshot.SnapshotLines.push_back(std::move(lineSnapshot));
            }
        }

        return snapshot;
    }

    ui64 TInMemoryMetricsRegistry::GetReuseWatermark() const noexcept {
        return Impl->ReuseWatermark.load(std::memory_order_acquire);
    }

    const TInMemoryMetricsConfig& TInMemoryMetricsRegistry::GetConfig() const noexcept {
        return Impl->Config;
    }

    void TInMemoryMetricsRegistry::OnBeforeStop(TActorSystem&) {
        Impl->ShuttingDown.store(true, std::memory_order_release);
    }

    TInMemoryMetricsRegistry& GetInMemoryMetrics(TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TInMemoryMetricsRegistry>();
        Y_ABORT_UNLESS(subSystem, "actor system in-memory metrics subsystem is not registered");
        return *subSystem;
    }

    const TInMemoryMetricsRegistry& GetInMemoryMetrics(const TActorSystem& actorSystem) {
        auto* subSystem = actorSystem.GetSubSystem<TInMemoryMetricsRegistry>();
        Y_ABORT_UNLESS(subSystem, "actor system in-memory metrics subsystem is not registered");
        return *subSystem;
    }

    TInMemoryMetricsRegistry& GetInMemoryMetrics() {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto* subSystem = actorSystem->GetSubSystem<TInMemoryMetricsRegistry>();
        Y_ABORT_UNLESS(subSystem, "actor system in-memory metrics subsystem is not registered");
        return *subSystem;
    }

} // namespace NActors
