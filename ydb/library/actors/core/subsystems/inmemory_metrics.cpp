#include "inmemory_metrics.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/util/datetime.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/ymath.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>
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

    struct TLineStorage {
        mutable TMutex Lock;
        std::atomic<TChunk*> Writable = nullptr;
        TVector<TChunk*> Chunks;
    };

    class TLine {
    public:
        ui32 LineId = 0;
        TLineKey Key;
        TLineMeta Meta;
        std::atomic<ELineState> State = ELineState::Open;
        bool HasLastPublished = false;
        ui64 LastPublishedValue = 0;
        NHPTimer::STime LastPublishedTs = 0;
        NHPTimer::STime LastObservedTs = 0;
        TLineStorage Storage;
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
        constexpr TStringBuf RegistryMetricsPrefix = "inmemory_metrics.";
        constexpr TStringBuf RegistryMemoryUsedBytesMetric = "inmemory_metrics.memory_used_bytes";
        constexpr TStringBuf RegistryCommittedBytesMetric = "inmemory_metrics.committed_bytes";
        constexpr TStringBuf RegistryFreeChunksMetric = "inmemory_metrics.free_chunks";
        constexpr TStringBuf RegistryUsedChunksMetric = "inmemory_metrics.used_chunks";
        constexpr TStringBuf RegistrySealedChunksMetric = "inmemory_metrics.sealed_chunks";
        constexpr TStringBuf RegistryWritableChunksMetric = "inmemory_metrics.writable_chunks";
        constexpr TStringBuf RegistryRetiringChunksMetric = "inmemory_metrics.retiring_chunks";
        constexpr TStringBuf RegistryLinesMetric = "inmemory_metrics.lines";
        constexpr TStringBuf RegistryClosedLinesMetric = "inmemory_metrics.closed_lines";
        constexpr TStringBuf RegistryReuseWatermarkMetric = "inmemory_metrics.reuse_watermark";
        constexpr TStringBuf RegistryAppendFailuresTotalMetric = "inmemory_metrics.append_failures_total";

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

        bool IsRegistryMetricName(TStringBuf name) noexcept {
            return name.StartsWith(RegistryMetricsPrefix);
        }

        bool ShouldPublishRecord(TLine* line, ui64 value, NHPTimer::STime nowTs) noexcept {
            switch (line->Meta.PublishPolicy) {
                case EPublishPolicy::Raw:
                    return true;
                case EPublishPolicy::OnChange:
                    return !line->HasLastPublished || line->LastPublishedValue != value;
                case EPublishPolicy::OnChangeWithHeartbeat:
                    if (!line->HasLastPublished || line->LastPublishedValue != value) {
                        return true;
                    }
                    if (line->Meta.Heartbeat <= TDuration::Zero()) {
                        return false;
                    }
                    return nowTs - line->LastPublishedTs >= static_cast<NHPTimer::STime>(Us2Ts(line->Meta.Heartbeat.MicroSeconds()));
            }
        }

        bool ShouldPublishPreviousObservedValue(const TLine* line, ui64 newValue) noexcept {
            return line->Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat
                && line->HasLastPublished
                && line->LastPublishedValue != newValue
                && line->LastObservedTs > line->LastPublishedTs;
        }

        void MarkRecordObserved(TLine* line, NHPTimer::STime nowTs) noexcept {
            line->LastObservedTs = nowTs;
        }

        void MarkRecordPublished(TLine* line, ui64 value, NHPTimer::STime nowTs) noexcept {
            line->HasLastPublished = true;
            line->LastPublishedValue = value;
            line->LastPublishedTs = nowTs;
            line->LastObservedTs = nowTs;
        }

    } // namespace

    class TInMemoryMetricsRegistry::TImpl {
    public:
        struct TSelfMetricsLines {
            ui32 MemoryUsedBytes = 0;
            ui32 CommittedBytes = 0;
            ui32 FreeChunks = 0;
            ui32 UsedChunks = 0;
            ui32 SealedChunks = 0;
            ui32 WritableChunks = 0;
            ui32 RetiringChunks = 0;
            ui32 Lines = 0;
            ui32 ClosedLines = 0;
            ui32 ReuseWatermark = 0;
            ui32 AppendFailuresTotal = 0;
        };

        explicit TImpl(TInMemoryMetricsConfig cfg)
            : Config(std::move(cfg))
            , ChunkCount(Config.ChunkSizeBytes ? Config.MemoryBytes / Config.ChunkSizeBytes : 0)
            , MaxLines(Config.MaxLines ? Config.MaxLines : ChunkCount / 2)
            , CommonLabels(Config.CommonLabels)
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
        std::atomic<ui64> AppendFailures = 0;
        std::atomic<bool> ShuttingDown = false;

        TTimeAnchor TimeAnchor;

        mutable TMutex RegistryLock;
        THashMap<TLineKey, std::unique_ptr<TLine>, TLineKeyHash> LinesByKey;
        THashMap<ui32, TLine*> LinesById;
        mutable TRWMutex CommonLabelsLock;
        TVector<TLabel> CommonLabels;

        mutable TMutex VictimLock;
        TVector<std::unique_ptr<TChunk>> Storage;
        TVector<TChunk*> FreeList;
        std::priority_queue<TVictimKey, TVector<TVictimKey>, TVictimCompare> VictimHeap;
        mutable TMutex SelfMetricsLock;
        bool SelfMetricsInitialized = false;
        TSelfMetricsLines SelfMetrics;
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

    ui32 TLineWriter::ReleaseLineId() noexcept {
        const ui32 lineId = GetLineId();
        Registry = nullptr;
        Line = nullptr;
        return lineId;
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
        bool hasLastRecord = false;
        TRecordView lastRecord;
        for (size_t chunkIndex : ChunkIndexes) {
            const auto& pinned = Data->Chunks[chunkIndex];
            const auto* records = reinterpret_cast<const TStoredRecord*>(pinned.Chunk->Payload.data());
            const ui32 recordsCount = pinned.View.CommittedBytes / sizeof(TStoredRecord);
            for (ui32 i = 0; i < recordsCount; ++i) {
                lastRecord = TRecordView{
                    .Timestamp = DecodeTs(Data->Anchor, records[i].TimestampTs),
                    .Value = records[i].Value,
                };
                hasLastRecord = true;
                cb(lastRecord);
            }
        }
        if (Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat
            && !Closed
            && hasLastRecord
            && HasObservedTail
            && lastRecord.Timestamp < ObservedTailTimestamp) {
            cb(TRecordView{
                .Timestamp = ObservedTailTimestamp,
                .Value = lastRecord.Value,
            });
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
        TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> labels) {
            TLineKey key;
            key.Name = TString(name);
            key.Labels.reserve(labels.size());
            for (const auto& label : labels) {
                key.Labels.push_back(label);
            }
            return key;
        }

        TVector<TLabel> NormalizeCommonLabels(std::span<const TLabel> labels) {
            TVector<TLabel> normalized;
            normalized.reserve(labels.size());
            for (const auto& label : labels) {
                auto it = FindIf(normalized, [&](const TLabel& current) {
                    return current.Name == label.Name;
                });
                if (it != normalized.end()) {
                    it->Value = label.Value;
                } else {
                    normalized.push_back(label);
                }
            }
            return normalized;
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
        return CreateLine(name, labels, {});
    }

    TLineWriter TInMemoryMetricsRegistry::CreateLine(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) {
        auto key = MakeLineKey(name, labels);

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
        line->Meta = meta;

        TLine* linePtr = line.get();
        Impl->LinesById.emplace(linePtr->LineId, linePtr);
        Impl->LinesByKey.emplace(linePtr->Key, std::move(line));
        return TLineWriter(this, linePtr);
    }

    void TInMemoryMetricsRegistry::SetCommonLabels(std::span<const TLabel> labels) {
        TWriteGuard guard(Impl->CommonLabelsLock);
        Impl->CommonLabels = NormalizeCommonLabels(labels);
    }

    TVector<TLabel> TInMemoryMetricsRegistry::GetCommonLabels() const {
        TReadGuard guard(Impl->CommonLabelsLock);
        return Impl->CommonLabels;
    }

    TInMemoryMetricsStats TInMemoryMetricsRegistry::GetStats() const {
        TInMemoryMetricsStats stats;

        for (const auto& chunk : Impl->Storage) {
            const EChunkState state = chunk->State.load(std::memory_order_acquire);
            if (state == EChunkState::Free) {
                ++stats.FreeChunks;
                continue;
            }

            ++stats.UsedChunks;
            stats.CommittedBytes += chunk->CommittedBytes.load(std::memory_order_acquire);
            switch (state) {
                case EChunkState::Writable:
                    ++stats.WritableChunks;
                    break;
                case EChunkState::Sealed:
                    ++stats.SealedChunks;
                    break;
                case EChunkState::Retiring:
                    ++stats.RetiringChunks;
                    break;
                case EChunkState::Free:
                    break;
            }
        }

        stats.MemoryUsedBytes = stats.UsedChunks * Impl->Config.ChunkSizeBytes;
        stats.ReuseWatermark = Impl->ReuseWatermark.load(std::memory_order_acquire);
        stats.AppendFailuresTotal = Impl->AppendFailures.load(std::memory_order_acquire);

        TGuard<TMutex> registryGuard(Impl->RegistryLock);
        for (const auto& [_, line] : Impl->LinesById) {
            if (IsRegistryMetricName(line->Key.Name)) {
                continue;
            }

            ++stats.Lines;
            if (line->State.load(std::memory_order_acquire) == ELineState::Closed) {
                ++stats.ClosedLines;
            }
        }

        return stats;
    }

    void TInMemoryMetricsRegistry::UpdateSelfMetrics() {
        const TInMemoryMetricsStats stats = GetStats();

        TGuard<TMutex> guard(Impl->SelfMetricsLock);
        if (!Impl->SelfMetricsInitialized) {
            const std::span<const TLabel> noLabels;
            const TLineMeta selfMetricMeta{
                .PublishPolicy = EPublishPolicy::OnChangeWithHeartbeat,
                .Heartbeat = TDuration::Hours(1),
            };
            Impl->SelfMetrics.MemoryUsedBytes = CreateLine(RegistryMemoryUsedBytesMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.CommittedBytes = CreateLine(RegistryCommittedBytesMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.FreeChunks = CreateLine(RegistryFreeChunksMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.UsedChunks = CreateLine(RegistryUsedChunksMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.SealedChunks = CreateLine(RegistrySealedChunksMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.WritableChunks = CreateLine(RegistryWritableChunksMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.RetiringChunks = CreateLine(RegistryRetiringChunksMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.Lines = CreateLine(RegistryLinesMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.ClosedLines = CreateLine(RegistryClosedLinesMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.ReuseWatermark = CreateLine(RegistryReuseWatermarkMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetrics.AppendFailuresTotal = CreateLine(RegistryAppendFailuresTotalMetric, noLabels, selfMetricMeta).ReleaseLineId();
            Impl->SelfMetricsInitialized = true;
        }

        auto appendIfPresent = [&](ui32 lineId, ui64 value) {
            if (!lineId) {
                return;
            }

            TLine* line = nullptr;
            {
                TGuard<TMutex> registryGuard(Impl->RegistryLock);
                if (const auto it = Impl->LinesById.find(lineId); it != Impl->LinesById.end()) {
                    line = it->second;
                }
            }
            if (line) {
                Append(line, value);
            }
        };

        appendIfPresent(Impl->SelfMetrics.MemoryUsedBytes, stats.MemoryUsedBytes);
        appendIfPresent(Impl->SelfMetrics.CommittedBytes, stats.CommittedBytes);
        appendIfPresent(Impl->SelfMetrics.FreeChunks, stats.FreeChunks);
        appendIfPresent(Impl->SelfMetrics.UsedChunks, stats.UsedChunks);
        appendIfPresent(Impl->SelfMetrics.SealedChunks, stats.SealedChunks);
        appendIfPresent(Impl->SelfMetrics.WritableChunks, stats.WritableChunks);
        appendIfPresent(Impl->SelfMetrics.RetiringChunks, stats.RetiringChunks);
        appendIfPresent(Impl->SelfMetrics.Lines, stats.Lines);
        appendIfPresent(Impl->SelfMetrics.ClosedLines, stats.ClosedLines);
        appendIfPresent(Impl->SelfMetrics.ReuseWatermark, stats.ReuseWatermark);
        appendIfPresent(Impl->SelfMetrics.AppendFailuresTotal, stats.AppendFailuresTotal);
    }

    namespace {
        bool RemoveChunkFromLineLocked(TLine* line, TChunk* chunk) {
            if (line->Storage.Writable.load(std::memory_order_acquire) == chunk) {
                line->Storage.Writable.store(nullptr, std::memory_order_release);
            }
            auto it = Find(line->Storage.Chunks, chunk);
            if (it != line->Storage.Chunks.end()) {
                line->Storage.Chunks.erase(it);
            }
            return line->State.load(std::memory_order_acquire) == ELineState::Closed && line->Storage.Chunks.empty();
        }
    }

    void TInMemoryMetricsRegistry::MaybeDropClosedLine(TLine* line) {
        TGuard<TMutex> guard(Impl->RegistryLock);
        auto it = Impl->LinesById.find(line->LineId);
        if (it == Impl->LinesById.end()) {
            return;
        }

        TLineKey key;
        TGuard<TMutex> lineGuard(line->Storage.Lock);
        if (line->State.load(std::memory_order_acquire) != ELineState::Closed || !line->Storage.Chunks.empty()) {
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
                TGuard<TMutex> lineGuard(owner->Storage.Lock);
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
        const auto fail = [&]() noexcept {
            Impl->AppendFailures.fetch_add(1, std::memory_order_relaxed);
            return false;
        };

        if (!line || Impl->ShuttingDown.load(std::memory_order_acquire)) {
            return fail();
        }
        if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
            return fail();
        }

        const NHPTimer::STime nowTs = static_cast<NHPTimer::STime>(GetCycleCountFast());
        if (line->HasLastPublished && line->LastPublishedValue == value) {
            MarkRecordObserved(line, nowTs);
        }
        if (!ShouldPublishRecord(line, value, nowTs)) {
            return true;
        }

        auto appendRecord = [&](const TStoredRecord& record) noexcept {
            while (true) {
                TChunk* chunk = line->Storage.Writable.load(std::memory_order_acquire);
                if (chunk && TryAppendToChunk(chunk, record)) {
                    return true;
                }

                TChunk* sealedChunk = nullptr;
                {
                    TGuard<TMutex> lineGuard(line->Storage.Lock);
                    if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
                        return fail();
                    }

                    chunk = line->Storage.Writable.load(std::memory_order_acquire);
                    if (chunk && TryAppendToChunk(chunk, record)) {
                        return true;
                    }

                    if (chunk && chunk->CommittedBytes.load(std::memory_order_acquire) == 0 && !CanFitRecord(chunk)) {
                        return fail();
                    }

                    if (chunk) {
                        chunk->State.store(EChunkState::Sealed, std::memory_order_release);
                        line->Storage.Writable.store(nullptr, std::memory_order_release);
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
                    return fail();
                }
                if (!CanFitRecord(newChunk)) {
                    ReturnChunkToFree(newChunk);
                    return fail();
                }

                TGuard<TMutex> lineGuard(line->Storage.Lock);
                if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
                    ReturnChunkToFree(newChunk);
                    return fail();
                }
                if (line->Storage.Writable.load(std::memory_order_acquire)) {
                    ReturnChunkToFree(newChunk);
                    continue;
                }

                ResetChunkForWrite(newChunk, line);
                line->Storage.Writable.store(newChunk, std::memory_order_release);
                line->Storage.Chunks.push_back(newChunk);
            }
        };

        if (ShouldPublishPreviousObservedValue(line, value)) {
            const TStoredRecord previousRecord{
                .TimestampTs = line->LastPublishedTs,
                .Value = line->LastPublishedValue,
            };
            if (!appendRecord(previousRecord)) {
                return false;
            }
            MarkRecordPublished(line, previousRecord.Value, previousRecord.TimestampTs);
        }

        const TStoredRecord record{
            .TimestampTs = nowTs,
            .Value = value,
        };
        if (!appendRecord(record)) {
            return false;
        }
        MarkRecordPublished(line, value, nowTs);
        return true;
    }

    void TInMemoryMetricsRegistry::CloseLine(TLine* line) noexcept {
        if (!line) {
            return;
        }

        bool shouldDrop = false;
        TChunk* sealedChunk = nullptr;
        TChunk* freeChunk = nullptr;
        {
            TGuard<TMutex> guard(line->Storage.Lock);
            line->State.store(ELineState::Closed, std::memory_order_release);
            if (TChunk* writable = line->Storage.Writable.exchange(nullptr, std::memory_order_acq_rel)) {
                if (writable->CommittedBytes.load(std::memory_order_acquire)) {
                    writable->State.store(EChunkState::Sealed, std::memory_order_release);
                    sealedChunk = writable;
                } else {
                    RemoveChunkFromLineLocked(line, writable);
                    freeChunk = writable;
                }
            }
            shouldDrop = line->Storage.Chunks.empty();
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
        snapshot.CommonLabels = GetCommonLabels();

        TGuard<TMutex> registryGuard(Impl->RegistryLock);
        snapshot.SnapshotLines.reserve(Impl->LinesById.size());

        for (const auto& [lineId, line] : Impl->LinesById) {
            Y_UNUSED(lineId);
            TLineSnapshot lineSnapshot;
            lineSnapshot.Data = data;
            lineSnapshot.LineId = line->LineId;
            lineSnapshot.Name = line->Key.Name;
            lineSnapshot.Labels = line->Key.Labels;
            lineSnapshot.Meta = line->Meta;

            TGuard<TMutex> lineGuard(line->Storage.Lock);
            lineSnapshot.Closed = line->State.load(std::memory_order_acquire) == ELineState::Closed;
            if (line->Meta.PublishPolicy == EPublishPolicy::OnChangeWithHeartbeat
                && line->HasLastPublished
                && line->LastObservedTs > line->LastPublishedTs) {
                lineSnapshot.HasObservedTail = true;
                lineSnapshot.ObservedTailTimestamp = DecodeTs(data->Anchor, line->LastObservedTs);
            }
            lineSnapshot.Chunks.reserve(line->Storage.Chunks.size());
            lineSnapshot.ChunkIndexes.reserve(line->Storage.Chunks.size());

            for (TChunk* chunk : line->Storage.Chunks) {
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

    namespace {
        class TInMemoryMetricsStatsActor final
            : public TActorBootstrapped<TInMemoryMetricsStatsActor>
        {
        public:
            explicit TInMemoryMetricsStatsActor(TDuration interval)
                : Interval(interval)
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                Publish(ctx);
                Become(&TThis::StateWork);
            }

            STRICT_STFUNC(StateWork,
                cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )

        private:
            void HandleWakeup() {
                Publish(TActivationContext::AsActorContext());
            }

            void Publish(const TActorContext& ctx) {
                GetInMemoryMetrics(*ctx.ActorSystem()).UpdateSelfMetrics();
                ctx.Schedule(Interval, new TEvents::TEvWakeup());
            }

        private:
            const TDuration Interval;
        };
    } // namespace

    IActor* CreateInMemoryMetricsStatsActor(TDuration interval) {
        return new TInMemoryMetricsStatsActor(interval);
    }

} // namespace NActors
