#include "inmemory_backend.h"
#include "lines/on_change_line_frontend.h"
#include "lines/raw_line_frontend.h"

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

        bool IsRegistryMetricName(TStringBuf name) noexcept {
            return name.StartsWith(RegistryMetricsPrefix);
        }

        void ResetChunkForWrite(TChunk* chunk, TLineReader* line) {
            chunk->Owner.store(line, std::memory_order_release);
            chunk->OwnerLineId.store(line->LineId, std::memory_order_release);
            chunk->CommittedBytes.store(0, std::memory_order_release);
            chunk->Readers.store(0, std::memory_order_release);
            chunk->FirstTs.store(0, std::memory_order_release);
            chunk->LastTs.store(0, std::memory_order_release);
            std::memset(chunk->Payload.data(), 0, chunk->Payload.size());
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

        bool RemoveChunkFromLineLocked(TLineReader* line, TChunk* chunk) {
            if (line->Storage.Writable.load(std::memory_order_acquire) == chunk) {
                line->Storage.Writable.store(nullptr, std::memory_order_release);
            }
            auto it = Find(line->Storage.Chunks, chunk);
            if (it != line->Storage.Chunks.end()) {
                line->Storage.Chunks.erase(it);
            }
            return line->State.load(std::memory_order_acquire) == ELineState::Closed && line->Storage.Chunks.empty();
        }

        bool TryAccessChunkMemory(TChunk* chunk, void* opaque, TAccessChunkMemoryFn accessChunkMemory) {
            TWritableChunkMemory chunkMemory{
                .Payload = std::span<char>(chunk->Payload.data(), chunk->Payload.size()),
                .FirstTs = chunk->FirstTs.load(std::memory_order_relaxed),
                .LastTs = chunk->LastTs.load(std::memory_order_relaxed),
            };
            if (!accessChunkMemory(opaque, chunkMemory)) {
                return false;
            }

            const TLineReader* owner = chunk->Owner.load(std::memory_order_acquire);
            const TLineFrontendOps* frontend = owner ? owner->Meta.Frontend : nullptr;
            const ui32 usedPayloadBytes = frontend && frontend->GetUsedPayloadBytes
                ? frontend->GetUsedPayloadBytes(std::span<const char>(chunk->Payload.data(), chunk->Payload.size()))
                : 0;
            Y_ABORT_UNLESS(usedPayloadBytes <= chunk->Payload.size());
            if (usedPayloadBytes != 0) {
                chunk->FirstTs.store(chunkMemory.FirstTs, std::memory_order_relaxed);
                chunk->LastTs.store(chunkMemory.LastTs, std::memory_order_relaxed);
            }
            chunk->CommittedBytes.store(usedPayloadBytes, std::memory_order_release);
            return true;
        }
    } // namespace

    class TInMemoryMetricsBackend::TImpl {
    public:
        using TSelfMetricLine = TLine<TOnChangeLineFrontend<>>;

        struct TSelfMetricsLines {
            TSelfMetricLine MemoryUsedBytes;
            TSelfMetricLine CommittedBytes;
            TSelfMetricLine FreeChunks;
            TSelfMetricLine UsedChunks;
            TSelfMetricLine SealedChunks;
            TSelfMetricLine WritableChunks;
            TSelfMetricLine RetiringChunks;
            TSelfMetricLine Lines;
            TSelfMetricLine ClosedLines;
            TSelfMetricLine ReuseWatermark;
            TSelfMetricLine AppendFailuresTotal;
        };

        explicit TImpl(TInMemoryMetricsConfig cfg)
            : Config(std::move(cfg))
            , ChunkCount(Config.ChunkSizeBytes ? Config.MemoryBytes / Config.ChunkSizeBytes : 0)
            , MaxLines(Config.MaxLines ? Config.MaxLines : ChunkCount / 2)
            , CommonLabels(Config.CommonLabels)
            , AllowedMetricPrefixes(Config.AllowedMetricPrefixes)
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

        TTimeAnchor TimeAnchor;

        mutable TMutex RegistryLock;
        THashMap<TLineKey, std::unique_ptr<TLineReader>, TLineKeyHash> LinesByKey;
        THashMap<ui32, TLineReader*> LinesById;
        mutable TRWMutex CommonLabelsLock;
        TVector<TLabel> CommonLabels;
        TVector<TString> AllowedMetricPrefixes;

        mutable TMutex VictimLock;
        TVector<std::unique_ptr<TChunk>> Storage;
        TVector<TChunk*> FreeList;
        std::priority_queue<TVictimKey, TVector<TVictimKey>, TVictimCompare> VictimHeap;
        mutable TMutex SelfMetricsLock;
        bool SelfMetricsInitialized = false;
        TSelfMetricsLines SelfMetrics;
    };

    TInMemoryMetricsBackend::TInMemoryMetricsBackend(TInMemoryMetricsConfig config)
        : Impl(std::make_unique<TImpl>(std::move(config)))
    {
    }

    TInMemoryMetricsBackend::~TInMemoryMetricsBackend() {
        if (!Impl) {
            return;
        }

        TGuard<TMutex> guard(Impl->SelfMetricsLock);
        Impl->SelfMetrics = TImpl::TSelfMetricsLines{};
        Impl->SelfMetricsInitialized = false;
    }

    TLineWriterState* TInMemoryMetricsBackend::CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) {
        if (!IsMetricAllowed(name)) {
            return nullptr;
        }

        auto key = MakeLineKey(name, labels);

        TGuard<TMutex> guard(Impl->RegistryLock);
        if (Impl->LinesByKey.contains(key)) {
            return nullptr;
        }

        if (Impl->LinesByKey.size() >= Impl->MaxLines) {
            return nullptr;
        }

        auto line = std::make_unique<TLineReader>();
        line->LineId = Impl->NextLineId.fetch_add(1, std::memory_order_relaxed);
        line->Key = std::move(key);
        line->Meta = meta;

        TLineReader* linePtr = line.get();
        Impl->LinesById.emplace(linePtr->LineId, linePtr);
        Impl->LinesByKey.emplace(linePtr->Key, std::move(line));
        linePtr->WriteState = std::make_unique<TLineWriterState>();
        linePtr->WriteState->Reader = linePtr;
        return linePtr->WriteState.get();
    }

    bool TInMemoryMetricsBackend::IsMetricAllowed(TStringBuf name) const noexcept {
        if (Impl->AllowedMetricPrefixes.empty()) {
            return true;
        }

        for (const auto& prefix : Impl->AllowedMetricPrefixes) {
            if (name.StartsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    void TInMemoryMetricsBackend::SetCommonLabels(std::span<const TLabel> labels) {
        TWriteGuard guard(Impl->CommonLabelsLock);
        Impl->CommonLabels = NormalizeCommonLabels(labels);
    }

    TVector<TLabel> TInMemoryMetricsBackend::GetCommonLabels() const {
        TReadGuard guard(Impl->CommonLabelsLock);
        return Impl->CommonLabels;
    }

    TInMemoryMetricsStats TInMemoryMetricsBackend::GetStats() const {
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

    void TInMemoryMetricsBackend::UpdateSelfMetrics() {
        const TInMemoryMetricsStats stats = GetStats();

        TGuard<TMutex> guard(Impl->SelfMetricsLock);
        if (!Impl->SelfMetricsInitialized) {
            const std::span<const TLabel> noLabels;
            Impl->SelfMetrics.MemoryUsedBytes = CreateLine<TOnChangeLineFrontend<>>(RegistryMemoryUsedBytesMetric, noLabels);
            Impl->SelfMetrics.CommittedBytes = CreateLine<TOnChangeLineFrontend<>>(RegistryCommittedBytesMetric, noLabels);
            Impl->SelfMetrics.FreeChunks = CreateLine<TOnChangeLineFrontend<>>(RegistryFreeChunksMetric, noLabels);
            Impl->SelfMetrics.UsedChunks = CreateLine<TOnChangeLineFrontend<>>(RegistryUsedChunksMetric, noLabels);
            Impl->SelfMetrics.SealedChunks = CreateLine<TOnChangeLineFrontend<>>(RegistrySealedChunksMetric, noLabels);
            Impl->SelfMetrics.WritableChunks = CreateLine<TOnChangeLineFrontend<>>(RegistryWritableChunksMetric, noLabels);
            Impl->SelfMetrics.RetiringChunks = CreateLine<TOnChangeLineFrontend<>>(RegistryRetiringChunksMetric, noLabels);
            Impl->SelfMetrics.Lines = CreateLine<TOnChangeLineFrontend<>>(RegistryLinesMetric, noLabels);
            Impl->SelfMetrics.ClosedLines = CreateLine<TOnChangeLineFrontend<>>(RegistryClosedLinesMetric, noLabels);
            Impl->SelfMetrics.ReuseWatermark = CreateLine<TOnChangeLineFrontend<>>(RegistryReuseWatermarkMetric, noLabels);
            Impl->SelfMetrics.AppendFailuresTotal = CreateLine<TOnChangeLineFrontend<>>(RegistryAppendFailuresTotalMetric, noLabels);
            Impl->SelfMetricsInitialized = true;
        }

        auto appendIfPresent = [&](TImpl::TSelfMetricLine& line, ui64 value) {
            if (!line) {
                return;
            }
            line.Append(value);
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

    void TInMemoryMetricsBackend::MaybeDropClosedLine(TLineReader* line) {
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

    TChunk* TInMemoryMetricsBackend::TryAcquireFreeChunk() {
        TGuard<TMutex> guard(Impl->VictimLock);
        if (Impl->FreeList.empty()) {
            return nullptr;
        }

        TChunk* chunk = Impl->FreeList.back();
        Impl->FreeList.pop_back();
        return chunk;
    }

    void TInMemoryMetricsBackend::PublishSealedChunk(TChunk* chunk) {
        TGuard<TMutex> guard(Impl->VictimLock);
        chunk->Generation.fetch_add(1, std::memory_order_acq_rel);
        TLineReader* owner = chunk->Owner.load(std::memory_order_acquire);
        Impl->VictimHeap.push(TVictimKey{
            .LastTs = chunk->LastTs.load(std::memory_order_acquire),
            .Closed = owner && owner->State.load(std::memory_order_acquire) == ELineState::Closed,
            .ChunkId = chunk->ChunkId,
            .Generation = chunk->Generation.load(std::memory_order_acquire),
            .Chunk = chunk,
        });
    }

    void TInMemoryMetricsBackend::ReturnChunkToFree(TChunk* chunk) {
        TGuard<TMutex> guard(Impl->VictimLock);
        FinalizeReturnedChunk(chunk);
        Impl->FreeList.push_back(chunk);
    }

    void TInMemoryMetricsBackend::ReleasePinnedChunk(TChunk* chunk) noexcept {
        const i32 prev = chunk->Readers.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == RetiringBias + 1) {
            ReturnChunkToFree(chunk);
        }
    }

    void TInMemoryMetricsBackend::RetireChunk(TChunk* chunk) {
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

    TChunk* TInMemoryMetricsBackend::TryStealOldestChunk() {
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
            TLineReader* owner = victim->Owner.load(std::memory_order_acquire);
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

    bool TInMemoryMetricsBackend::AccessChunkMemory(
        TLineWriterState* state,
        void* opaque,
        TAccessChunkMemoryFn accessChunkMemory) noexcept {
        const auto fail = [&]() noexcept {
            Impl->AppendFailures.fetch_add(1, std::memory_order_relaxed);
            return false;
        };

        if (!state || !state->Reader) {
            return fail();
        }
        TLineReader* line = state->Reader;
        if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
            return fail();
        }

        while (true) {
            TChunk* chunk = line->Storage.Writable.load(std::memory_order_acquire);
            if (chunk && TryAccessChunkMemory(chunk, opaque, accessChunkMemory)) {
                return true;
            }

            TChunk* sealedChunk = nullptr;
            TChunk* emptyUnusableChunk = nullptr;
            {
                TGuard<TMutex> lineGuard(line->Storage.Lock);
                if (line->State.load(std::memory_order_acquire) != ELineState::Open) {
                    return fail();
                }

                chunk = line->Storage.Writable.load(std::memory_order_acquire);
                if (chunk && TryAccessChunkMemory(chunk, opaque, accessChunkMemory)) {
                    return true;
                }

                if (chunk && chunk->CommittedBytes.load(std::memory_order_acquire) == 0) {
                    RemoveChunkFromLineLocked(line, chunk);
                    emptyUnusableChunk = chunk;
                }
                if (chunk && !emptyUnusableChunk) {
                    chunk->State.store(EChunkState::Sealed, std::memory_order_release);
                    line->Storage.Writable.store(nullptr, std::memory_order_release);
                    sealedChunk = chunk;
                }
            }

            if (emptyUnusableChunk) {
                ReturnChunkToFree(emptyUnusableChunk);
                return fail();
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
    }

    void TInMemoryMetricsBackend::CloseLine(TLineWriterState* state) noexcept {
        if (!state || !state->Reader) {
            return;
        }
        TLineReader* line = state->Reader;
        state->Reader = nullptr;

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

    void TInMemoryMetricsBackend::ReadSnapshot(const std::function<void(const TSnapshot&)>& cb) const {
        TSnapshot snapshot;
        snapshot.Anchor = Impl->TimeAnchor;
        snapshot.CommonLabels = GetCommonLabels();

        TGuard<TMutex> registryGuard(Impl->RegistryLock);
        snapshot.SnapshotLines.reserve(Impl->LinesById.size());

        for (const auto& [lineId, line] : Impl->LinesById) {
            Y_UNUSED(lineId);
            TLineSnapshot lineSnapshot;
            lineSnapshot.Owner = &snapshot;
            lineSnapshot.LineId = line->LineId;
            lineSnapshot.Name = line->Key.Name;
            lineSnapshot.Labels = line->Key.Labels;
            lineSnapshot.Meta = line->Meta;
            TGuard<TMutex> lineGuard(line->Storage.Lock);
            lineSnapshot.Closed = line->State.load(std::memory_order_acquire) == ELineState::Closed;
            lineSnapshot.ChunkBegin = snapshot.SnapshotChunks.size();

            for (TChunk* chunk : line->Storage.Chunks) {
                if (!NInMemoryMetricsPrivate::TryPinChunk(chunk)) {
                    continue;
                }

                const TChunkView view{
                    .ChunkId = chunk->ChunkId,
                    .FirstTs = NInMemoryMetricsPrivate::DecodeTs(snapshot.Anchor, chunk->FirstTs.load(std::memory_order_acquire)),
                    .LastTs = NInMemoryMetricsPrivate::DecodeTs(snapshot.Anchor, chunk->LastTs.load(std::memory_order_acquire)),
                };
                snapshot.SnapshotChunks.push_back(TSnapshotPinnedChunk{
                    .Backend = const_cast<TInMemoryMetricsBackend*>(this),
                    .Chunk = chunk,
                    .View = view,
                });
                ++lineSnapshot.ChunkCount;
            }

            if (lineSnapshot.ChunkCount != 0 || lineSnapshot.Closed) {
                snapshot.SnapshotLines.push_back(std::move(lineSnapshot));
            }
        }
        cb(snapshot);
    }

    ui64 TInMemoryMetricsBackend::GetReuseWatermark() const noexcept {
        return Impl->ReuseWatermark.load(std::memory_order_acquire);
    }

    const TInMemoryMetricsConfig& TInMemoryMetricsBackend::GetConfig() const noexcept {
        return Impl->Config;
    }

    NHPTimer::STime TInMemoryMetricsBackend::CurrentTimestampTs() const noexcept {
        return static_cast<NHPTimer::STime>(GetCycleCountFast());
    }

    std::optional<ui64> TInMemoryMetricsBackend::GetLastMaterializedValue(const TLineWriterState* state) const noexcept {
        if (!state || !state->HasLastMaterialized.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return state->LastMaterializedValue.load(std::memory_order_acquire);
    }

    ui32 TInMemoryMetricsBackend::GetLineId(const TLineWriterState* state) const noexcept {
        return state && state->Reader ? state->Reader->LineId : 0;
    }

    void TInMemoryMetricsBackend::MarkMaterialized(TLineWriterState* state, ui64 value) noexcept {
        if (state) {
            state->LastMaterializedValue.store(value, std::memory_order_release);
            state->HasLastMaterialized.store(true, std::memory_order_release);
        }
    }

} // namespace NActors
