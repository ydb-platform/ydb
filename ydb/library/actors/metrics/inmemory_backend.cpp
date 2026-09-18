#include "inmemory_backend.h"
#include "lines/on_change_line_frontend.h"
#include "lines/raw_line_frontend.h"

#include <ydb/library/actors/util/datetime.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/ymath.h>
#include <util/system/hp_timer.h>
#include <util/system/yassert.h>

#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <limits>
#include <memory>

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

        void PrepareChunk(TChunk* chunk, TLineReader* line) {
            chunk->Owner.store(line, std::memory_order_release);
            chunk->OwnerLineId.store(line->LineId, std::memory_order_release);
            chunk->CommittedBytes.store(0, std::memory_order_release);
            chunk->Readers.store(0, std::memory_order_release);
            chunk->FirstTs.store(0, std::memory_order_release);
            chunk->LastTs.store(0, std::memory_order_release);
            std::memset(chunk->Payload.data(), 0, chunk->Payload.size());
            chunk->Generation.fetch_add(1, std::memory_order_acq_rel);
            chunk->State.store(EChunkState::Reserved, std::memory_order_release);
        }

        bool RemoveChunkFromLine(TLineReader* line, TChunk* chunk) {
            line->Storage.Chunks.Remove(chunk);
            return line->State.load(std::memory_order_acquire) == ELineState::Closed && line->Storage.Chunks.Empty();
        }

        bool TryAccessChunkMemory(TChunk* chunk, void* opaque, TAccessChunkMemoryFn accessChunkMemory) {
            TWritableChunkMemory chunkMemory{
                .Payload = std::span<char>(chunk->Payload.data(), chunk->Payload.size()),
                .UsedPayloadBytes = chunk->CommittedBytes.load(std::memory_order_relaxed),
                .FirstTs = chunk->FirstTs.load(std::memory_order_relaxed),
                .LastTs = chunk->LastTs.load(std::memory_order_relaxed),
            };
            if (!accessChunkMemory(opaque, chunkMemory)) {
                return false;
            }

            const ui32 usedPayloadBytes = chunkMemory.UsedPayloadBytes;
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

        explicit TImpl(TInMemoryMetricsConfig cfg, std::function<void()> notify)
            : Config(std::move(cfg))
            , ChunkCount(Config.ChunkSizeBytes ? Config.MemoryBytes / Config.ChunkSizeBytes : 0)
            , MaxLines(Config.MaxLines ? Config.MaxLines : ChunkCount / 2)
            , CommonLabels(Config.CommonLabels)
            , AllowedMetricPrefixes(Config.AllowedMetricPrefixes)
            , Notify(std::move(notify))
        {
            Y_ABORT_UNLESS(Config.ReserveChunks);
            TimeAnchor.BaseCycles = GetCycleCountFast();
            TimeAnchor.BaseWallClock = TInstant::Now();

            static_assert(std::atomic<ui64>::is_always_lock_free);
            Pool = std::make_shared<TChunkPool>(ChunkCount, Config.ChunkSizeBytes);
        }

        TInMemoryMetricsConfig Config;
        ui32 ChunkCount = 0;
        ui32 MaxLines = 0;

        std::atomic<ui32> NextLineId = 1;
        std::atomic<ui64> ReuseWatermark = 0;
        std::atomic<ui64> AppendFailures = 0;

        TTimeAnchor TimeAnchor;

        // Retain chunks until all intrusive line lists have been destroyed,
        // even if the last external snapshot is released concurrently.
        std::shared_ptr<TChunkPool> Pool;
        THashMap<TLineKey, std::shared_ptr<TLineReader>, TLineKeyHash> LinesByKey;
        THashMap<ui32, TLineReader*> LinesById;
        TVector<TLabel> CommonLabels;
        TVector<TString> AllowedMetricPrefixes;

        TPriorityQueue<TVictimKey, TVector<TVictimKey>, TVictimCompare> VictimHeap;
        const std::function<void()> Notify;
        std::atomic<bool> MaintenanceScheduled = false;
        std::atomic<bool> Stopping = false;
        ui32 NextRefillLineId = 0;
        bool SelfMetricsInitialized = false;
        TSelfMetricsLines SelfMetrics;
    };

    TInMemoryMetricsBackend::TInMemoryMetricsBackend(TInMemoryMetricsConfig config, std::function<void()> notify)
        : Impl(std::make_unique<TImpl>(std::move(config), std::move(notify)))
    {
    }

    TInMemoryMetricsBackend::~TInMemoryMetricsBackend() {
        if (!Impl) {
            return;
        }

        StopManagement();
        Impl->SelfMetrics = TImpl::TSelfMetricsLines{};
        Impl->SelfMetricsInitialized = false;
    }

    std::shared_ptr<TLineWriterState> TInMemoryMetricsBackend::CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) {
        auto state = std::make_shared<TLineWriterState>();
        RegisterLine(state, MakeLineKey(name, labels), meta);
        return state;
    }

    void TInMemoryMetricsBackend::RegisterLine(const std::shared_ptr<TLineWriterState>& state, TLineKey key, const TLineMeta& meta) {
        if (state->Status.load(std::memory_order_acquire) != ELineWriterStatus::Pending) {
            return;
        }
        auto reject = [&] {
            auto pending = ELineWriterStatus::Pending;
            state->Status.compare_exchange_strong(pending, ELineWriterStatus::Rejected, std::memory_order_release);
        };
        if (Impl->Stopping.load(std::memory_order_acquire) || !IsMetricAllowed(key.Name)) {
            reject();
            return;
        }
        if (auto it = Impl->LinesByKey.find(key); it != Impl->LinesByKey.end()) {
            auto previous = it->second;
            if (previous->WriteState->Status.load(std::memory_order_acquire) != ELineWriterStatus::Closed) {
                reject();
                return;
            }
            CloseLineNow(previous->WriteState.get());
            TryDropClosedLine(previous.get());
        }
        if (Impl->LinesByKey.size() >= Impl->MaxLines && !TryDropClosedLine()) {
            reject();
            return;
        }
        auto line = std::make_shared<TLineReader>();
        line->LineId = Impl->NextLineId.fetch_add(1, std::memory_order_relaxed);
        line->Key = std::move(key);
        line->Meta = meta;
        line->Reserve = std::make_unique<TChunkReserve>(Min(Impl->Config.ReserveChunks, Max(1u, Impl->ChunkCount)));
        line->WriteState = state;
        state->Reader = line.get();
        Impl->LinesById.emplace(line->LineId, line.get());
        Impl->LinesByKey.emplace(line->Key, line);
        auto pending = ELineWriterStatus::Pending;
        if (!state->Status.compare_exchange_strong(pending, ELineWriterStatus::Ready, std::memory_order_release)) {
            // Close won while registration was preparing the reader.
            CloseLineNow(state.get());
        }
    }

    void TInMemoryMetricsBackend::RequestMaintenance() noexcept {
        if (Impl->Notify && !Impl->Stopping.load(std::memory_order_acquire)
            && !Impl->MaintenanceScheduled.exchange(true, std::memory_order_acq_rel)) {
            Impl->Notify();
        }
    }

    void TInMemoryMetricsBackend::StopManagement() noexcept {
        Impl->Stopping.store(true, std::memory_order_release);
    }

    void TInMemoryMetricsBackend::BeginMaintenance() noexcept {
        // Reset before draining commands AND scanning lines. Every producer
        // publishes its request through an acq_rel exchange, including coalescing.
        Impl->MaintenanceScheduled.exchange(false, std::memory_order_acq_rel);
    }

    void TInMemoryMetricsBackend::ProcessMaintenance() {
        TVector<std::shared_ptr<TLineReader>> lines;
        for (const auto& [_, line] : Impl->LinesByKey) {
            lines.push_back(line);
        }
        for (const auto& line : lines) {
            for (TChunk& entry : line->Storage.Chunks) {
                TChunk* chunk = &entry;
                if (chunk->State.load(std::memory_order_acquire) == EChunkState::PendingSeal) {
                    chunk->State.store(EChunkState::Sealed, std::memory_order_release);
                    PublishSealedChunk(chunk);
                }
            }
            if (line->WriteState->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed) {
                CloseLineNow(line->WriteState.get());
            }
        }
        if (Impl->Stopping.load(std::memory_order_acquire) || lines.empty()) {
            return;
        }

        Sort(lines, [](const auto& lhs, const auto& rhs) { return lhs->LineId < rhs->LineId; });
        auto start = std::lower_bound(lines.begin(), lines.end(), Impl->NextRefillLineId,
            [](const auto& line, ui32 id) { return line->LineId < id; });
        std::rotate(lines.begin(), start, lines.end());

        // One chunk per line per round, with a bounded number of allocations.
        // Pinned capacity is retried by the periodic tick or a new writer request.
        ui32 budget = 64;
        bool supplied;
        do {
            supplied = false;
            for (const auto& line : lines) {
                if (line->WriteState->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed || line->Reserve->Full()) {
                    continue;
                }
                TChunk* chunk = TryAcquireFreeChunk();
                if (!chunk) {
                    chunk = TryStealOldestChunk();
                }
                if (!chunk) {
                    return;
                }
                PrepareChunk(chunk, line.get());
                line->Storage.Chunks.PushBack(chunk);
                Y_ABORT_UNLESS(line->Reserve->TryPush(chunk));
                supplied = true;
                Impl->NextRefillLineId = line->LineId + 1;
                if (!--budget) {
                    RequestMaintenance();
                    return;
                }
            }
        } while (supplied);
    }

    bool TInMemoryMetricsBackend::AccessChunkMemory(
        TLineWriterState* state, void* opaque, TAccessChunkMemoryFn accessChunkMemory) noexcept
    {
        const auto fail = [&] {
            Impl->AppendFailures.fetch_add(1, std::memory_order_relaxed);
            return false;
        };
        if (!state || state->Status.load(std::memory_order_acquire) != ELineWriterStatus::Ready || !state->Reader || Impl->Stopping.load(std::memory_order_acquire)) {
            return fail();
        }
        TLineReader* line = state->Reader;
        TChunk* current = line->Storage.Writable.load(std::memory_order_relaxed);
        if (current && TryAccessChunkMemory(current, opaque, accessChunkMemory)) {
            return true;
        }
        // An unusably small chunk cannot be repaired by consuming more reserves.
        if (current && !current->CommittedBytes.load(std::memory_order_relaxed)) {
            return fail();
        }
        if (current) {
            line->Storage.Writable.store(nullptr, std::memory_order_release);
            current->State.store(EChunkState::PendingSeal, std::memory_order_release);
        }
        TChunk* next = line->Reserve->TryPop();
        if (next) {
            next->State.store(EChunkState::Writable, std::memory_order_release);
            line->Storage.Writable.store(next, std::memory_order_release);
        }
        // Notify on EVERY consumption, while remaining reserves cover delivery.
        // Also notify an empty queue so a failed write can request a retry.
        RequestMaintenance();
        return next && TryAccessChunkMemory(next, opaque, accessChunkMemory) ? true : fail();
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
        Impl->CommonLabels = NormalizeCommonLabels(labels);
    }

    TVector<TLabel> TInMemoryMetricsBackend::GetCommonLabels() const {
        return Impl->CommonLabels;
    }

    TInMemoryMetricsStats TInMemoryMetricsBackend::GetStats() const {
        TInMemoryMetricsStats stats;

        for (const auto& chunk : Impl->Pool->Storage) {
            const EChunkState state = chunk->State.load(std::memory_order_acquire);
            if (state == EChunkState::Free) {
                ++stats.FreeChunks;
                continue;
            }

            ++stats.UsedChunks;
            stats.CommittedBytes += chunk->CommittedBytes.load(std::memory_order_acquire);
            switch (state) {
                case EChunkState::Reserved:
                    break;
                case EChunkState::Writable:
                    ++stats.WritableChunks;
                    break;
                case EChunkState::PendingSeal:
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

    bool TInMemoryMetricsBackend::TryDropClosedLine(TLineReader* target) {
        for (const auto [_, line] : Impl->LinesById) {
            if ((target && line != target) || line->WriteState->Status.load(std::memory_order_acquire) != ELineWriterStatus::Closed) {
                continue;
            }

            // Closing an empty line may erase it from both maps. Keep its
            // reader alive until admission cleanup is complete.
            auto keepAlive = Impl->LinesByKey.at(line->Key);
            CloseLineNow(line->WriteState.get());
            {
                // Do not leave heap entries behind when admission, rather than
                // chunk pressure, evicts history repeatedly.
                auto& entries = Impl->VictimHeap.Container();
                std::erase_if(entries, [line](const TVictimKey& entry) {
                    return entry.Chunk->Owner.load(std::memory_order_acquire) == line
                        || entry.Chunk->State.load(std::memory_order_acquire) != EChunkState::Sealed
                        || entry.Chunk->Generation.load(std::memory_order_acquire) != entry.Generation;
                });
                std::make_heap(entries.begin(), entries.end(), TVictimCompare{});
            }
            while (!line->Storage.Chunks.Empty()) {
                RetireChunk(line->Storage.Chunks.PopFront());
            }
            MaybeDropClosedLine(line);
            return true;
        }
        return false;
    }

    void TInMemoryMetricsBackend::MaybeDropClosedLine(TLineReader* line) {
        if (line->State.load(std::memory_order_relaxed) != ELineState::Closed || !line->Storage.Chunks.Empty()) {
            return;
        }
        const auto key = line->Key;
        Impl->LinesById.erase(line->LineId);
        Impl->LinesByKey.erase(key);
    }

    TChunk* TInMemoryMetricsBackend::TryAcquireFreeChunk() {
        return Impl->Pool->TryAcquire();
    }

    void TInMemoryMetricsBackend::PublishSealedChunk(TChunk* chunk) {
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
        Impl->Pool->Return(chunk);
    }


    void TInMemoryMetricsBackend::RetireChunk(TChunk* chunk) {
        const auto lastTs = static_cast<ui64>(chunk->LastTs.load(std::memory_order_acquire));
        ui64 watermark = Impl->ReuseWatermark.load(std::memory_order_relaxed);
        while (watermark < lastTs
               && !Impl->ReuseWatermark.compare_exchange_weak(watermark, lastTs, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }

        // Publish the state before the CAS lets the last reader reclaim the chunk.
        chunk->State.store(EChunkState::Retiring, std::memory_order_release);
        i32 readers = chunk->Readers.load(std::memory_order_acquire);
        while (true) {
            Y_ABORT_UNLESS(readers >= 0);
            if (chunk->Readers.compare_exchange_weak(readers, RetiringBias + readers, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        if (readers == 0) {
            ReturnChunkToFree(chunk);
        }
    }

    TChunk* TInMemoryMetricsBackend::TryStealOldestChunk() {
        while (!Impl->VictimHeap.empty()) {
            const auto top = Impl->VictimHeap.top();
            Impl->VictimHeap.pop();
            TChunk* victim = top.Chunk;
            if (victim->State.load(std::memory_order_acquire) != EChunkState::Sealed
                || victim->Generation.load(std::memory_order_acquire) != top.Generation) {
                continue;
            }
            TLineReader* owner = victim->Owner.load(std::memory_order_relaxed);
            const bool drop = RemoveChunkFromLine(owner, victim);
            RetireChunk(victim);
            if (drop) {
                MaybeDropClosedLine(owner);
            }
            if (TChunk* free = TryAcquireFreeChunk()) {
                return free;
            }
        }
        return nullptr;
    }

    void TInMemoryMetricsBackend::CloseLine(TLineWriterState* state) noexcept {
        if (state) {
            state->ClosedAtTs = CurrentTimestampTs();
            state->Status.store(ELineWriterStatus::Closed, std::memory_order_release);
            RequestMaintenance();
        }
    }

    void TInMemoryMetricsBackend::CloseLineNow(TLineWriterState* state) noexcept {
        TLineReader* line = state->Reader;
        if (!line || line->State.load(std::memory_order_relaxed) == ELineState::Closed) {
            return;
        }
        line->State.store(ELineState::Closed, std::memory_order_relaxed);
        // Status=Closed hands the consumer role over after the last Append.
        while (TChunk* spare = line->Reserve->TryPop()) {
            RemoveChunkFromLine(line, spare);
            ReturnChunkToFree(spare);
        }
        if (TChunk* writable = line->Storage.Writable.exchange(nullptr, std::memory_order_relaxed)) {
            if (writable->CommittedBytes.load(std::memory_order_relaxed)) {
                writable->State.store(EChunkState::Sealed, std::memory_order_release);
                PublishSealedChunk(writable);
            } else {
                RemoveChunkFromLine(line, writable);
                RetireChunk(writable);
            }
        }
        MaybeDropClosedLine(line);
    }

    TInMemorySnapshot TInMemoryMetricsBackend::CaptureSnapshot() const {
        auto data = std::make_shared<NInMemoryMetricsPrivate::TSnapshot>();
        auto& snapshot = *data;
        snapshot.Pool = Impl->Pool;
        snapshot.Anchor = Impl->TimeAnchor;
        snapshot.CommonLabels = GetCommonLabels();

        {
            snapshot.SnapshotLines.reserve(Impl->LinesById.size());

            for (const auto& [lineId, line] : Impl->LinesById) {
                Y_UNUSED(lineId);
                TLineSnapshot lineSnapshot;
                lineSnapshot.Owner = &snapshot;
                lineSnapshot.LineId = line->LineId;
                lineSnapshot.Name = line->Key.Name;
                lineSnapshot.Labels = line->Key.Labels;
                lineSnapshot.Meta = line->Meta;
                lineSnapshot.Closed = line->WriteState->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed;
                if (lineSnapshot.Closed) {
                    lineSnapshot.ClosedAt = NInMemoryMetricsPrivate::DecodeTs(snapshot.Anchor, line->WriteState->ClosedAtTs);
                }
                lineSnapshot.ChunkBegin = snapshot.SnapshotChunks.size();

                for (TChunk& entry : line->Storage.Chunks) {
                    TChunk* chunk = &entry;
                    if (chunk->State.load(std::memory_order_acquire) == EChunkState::Reserved) {
                        continue;
                    }
                    if (!NInMemoryMetricsPrivate::TryPinChunk(chunk)) {
                        continue;
                    }

                    // Acquire the published prefix once; the writer may keep appending,
                    // but the records in this prefix remain immutable while pinned.
                    const ui32 committedBytes = chunk->CommittedBytes.load(std::memory_order_acquire);
                    const TChunkView view{
                        .ChunkId = chunk->ChunkId,
                        .FirstTs = NInMemoryMetricsPrivate::DecodeTs(snapshot.Anchor, chunk->FirstTs.load(std::memory_order_acquire)),
                        .LastTs = NInMemoryMetricsPrivate::DecodeTs(snapshot.Anchor, chunk->LastTs.load(std::memory_order_acquire)),
                    };
                    snapshot.SnapshotChunks.push_back(NInMemoryMetricsPrivate::TSnapshotPinnedChunk{
                        .Chunk = chunk,
                        .View = view,
                        .CommittedBytes = committedBytes,
                    });
                    ++lineSnapshot.ChunkCount;
                }

                if (lineSnapshot.ChunkCount != 0 || lineSnapshot.Closed) {
                    snapshot.SnapshotLines.push_back(std::move(lineSnapshot));
                }
            }
        }

        return TInMemorySnapshot(std::move(data));
    }

    void TInMemoryMetricsBackend::ReadSnapshot(const TReadSnapshotCallback& cb) const {
        CaptureSnapshot().Read(cb);
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
        if (!state || state->Status.load(std::memory_order_acquire) != ELineWriterStatus::Ready || !state->HasLastMaterialized.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        {
            if (Impl->Stopping.load(std::memory_order_acquire)) {
                return std::nullopt;
            }
            TChunk* chunk = state->LastMaterializedChunk.load(std::memory_order_acquire);
            if (!chunk || chunk->Generation.load(std::memory_order_acquire) != state->LastMaterializedGeneration.load(std::memory_order_acquire)
                || chunk->Owner.load(std::memory_order_acquire) != state->Reader) {
                return std::nullopt;
            }
            const auto chunkState = chunk->State.load(std::memory_order_acquire);
            if (chunkState != EChunkState::Writable && chunkState != EChunkState::PendingSeal && chunkState != EChunkState::Sealed) {
                return std::nullopt;
            }
        }
        return state->LastMaterializedValue.load(std::memory_order_acquire);
    }

    ui32 TInMemoryMetricsBackend::GetLineId(const TLineWriterState* state) const noexcept {
        return state && state->Status.load(std::memory_order_acquire) == ELineWriterStatus::Ready && state->Reader ? state->Reader->LineId : 0;
    }

    void TInMemoryMetricsBackend::MarkMaterialized(TLineWriterState* state, ui64 value) noexcept {
        if (state) {
            {
                TChunk* chunk = state->Reader->Storage.Writable.load(std::memory_order_acquire);
                state->LastMaterializedChunk.store(chunk, std::memory_order_release);
                state->LastMaterializedGeneration.store(chunk->Generation.load(std::memory_order_acquire), std::memory_order_release);
            }
            state->LastMaterializedValue.store(value, std::memory_order_release);
            state->HasLastMaterialized.store(true, std::memory_order_release);
        }
    }

} // namespace NActors
