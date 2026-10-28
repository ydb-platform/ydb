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
#include <util/system/yassert.h>
#include <util/thread/lfqueue.h>

#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <limits>
#include <memory>

namespace NActors {

    namespace {
        // At most one heap entry per sealed chunk. Explicit removal avoids stale
        // entries and rebuilding the entire heap when a line is evicted.
        class TVictimHeap {
        public:
            bool Empty() const { return Chunks.empty(); }
            size_t Size() const { return Chunks.size(); }
            TChunk* Front() const { return Chunks.front(); }

            void Push(TChunk* chunk) {
                Y_ABORT_UNLESS(chunk->VictimIndex == TChunk::NoVictimIndex);
                chunk->VictimIndex = Chunks.size();
                Chunks.push_back(chunk);
                SiftUp(chunk->VictimIndex);
            }

            void Remove(TChunk* chunk) {
                const size_t index = chunk->VictimIndex;
                if (index == TChunk::NoVictimIndex) {
                    return;
                }
                Swap(index, Chunks.size() - 1);
                Chunks.pop_back();
                chunk->VictimIndex = TChunk::NoVictimIndex;
                if (index < Chunks.size()) {
                    if (index && Before(Chunks[index], Chunks[(index - 1) / 2])) {
                        SiftUp(index);
                    } else {
                        SiftDown(index);
                    }
                }
            }

        private:
            static bool Before(const TChunk* lhs, const TChunk* rhs) {
                const auto leftTs = lhs->LastTs.load(std::memory_order_relaxed);
                const auto rightTs = rhs->LastTs.load(std::memory_order_relaxed);
                return leftTs != rightTs ? leftTs < rightTs : lhs->ChunkId < rhs->ChunkId;
            }

            void Swap(size_t lhs, size_t rhs) {
                std::swap(Chunks[lhs], Chunks[rhs]);
                Chunks[lhs]->VictimIndex = lhs;
                Chunks[rhs]->VictimIndex = rhs;
            }

            void SiftUp(size_t index) {
                while (index && Before(Chunks[index], Chunks[(index - 1) / 2])) {
                    const size_t parent = (index - 1) / 2;
                    Swap(index, parent);
                    index = parent;
                }
            }

            void SiftDown(size_t index) {
                while (index * 2 + 1 < Chunks.size()) {
                    size_t child = index * 2 + 1;
                    if (child + 1 < Chunks.size() && Before(Chunks[child + 1], Chunks[child])) {
                        ++child;
                    }
                    if (!Before(Chunks[child], Chunks[index])) {
                        return;
                    }
                    Swap(index, child);
                    index = child;
                }
            }

            TVector<TChunk*> Chunks;
        };

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
            // The manager owns this free chunk; the SPSC reserve publishes
            // its initialized fields and payload to the next writer.
            chunk->Owner.store(line, std::memory_order_relaxed);
            chunk->OwnerLineId.store(line->LineId, std::memory_order_relaxed);
            chunk->CommittedBytes.store(0, std::memory_order_relaxed);
            chunk->Readers.store(0, std::memory_order_relaxed);
            chunk->FirstTs.store(0, std::memory_order_relaxed);
            chunk->LastTs.store(0, std::memory_order_relaxed);
            std::memset(chunk->Payload.data(), 0, chunk->Payload.size());
            chunk->Generation.fetch_add(1, std::memory_order_relaxed);
            chunk->State.store(EChunkState::Reserved, std::memory_order_release);
        }

        bool RemoveChunkFromLine(TLineReader* line, TChunk* chunk) {
            line->Storage.Chunks.Remove(chunk);
            return line->State.load(std::memory_order_acquire) == ELineState::Closed && line->Storage.Chunks.Empty();
        }

        bool TryAccessChunkMemory(TChunk* chunk, void* opaque, TAccessChunkMemoryFn accessChunkMemory) {
            const ui32 previousUsedPayloadBytes = chunk->CommittedBytes.load(std::memory_order_relaxed);
            TWritableChunkMemory chunkMemory{
                .Payload = std::span<char>(chunk->Payload.data(), chunk->Payload.size()),
                .UsedPayloadBytes = previousUsedPayloadBytes,
                .FirstTs = chunk->FirstTs.load(std::memory_order_relaxed),
                .LastTs = chunk->LastTs.load(std::memory_order_relaxed),
            };
            if (!accessChunkMemory(opaque, chunkMemory)) {
                return false;
            }

            const ui32 usedPayloadBytes = chunkMemory.UsedPayloadBytes;
            Y_ABORT_UNLESS(usedPayloadBytes <= chunk->Payload.size());
            if (usedPayloadBytes != 0) {
                if (previousUsedPayloadBytes == 0) {
                    chunk->FirstTs.store(chunkMemory.FirstTs, std::memory_order_relaxed);
                }
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
            Y_ABORT_UNLESS(Config.FreeChunkReservePercent <= 100);
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

        TVictimHeap VictimHeap;
        THashSet<TLineReader*> OpenLines;
        THashSet<TLineReader*> ClosedLines;
        ui64 UserLines = 0;
        ui64 ClosedUserLines = 0;
        // Close is infrequent and never requeues a state. Keep it separate from
        // refill traffic so admission can observe completed closes directly.
        TLockFreeQueue<std::shared_ptr<TLineWriterState>> CloseRequests;
        const std::function<void()> Notify;
        std::atomic<bool> MaintenanceScheduled = false;
        std::atomic<bool> Stopping = false;
        TIntrusiveFunnelQueue<TLineWriterState> LineRequests;
        TIntrusiveList<TLineReader> RefillLines;
        bool FreeReserveRefilling = false;
        bool SelfMetricsInitialized = false;
        TSelfMetricsLines SelfMetrics;
    };

    TInMemoryMetricsBackend::TInMemoryMetricsBackend(TInMemoryMetricsConfig config, std::function<void()> notify)
        : Impl(std::make_unique<TImpl>(std::move(config), std::move(notify)))
    {
        Impl->Pool->SetNotify([this] { RequestMaintenance(); });
    }

    TInMemoryMetricsBackend::~TInMemoryMetricsBackend() {
        if (!Impl) {
            return;
        }

        StopManagement();
        Impl->SelfMetrics = TImpl::TSelfMetricsLines{};
        Impl->SelfMetricsInitialized = false;
        // Writers have stopped; release queue-owned references, including Close
        // requests from the self-metric handles destroyed above.
        DrainClosedLines(std::numeric_limits<size_t>::max());
        DrainLineRequests();
    }

    std::shared_ptr<TLineWriterState> TInMemoryMetricsBackend::CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) {
        auto state = std::make_shared<TLineWriterState>(this);
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
        DrainClosedLines(Impl->LinesByKey.size() >= Impl->MaxLines
            ? Max<size_t>(Impl->MaxLines, NInMemoryMetricsPrivate::MaintenanceBatchSize)
            : NInMemoryMetricsPrivate::MaintenanceBatchSize);
        if (auto it = Impl->LinesByKey.find(key); it != Impl->LinesByKey.end()) {
            auto previous = it->second;
            if (previous->WriteState->Status.load(std::memory_order_acquire) != ELineWriterStatus::Closed) {
                reject();
                return;
            }
            DropClosedLine(previous.get());
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
        state->RegisteredReader = line;
        Impl->LinesById.emplace(line->LineId, line.get());
        Impl->LinesByKey.emplace(line->Key, line);
        Impl->OpenLines.insert(line.get());
        if (!IsRegistryMetricName(line->Key.Name)) {
            ++Impl->UserLines;
        }
        auto pending = ELineWriterStatus::Pending;
        if (!state->Status.compare_exchange_strong(pending, ELineWriterStatus::Ready, std::memory_order_release)) {
            // Close won while registration was preparing the reader.
            CloseLineNow(state.get());
        } else {
            RequestLineMaintenance(state.get());
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
        Impl->Pool->StopNotify();
    }

    void TInMemoryMetricsBackend::BeginMaintenance() noexcept {
        // Reset before draining commands and line requests. Every producer
        // publishes its request through an acq_rel exchange, including coalescing.
        Impl->MaintenanceScheduled.exchange(false, std::memory_order_acq_rel);
    }

    void TInMemoryMetricsBackend::ProcessMaintenance() {
        DrainSealedChunks();
        if (Impl->Pool->DrainReleased()) {
            RequestMaintenance();
        }
        ReplenishFreeReserve();
        DrainClosedLines(Impl->Stopping.load(std::memory_order_acquire)
            ? std::numeric_limits<size_t>::max() : NInMemoryMetricsPrivate::MaintenanceBatchSize);
        DrainLineRequests();
        if (Impl->Stopping.load(std::memory_order_acquire)) {
            return;
        }
        // Only requested lines participate; one chunk per turn in FIFO order.
        ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize;
        while (budget && !Impl->RefillLines.Empty()) {
            --budget;
            TLineReader* line = Impl->RefillLines.PopFront();
            if (line->WriteState->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed) {
                line->WaitingForRefill = false;
                CloseLineNow(line->WriteState.get());
                continue;
            }
            if (line->Reserve->Full()) {
                line->WaitingForRefill = false;
                continue;
            }
            TChunk* chunk = TryAcquireFreeChunk();
            if (!chunk) {
                chunk = TryStealOldestChunk();
            }
            if (!chunk) {
                Impl->RefillLines.PushBack(line);
                // Keep the request until a chunk return, new work, or the tick.
                // Do not reschedule ourselves while capacity is pinned.
                return;
            }
            PrepareChunk(chunk, line);
            line->Storage.Chunks.PushBack(chunk);
            Y_ABORT_UNLESS(line->Reserve->TryPush(chunk));
            if (!line->Reserve->Full()) {
                Impl->RefillLines.PushBack(line);
            } else {
                line->WaitingForRefill = false;
            }
        }
        if (!Impl->RefillLines.Empty()) {
            RequestMaintenance();
        }
    }

    void TInMemoryMetricsBackend::RequestLineMaintenance(TLineWriterState* state) noexcept {
        if (!state->MaintenanceQueued.exchange(true, std::memory_order_acq_rel)) {
            state->QueuedOwner = state->shared_from_this();
            Impl->LineRequests.Push(state);
        }
        RequestMaintenance();
    }

    void TInMemoryMetricsBackend::DrainLineRequests() {
        ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize;
        const bool stopping = Impl->Stopping.load(std::memory_order_acquire);
        while (stopping || budget) {
            if (!stopping) {
                --budget;
            }
            const auto result = Impl->LineRequests.TryPop();
            if (result.Status == TIntrusiveFunnelQueue<TLineWriterState>::ETryPopStatus::Empty) {
                return;
            }
            if (result.Status == TIntrusiveFunnelQueue<TLineWriterState>::ETryPopStatus::Retry) {
                RequestMaintenance();
                return;
            }
            auto state = std::move(result.Item->QueuedOwner);
            // Detach first, then permit a producer to reuse the hook/owner slot.
            // Exchange also acquires coalesced requests before reading Status.
            state->MaintenanceQueued.exchange(false, std::memory_order_acq_rel);
            auto line = state->RegisteredReader.lock();
            if (!line) {
                continue;
            }
            if (state->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed) {
                CloseLineNow(state.get());
            } else if (!stopping && !line->WaitingForRefill && !line->Reserve->Full()) {
                line->WaitingForRefill = true;
                Impl->RefillLines.PushBack(line.get());
            }
        }
        RequestMaintenance();
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
            // The queue owns a pin until the manager fully detaches the node.
            current->Readers.fetch_add(1, std::memory_order_acq_rel);
            current->State.store(EChunkState::PendingSeal, std::memory_order_release);
            Impl->Pool->Sealed.Push(current);
        }
        TChunk* next = line->Reserve->TryPop();
        if (next) {
            next->State.store(EChunkState::Writable, std::memory_order_release);
            line->Storage.Writable.store(next, std::memory_order_release);
        }
        // Queue this line on every consumption, or retry after an empty reserve.
        RequestLineMaintenance(state);
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

        stats.FreeChunks = Impl->Pool->FreeCount();
        stats.UsedChunks = Impl->ChunkCount - stats.FreeChunks;
        stats.MemoryUsedBytes = stats.UsedChunks * Impl->Config.ChunkSizeBytes;
        stats.RetiringChunks = Impl->Pool->RetiringCount;
        stats.SealedChunks = Impl->VictimHeap.Size();
        stats.CommittedBytes = Impl->Pool->AccountedCommittedBytes;
        // Only chunks still owned by writers need live reads. Sealed/retiring
        // bytes are accounted once by the manager and removed on return.
        for (const auto* chunk : Impl->Pool->UnaccountedChunks) {
            const auto state = chunk->State.load(std::memory_order_acquire);
            stats.CommittedBytes += chunk->CommittedBytes.load(std::memory_order_acquire);
            stats.WritableChunks += state == EChunkState::Writable;
            stats.SealedChunks += state == EChunkState::PendingSeal;
        }

        stats.ReuseWatermark = Impl->ReuseWatermark.load(std::memory_order_acquire);
        stats.AppendFailuresTotal = Impl->AppendFailures.load(std::memory_order_acquire);

        stats.Lines = Impl->UserLines;
        stats.ClosedLines = Impl->ClosedUserLines;
        // Include closes published by writers but not yet processed by the actor.
        for (const auto* line : Impl->OpenLines) {
            if (!IsRegistryMetricName(line->Key.Name)
                && line->WriteState->Status.load(std::memory_order_acquire) == ELineWriterStatus::Closed) {
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

    void TInMemoryMetricsBackend::DrainClosedLines(size_t budget) {
        std::shared_ptr<TLineWriterState> state;
        while (budget && Impl->CloseRequests.Dequeue(&state)) {
            --budget;
            CloseLineNow(state.get());
        }
        if (!budget) {
            RequestMaintenance();
        }
    }

    bool TInMemoryMetricsBackend::TryDropClosedLine() {
        if (Impl->ClosedLines.empty()) {
            return false;
        }
        DropClosedLine(*Impl->ClosedLines.begin());
        return true;
    }

    void TInMemoryMetricsBackend::DropClosedLine(TLineReader* line) {
        auto keepAlive = Impl->LinesByKey.at(line->Key);
        CloseLineNow(line->WriteState.get());
        while (!line->Storage.Chunks.Empty()) {
            RetireChunk(line->Storage.Chunks.PopFront());
        }
        MaybeDropClosedLine(line);
    }

    void TInMemoryMetricsBackend::MaybeDropClosedLine(TLineReader* line) {
        if (line->State.load(std::memory_order_relaxed) != ELineState::Closed || !line->Storage.Chunks.Empty()) {
            return;
        }
        if (!Impl->LinesById.erase(line->LineId)) {
            return;
        }
        Impl->ClosedLines.erase(line);
        if (!IsRegistryMetricName(line->Key.Name)) {
            --Impl->UserLines;
            --Impl->ClosedUserLines;
        }
        const auto key = line->Key;
        Impl->LinesByKey.erase(key);
    }

    TChunk* TInMemoryMetricsBackend::TryAcquireFreeChunk() {
        return Impl->Pool->TryAcquire();
    }

    void TInMemoryMetricsBackend::PublishSealedChunk(TChunk* chunk) {
        Impl->Pool->AccountCommittedBytes(chunk);
        Impl->VictimHeap.Push(chunk);
    }

    void TInMemoryMetricsBackend::ReturnChunkToFree(TChunk* chunk) {
        Impl->Pool->Return(chunk);
    }


    void TInMemoryMetricsBackend::RetireChunk(TChunk* chunk) {
        Impl->VictimHeap.Remove(chunk);
        Impl->Pool->AccountCommittedBytes(chunk);
        const auto lastTs = static_cast<ui64>(chunk->LastTs.load(std::memory_order_acquire));
        ui64 watermark = Impl->ReuseWatermark.load(std::memory_order_relaxed);
        while (watermark < lastTs
               && !Impl->ReuseWatermark.compare_exchange_weak(watermark, lastTs, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }

        // Publish the state before the CAS lets the last reader reclaim the chunk.
        ++Impl->Pool->RetiringCount;
        chunk->State.store(EChunkState::Retiring, std::memory_order_release);
        i32 readers = chunk->Readers.load(std::memory_order_acquire);
        while (true) {
            Y_ABORT_UNLESS(readers >= 0);
            if (chunk->Readers.compare_exchange_weak(readers, RetiringBias + readers, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
        }
        if (readers == 0) {
            --Impl->Pool->RetiringCount;
            ReturnChunkToFree(chunk);
        }
    }

    bool TInMemoryMetricsBackend::RetireOldestChunk() {
        if (Impl->VictimHeap.Empty()) {
            return false;
        }
        TChunk* victim = Impl->VictimHeap.Front();
        TLineReader* owner = victim->Owner.load(std::memory_order_relaxed);
        const bool drop = RemoveChunkFromLine(owner, victim);
        RetireChunk(victim);
        if (drop) {
            MaybeDropClosedLine(owner);
        }
        return true;
    }

    TChunk* TInMemoryMetricsBackend::TryStealOldestChunk() {
        const size_t pendingLimit = Max<ui64>(1,
            ui64(Impl->ChunkCount) * Impl->Config.FreeChunkReservePercent / 100);
        while (Impl->Pool->RetiringCount < pendingLimit && RetireOldestChunk()) {
            if (TChunk* free = TryAcquireFreeChunk()) {
                return free;
            }
        }
        return nullptr;
    }

    void TInMemoryMetricsBackend::DrainSealedChunks() {
        for (ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize; budget; --budget) {
            const auto result = Impl->Pool->Sealed.TryPop();
            if (result.Status == TIntrusiveFunnelQueue<TChunk>::ETryPopStatus::Empty) {
                return;
            }
            if (result.Status == TIntrusiveFunnelQueue<TChunk>::ETryPopStatus::Retry) {
                RequestMaintenance();
                return;
            }
            TChunk* chunk = result.Item;
            if (chunk->State.load(std::memory_order_acquire) == EChunkState::PendingSeal) {
                chunk->State.store(EChunkState::Sealed, std::memory_order_release);
                PublishSealedChunk(chunk);
            }
            Impl->Pool->ReleasePin(chunk);
        }
        RequestMaintenance();
    }

    void TInMemoryMetricsBackend::ReplenishFreeReserve() {
        if (Impl->Stopping.load(std::memory_order_acquire)) {
            return;
        }
        const size_t target = ui64(Impl->ChunkCount) * Impl->Config.FreeChunkReservePercent / 100;
        const size_t low = (target + 1) / 2;
        if (Impl->Pool->FreeCount() < low) {
            Impl->FreeReserveRefilling = true;
        }
        if (!Impl->FreeReserveRefilling) {
            return;
        }
        ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize;
        while (Impl->Pool->FreeCount() + Impl->Pool->RetiringCount < target && budget) {
            if (!RetireOldestChunk()) {
                return;
            }
            --budget;
        }
        if (Impl->Pool->FreeCount() + Impl->Pool->RetiringCount >= target) {
            Impl->FreeReserveRefilling = false;
        } else if (!budget) {
            RequestMaintenance();
        }
    }

    void TInMemoryMetricsBackend::CloseLine(TLineWriterState* state) noexcept {
        if (state) {
            state->ClosedAtTs = CurrentTimestampTs();
            const auto previous = state->Status.exchange(ELineWriterStatus::Closed, std::memory_order_acq_rel);
            if (previous == ELineWriterStatus::Ready) {
                Impl->CloseRequests.Enqueue(state->shared_from_this());
                RequestMaintenance();
            }
            // Pending registration observes Closed and never admits this state.
        }
    }

    void TInMemoryMetricsBackend::CloseLineNow(TLineWriterState* state) noexcept {
        auto owner = state->RegisteredReader.lock();
        TLineReader* line = owner.get();
        if (!line || line->State.load(std::memory_order_relaxed) == ELineState::Closed) {
            return;
        }
        if (line->WaitingForRefill) {
            Impl->RefillLines.Remove(line);
            line->WaitingForRefill = false;
        }
        Impl->OpenLines.erase(line);
        Impl->ClosedLines.insert(line);
        if (!IsRegistryMetricName(line->Key.Name)) {
            ++Impl->ClosedUserLines;
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
        return CaptureSnapshot(nullptr, true);
    }

    TInMemorySnapshot TInMemoryMetricsBackend::CaptureSnapshot(ui32 lineId) const {
        const auto it = Impl->LinesById.find(lineId);
        return CaptureSnapshot(it != Impl->LinesById.end() ? it->second : nullptr, false);
    }

    TInMemorySnapshot TInMemoryMetricsBackend::CaptureSnapshot(TStringBuf name, std::span<const TLabel> labels) const {
        const auto it = Impl->LinesByKey.find(MakeLineKey(name, labels));
        return CaptureSnapshot(it != Impl->LinesByKey.end() ? it->second.get() : nullptr, false);
    }

    TInMemorySnapshot TInMemoryMetricsBackend::CaptureSnapshot(TLineReader* selectedLine, bool allLines) const {
        auto data = std::make_shared<NInMemoryMetricsPrivate::TSnapshot>();
        auto& snapshot = *data;
        snapshot.Pool = Impl->Pool;
        snapshot.Anchor = Impl->TimeAnchor;
        snapshot.CommonLabels = GetCommonLabels();

        snapshot.SnapshotLines.reserve(allLines ? Impl->LinesById.size() : (selectedLine ? 1 : 0));
        auto captureLine = [&](TLineReader* line) {
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
        };
        if (allLines) {
            for (const auto& [_, line] : Impl->LinesById) {
                captureLine(line);
            }
        } else if (selectedLine) {
            captureLine(selectedLine);
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
