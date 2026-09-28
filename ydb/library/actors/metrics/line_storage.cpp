#include "line_storage.h"

#include <ydb/library/actors/util/datetime.h>
#include <thread>
#include <limits>
#include <cstring>

namespace NActors {

    TChunkPool::TChunkPool(ui32 chunkCount, ui32 chunkSize)
    {
        Storage.reserve(chunkCount);
        for (ui32 i = 0; i < chunkCount; ++i) {
            Storage.push_back(std::make_unique<TChunk>(i, chunkSize));
            Free.push_back(Storage.back().get());
        }
    }

    TChunk* TChunkPool::TryAcquire() {
        if (Free.empty()) {
            return nullptr;
        }
        TChunk* chunk = Free.front();
        Free.pop_front();
        UnaccountedChunks.insert(chunk);
        return chunk;
    }

    void TChunkPool::SetNotify(std::function<void()> notify) {
        Notify = std::move(notify); // Before publishing the pool to writers/readers.
    }

    void TChunkPool::StopNotify() noexcept {
        NotifyGate.fetch_or(1, std::memory_order_acq_rel);
        // Shutdown only. No callback may outlive its backend/actor endpoint.
        while (NotifyGate.load(std::memory_order_acquire) != 1) {
            std::this_thread::yield();
        }
    }

    void TChunkPool::NotifyReleased() noexcept {
        ui64 gate = NotifyGate.load(std::memory_order_relaxed);
        do {
            if (gate & 1) {
                return;
            }
        } while (!NotifyGate.compare_exchange_weak(gate, gate + 2, std::memory_order_acquire, std::memory_order_relaxed));
        if (Notify) {
            Notify();
        }
        NotifyGate.fetch_sub(2, std::memory_order_release);
    }

    bool TChunkPool::DrainReleased() {
        for (ui32 budget = NInMemoryMetricsPrivate::MaintenanceBatchSize; budget; --budget) {
            const auto result = Released.TryPop();
            if (result.Status == TIntrusiveFunnelQueue<TChunk>::ETryPopStatus::Empty) {
                return false;
            }
            if (result.Status == TIntrusiveFunnelQueue<TChunk>::ETryPopStatus::Retry) {
                return true;
            }
            Y_ABORT_UNLESS(RetiringCount);
            --RetiringCount;
            Return(result.Item);
        }
        return true;
    }

    void TChunkPool::AccountCommittedBytes(TChunk* chunk) {
        if (UnaccountedChunks.erase(chunk)) {
            chunk->AccountedCommittedBytes = chunk->CommittedBytes.load(std::memory_order_acquire);
            AccountedCommittedBytes += chunk->AccountedCommittedBytes;
        }
    }

    void TChunkPool::Return(TChunk* chunk) noexcept {
        UnaccountedChunks.erase(chunk);
        AccountedCommittedBytes -= chunk->AccountedCommittedBytes;
        chunk->AccountedCommittedBytes = 0;
        chunk->Owner.store(nullptr, std::memory_order_relaxed);
        chunk->OwnerLineId.store(0, std::memory_order_relaxed);
        chunk->CommittedBytes.store(0, std::memory_order_relaxed);
        chunk->FirstTs.store(0, std::memory_order_relaxed);
        chunk->LastTs.store(0, std::memory_order_relaxed);
        chunk->Readers.store(0, std::memory_order_relaxed);
        chunk->Generation.fetch_add(1, std::memory_order_release);
        chunk->State.store(EChunkState::Free, std::memory_order_release);
        Free.push_back(chunk);
    }

    void TChunkPool::ReleasePin(TChunk* chunk) noexcept {
        const i32 previous = chunk->Readers.fetch_sub(1, std::memory_order_acq_rel);
        if (previous == std::numeric_limits<i32>::min() + 1) {
            Released.Push(chunk);
            NotifyReleased();
        }
    }

    void TInMemorySnapshot::Read(const TReadSnapshotCallback& cb) const {
        TSnapshotView view;
        if (Data) {
            view.CommonLabels = Data->CommonLabels.data();
            view.CommonLabelsCount = Data->CommonLabels.size();
            view.Lines = Data->SnapshotLines.data();
            view.LinesCount = Data->SnapshotLines.size();
        }
        cb(view);
    }



    namespace NInMemoryMetricsPrivate {
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
    } // namespace NInMemoryMetricsPrivate

    TLineSnapshot::TLineSnapshot() = default;
    TLineSnapshot::TLineSnapshot(TLineSnapshot&&) noexcept = default;
    TLineSnapshot& TLineSnapshot::operator=(TLineSnapshot&&) noexcept = default;
    TLineSnapshot::~TLineSnapshot() = default;

    TInstant TLineSnapshot::DecodeTimestampTs(NHPTimer::STime ts) const noexcept {
        return Owner ? NInMemoryMetricsPrivate::DecodeTs(Owner->Anchor, ts) : TInstant::Zero();
    }

    NInMemoryMetricsPrivate::TSnapshot::TSnapshot() = default;

    NInMemoryMetricsPrivate::TSnapshot::~TSnapshot() {
        for (const auto& chunk : SnapshotChunks) {
            Pool->ReleasePin(chunk.Chunk);
        }
    }

} // namespace NActors
