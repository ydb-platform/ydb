#include "line_storage.h"

#include <ydb/library/actors/util/datetime.h>
#include <bit>
#include <limits>
#include <cstring>

namespace NActors {

    TChunkPool::TChunkPool(ui32 chunkCount, ui32 chunkSize)
        : FreeBitmapWords((static_cast<size_t>(chunkCount) + 63) / 64)
        , FreeBitmap(std::make_unique<std::atomic<ui64>[]>(FreeBitmapWords))
    {
        Storage.reserve(chunkCount);
        for (ui32 i = 0; i < chunkCount; ++i) {
            Storage.push_back(std::make_unique<TChunk>(i, chunkSize));
            FreeBitmap[i / 64].fetch_or(ui64{1} << (i % 64), std::memory_order_relaxed);
        }
    }

    TChunk* TChunkPool::TryAcquire() {
        for (size_t word = 0; word < FreeBitmapWords; ++word) {
            auto& available = FreeBitmap[word];
            ui64 bits = available.load(std::memory_order_relaxed);
            while (bits) {
                const unsigned bit = std::countr_zero(bits);
                if (available.compare_exchange_weak(bits, bits & ~(ui64{1} << bit),
                        std::memory_order_acquire, std::memory_order_relaxed)) {
                    return Storage[word * 64 + bit].get();
                }
            }
        }
        return nullptr;
    }

    void TChunkPool::Return(TChunk* chunk) noexcept {
        chunk->Owner.store(nullptr, std::memory_order_relaxed);
        chunk->OwnerLineId.store(0, std::memory_order_relaxed);
        chunk->CommittedBytes.store(0, std::memory_order_relaxed);
        chunk->FirstTs.store(0, std::memory_order_relaxed);
        chunk->LastTs.store(0, std::memory_order_relaxed);
        chunk->Readers.store(0, std::memory_order_relaxed);
        chunk->Generation.fetch_add(1, std::memory_order_release);
        chunk->State.store(EChunkState::Free, std::memory_order_release);
        FreeBitmap[chunk->ChunkId / 64].fetch_or(ui64{1} << (chunk->ChunkId % 64), std::memory_order_release);
    }

    void TChunkPool::ReleasePin(TChunk* chunk) noexcept {
        const i32 previous = chunk->Readers.fetch_sub(1, std::memory_order_acq_rel);
        if (previous == std::numeric_limits<i32>::min() + 1) {
            Return(chunk);
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
