#include "line_storage.h"
#include "inmemory_backend.h"

#include <ydb/library/actors/util/datetime.h>

namespace NActors {

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
            if (chunk.Backend && chunk.Chunk) {
                chunk.Backend->ReleasePinnedChunk(chunk.Chunk);
            }
        }
    }

} // namespace NActors
