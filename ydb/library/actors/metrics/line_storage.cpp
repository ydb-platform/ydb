#include "line_storage.h"
#include "lines/raw_line_frontend.h"

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

    TSnapshotData::~TSnapshotData() {
        for (const auto& chunk : Chunks) {
            if (chunk.Backend && chunk.Chunk) {
                chunk.Backend->ReleasePinnedChunk(chunk.Chunk);
            }
        }
    }

    TLineSnapshot::TLineSnapshot() = default;
    TLineSnapshot::TLineSnapshot(const TLineSnapshot&) = default;
    TLineSnapshot::TLineSnapshot(TLineSnapshot&&) noexcept = default;
    TLineSnapshot& TLineSnapshot::operator=(const TLineSnapshot&) = default;
    TLineSnapshot& TLineSnapshot::operator=(TLineSnapshot&&) noexcept = default;
    TLineSnapshot::~TLineSnapshot() = default;

    void TLineSnapshot::ForEachRecord(const std::function<void(const TRecordView&)>& cb) const {
        ForEachRecordInRange(TInstant::Zero(), TInstant::Max(), cb);
    }

    void TLineSnapshot::ForEachChunk(const std::function<void(const TChunkSnapshotView&)>& cb) const {
        if (!Data) {
            return;
        }
        for (size_t chunkIndex : ChunkIndexes) {
            const auto& pinned = Data->Chunks[chunkIndex];
            cb(TChunkSnapshotView{
                .Meta = pinned.View,
                .Payload = std::span<const char>(pinned.Chunk->Payload.data(), pinned.View.CommittedBytes),
            });
        }
    }

    TInstant TLineSnapshot::DecodeTimestampTs(NHPTimer::STime ts) const noexcept {
        return Data ? NInMemoryMetricsPrivate::DecodeTs(Data->Anchor, ts) : TInstant::Zero();
    }

    void TLineSnapshot::ForEachRecordInRange(TInstant beginTs, TInstant endTs, const std::function<void(const TRecordView&)>& cb) const {
        const auto* frontend = Meta.Frontend ? Meta.Frontend : &TRawLineFrontend<>::Descriptor();
        if (frontend->ReadRange) {
            frontend->ReadRange(*this, beginTs, endTs, cb);
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

} // namespace NActors
