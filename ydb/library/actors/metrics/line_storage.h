#pragma once

#include "line_base.h"

#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <atomic>
#include <memory>

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

        std::atomic<TLineReader*> Owner = nullptr;
        std::atomic<ui32> OwnerLineId = 0;
        TVector<char> Payload;
    };

    struct TLineStorage {
        mutable TMutex Lock;
        std::atomic<TChunk*> Writable = nullptr;
        TVector<TChunk*> Chunks;
    };

    class TLineReader {
    public:
        ui32 LineId = 0;
        TLineKey Key;
        TLineMeta Meta;
        std::atomic<ELineState> State = ELineState::Open;
        TLineStorage Storage;
        std::unique_ptr<TLineWriterState> Writer;
    };

    struct TSnapshotPinnedChunk {
        TLineWriteBackend* Backend = nullptr;
        TChunk* Chunk = nullptr;
        TChunkView View;
    };

    struct TSnapshotData {
        ~TSnapshotData();

        TTimeAnchor Anchor;
        TVector<TSnapshotPinnedChunk> Chunks;
    };

    namespace NInMemoryMetricsPrivate {
        TInstant DecodeTs(const TTimeAnchor& anchor, NHPTimer::STime ts) noexcept;
        bool TryPinChunk(TChunk* chunk) noexcept;
    } // namespace NInMemoryMetricsPrivate

} // namespace NActors
