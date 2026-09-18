#pragma once

#include "chunk_reserve.h"
#include "line_read.h"
#include "line_write.h"

#include <util/generic/vector.h>
#include <util/generic/intrlist.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>

namespace NActors {

    enum class ELineState : ui8 {
        Open,
        Closed,
    };

    enum class EChunkState : ui8 {
        Free,
        Reserved,
        Writable,
        PendingSeal,
        Sealed,
        Retiring,
    };

    class TAlignedChunkPayload {
    public:
        static constexpr size_t Alignment = 4096;

        explicit TAlignedChunkPayload(size_t size)
            : Size(size)
            , Data(static_cast<char*>(::operator new(size, std::align_val_t(Alignment))))
        {
        }

        ~TAlignedChunkPayload() {
            ::operator delete(Data, std::align_val_t(Alignment));
        }

        TAlignedChunkPayload(const TAlignedChunkPayload&) = delete;
        TAlignedChunkPayload& operator=(const TAlignedChunkPayload&) = delete;
        TAlignedChunkPayload(TAlignedChunkPayload&&) = delete;
        TAlignedChunkPayload& operator=(TAlignedChunkPayload&&) = delete;

        char* data() noexcept {
            return Data;
        }

        const char* data() const noexcept {
            return Data;
        }

        size_t size() const noexcept {
            return Size;
        }

    private:
        size_t Size = 0;
        char* Data = nullptr;
    };

    struct TChunk : public TIntrusiveListItem<TChunk> {
        static constexpr size_t PayloadAlignment = TAlignedChunkPayload::Alignment;

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
        TAlignedChunkPayload Payload;
    };

    struct TLineStorage {
        std::atomic<TChunk*> Writable = nullptr;
        // Links are owned exclusively by the metadata owner, never the writer.
        TIntrusiveList<TChunk> Chunks;
    };

    class TLineReader {
    public:
        ui32 LineId = 0;
        TLineKey Key;
        TLineMeta Meta;
        std::atomic<ELineState> State = ELineState::Open;
        TLineStorage Storage;
        std::unique_ptr<TChunkReserve> Reserve;
        std::shared_ptr<TLineWriterState> WriteState;
    };

    // Shared with owning snapshots. No callback into an actor or backend when
    // the last pin is released, including after actor-system destruction.
    class TChunkPool {
    public:
        TChunkPool(ui32 chunkCount, ui32 chunkSize);
        TChunk* TryAcquire();
        void Return(TChunk* chunk) noexcept;
        void ReleasePin(TChunk* chunk) noexcept;

        TVector<std::unique_ptr<TChunk>> Storage;

    private:
        size_t FreeBitmapWords = 0;
        std::unique_ptr<std::atomic<ui64>[]> FreeBitmap;
    };

    namespace NInMemoryMetricsPrivate {
        struct TChunkSnapshotView {
            TChunkView Meta;
            std::span<const char> Payload;
        };

        struct TSnapshotPinnedChunk {
            TChunk* Chunk = nullptr;
            TChunkView View;
            ui32 CommittedBytes = 0;
        };

        class TSnapshot {
        public:
            TSnapshot(const TSnapshot&) = delete;
            TSnapshot(TSnapshot&&) = delete;
            TSnapshot& operator=(const TSnapshot&) = delete;
            TSnapshot& operator=(TSnapshot&&) = delete;

            TSnapshot();
            ~TSnapshot();

            std::shared_ptr<TChunkPool> Pool;
            TTimeAnchor Anchor;
            TVector<TSnapshotPinnedChunk> SnapshotChunks;
            TVector<TLineSnapshot> SnapshotLines;
            TVector<TLabel> CommonLabels;
        };

        TInstant DecodeTs(const TTimeAnchor& anchor, NHPTimer::STime ts) noexcept;
        bool TryPinChunk(TChunk* chunk) noexcept;
    } // namespace NInMemoryMetricsPrivate

    template<class TCallback>
    void TLineSnapshot::ForEachChunk(TCallback&& cb) const {
        if (!Owner) {
            return;
        }
        for (size_t i = 0; i < ChunkCount; ++i) {
            const auto& pinned = Owner->SnapshotChunks[ChunkBegin + i];
            cb(NInMemoryMetricsPrivate::TChunkSnapshotView{
                .Meta = pinned.View,
                .Payload = std::span<const char>(pinned.Chunk->Payload.data(), pinned.CommittedBytes),
            });
        }
    }

    template<class TCallback>
    void NInMemoryMetricsPrivate::TLineSnapshotAccess::ForEachChunk(const TLineSnapshot& snapshot, TCallback&& cb) {
        snapshot.ForEachChunk(std::forward<TCallback>(cb));
    }

    inline TInstant NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(const TLineSnapshot& snapshot, NHPTimer::STime ts) noexcept {
        return snapshot.DecodeTimestampTs(ts);
    }

} // namespace NActors
