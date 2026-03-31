#pragma once

#include "line_read.h"
#include "line_write.h"

#include <util/generic/vector.h>
#include <util/system/mutex.h>

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
        Writable,
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

    struct TChunk {
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
        std::unique_ptr<TLineWriterState> WriteState;
    };

    namespace NInMemoryMetricsPrivate {
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
            cb(TChunkSnapshotView{
                .Meta = pinned.View,
                .Payload = std::span<const char>(pinned.Chunk->Payload.data(), pinned.Chunk->Payload.size()),
            });
        }
    }

    template<class TCallback>
    void TLineSnapshotAccess::ForEachChunk(const TLineSnapshot& snapshot, TCallback&& cb) {
        snapshot.ForEachChunk(std::forward<TCallback>(cb));
    }

    inline TInstant TLineSnapshotAccess::DecodeTimestampTs(const TLineSnapshot& snapshot, NHPTimer::STime ts) noexcept {
        return snapshot.DecodeTimestampTs(ts);
    }

} // namespace NActors
