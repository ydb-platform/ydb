#pragma once

#include "chunk_reserve.h"
#include <ydb/library/actors/util/intrusive_funnel_queue.h>
#include <util/generic/deque.h>
#include <functional>
#include "line_read.h"
#include "line_write.h"

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/vector.h>

#include <atomic>
#include <cstddef>
#include <limits>
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

    struct TChunk : public TIntrusiveListItem<TChunk>, public TIntrusiveFunnelQueueItem<TChunk> {
        static constexpr size_t PayloadAlignment = TAlignedChunkPayload::Alignment;

        explicit TChunk(ui32 chunkId, ui32 chunkSize)
            : ChunkId(chunkId)
            , Payload(chunkSize)
        {
        }

        // Manager-only heap position and contribution to frozen byte statistics.
        static constexpr size_t NoVictimIndex = std::numeric_limits<size_t>::max();
        size_t VictimIndex = NoVictimIndex;
        ui32 AccountedCommittedBytes = 0;
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

    class TLineReader : public TIntrusiveListItem<TLineReader> {
    public:
        bool WaitingForRefill = false; // Owner-only membership in refill list.
        ui32 LineId = 0;
        TLineKey Key;
        TLineMeta Meta;
        std::atomic<ELineState> State = ELineState::Open;
        TLineStorage Storage;
        std::unique_ptr<TChunkReserve> Reserve;
        std::shared_ptr<TLineWriterState> WriteState;
    };

    // Shared with owning snapshots. Last-pin release publishes a return;
    // shutdown disables and drains notification callbacks before backend teardown.
    class TChunkPool {
    public:
        TChunkPool(ui32 chunkCount, ui32 chunkSize);
        TChunk* TryAcquire();
        void Return(TChunk* chunk) noexcept;
        void ReleasePin(TChunk* chunk) noexcept;
        void SetNotify(std::function<void()> notify);
        void StopNotify() noexcept;
        bool DrainReleased();
        size_t FreeCount() const noexcept { return Free.size(); }
        // Freeze only after the writer has handed the chunk to the manager.
        void AccountCommittedBytes(TChunk* chunk);
        ui64 AccountedCommittedBytes = 0;
        THashSet<TChunk*> UnaccountedChunks; // Reserved/writable/pending seal only.
        size_t RetiringCount = 0; // Owner-only; includes queued returns.
        TIntrusiveFunnelQueue<TChunk> Sealed;
        TIntrusiveFunnelQueue<TChunk> Released;

        TVector<std::unique_ptr<TChunk>> Storage;

    private:
        void NotifyReleased() noexcept;
        TDeque<TChunk*> Free; // Owner-only.
        std::function<void()> Notify;
        // Low bit closes admission; remaining bits count callbacks in flight.
        std::atomic<ui64> NotifyGate = 0;
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
