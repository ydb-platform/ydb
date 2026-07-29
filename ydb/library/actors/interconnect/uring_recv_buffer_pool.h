#pragma once

#include <ydb/library/actors/util/rc_buf.h>

#include <util/system/mutex.h>
#include <util/system/guard.h>
#include <util/generic/vector.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <cstdlib>
#include <cstring>
#include <memory>

namespace NActors {

    // Provided-buffer ring for io_uring multishot recv with true zero-copy delivery.
    //
    // The kernel fills one of the registered buffers and reports its index in the CQE.
    // Instead of copying the bytes out, we hand the buffer to the input session wrapped
    // in a TRcBuf backed by TUringRecvBufferChunk. When the last reference to that TRcBuf
    // is dropped (possibly on an arbitrary worker thread, since the event payload outlives
    // the input session), the chunk pushes the buffer index onto a thread-safe freelist.
    // The reaper thread drains that freelist and re-adds the buffers to the kernel ring
    // (DrainFreelist), keeping all io_uring_buf_ring_* calls on a single thread.
    class TUringRecvBufferPool {
    public:
        static constexpr ui32 NumBuffers = 256;
        static constexpr ui32 BufferSize = 16384;

        // Shared between the pool (reaper-owned) and any outstanding chunks. Keeps the
        // backing memory alive until the last TRcBuf referencing it is released, even if
        // the pool/session is torn down first.
        struct TCore : public TThrRefBase {
            char* Memory = nullptr;
            TMutex FreeLock;
            TVector<ui16> FreeList; // indices released by consumers, awaiting re-add to the ring

            ~TCore() {
                free(Memory);
            }

            char* GetBuffer(ui16 index) const {
                return Memory + static_cast<size_t>(index) * BufferSize;
            }

            void Release(ui16 index) {
                with_lock (FreeLock) {
                    FreeList.push_back(index);
                }
            }
        };

        class TUringRecvBufferChunk : public IContiguousChunk {
        public:
            TUringRecvBufferChunk(TIntrusivePtr<TCore> core, ui16 index, ui32 len)
                : Core(std::move(core))
                , Index(index)
                , Len(len)
            {}

            ~TUringRecvBufferChunk() override {
                Core->Release(Index);
            }

            TContiguousSpan GetData() const override {
                return {Core->GetBuffer(Index), Len};
            }

            TMutableContiguousSpan UnsafeGetDataMut() override {
                return {Core->GetBuffer(Index), Len};
            }

            size_t GetOccupiedMemorySize() const override {
                return BufferSize;
            }

            IContiguousChunk::TPtr Clone() override {
                return MakeIntrusive<TOwnedCopyChunk>(TString(Core->GetBuffer(Index), Len));
            }

        private:
            // Independent heap copy used when a private mutable copy is required.
            class TOwnedCopyChunk : public IContiguousChunk {
            public:
                explicit TOwnedCopyChunk(TString data)
                    : Data(std::move(data))
                {}
                TContiguousSpan GetData() const override { return {Data.data(), Data.size()}; }
                TMutableContiguousSpan UnsafeGetDataMut() override { return {Data.Detach(), Data.size()}; }
                size_t GetOccupiedMemorySize() const override { return Data.capacity(); }
                IContiguousChunk::TPtr Clone() override { return MakeIntrusive<TOwnedCopyChunk>(Data); }
            private:
                TString Data;
            };

            TIntrusivePtr<TCore> Core;
            ui16 Index;
            ui32 Len;
        };

        TUringRecvBufferPool() = default;

        ~TUringRecvBufferPool() {
            if (BufRing) {
                io_uring_free_buf_ring(Ring, BufRing, NumBuffers, BufGroupId);
            }
            // Core (and its backing memory) is kept alive by any outstanding chunks.
        }

        bool Init(struct io_uring* ring, ui16 bufGroupId) {
            Ring = ring;
            BufGroupId = bufGroupId;

            Core = MakeIntrusive<TCore>();
            Core->Memory = static_cast<char*>(aligned_alloc(4096, NumBuffers * BufferSize));
            if (!Core->Memory) {
                Core.Reset();
                return false;
            }
            memset(Core->Memory, 0, NumBuffers * BufferSize);

            int ret = 0;
            BufRing = io_uring_setup_buf_ring(ring, NumBuffers, bufGroupId, 0, &ret);
            if (!BufRing) {
                Core.Reset();
                return false;
            }

            for (ui32 i = 0; i < NumBuffers; ++i) {
                io_uring_buf_ring_add(BufRing, Core->GetBuffer(i), BufferSize, i,
                                      io_uring_buf_ring_mask(NumBuffers), i);
            }
            io_uring_buf_ring_advance(BufRing, NumBuffers);

            return true;
        }

        ui16 GetBufGroupId() const {
            return BufGroupId;
        }

        // Zero-copy: wrap the kernel-filled buffer; recycled when the TRcBuf is released.
        TRcBuf WrapBuffer(ui16 index, ui32 len) {
            return TRcBuf(MakeIntrusive<TUringRecvBufferChunk>(Core, index, len));
        }

        // Called on the reaper thread: return consumer-released buffers to the kernel ring.
        void DrainFreelist() {
            if (!BufRing || !Core) {
                return;
            }
            TVector<ui16> local;
            with_lock (Core->FreeLock) {
                local.swap(Core->FreeList);
            }
            if (local.empty()) {
                return;
            }
            const int mask = io_uring_buf_ring_mask(NumBuffers);
            for (size_t j = 0; j < local.size(); ++j) {
                const ui16 idx = local[j];
                io_uring_buf_ring_add(BufRing, Core->GetBuffer(idx), BufferSize, idx, mask, j);
            }
            io_uring_buf_ring_advance(BufRing, local.size());
        }

    private:
        struct io_uring* Ring = nullptr;
        struct io_uring_buf_ring* BufRing = nullptr;
        TIntrusivePtr<TCore> Core;
        ui16 BufGroupId = 0;
    };

} // namespace NActors
