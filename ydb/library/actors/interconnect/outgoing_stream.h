#pragma once

#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include "rdma/mem_pool.h"
#include "rdma/rdma.h"
#include <deque>
#include <variant>

namespace NInterconnect {

    template<size_t TotalSize>
    class TOutgoingStreamT {
        static constexpr size_t BufferSize = TotalSize - sizeof(ui32) * 2;

        struct TBuffer {
            char Data[BufferSize];
            ui32 RefCount;
            ui32 Index;

            struct TBufOwner {
                TBufOwner() = default;

                TBufOwner(NRdma::TMemRegionPtr region)
                    : MemRegion(std::move(region))
                {}

                NRdma::TMemRegionPtr MemRegion;
                void operator ()(void *buffer) const noexcept {
                    if (!MemRegion) {
                        free(buffer);
                    }
                }
            };
        };

        static_assert(sizeof(TBuffer) == TotalSize);

        using TBufferPtr = std::unique_ptr<TBuffer, typename TBuffer::TBufOwner>;

        struct TSendChunk {
            TContiguousSpan Span;
            TBuffer *Buffer;
            std::variant<ui32*, const NRdma::TMemRegion*> AuxData = static_cast<ui32*>(nullptr);
        };

        std::vector<TBufferPtr> Buffers;
        std::vector<TBufferPtr> PreallocatedBuffers;
        TBuffer *AppendBuffer = nullptr;
        size_t AppendOffset = BufferSize; // into the last buffer
        std::deque<TSendChunk> SendQueue;
        size_t SendQueuePos = 0;
        size_t SendOffset = 0;
        size_t UnsentBytes = 0;

        std::shared_ptr<NRdma::IMemPool> Allocator;

    public:
        explicit TOutgoingStreamT(std::shared_ptr<NRdma::IMemPool> allocator)
            : Allocator(std::move(allocator))
        { }

        TOutgoingStreamT() = default;
        /*
         * Allow to share buffer between socket to produce safe zero copy operation
         */
        class TBufController {
        public:
            explicit TBufController(ui32* b)
                : ZcTransferId(b)
            {}

            /*
             * Set or update external handler id. For example sequence number of successful MSG_ZEROCOPY call
             * Should not be called in period between MakeBuffersShared and before CompleteSharedBuffers call
             */
            void Update(ui32 handler) {
                if (!ZcTransferId) {
                    return;
                }
                *ZcTransferId = handler;
            }

            bool ZcReady() const {
                return ZcTransferId != nullptr;
            }

        private:
            ui32 * const ZcTransferId;
        };

        operator bool() const {
            return SendQueuePos != SendQueue.size();
        }

        size_t CalculateOutgoingSize() const {
            size_t res = 0;
            for (const TSendChunk& chunk : SendQueue) {
                res += chunk.Span.size();
            }
            return res;
        }

        size_t CalculateUnsentSize() const {
#ifndef NDEBUG
            size_t res = 0;
            for (auto it = SendQueue.begin() + SendQueuePos; it != SendQueue.end(); ++it) {
                res += it->Span.size();
            }
            Y_ABORT_UNLESS(UnsentBytes == res - SendOffset);
#endif
            return UnsentBytes;
        }

        size_t GetSendQueueSize() const {
            return SendQueue.size();
        }

        bool PreallocateForWriting(size_t len) {
            const size_t buffersNeeded = GetBuffersNeededForWriting(len);
            if (buffersNeeded <= PreallocatedBuffers.size()) {
                return true;
            }

            const size_t extraBuffersNeeded = buffersNeeded - PreallocatedBuffers.size();
            std::vector<TBufferPtr> allocatedBuffers;
            allocatedBuffers.reserve(extraBuffersNeeded);
            for (size_t i = 0; i != extraBuffersNeeded; ++i) {
                TBufferPtr buffer = AllocateBuffer();
                if (!buffer) {
                    return false;
                }
                allocatedBuffers.emplace_back(std::move(buffer));
            }

            PreallocatedBuffers.reserve(PreallocatedBuffers.size() + extraBuffersNeeded);
            for (auto& buffer : allocatedBuffers) {
                PreallocatedBuffers.emplace_back(std::move(buffer));
            }
            return true;
        }

        TMutableContiguousSpan AcquireSpanForWriting(size_t maxLen) {
            if (!maxLen) {
                return {nullptr, 0};
            }
            if (AppendOffset == BufferSize) { // we have no free buffer, allocate one
                auto res = AddBuffer();
                Y_ABORT_UNLESS(res);
                AppendBuffer = Buffers.back().get();
                Y_ABORT_UNLESS(AppendBuffer);
                AppendOffset = 0;
            }
            return {AppendBuffer->Data + AppendOffset, Min(maxLen, BufferSize - AppendOffset)};
        }

        TMutableContiguousSpan AcquireSpanForWritingNoAlloc(size_t maxLen) {
            if (!maxLen) {
                return {nullptr, 0};
            }
            if (AppendOffset == BufferSize) {
                Y_ABORT_UNLESS(!PreallocatedBuffers.empty());
                Buffers.emplace_back(std::move(PreallocatedBuffers.back()));
                PreallocatedBuffers.pop_back();
                AppendBuffer = Buffers.back().get();
                Y_ABORT_UNLESS(AppendBuffer);
                AppendBuffer->RefCount = 1; // through AppendBuffer pointer
                AppendBuffer->Index = Buffers.size() - 1;
                AppendOffset = 0;
            }
            return {AppendBuffer->Data + AppendOffset, Min(maxLen, BufferSize - AppendOffset)};
        }

        void Align() {
            if (AppendOffset != BufferSize) {
                AppendOffset += -(reinterpret_cast<uintptr_t>(AppendBuffer->Data) + AppendOffset) & 63;
                if (AppendOffset > BufferSize) {
                    AppendOffset = BufferSize;
                    DropBufferReference(std::exchange(AppendBuffer, nullptr));
                }
            }
        }

        void Append(TContiguousSpan span, ui32* const zcHandle) {
            AppendImpl(span, zcHandle);
        }

        void Append(TContiguousSpan span, const NRdma::TMemRegion* memRegion) {
            Y_ABORT_UNLESS(memRegion);
            AppendImpl(span, memRegion);
        }

        void Write(TContiguousSpan in) {
            while (in.size()) {
                auto outChunk = AcquireSpanForWriting(in.size());
                memcpy(outChunk.data(), in.data(), outChunk.size());
                AppendAcquiredSpan(outChunk);
                in = in.SubSpan(outChunk.size(), Max<size_t>());
            }
        }

        using TBookmark = TStackVec<TMutableContiguousSpan, 2>;

        TBookmark Bookmark(size_t len) {
            TBookmark bookmark;

            while (len) {
                const auto span = AcquireSpanForWriting(len);
                AppendAcquiredSpan(span);
                bookmark.push_back(span);
                len -= span.size();
            }

            return bookmark;
        }

        void WriteBookmark(TBookmark&& bookmark, TContiguousSpan in) {
            for (auto& outChunk : bookmark) {
                Y_DEBUG_ABORT_UNLESS(outChunk.size() <= in.size());
                memcpy(outChunk.data(), in.data(), outChunk.size());
                in = in.SubSpan(outChunk.size(), Max<size_t>());
            }
        }

        void Rewind() {
            SendQueuePos = 0;
            SendOffset = 0;
            UnsentBytes = 0;
            for (const auto& item : SendQueue) {
                UnsentBytes += item.Span.size();
            }
        }

        void RewindToEnd() {
            SendQueuePos = SendQueue.size();
            SendOffset = 0;
            UnsentBytes = 0;
        }

        template<typename T, typename U = std::vector<TBufController>>
        void ProduceIoVec(T& container, size_t maxItems, size_t maxBytes, U* controllers = nullptr) {
            size_t offset = SendOffset;
            for (auto it = SendQueue.begin() + SendQueuePos; it != SendQueue.end() && std::size(container) < maxItems && maxBytes; ++it) {
                const TContiguousSpan span = it->Span.SubSpan(offset, maxBytes);
                container.push_back(NActors::TConstIoVec{span.data(), span.size()});
                if (controllers) {
                    const auto* zcTransferId = std::get_if<ui32*>(&it->AuxData);
                    controllers->push_back(TBufController(zcTransferId ? *zcTransferId : nullptr));
                }
                offset = 0;
                maxBytes -= span.size();
            }
        }

        template<typename T>
        size_t ProduceRdmaSendVec(T& container, size_t maxItems, size_t maxBytes) const {
            size_t offset = SendOffset;
            const size_t totalBytes = maxBytes;
            for (auto it = SendQueue.begin() + SendQueuePos; it != SendQueue.end() && std::size(container) < maxItems && maxBytes; ++it) {
                const TContiguousSpan span = it->Span.SubSpan(offset, maxBytes);
                const NRdma::TMemRegion* memRegion = nullptr;
                if (const auto* attachedMemRegion = std::get_if<const NRdma::TMemRegion*>(&it->AuxData)) {
                    memRegion = *attachedMemRegion;
                } else {
                    Y_ABORT_UNLESS(it->Buffer);
                    const auto& holder = Buffers[it->Buffer->Index];
                    Y_ABORT_UNLESS(holder.get() == it->Buffer);
                    memRegion = holder.get_deleter().MemRegion.Get();
                }
                Y_ABORT_UNLESS(memRegion);
                container.push_back(NRdma::TSendSge{
                    .Data = span.data(),
                    .Size = span.size(),
                    .MemRegion = memRegion,
                });
                offset = 0;
                maxBytes -= span.size();
            }
            return totalBytes - maxBytes;
        }

        void Advance(size_t numBytes) { // called when numBytes portion of data has been sent
            Y_DEBUG_ABORT_UNLESS(numBytes == 0 || SendQueuePos != SendQueue.size());
            Y_DEBUG_ABORT_UNLESS(numBytes <= UnsentBytes);
            SendOffset += numBytes;
            UnsentBytes -= numBytes;
            for (auto it = SendQueue.begin() + SendQueuePos; SendOffset && it->Span.size() <= SendOffset; ++SendQueuePos, ++it) {
                SendOffset -= it->Span.size();
                Y_DEBUG_ABORT_UNLESS(SendOffset == 0 || SendQueuePos != SendQueue.size() - 1);
            }
        }

        void DropFront(size_t numBytes) { // drops first numBytes from the queue, freeing buffers when necessary
            while (numBytes) {
                Y_DEBUG_ABORT_UNLESS(!SendQueue.empty());
                auto& front = SendQueue.front();
                if (numBytes < front.Span.size()) {
                    front.Span = front.Span.SubSpan(numBytes, Max<size_t>());
                    if (SendQueuePos == 0) {
                        Y_DEBUG_ABORT_UNLESS(numBytes <= SendOffset, "numBytes# %zu SendOffset# %zu SendQueuePos# %zu"
                            " SendQueue.size# %zu CalculateUnsentSize# %zu", numBytes, SendOffset, SendQueuePos,
                            SendQueue.size(), CalculateUnsentSize());
                        SendOffset -= numBytes;
                    }
                    break;
                } else {
                    numBytes -= front.Span.size();
                }
                Y_DEBUG_ABORT_UNLESS(!front.Buffer || (front.Span.data() >= front.Buffer->Data &&
                    front.Span.data() + front.Span.size() <= front.Buffer->Data + BufferSize));
                DropBufferReference(front.Buffer);
                SendQueue.pop_front();
                if (SendQueuePos) {
                    --SendQueuePos;
                } else {
                    SendOffset = 0;
                }
            }
        }

        template<typename T>
        void ScanLastBytes(size_t numBytes, T&& callback) const {
            auto it = SendQueue.end();
            ssize_t offset = -numBytes;
            while (offset < 0) {
                Y_DEBUG_ABORT_UNLESS(it != SendQueue.begin());
                const TSendChunk& chunk = *--it;
                offset += chunk.Span.size();
            }
            for (; it != SendQueue.end(); ++it, offset = 0) {
                callback(it->Span.SubSpan(offset, Max<size_t>()));
            }
        }

        void CompleteSharedBuffers() {
            PreallocatedBuffers.clear();
            for (size_t i = 0; i < Buffers.size(); i++) {
                DropBufferReference(Buffers[i]);
            }
            Buffers.clear();
        }

    private:
        void AppendImpl(TContiguousSpan span, std::variant<ui32*, const NRdma::TMemRegion*> auxData) {
            if (AppendBuffer && span.data() == AppendBuffer->Data + AppendOffset) { // the only valid case to use previously acquired span
                AppendAcquiredSpan(span, auxData);
            } else {
#ifndef NDEBUG
                // ensure this span does not point into any existing buffer part
                const char *begin = span.data();
                const char *end = span.data() + span.size();
                for (const auto& buffer : Buffers) {
                    const char *bufferBegin = buffer->Data;
                    const char *bufferEnd = bufferBegin + BufferSize;
                    if (bufferBegin < end && begin < bufferEnd) {
                        Y_ABORT();
                    }
                }
#endif
                AppendSpanWithGlueing(span, nullptr, auxData);
            }
        }

        size_t GetBuffersNeededForWriting(size_t len) const {
            const size_t freeBytes = AppendOffset == BufferSize ? 0 : BufferSize - AppendOffset;
            if (len <= freeBytes) {
                return 0;
            }
            return (len - freeBytes + BufferSize - 1) / BufferSize;
        }

        TBufferPtr AllocateBuffer() noexcept {
            if (auto memPool = Allocator.get()) {
                if (NRdma::TMemRegionPtr p = memPool->Alloc(sizeof(TBuffer), NRdma::IMemPool::EMPTY)) {
                    return TBufferPtr(static_cast<TBuffer*>(p->GetAddr()), typename TBuffer::TBufOwner(std::move(p)));
                } else {
                    return nullptr;
                }
            } else {
                void* p = malloc(sizeof(TBuffer));
                if (Y_UNLIKELY(!p)) {
                    return nullptr;
                }
                return TBufferPtr(static_cast<TBuffer*>(p));
            }
        }

        bool AddBuffer() noexcept {
            TBufferPtr buffer = AllocateBuffer();
            if (!buffer) {
                return false;
            }
            buffer->RefCount = 1;
            buffer->Index = Buffers.size();
            Buffers.emplace_back(std::move(buffer));
            return true;
        }

        void AppendAcquiredSpan(TContiguousSpan span,
                std::variant<ui32*, const NRdma::TMemRegion*> auxData = static_cast<ui32*>(nullptr)) {
            TBuffer *buffer = AppendBuffer;
            Y_DEBUG_ABORT_UNLESS(buffer);
            Y_DEBUG_ABORT_UNLESS(span.data() == AppendBuffer->Data + AppendOffset);
            AppendOffset += span.size();
            Y_DEBUG_ABORT_UNLESS(AppendOffset <= BufferSize);
            if (AppendOffset == BufferSize) {
                AppendBuffer = nullptr;
            } else {
                ++buffer->RefCount;
            }
            AppendSpanWithGlueing(span, buffer, auxData);
        }

        void AppendSpanWithGlueing(TContiguousSpan span, TBuffer *buffer,
                std::variant<ui32*, const NRdma::TMemRegion*> auxData = static_cast<ui32*>(nullptr)) {
            UnsentBytes += span.size();
            if (!SendQueue.empty()) {
                auto& back = SendQueue.back();
                if (back.Span.data() + back.Span.size() == span.data()
                        && buffer == back.Buffer
                        && auxData == back.AuxData) { // check if it is possible just to extend the last span
                    if (SendQueuePos == SendQueue.size()) {
                        --SendQueuePos;
                        SendOffset = back.Span.size();
                    }
                    back.Span = {back.Span.data(), back.Span.size() + span.size()};
                    DropBufferReference(buffer);
                    return;
                }
            }
            SendQueue.push_back(TSendChunk{span, buffer, auxData});
        }

        void DropBufferReference(TBuffer *buffer) {
            if (buffer && !--buffer->RefCount) {
                const size_t index = buffer->Index;
                auto& cell = Buffers[index];
                Y_DEBUG_ABORT_UNLESS(cell.get() == buffer);
                std::swap(cell, Buffers.back());
                cell->Index = index;
                Buffers.pop_back();
            }
        }
    };
    

    using TOutgoingStream = TOutgoingStreamT<32768>;

} // NInterconnect
