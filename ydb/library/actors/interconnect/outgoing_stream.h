#pragma once

#include <ydb/library/actors/core/event_load.h>
#include <ydb/library/actors/util/rc_buf.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <deque>

namespace NInterconnect {

    template<size_t TotalSize>
    class TOutgoingStreamT {
        static constexpr size_t BufferSize = TotalSize - sizeof(ui32) * 2;

        struct TBuffer {
            char Data[BufferSize];
            ui32 RefCount;
            ui32 Index;

            struct TDeleter {
                void operator ()(TBuffer *buffer) const {
                    free(buffer);
                }
            };
        };

        static_assert(sizeof(TBuffer) == TotalSize);

        struct TSendChunk {
            TContiguousSpan Span;
            TBuffer *Buffer;
            ui32* ZcTransferId = nullptr;
        };

        std::vector<std::unique_ptr<TBuffer, typename TBuffer::TDeleter>> Buffers;
        TBuffer *AppendBuffer = nullptr;
        size_t AppendOffset = BufferSize; // into the last buffer
        std::deque<TSendChunk> SendQueue;
        size_t SendQueuePos = 0;
        size_t SendOffset = 0;
        size_t UnsentBytes = 0;

    public:
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

        TMutableContiguousSpan AcquireSpanForWriting(size_t maxLen) {
            if (!maxLen) {
                return {nullptr, 0};
            }
            if (AppendOffset == BufferSize) { // we have no free buffer, allocate one
                Buffers.emplace_back(static_cast<TBuffer*>(malloc(sizeof(TBuffer))));
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
            if (AppendBuffer && span.data() == AppendBuffer->Data + AppendOffset) { // the only valid case to use previously acquired span
                AppendAcquiredSpan(span);
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
                AppendSpanWithGlueing(span, nullptr);
            }
            SendQueue.back().ZcTransferId = zcHandle;
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
                    controllers->push_back(TBufController(it->ZcTransferId));
                }
                offset = 0;
                maxBytes -= span.size();
            }
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
            for (size_t i = 0; i < Buffers.size(); i++) {
                DropBufferReference(Buffers[i]);
            }
            Buffers.clear();
        }

    private:
        void AppendAcquiredSpan(TContiguousSpan span) {
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
            AppendSpanWithGlueing(span, buffer);
        }

        void AppendSpanWithGlueing(TContiguousSpan span, TBuffer *buffer) {
            UnsentBytes += span.size();
            if (!SendQueue.empty()) {
                auto& back = SendQueue.back();
                if (back.Span.data() + back.Span.size() == span.data()) { // check if it is possible just to extend the last span
                    Y_DEBUG_ABORT_UNLESS(buffer == back.Buffer);
                    if (SendQueuePos == SendQueue.size()) {
                        --SendQueuePos;
                        SendOffset = back.Span.size();
                    }
                    back.Span = {back.Span.data(), back.Span.size() + span.size()};
                    DropBufferReference(buffer);
                    return;
                }
            }
            SendQueue.push_back(TSendChunk{span, buffer});
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
    

    using TOutgoingStream = TOutgoingStreamT<262144>;

} // NInterconnect
