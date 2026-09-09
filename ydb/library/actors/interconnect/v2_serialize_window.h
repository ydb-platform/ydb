#pragma once

#include <util/generic/algorithm.h>

namespace NActors {

    // Main and XDC writes share one serialization budget. Adapt it after the batch's byte endpoints
    // have completed, remembering short writes across retries on either socket.
    class TSerializeWindow {
        size_t Size;
        bool WindowWasFull = false;
        bool HadShortWrite = false;
        bool BatchActive = false;
        bool HasBatchEndpoints = false;
        ui64 BatchMainEnd = 0;
        ui64 BatchXdcEnd = 0;

        void FinishBatch(size_t minSize, size_t maxSize) {
            if (WindowWasFull && !HadShortWrite) {
                Size = Min(Size + minSize, maxSize);
            } else {
                Size = Max(Size - minSize, minSize);
            }
            BatchActive = false;
            HasBatchEndpoints = false;
        }

    public:
        explicit TSerializeWindow(size_t size = 0)
            : Size(size)
        {}

        size_t GetSize() const {
            return Size;
        }

        // Legacy form retained for single-stream users and focused unit tests.
        void BeginBatch(size_t unsentBytes) {
            WindowWasFull = unsentBytes >= Size;
            HadShortWrite = false;
            BatchActive = true;
            HasBatchEndpoints = false;
        }

        // Endpoint-based batches remain active until the corresponding bytes have actually completed
        // on both streams. Socket-idle state no longer identifies the end of a batch once serialization
        // is allowed to continue while a write is in flight.
        void BeginBatch(size_t unsentBytes, ui64 mainEnd, ui64 xdcEnd) {
            BeginBatch(unsentBytes);
            HasBatchEndpoints = true;
            BatchMainEnd = mainEnd;
            BatchXdcEnd = xdcEnd;
        }

        bool IsBatchActive() const {
            return BatchActive;
        }

        void CompleteWrite(size_t num, size_t requested, bool lastWrite, size_t minSize, size_t maxSize) {
            HadShortWrite |= num != requested;
            if (!lastWrite) {
                return;
            }
            FinishBatch(minSize, maxSize);
        }

        void CompleteWrite(size_t num, size_t requested, ui64 committedMain, ui64 committedXdc,
                size_t minSize, size_t maxSize) {
            HadShortWrite |= num != requested;
            if (!HasBatchEndpoints || (committedMain >= BatchMainEnd && committedXdc >= BatchXdcEnd)) {
                FinishBatch(minSize, maxSize);
            }
        }
    };

} // namespace NActors
