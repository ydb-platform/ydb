#pragma once

#include <util/generic/algorithm.h>

namespace NActors {

    // Main and XDC writes share one serialization budget. Adapt it once both sockets become idle,
    // using the batch's original size and remembering short writes across retries on either socket.
    class TSerializeWindow {
        size_t Size;
        bool WindowWasFull = false;
        bool HadShortWrite = false;

    public:
        explicit TSerializeWindow(size_t size = 0)
            : Size(size)
        {}

        size_t GetSize() const {
            return Size;
        }

        // Called before submitting writes, only when neither socket has a write in flight.
        void BeginBatch(size_t unsentBytes) {
            WindowWasFull = unsentBytes >= Size;
            HadShortWrite = false;
        }

        void CompleteWrite(size_t num, size_t requested, bool lastWrite, size_t minSize, size_t maxSize) {
            HadShortWrite |= num != requested;
            if (!lastWrite) {
                return;
            }
            if (WindowWasFull && !HadShortWrite) {
                Size = Min(Size + minSize, maxSize);
            } else {
                Size = Max(Size - minSize, minSize);
            }
        }
    };

} // namespace NActors
