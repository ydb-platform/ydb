#pragma once

#include <util/generic/buffer.h>
#include <util/generic/noncopyable.h>
#include <util/system/yassert.h>

namespace NBus {
    namespace NPrivate {
        class TLeftRightBuffer : TNonCopyable {
        private:
            TBuffer Buffer;
            size_t Left;

            void CheckInvariant() {
                Y_ASSERT(Left <= Buffer.Size());
            }

        public:
            TLeftRightBuffer()
                : Left(0)
            {
            }

            TBuffer& GetBuffer() {
                return Buffer;
            }

            size_t Capacity() {
                return Buffer.Capacity();
            }

            void Clear() {
                Buffer.Clear();
                Left = 0;
            }

            void Reset() {
                Buffer.Reset();
                Left = 0;
            }

            void Compact() {
                Buffer.ChopHead(Left);
                Left = 0;
            }

            char* LeftPos() {
                return Buffer.Data() + Left;
            }

            size_t LeftSize() {
                return Left;
            }

            void LeftProceed(size_t count) {
                Y_ASSERT(count <= Size());
                Left += count;
            }

            size_t Size() {
                return Buffer.Size() - Left;
            }

            bool Empty() {
                return Size() == 0;
            }

            char* RightPos() {
                return Buffer.Data() + Buffer.Size();
            }

            size_t Avail() {
                return Buffer.Avail();
            }
        };

    }
}
