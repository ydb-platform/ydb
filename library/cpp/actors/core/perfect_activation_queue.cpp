#include "perfect_activation_queue.h"

#include <util/string/builder.h>

namespace NActors {

    static constexpr ui32 SizeBits = 20;
    static constexpr ui32 SizeItems = 1 << SizeBits;
    static constexpr ui32 SizeMask = SizeItems - 1;

    // index conversion: CCCCCCCCCBBBBBAAA is converted to CCCCCCCCCAAABBBBB
    static constexpr ui32 StripeSizeBitsA = 4; // makes a cacheline of 64 bytes
    static constexpr ui32 StripeSizeBitsB = 4;

    static void *AllocateItems(size_t size, size_t alignment) {
#if defined(_win_)
        return _aligned_malloc(size, alignment);
#else
        return aligned_alloc(alignment, size);
#endif
    }

    TPerfectActivationQueue::TPerfectActivationQueue()
        : QueueData(static_cast<TItem*>(AllocateItems(SizeItems * sizeof(TItem), 4096)))
    {
        Y_VERIFY(QueueData);
        memset(QueueData, 0, SizeItems * sizeof(TItem));
    }

    TPerfectActivationQueue::~TPerfectActivationQueue() {
        free(QueueData);
    }

    void TPerfectActivationQueue::Push(ui32 value) {
        Y_VERIFY(value && value != Max<ui32>());
        for (;;) {
            const ui64 writeIndex = WriteIndex++;
            TItem *ptr = QueueData + ConvertIndex(writeIndex & SizeMask);
            const TItem cell = __atomic_exchange_n(ptr, value, __ATOMIC_SEQ_CST);

            if (!cell) { // cell was free
                return;
            } else if (cell == Max<ui32>()) { // cell was forbidden
                __atomic_store_n(ptr, 0, __ATOMIC_SEQ_CST);
            } else {
                Y_FAIL();
            }
        }
    }

    ui32 TPerfectActivationQueue::Pop() {
        for (;;) {
            const ui64 readIndex = ReadIndex++;
            TItem *ptr = QueueData + ConvertIndex(readIndex & SizeMask);
            TItem cell = __atomic_exchange_n(ptr, 0, __ATOMIC_SEQ_CST);
            if (cell) {
                Y_VERIFY(cell < Max<ui32>());
                return cell;
            }

            // value is either not yet written, or this is a free cell -- forbid this cell for now
            for (ui32 i = 0; i < 10; ++i) {
                _mm_pause();
            }

            cell = __atomic_exchange_n(ptr, Max<ui32>(), __ATOMIC_SEQ_CST);
            if (cell) {
                Y_VERIFY(cell < Max<ui32>());
                __atomic_store_n(ptr, 0, __ATOMIC_SEQ_CST);
            } else if (ReadIndex < WriteIndex) {
                continue; // give it another try, it was just a race
            }

            return cell;
        }
    }

    void TPerfectActivationQueue::PushBulk(ui32 *values, size_t count) {
        while (count--) {
            Push(*values++);
        }
    }

    ui32 TPerfectActivationQueue::ConvertIndex(ui32 index) {
        constexpr ui32 maskA = (1 << StripeSizeBitsA) - 1;
        const ui32 a = index & maskA;

        constexpr ui32 maskB = (1 << StripeSizeBitsB) - 1;
        const ui32 b = index >> StripeSizeBitsA & maskB;

        const ui32 other = index >> StripeSizeBitsA + StripeSizeBitsB;
        return other << StripeSizeBitsA + StripeSizeBitsB | a << StripeSizeBitsB | b;
    }

}
