#include "page_hash_table.h"

#include <ydb/library/yql/utils/simd/simd.h>
#include <arrow/util/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {


// -----------------------------------------------------------------
THolder<TPageHashTable> TPageHashTable::Create(const TTupleLayout* layout, ui32 capacity) {
    if (NX86::HaveAVX2()) {
        return MakeHolder<TPageHashTableImpl<NSimd::TSimdAVX2Traits>>(layout, capacity);
    }

    if (NX86::HaveSSE42()) {
        return MakeHolder<TPageHashTableImpl<NSimd::TSimdSSE42Traits>>(layout, capacity);
    }

    return MakeHolder<TPageHashTableImpl<NSimd::TSimdFallbackTraits>>(layout, capacity);
}


// -----------------------------------------------------------------
template <typename TTraits>
TPageHashTableImpl<TTraits>::TPageHashTableImpl(const TTupleLayout* layout, ui32 capacity)
    : KeySize_(layout->KeyColumnsSize)
    , KeyOffset_(layout->KeyColumnsOffset)
    , PayloadSize_(layout->PayloadSize)
    , TupleSize_(layout->TotalRowSize)
    , PageHeaderSize_(SlotsCount_ + 1 /* One byte is used for filled slots count */)
    , PageSize_(PageHeaderSize_ + SlotSize_ * SlotsCount_)
    , TotalSlots_((7 * (capacity + 7)) / 3)
    , TotalPages_((TotalSlots_ * SlotSize_ + PageSize_ - 1) / PageSize_ + 7)
    , Layout_(layout)
    , OriginalData_(nullptr) {
    Y_ASSERT(TotalPages_ > 0);
    PageIndexOffset_ = 32 /* size of crc32 in bits */ - arrow::BitUtil::NumRequiredBits(TotalPages_ - 1);
    Data_.resize(TotalPages_ * PageSize_, 0);
}


// -----------------------------------------------------------------
template <typename TTraits>
void TPageHashTableImpl<TTraits>::Build(const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>* overflow, ui32 nItems) {
    OriginalData_ = data;
    OriginalOverflow_ = overflow;

    ui8 isEmbedded = static_cast<ui8>(TupleSize_ <= SlotSize_); // this flag used to dispatch where to store tuple
    static const void* embeddedDispatch[] = {&&inderected, &&embedded};

    for (ui32 i = 0; i < nItems; i++) {
        const ui8* tuple = OriginalData_ + i * TupleSize_;
        ui32 hash = ReadUnaligned<ui32>(tuple);
        ui64 pageNum = (hash >> PageIndexOffset_) % TotalPages_;

        ui64 pagePos = pageNum * PageSize_;
        ui8* pageAddr = Data_.data() + pagePos;
        ui8 filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);

        while (filledSlotsCounter >= SlotsCount_) { // while page is completely filled
            pageAddr += PageSize_;

            if (pageAddr >= Data_.end()) { // cyclic buffer
                pageAddr = Data_.data();
            }

            filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);
        }

        // found page with empty slot
        pageAddr[filledSlotsCounter + 1] = (hash >> PROBE_BYTE_INDEX) & 0xFF;
        pageAddr[0] = filledSlotsCounter + 1;

        goto *embeddedDispatch[isEmbedded]; // it is expected that goto will be faster than if statement in for-loop

    inderected:
        // put index to original buffer into slot
        WriteUnaligned<ui32>(pageAddr + PageHeaderSize_ + SlotSize_ * filledSlotsCounter, i);
        continue;

    embedded:
        // copy tuple from original buffer into slot
        std::memcpy(pageAddr + PageHeaderSize_ + SlotSize_ * filledSlotsCounter, tuple, TupleSize_);
        continue;
    }
}

template __attribute__((target("avx2"))) void
TPageHashTableImpl<NSimd::TSimdAVX2Traits>::Build(const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>* overflow, ui32 nItems);

template __attribute__((target("sse4.2"))) void
TPageHashTableImpl<NSimd::TSimdSSE42Traits>::Build(const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>* overflow, ui32 nItems);


// -----------------------------------------------------------------
template <typename TTraits>
ui32 TPageHashTableImpl<TTraits>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) {
    Y_ASSERT(OriginalData_ != nullptr); // Build was called before Apply
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;

    ui64 found = 0;

    static const void* keySizeDispatch[] = {&&keySize1, &&keySize2, &&keySize4, &&keySize8, &&bigKeySize};
    ui32 keySizeToCmp; // this variable used to dispatch how to load as compare keys
    switch (KeySize_) {
        case 1: {
            keySizeToCmp = 0;
            break;
        }
        case 2: {
            keySizeToCmp = 1;
            break;
        }
        case 4: {
            keySizeToCmp = 2;
            break;
        }
        case 8: {
            keySizeToCmp = 3;
            break;
        }
        default: {
            keySizeToCmp = 4;
            break;
        }
    }
    if (Layout_->KeyColumnsFixedNum != Layout_->KeyColumnsNum) { // if the layout contains keys of variable size, then we cannot use the usual byte-by-byte comparison
        keySizeToCmp = 4;
    }

    static const void* embeddedDispatch[] = {&&inderected, &&embedded};
    ui8 isEmbedded = static_cast<ui8>(TupleSize_ <= SlotSize_); // this flag used to dispatch where to store tuple

    for (ui32 i = 0; i < nItems; i++) {
        const ui8* tupleToFind = data + layout->TotalRowSize * i;
        ui32 hash = ReadUnaligned<ui32>(tupleToFind);
        const ui8* keyToFind = tupleToFind + layout->KeyColumnsOffset;

        ui64 pageNum = (hash >> PageIndexOffset_) % TotalPages_;
        ui64 pagePos = pageNum * PageSize_;
        ui8* pageAddr = Data_.data() + pagePos;

        while (true) { // Check all possible pages in collisions cycle
            ui8 filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);
            TSimd8 headerReg = TSimd8(pageAddr + 1);                                          // [<Probe byte 1> ... <Probe byte K>]
            TSimd8 hashPartReg = TSimd8(static_cast<ui8>((hash >> PROBE_BYTE_INDEX) & 0xFF)); // [< Hash byte  > ... < Hash byte  >]
            ui32 foundBitMask = (headerReg == hashPartReg).ToBitMask(); // get all possible positions where desired tuples could be stored

            ui32 nextSetPos = 0;
            while (foundBitMask != 0) { // Check all possible matches in single page
                ui32 moveMask = foundBitMask & -foundBitMask; // find least significant not null bit
                nextSetPos = __builtin_ctzl(foundBitMask); // find position of least significant not null bit
                foundBitMask ^= moveMask; // remove least significant not null bit

                if (nextSetPos >= filledSlotsCounter) {
                    break;
                }

                const ui8* tupleAddr;
                const ui8* keyAddr;
                goto *embeddedDispatch[isEmbedded]; // it is expected that goto will be faster than if statement in for-loop

                // get address of tuple if it is embedded or if it is lays in original buffer
                inderected: {
                    ui32 index = ReadUnaligned<ui32>(pageAddr + PageHeaderSize_ + SlotSize_ * nextSetPos);
                    tupleAddr = OriginalData_ + index * TupleSize_;
                    goto embeddedDispatchEnd;
                }
                embedded: {
                    tupleAddr = pageAddr + PageHeaderSize_ + SlotSize_ * nextSetPos;
                }

                embeddedDispatchEnd:;
                keyAddr = tupleAddr + KeyOffset_;
                goto *keySizeDispatch[keySizeToCmp]; // it is expected that goto will be faster than if statement in while-loop

                // compare keys
                keySize1: {
                    if (ReadUnaligned<ui8>(keyAddr) == ReadUnaligned<ui8>(keyToFind)) {
                        found++;
                    }
                    continue;
                }
                keySize2: {
                    if (ReadUnaligned<ui16>(keyAddr) == ReadUnaligned<ui16>(keyToFind)) {
                        found++;
                    }
                    continue;
                }
                keySize4: {
                    if (ReadUnaligned<ui32>(keyAddr) == ReadUnaligned<ui32>(keyToFind)) {
                        found++;
                    }
                    continue;
                }
                keySize8: {
                    if (ReadUnaligned<ui64>(keyAddr) == ReadUnaligned<ui64>(keyToFind)) {
                        found++;
                    }
                    continue;
                }
                bigKeySize: {
                    if (CompareKeys(Layout_, tupleAddr, *OriginalOverflow_, layout, tupleToFind, overflow)) {
                        found++;
                    }
                    continue;
                }
            }

            if (filledSlotsCounter < SlotsCount_) { // Page has empty slots, i.e. there will definitely be no further occurrences
                break;
            }

            pageAddr += PageSize_;
            if (pageAddr >= Data_.end()) { // cyclic buffer
                pageAddr = Data_.data();
            }
        }
    }

    return found;
}

template __attribute__((target("avx2"))) ui32
TPageHashTableImpl<NSimd::TSimdAVX2Traits>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);

template __attribute__((target("sse4.2"))) ui32
TPageHashTableImpl<NSimd::TSimdSSE42Traits>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);


// -----------------------------------------------------------------
template <typename TTraits>
void TPageHashTableImpl<TTraits>::Clear() {
    OriginalData_ = nullptr;
    for (ui32 i = 0; i < TotalPages_; i++) {
        ui8* pageAddr = Data_.data() + i * PageSize_;
        std::memset(pageAddr, 0, PageHeaderSize_);
    }
}


}
}
}
