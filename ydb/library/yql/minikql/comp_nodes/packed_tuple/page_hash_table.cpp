// TODO: POSSIBLY DEPRECATED AND NEED TO BE DELETED DUE TO PERFORMANCE ISSUES
#include "page_hash_table.h"

#include <ydb/library/yql/utils/simd/simd.h>

#include <arrow/util/bit_util.h>


namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {


// -----------------------------------------------------------------
THolder<TPageHashTable> TPageHashTable::Create(const TTupleLayout* layout, ui32) {
    if (NX86::HaveAVX2()) {
        return MakeHolder<TPageHashTableImpl<NSimd::TSimdAVX2Traits>>(layout);
    }

    if (NX86::HaveSSE42()) {
        return MakeHolder<TPageHashTableImpl<NSimd::TSimdSSE42Traits>>(layout);
    }

    return MakeHolder<TPageHashTableImpl<NSimd::TSimdFallbackTraits>>(layout);
}


// -----------------------------------------------------------------
template <typename TTraits, bool Prefetch>
void TPageHashTableImpl<TTraits, Prefetch>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems) {
    TotalPages_ = (2 * nItems * SlotSize_ + PageSize_ - 1) / PageSize_;
    const ui32 logTotalPages = arrow::BitUtil::NumRequiredBits(TotalPages_ - 1);
    TotalPages_ = 1ul << logTotalPages;    
    ProbeByteIndex_ = std::min<ui32>(32 - 8, logTotalPages);

    Data_.assign(TotalPages_ * PageSize_, 0);

    OriginalData_ = data;
    OriginalOverflow_ = overflow;

    constexpr ui32 prefetchInAdvance = 16;

    if constexpr (Prefetch) {
        for (ui32 i = 0; i < std::min<ui32>(prefetchInAdvance, nItems); i++) {
            const ui8* tuple = OriginalData_ + i * TupleSize_;
            ui32 hash = ReadUnaligned<ui32>(tuple);
            ui64 pageNum = PageNum(hash);
    
            ui64 pagePos = pageNum * PageSize_;
            ui8* pageAddr = Data_.data() + pagePos;

            NYql::PrefetchForWrite(pageAddr);
        }
    }

    for (ui32 i = 0; i < nItems; i++) {
        if constexpr (Prefetch) {
            if (i + prefetchInAdvance < nItems) {
                const ui8* tuple = OriginalData_ + (i + prefetchInAdvance) * TupleSize_;
                ui32 hash = ReadUnaligned<ui32>(tuple);
                ui64 pageNum = PageNum(hash);
        
                ui64 pagePos = pageNum * PageSize_;
                ui8* pageAddr = Data_.data() + pagePos;
                
                NYql::PrefetchForWrite(pageAddr);
            }
        }    

        const ui8* tuple = OriginalData_ + i * TupleSize_;
        ui32 hash = ReadUnaligned<ui32>(tuple);
        ui64 pageNum = PageNum(hash);

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
        pageAddr[filledSlotsCounter + 1] = (hash >> ProbeByteIndex_) & 0xFF;
        pageAddr[0] = filledSlotsCounter + 1;

        if (!IsEmbedded_) {
            // put index to original buffer into slot
            WriteUnaligned<ui32>(pageAddr + PageHeaderSize_ + SlotSize_ * filledSlotsCounter, i);
        } else {
            // copy tuple from original buffer into slot
            std::memcpy(pageAddr + PageHeaderSize_ + SlotSize_ * filledSlotsCounter, tuple, TupleSize_);
        }
    }
}

template void TPageHashTableImpl<NSimd::TSimdFallbackTraits, false>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);
template __attribute__((target("avx2"))) void
TPageHashTableImpl<NSimd::TSimdAVX2Traits, false>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);
template __attribute__((target("sse4.2"))) void
TPageHashTableImpl<NSimd::TSimdSSE42Traits, false>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);
template void TPageHashTableImpl<NSimd::TSimdFallbackTraits, true>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);
template __attribute__((target("avx2"))) void
TPageHashTableImpl<NSimd::TSimdAVX2Traits, true>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);
template __attribute__((target("sse4.2"))) void
TPageHashTableImpl<NSimd::TSimdSSE42Traits, true>::Build(const ui8* data, const ui8 *const overflow, ui32 nItems);


// -----------------------------------------------------------------
template <typename TTraits, bool Prefetch>
ui32 TPageHashTableImpl<TTraits, Prefetch>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) {
    Y_ASSERT(OriginalData_ != nullptr); // Build was called before Apply
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;

    ui64 found = 0;

    for (ui32 i = 0; i < nItems; i++) {
        const ui8* tupleToFind = data + layout->TotalRowSize * i;
        ui32 hash = ReadUnaligned<ui32>(tupleToFind);

        ui64 pageNum = PageNum(hash);
        ui64 pagePos = pageNum * PageSize_;
        ui8* pageAddr = Data_.data() + pagePos;

        while (true) { // Check all possible pages in collisions cycle
            ui8 filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);
            TSimd8 headerReg = TSimd8(reinterpret_cast<const ui8*>(pageAddr + 1));            // [<Probe byte 1> ... <Probe byte K>]
            TSimd8 hashPartReg = TSimd8(static_cast<ui8>((hash >> ProbeByteIndex_) & 0xFF)); // [< Hash byte  > ... < Hash byte  >]
            ui32 foundBitMask = (headerReg == hashPartReg).ToBitMask() & ((1ULL << filledSlotsCounter) - 1); // get all possible positions where desired tuples could be stored

            ui32 nextSetPos = 0;
            while (foundBitMask != 0) { // Check all possible matches in single page
                ui32 moveMask = foundBitMask & -foundBitMask; // find least significant not null bit
                nextSetPos = __builtin_ctzl(foundBitMask); // find position of least significant not null bit
                foundBitMask ^= moveMask; // remove least significant not null bit

                if (nextSetPos >= filledSlotsCounter) {
                    break;
                }

                const ui8* tupleAddr;
                // get address of tuple if it is embedded or if it is lays in original buffer
                if (!IsEmbedded_) {
                    ui32 index = ReadUnaligned<ui32>(pageAddr + PageHeaderSize_ + SlotSize_ * nextSetPos);
                    tupleAddr = OriginalData_ + index * TupleSize_;
                } else {
                    tupleAddr = pageAddr + PageHeaderSize_ + SlotSize_ * nextSetPos;
                }

                if (Layout_->KeysEqual(tupleAddr, OriginalOverflow_, tupleToFind, overflow.data())) {
                    found++;
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

template ui32 TPageHashTableImpl<NSimd::TSimdFallbackTraits, false>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);
template __attribute__((target("avx2"))) ui32
TPageHashTableImpl<NSimd::TSimdAVX2Traits, false>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);
template __attribute__((target("sse4.2"))) ui32
TPageHashTableImpl<NSimd::TSimdSSE42Traits, false>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);
template ui32 TPageHashTableImpl<NSimd::TSimdFallbackTraits, true>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);
template __attribute__((target("avx2"))) ui32
TPageHashTableImpl<NSimd::TSimdAVX2Traits, true>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);
template __attribute__((target("sse4.2"))) ui32
TPageHashTableImpl<NSimd::TSimdSSE42Traits, true>::FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems);

// -----------------------------------------------------------------
template <typename TTraits, bool Prefetch>
void TPageHashTableImpl<TTraits, Prefetch>::Clear() {
    if (OriginalData_ == nullptr) {
        return;
    }

    OriginalData_ = nullptr;
    for (ui32 i = 0; i < TotalPages_; i++) {
        ui8* pageAddr = Data_.data() + i * PageSize_;
        std::memset(pageAddr, 0, PageHeaderSize_);
    }
}

template void TPageHashTableImpl<NSimd::TSimdFallbackTraits, false>::Clear();
template void TPageHashTableImpl<NSimd::TSimdSSE42Traits, false>::Clear();
template void TPageHashTableImpl<NSimd::TSimdAVX2Traits, false>::Clear();
template void TPageHashTableImpl<NSimd::TSimdFallbackTraits, true>::Clear();
template void TPageHashTableImpl<NSimd::TSimdSSE42Traits, true>::Clear();
template void TPageHashTableImpl<NSimd::TSimdAVX2Traits, true>::Clear();

}
}
}
