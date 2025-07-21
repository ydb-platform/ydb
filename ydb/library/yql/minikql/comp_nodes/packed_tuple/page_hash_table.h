#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/prefetch.h>

#include <util/generic/buffer.h>

#include <ydb/library/yql/utils/simd/simd.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {


/*
* Class TPageHashTable provides open address hash table implementation for packed tuples.
*
* TPageHashTable has the following layout:
* HashTable:    [Page 1][Page 2]...[Page N]
* Page:         [<Header> <Slot 1> <Slot 2> ... <Slot K>]
* Header:       [<Filled slots counter (1 byte)> <Probe byte 1> ... <Probe byte K>]
*/
class TPageHashTable {
public:
    class TIterator;

protected:
    struct TIteratorImpl {
        static TIteratorImpl& UpCast(TIterator& iter) {
            return static_cast<TIteratorImpl&>(iter);
        }
        TIterator& DownCast() {
            return static_cast<TIterator&>(*this);
        }
        
        TIteratorImpl(): PageAddr_(nullptr) {}

    public:
        const ui8* Row_;
        const ui8* PageAddr_;
        ui32 FoundBitMask_; 
        ui8 HashByte_;
    };

public:
    class TIterator : private TIteratorImpl {
        friend TIteratorImpl;

    public:
        TIterator() = default;
        TIterator(const TIterator&) = default;
        TIterator& operator=(const TIterator&) = default;
    };

    virtual ~TPageHashTable() {};

    // Creates hash table for given layout with given capacity
    static THolder<TPageHashTable> Create(const TTupleLayout* layout, ui32 capacity);

    // Build new hash table on passed data.
    // Data must be presented as a TupleLayout.
    virtual void Build(const ui8* data, const ui8 *const overflow, ui32 nItems) = 0;

    // Finds matches in hash table
    // This method is temporary. It is expected that this method will return a batch of found tuples
    virtual ui32 FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) = 0;

    // Clears hash table content
    virtual void Clear() = 0;
};


// -----------------------------------------------------------------
template <typename TTraits, bool Prefetch = true>
class TPageHashTableImpl: public TPageHashTable {
public:
    TPageHashTableImpl() = default;

    explicit TPageHashTableImpl(const TTupleLayout* layout) {
        SetTupleLayout(layout);
    }

    void SetTupleLayout(const TTupleLayout *layout) {
        Clear();

        TupleSize_ = layout->TotalRowSize;
        Layout_ = layout;

        IsEmbedded_ = TupleSize_ <= SlotSize_; // this flag used to dispatch where to store tuple
    }

    TPageHashTableImpl(const TPageHashTableImpl &) = delete;
    void operator=(const TPageHashTableImpl &) = delete;

    void Build(const ui8* data, const ui8 *const overflow, ui32 nItems) override;

    ui32 FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) override;
    void Apply(const ui8 *const row, const ui8 *const overflow, auto &&onMatch);

    void Clear() override;

    const ui8 *NextMatch(TPageHashTable::TIterator &iter, const ui8 *overflow) const;
    TIterator Find(const ui8 *const row, const ui8 *const overflow) const;

    template <size_t Size>
    std::array<TIterator, Size>
    FindBatch(const std::array<const ui8 *, Size> &rows,
              const ui8 *const overflow) const;

private:
    ui64 PageNum(ui64 hash) const {
        return hash & (TotalPages_ - 1);
    }

private:
    static constexpr ui32 SlotSize_{16};              // Amount of memory allocated per one slot
    static constexpr ui32 SlotsCount_{TTraits::Size}; // Count of slots per page
    static constexpr ui32 PageHeaderSize_{SlotsCount_ + 1};  // Size in bytes of the hash table page header. Page consists of header plus slots
    static constexpr ui32 PageSize_{PageHeaderSize_ + SlotSize_ * SlotsCount_};  // Page size in bytes

    ui32    TupleSize_{0};              // Amount of memory allocated per one tuple
    bool    IsEmbedded_{false};
    ui32    TotalPages_{0};             // Total number of pages in hash table
    ui32    ProbeByteIndex_{0};

    const TTupleLayout*                     Layout_{nullptr};   // Tuple layout description
    std::vector<ui8, TMKQLAllocator<ui8>>   Data_;              // Place to store hash table data
    const ui8*                              OriginalData_{nullptr};     // Data passed to build method
    const ui8*                              OriginalOverflow_{nullptr}; // Overflow data passed to build method
};

template <typename TTraits, bool Prefetch>
void TPageHashTableImpl<TTraits, Prefetch>::Apply(const ui8 *const row, const ui8 *const overflow, auto &&onMatch) {
    Y_ASSERT(OriginalData_ != nullptr); // Build was called before Apply
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;
    
    ui32 hash = ReadUnaligned<ui32>(row);

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

            // compare keys
            if (Layout_->KeysEqual(tupleAddr, OriginalOverflow_, row, overflow)) {
                onMatch(tupleAddr);
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

template <typename TTraits, bool Prefetch>
const ui8 *TPageHashTableImpl<TTraits, Prefetch>::NextMatch(TPageHashTable::TIterator &it, const ui8 *overflow) const {
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;

    auto &iter = TIteratorImpl::UpCast(it);
    if (iter.PageAddr_ == nullptr) {
        return nullptr;
    }

    for (;;) { // Check all possible pages in collisions cycle
        const ui8 filledSlotsCounter = ReadUnaligned<ui8>(iter.PageAddr_);

        ui32 nextSetPos = 0;
        while (iter.FoundBitMask_ != 0) { // Check all possible matches in single page
            ui32 moveMask = iter.FoundBitMask_ & -iter.FoundBitMask_; // find least significant not null bit
            nextSetPos = __builtin_ctzl(iter.FoundBitMask_); // find position of least significant not null bit
            iter.FoundBitMask_ ^= moveMask; // remove least significant not null bit

            if (nextSetPos >= filledSlotsCounter) {
                break;
            }

            const ui8* tupleAddr;
            // get address of tuple if it is embedded or if it is lays in original buffer
            if (!IsEmbedded_) {
                ui32 index = ReadUnaligned<ui32>(iter.PageAddr_ + PageHeaderSize_ + SlotSize_ * nextSetPos);
                tupleAddr = OriginalData_ + index * TupleSize_;
            } else {
                tupleAddr = iter.PageAddr_ + PageHeaderSize_ + SlotSize_ * nextSetPos;
            }

            if (Layout_->KeysEqual(tupleAddr, OriginalOverflow_, iter.Row_, overflow)) {
                return tupleAddr;
            }
        }

        if (filledSlotsCounter < SlotsCount_) { // Page has empty slots, i.e. there will definitely be no further occurrences
            iter = {};
            break;
        }

        iter.PageAddr_ += PageSize_;
        if (iter.PageAddr_ >= Data_.end()) { // cyclic buffer
            iter.PageAddr_ = Data_.data();
        }

        const TSimd8 headerReg = TSimd8(reinterpret_cast<const ui8*>(iter.PageAddr_ + 1)); // [<Probe byte 1> ... <Probe byte K>]
        const TSimd8 hashPartReg = TSimd8(iter.HashByte_); // [< Hash byte  > ... < Hash byte  >]
        iter.FoundBitMask_ = (headerReg == hashPartReg).ToBitMask() & ((1ULL << filledSlotsCounter) - 1); // get all possible positions where desired tuples could be stored
    }

    return nullptr;
}

template <typename TTraits, bool Prefetch>
typename TPageHashTableImpl<TTraits, Prefetch>::TIterator
TPageHashTableImpl<TTraits, Prefetch>::Find(const ui8 *const row,
                                            const ui8 *const /*overflow*/) const {
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;

    const ui32 hash = ReadUnaligned<ui32>(row);
    const ui64 pageNum = PageNum(hash);
    const ui64 pagePos = pageNum * PageSize_;
    const ui8 *const pageAddr = Data_.data() + pagePos;
    const ui8 filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);
    const TSimd8 headerReg = TSimd8(reinterpret_cast<const ui8*>(pageAddr + 1));            // [<Probe byte 1> ... <Probe byte K>]
    const TSimd8 hashPartReg = TSimd8(static_cast<ui8>((hash >> ProbeByteIndex_) & 0xFF)); // [< Hash byte  > ... < Hash byte  >]
    const ui32 foundBitMask = (headerReg == hashPartReg).ToBitMask() & ((1ULL << filledSlotsCounter) - 1);
    const ui8 hashByte = (hash >> ProbeByteIndex_) & 0xFF;

    TIteratorImpl iter;
    iter.Row_ = row;
    iter.PageAddr_ = pageAddr;
    iter.FoundBitMask_ = foundBitMask;
    iter.HashByte_ = hashByte;

    return iter.DownCast();
}

template <typename TTraits, bool Prefetch>
template <size_t Size>
std::array<typename TPageHashTableImpl<TTraits, Prefetch>::TIterator, Size>
TPageHashTableImpl<TTraits, Prefetch>::FindBatch(
    const std::array<const ui8 *, Size> &rows,
    const ui8 *const overflow) const {
    if constexpr (Prefetch) {
        for (ui32 i = 0; i < Size && rows[i]; i++) {
            const ui32 hash = ReadUnaligned<ui32>(rows[i]);
            const ui64 pageNum = PageNum(hash);
            const ui64 pagePos = pageNum * PageSize_;
            const ui8 *const pageAddr = Data_.data() + pagePos;;
            NYql::PrefetchForRead(pageAddr);
        }
    }

    std::array<TIterator, Size> iters;
    for (ui32 i = 0; i < Size && rows[i]; i++) {
        iters[i] = Find(rows[i], overflow);
    }

    return iters;
}


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr