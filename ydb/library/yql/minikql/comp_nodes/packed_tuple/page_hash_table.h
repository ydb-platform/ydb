#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>

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
    typedef void OnMatchCallback(const ui8* tuple);

public:
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
template <typename TTraits>
class TPageHashTableImpl: public TPageHashTable {
public:
    TPageHashTableImpl() = default;

    explicit TPageHashTableImpl(const TTupleLayout* layout) {
        SetTupleLayout(layout);
    }

    void SetTupleLayout(const TTupleLayout *layout) {
        Clear();

        KeySize_ = layout->KeyColumnsSize;
        KeyOffset_ = layout->KeyColumnsOffset;
        PayloadSize_ = layout->PayloadSize;
        TupleSize_ = layout->TotalRowSize;
        Layout_ = layout;
    }

    TPageHashTableImpl(const TPageHashTableImpl &) = delete;
    void operator=(const TPageHashTableImpl &) = delete;

    void Build(const ui8* data, const ui8 *const overflow, ui32 nItems) override;

    ui32 FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) override;
    void Apply(const ui8 *const row, const ui8 *const overflow, auto &&onMatch);

    void Clear() override;

private:
    static constexpr ui32 PROBE_BYTE_INDEX = 3; // Index of probe byte. This byte is used to check whether the page contains a tuple

private:
    static constexpr ui32 SlotSize_{16};              // Amount of memory allocated per one slot
    static constexpr ui32 SlotsCount_{TTraits::Size}; // Count of slots per page
    static constexpr ui32 PageHeaderSize_{SlotsCount_ + 1};  // Size in bytes of the hash table page header. Page consists of header plus slots
    static constexpr ui32 PageSize_{PageHeaderSize_ + SlotSize_ * SlotsCount_};  // Page size in bytes
    
    ui32    KeySize_{0};                // Size of the key in bytes
    ui32    KeyOffset_{0};              // Offset of the key in tuple layout
    ui32    PayloadSize_{0};            // Size of payload to store together with key
    ui32    TupleSize_{0};              // Amount of memory allocated per one tuple
    ui64    TotalSlots_{0};             // Total slots in hash table
    ui32    TotalPages_{0};             // Total number of pages in hash table
    ui64    NTuples_{0};                // Number of allocated tuples to store in hash table
    ui64    PageIndexOffset_{0};        // Offset used to index page in hash table, We have to use most significant bits,because after partitioning stage values in one hashmap highly likely have same least significant bits
    const TTupleLayout*                     Layout_{nullptr};   // Tuple layout description
    std::vector<ui8, TMKQLAllocator<ui8>>   Data_;              // Place to store hash table data
    const ui8*                              OriginalData_{nullptr};     // Data passed to build method
    const ui8*                              OriginalOverflow_{nullptr}; // Overflow data passed to build method
};

template <typename TTraits>
void TPageHashTableImpl<TTraits>::Apply(const ui8 *const row, const ui8 *const overflow, auto &&onMatch) {
    Y_ASSERT(OriginalData_ != nullptr); // Build was called before Apply
    using TSimd8 = typename TTraits:: template TSimd8<ui8>;
    
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

    ui32 hash = ReadUnaligned<ui32>(row);
    const ui8* keyToFind = row + Layout_->KeyColumnsOffset;

    ui64 pageNum = (hash >> PageIndexOffset_) % TotalPages_;
    ui64 pagePos = pageNum * PageSize_;
    ui8* pageAddr = Data_.data() + pagePos;

    while (true) { // Check all possible pages in collisions cycle
        ui8 filledSlotsCounter = ReadUnaligned<ui8>(pageAddr);
        TSimd8 headerReg = TSimd8(reinterpret_cast<const ui8*>(pageAddr + 1));            // [<Probe byte 1> ... <Probe byte K>]
        TSimd8 hashPartReg = TSimd8(static_cast<ui8>((hash >> PROBE_BYTE_INDEX) & 0xFF)); // [< Hash byte  > ... < Hash byte  >]
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
                    onMatch(tupleAddr);
                }
                continue;
            }
            keySize2: {
                if (ReadUnaligned<ui16>(keyAddr) == ReadUnaligned<ui16>(keyToFind)) {
                    onMatch(tupleAddr);
                }
                continue;
            }
            keySize4: {
                if (ReadUnaligned<ui32>(keyAddr) == ReadUnaligned<ui32>(keyToFind)) {
                    onMatch(tupleAddr);
                }
                continue;
            }
            keySize8: {
                if (ReadUnaligned<ui64>(keyAddr) == ReadUnaligned<ui64>(keyToFind)) {
                    onMatch(tupleAddr);
                }
                continue;
            }
            bigKeySize: {
                if (Layout_->KeysEqual(tupleAddr, OriginalOverflow_, row, overflow)) {
                    onMatch(tupleAddr);
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

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr