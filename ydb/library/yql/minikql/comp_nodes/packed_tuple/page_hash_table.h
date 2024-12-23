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
    virtual void Build(const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>* overflow, ui32 nItems) = 0;

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
    TPageHashTableImpl(const TTupleLayout* layout, ui32 capacity);

    void Build(const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>* overflow, ui32 nItems) override;

    ui32 FindMatches(const TTupleLayout* layout, const ui8* data, const std::vector<ui8, TMKQLAllocator<ui8>>& overflow, ui32 nItems) override;

    void Clear() override;

private:
    static constexpr ui32 PROBE_BYTE_INDEX = 3; // Index of probe byte. This byte is used to check whether the page contains a tuple

private:
    ui32    KeySize_{0};                // Size of the key in bytes
    ui32    KeyOffset_{0};              // Offset of the key in tuple layout
    ui32    PayloadSize_{0};            // Size of payload to store together with key
    ui32    TupleSize_{0};              // Amount of memory allocated per one tuple
    ui32    SlotSize_{16};              // Amount of memory allocated per one slot
    ui32    SlotsCount_{TTraits::Size}; // Count of slots per page
    ui32    PageHeaderSize_{0};         // Size in bytes of the hash table page header. Page consists of header plus slots
    ui32    PageSize_{0};               // Page size in bytes
    ui64    TotalSlots_{0};             // Total slots in hash table
    ui32    TotalPages_{0};             // Total number of pages in hash table
    ui64    NTuples_{0};                // Number of allocated tuples to store in hash table
    ui64    PageIndexOffset_{0};        // Offset used to index page in hash table, We have to use most significant bits,because after partitioning stage values in one hashmap highly likely have same least significant bits
    const TTupleLayout*                     Layout_;       // Tuple layout description
    std::vector<ui8, TMKQLAllocator<ui8>>   Data_;         // Place to store hash table data
    const ui8*                              OriginalData_; // Data passed to build method
    const std::vector<ui8, TMKQLAllocator<ui8>>*    OriginalOverflow_; // Overflow data passed to build method
};


} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
