#pragma once

#include "key_bound.h"
#include "comparator.h"
#include "row_buffer.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Helper for isomorphic "compression" of key bounds for easier perception.
//! Example:
//!
//! Preimage        ComponentWise       Global
//! >= []           >= []               0
//! >= [bar]        >= [0]              1
//! >= [bar, 42]    >= [0, 0]           2
//! <= [bar, 57]    <= [0, 1]           3
//! <= [bar]        <= [0]              4
//! <  [foo]        <  [1]              5
//! >= [foo]        >= [1]              5
//! >  [foo; 30]    >  [1, 0]           6
//! <  [foo; 99]    <  [1, 1]           7
//! <= [foo; 99]    <= [1, 1]           7
//! >  [foo; 99]    >  [1, 1]           7
//! <  [qux; 18]    <  [2, 0]           8
//! <  [zzz]        <  [3]              9
//! <= []           <  []               10
//! >  []           >  []               10
//!
//! Note that ComponentWise is a TKeyBound -> TKeyBound mapping,
//! while Global is a TKeyBound -> int mapping which identifies
//! complementary key bounds and also full-length key bounds with
//! coinciding prefixes.
class TKeyBoundCompressor
{
public:
    explicit TKeyBoundCompressor(const TComparator& comparator);

    void Add(TKeyBound keyBound);

    void InitializeMapping();

    void Dump(const NLogging::TLogger& logger);

    struct TImage
    {
        TKeyBound ComponentWise;
        int Global = -1;
    };

    TImage GetImage(TKeyBound keyBound) const;

private:
    const TComparator Comparator_;

    THashSet<TKeyBound> AddedKeyBounds_;

    std::vector<TKeyBound> SortedKeyBounds_;
    std::vector<TMutableUnversionedRow> ComponentWisePrefixes_;

    THashMap<TKeyBound, TImage> Mapping_;
    bool MappingInitialized_ = false;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    //! Recursive method for calculating component-wise images.
    void CalculateComponentWise(int fromIndex, int toIndex, int componentIndex);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
