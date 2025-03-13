#pragma once
#include "flat_part_iface.h"

namespace NKikimr {
namespace NTable {

    template<typename TItem>
    struct TOrderByEpoch {
        using TUnit = TIntrusiveConstPtr<TItem>;

        bool operator()(const TUnit &left, const TUnit &right) const noexcept
        {
            return left->Epoch < right->Epoch;
        }
    };

    IPages::TResult MemTableRefLookup(const TMemTable*, ui64 ref, ui32 tag);
}
}
