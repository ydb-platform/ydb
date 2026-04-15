#pragma once

#include <util/system/types.h>

#include <vector>

namespace NKikimr {

class TWeightedIndices {
public:
    ui32 AddWeight(ui64 weight);

    void Clear();
    bool Empty() const;
    ui32 Size() const;

    ui32 GetRandomIndex() const;

private:
    struct TEntry {
        ui32 Index = 0;
        ui64 Weight = 0;
    };

    void PrepareIfNeeded() const;

private:
    mutable std::vector<TEntry> Entries;
    mutable std::vector<ui64> AccumWeights;
    mutable ui64 TotalWeight = 0;
    mutable bool IsPrepared = false;
};

} // namespace NKikimr
