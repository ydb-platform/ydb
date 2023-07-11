#pragma once

#include <util/system/yassert.h>
#include <util/generic/vector.h>

// Implementation of disjoint-set data structure with union by rank and path compression.
// See http://en.wikipedia.org/wiki/Disjoint-set_data_structure
class TDisjointSets {
public:
    using TElement = size_t;

private:
    mutable TVector<TElement> Parents;
    TVector<size_t> Ranks;
    TVector<size_t> Sizes;
    size_t NumberOfSets;

public:
    TDisjointSets(size_t setCount)
        : Parents(setCount)
        , Ranks(setCount, 0)
        , Sizes(setCount, 1)
        , NumberOfSets(setCount)
    {
        for (size_t i = 0; i < setCount; ++i)
            Parents[i] = i;
    }

    TElement CanonicSetElement(TElement item) const {
        if (Parents[item] != item)
            Parents[item] = CanonicSetElement(Parents[item]);
        return Parents[item];
    }

    size_t SizeOfSet(TElement item) const {
        return Sizes[CanonicSetElement(item)];
    }

    size_t InitialSetCount() const {
        return Parents.size();
    }

    size_t SetCount() const {
        return NumberOfSets;
    }

    void UnionSets(TElement item1, TElement item2) {
        TElement canonic1 = CanonicSetElement(item1);
        TElement canonic2 = CanonicSetElement(item2);
        if (canonic1 == canonic2)
            return;

        --NumberOfSets;
        if (Ranks[canonic1] < Ranks[canonic2]) {
            Parents[canonic1] = canonic2;
            Sizes[canonic2] += Sizes[canonic1];
        } else {
            Parents[canonic2] = canonic1;
            Sizes[canonic1] += Sizes[canonic2];
            Ranks[canonic2] += Ranks[canonic1] == Ranks[canonic2] ? 1 : 0;
        }
    }

    void Expand(size_t setCount) {
        if (setCount < Parents.size()) {
            return;
        }

        size_t prevSize = Parents.size();
        Parents.resize(setCount);
        Ranks.resize(setCount);
        Sizes.resize(setCount);
        NumberOfSets += setCount - prevSize;

        for (size_t i = prevSize; i < setCount; ++i) {
            Parents[i] = i;
            Ranks[i] = 0;
            Sizes[i] = 1;
        }
    }
};
