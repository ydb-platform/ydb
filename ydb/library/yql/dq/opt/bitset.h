#pragma once

#include <stdlib.h>

/* 
 * This header contains helper functions for working with bitsets.
 * They are templated by TNodeSet, which is a std::bitset<>.
 * We use the the template for efficiency: for 64 bit nodesets we implement a faster next subset functionality
 */

namespace NYql::NDq {

template <typename TNodeSet>
inline bool Overlaps(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) != 0;
}

template <typename TNodeSet>
inline bool IsSubset(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) == lhs;
}

template <typename TNodeSet>
inline bool HasSingleBit(TNodeSet nodeSet) {
    return nodeSet.count() == 1;
}

template <typename TNodeSet>
inline size_t GetLowestSetBit(TNodeSet nodeSet) {
    for (size_t i = 0; i < nodeSet.size(); ++i) {
        if (nodeSet[i]) {
            return i;
        }
    }

    Y_ASSERT(false);
    return nodeSet.size();
}

/* Iterates the indecies of the set bits in the TNodeSet. */
template <typename TNodeSet>
class TSetBitsIt {
public:
    TSetBitsIt(TNodeSet nodeSet)
        : NodeSet_(nodeSet)
        , Size_(nodeSet.size())
        , BitId_(0)
    {
        SkipUnsetBits();
    }

    bool HasNext() {
        return BitId_ < Size_;
    }

    size_t Next() {
        size_t bitId = BitId_++;
        SkipUnsetBits();

        return bitId;
    }

private:
    void SkipUnsetBits() {
        while (BitId_ < Size_ && !NodeSet_[BitId_]) {
            ++BitId_;
        }
    }

private:
    TNodeSet NodeSet_;
    size_t Size_;
    size_t BitId_;
};

} // namespace NYql::NDq
