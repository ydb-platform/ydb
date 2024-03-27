#pragma once

#include "dphyp_join_tree_node.h"

#include <stdlib.h>

namespace NYql::NDq::NDphyp {

template <typename TNodeSet>
inline bool AreOverlaps(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) != 0;
}

template <typename TNodeSet>
inline bool IsSubset(const TNodeSet& lhs, const TNodeSet& rhs) {
    return (lhs & rhs) == lhs;
}

template <typename TNodeSet>
inline bool HasSingleBit(TNodeSet nodeSet) {
    return nodeSet && !(nodeSet & (nodeSet - 1));
}

template <typename TNodeSet>
inline size_t GetLowestSetBit(TNodeSet nodeSet) {
    for (size_t i = 0; i < nodeSet.size(); ++i) {
        if (nodeSet[i]) {
            return i;
        }
    }

    return nodeSet.size();
}

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
