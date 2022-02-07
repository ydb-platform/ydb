#pragma once

#include "ipmath.h"

#include <util/generic/set.h>
#include <util/ysaveload.h>


/// @brief Maintains a disjoint set of added ranges. Allows for efficient membership queries
/// for an address in a set of IP ranges.
class TIpRangeSet {
    struct TRangeLess {
        bool operator()(const TIpAddressRange& lhs, const TIpAddressRange& rhs) const;
    };

    using TTree = TSet<TIpAddressRange, TRangeLess>;

public:
    using iterator = TTree::iterator;
    using const_iterator = TTree::const_iterator;
    using value_type = TTree::value_type;
    using TIterator = TTree::iterator;
    using TConstIterator = TTree::const_iterator;

    TIpRangeSet();
    ~TIpRangeSet();

    void Add(TIpAddressRange range);

    template <typename TContainer>
    void Add(TContainer&& addrs) {
        using T = typename std::decay<TContainer>::type::value_type;
        static_assert(std::is_convertible<T, TIpAddressRange>::value);

        for (auto&& addr : addrs) {
            Add(addr);
        }
    }

    TIpAddressRange::TIpType Type() const;

    bool IsEmpty() const;
    bool Contains(TIpv6Address addr) const;
    TConstIterator Find(TIpv6Address addr) const;

    TConstIterator Begin() const {
        return Ranges_.begin();
    }

    TConstIterator End() const {
        return Ranges_.end();
    }

    TConstIterator begin() const {
        return Begin();
    }

    TConstIterator end() const {
        return End();
    }

    Y_SAVELOAD_DEFINE(Ranges_);

private:
    TTree Ranges_;
};
