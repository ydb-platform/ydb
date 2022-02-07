#include "range_set.h"

#include <util/generic/algorithm.h>

namespace {
    bool ShouldJoin(const TIpAddressRange& lhs, const TIpAddressRange& rhs) {
        return lhs.Overlaps(rhs) || lhs.IsConsecutive(rhs);
    }
}

bool TIpRangeSet::TRangeLess::operator()(const TIpAddressRange& lhs, const TIpAddressRange& rhs) const {
    return *lhs.Begin() < *rhs.Begin();
}

TIpRangeSet::TIpRangeSet() = default;
TIpRangeSet::~TIpRangeSet() = default;

void TIpRangeSet::Add(TIpAddressRange r) {
    Y_ENSURE(IsEmpty() || r.Type() == Type(), "Mixing IPv4 and IPv6 ranges is disallowed");

    auto lowerIt = Ranges_.lower_bound(r);

    // still may overlap the last interval in our tree
    if (IsEmpty()) {
        Ranges_.insert(r);
        return;
    } else if (lowerIt == Ranges_.end()) {
        if (auto it = Ranges_.rbegin(); ShouldJoin(*it, r)) {
            auto unitedRange = it->Union(r);
            Ranges_.erase(--it.base());
            Ranges_.insert(unitedRange);
        } else {
            Ranges_.insert(r);
        }

        return;
    }


    TIpAddressRange unitedRange{r};

    auto joined = lowerIt;
    if (lowerIt != Ranges_.begin()) {
        if (ShouldJoin(unitedRange, *(--joined))) {
            unitedRange = unitedRange.Union(*joined);
        } else {
            ++joined;
        }
    }

    auto it = lowerIt;
    for (; it != Ranges_.end() && ShouldJoin(*it, unitedRange); ++it) {
        unitedRange = unitedRange.Union(*it);
    }

    Ranges_.erase(joined, it);
    Ranges_.insert(unitedRange);
}

TIpAddressRange::TIpType TIpRangeSet::Type() const {
    return IsEmpty()
        ? TIpAddressRange::TIpType::LAST
        : Ranges_.begin()->Type();
}

bool TIpRangeSet::IsEmpty() const {
    return Ranges_.empty();
}

TIpRangeSet::TIterator TIpRangeSet::Find(TIpv6Address addr) const {
    if (IsEmpty() || addr.Type() != Type()) {
        return End();
    }

    auto lowerIt = Ranges_.lower_bound(TIpAddressRange(addr, addr));

    if (lowerIt == Ranges_.begin()) {
        return lowerIt->Contains(addr)
            ? lowerIt
            : End();
    } else if (lowerIt == Ranges_.end()) {
        auto rbegin = Ranges_.crbegin();
        return rbegin->Contains(addr)
            ? (++rbegin).base()
            : End();
    } else if (lowerIt->Contains(addr)) {
        return lowerIt;
    }

    --lowerIt;

    return lowerIt->Contains(addr)
        ? lowerIt
        : End();
}

bool TIpRangeSet::Contains(TIpv6Address addr) const {
    return Find(addr) != End();
}
