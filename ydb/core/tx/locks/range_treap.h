#pragma once

#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/range_treap/range_treap.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NDataShard {

class TRangeTreeOwnedCellVecComparator {
private:
    using TBorder = NRangeTreap::TBorder<TConstArrayRef<TCell>>;

    TVector<NScheme::TTypeInfo> KeyTypes;

private:
    static EPrefixMode GetPrefixMode(const NRangeTreap::EBorderMode mode) {
        switch (mode) {
            case NRangeTreap::LeftExclusive:
                return EPrefixMode::PrefixModeLeftBorderNonInclusive;
            case NRangeTreap::LeftInclusive:
                return EPrefixMode::PrefixModeLeftBorderInclusive;
            case NRangeTreap::RightExclusive:
                return EPrefixMode::PrefixModeRightBorderNonInclusive;
            case NRangeTreap::RightInclusive:
                return EPrefixMode::PrefixModeRightBorderInclusive;
        }
    }

public:
    int Compare(const TBorder& lhs, const TBorder& rhs) const {
        return ComparePrefixBorders(KeyTypes, lhs.GetKey(), GetPrefixMode(lhs.GetMode()), rhs.GetKey(), GetPrefixMode(rhs.GetMode()));
    }

    void ValidateKey(const TOwnedCellVec& key) const {
        Y_ENSURE(key.size() <= KeyTypes.size(), "Range key is too large");
    }

    void SetKeyTypes(const TVector<NScheme::TTypeInfo>& keyTypes) {
        Y_ENSURE(keyTypes.size() >= KeyTypes.size(), "Number of key columns must not decrease over time");
        KeyTypes = keyTypes;
    }

    ui64 KeyColumns() const {
        return KeyTypes.size();
    }
};

template <class TValue>
struct TRangeTreapDefaultValueTraits {
    using TKeyView = TConstArrayRef<NKikimr::TCell>;

    static bool Less(const TValue& a, const TValue& b) {
        return a < b;
    }

    static bool Equal(const TValue& a, const TValue& b) {
        return a == b;
    }
};

struct TRangeTreapTraits {
    using TKey = TOwnedCellVec;
    using TKeyView = TConstArrayRef<TCell>;
    using TRange = NRangeTreap::TRange<TKey, TKeyView>;
    using TOwnedRange = NRangeTreap::TOwnedRange<TKey>;
};

template <class TValue, class TTypeTraits = TRangeTreapDefaultValueTraits<TValue>>
using TRangeTreap = NKikimr::NRangeTreap::TRangeTreap<TRangeTreapTraits::TKey, TValue, TRangeTreapTraits::TKeyView, TTypeTraits,
    TRangeTreeOwnedCellVecComparator>;

}   // namespace NDataShard
}   // namespace NKikimr
