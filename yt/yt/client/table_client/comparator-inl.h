#ifndef COMPARATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include comparator.h"
// For the sake of sane code completion.
#include "comparator.h"
#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TComparer>
int CompareKeys(TUnversionedValueRange lhs, TUnversionedValueRange rhs, const TComparer& prefixComparer)
{
    auto minCount = std::min(lhs.Size(), rhs.Size());
    auto result = prefixComparer(lhs.Begin(), rhs.Begin(), minCount);
    if (result == 0) {
        if (lhs.Size() < rhs.Size()) {
            result = -(minCount + 1);
        } else if (lhs.Size() > rhs.Size()) {
            result = minCount + 1;
        }
    }

    return GetCompareSign(result);
}

template <typename TComparer>
int CompareWithWidening(
    TUnversionedValueRange keyPrefix,
    TUnversionedValueRange boundKey,
    const TComparer& prefixComparer)
{
    int result;
    if (keyPrefix.size() < boundKey.size()) {
        result = prefixComparer(keyPrefix.begin(), boundKey.begin(), keyPrefix.size());

        if (result == 0) {
            // Key is widened with nulls. Compare them with bound.
            int index = keyPrefix.size();
            while (index < std::ssize(boundKey)) {
                if (boundKey[index].Type != EValueType::Null) {
                    // Negative value because null is less than non-null value type.
                    result = -(index + 1);
                    break;
                }
                ++index;
            }
        }
    } else {
        result = prefixComparer(keyPrefix.begin(), boundKey.begin(), boundKey.size());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
