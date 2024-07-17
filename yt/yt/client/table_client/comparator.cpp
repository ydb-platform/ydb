#include "comparator.h"

#include "key_bound.h"
#include "serialize.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NYson;
using namespace NYTree;

//! Used only for YT_LOG_FATAL below.
YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TableClientComparator");

////////////////////////////////////////////////////////////////////////////////

TComparator::TComparator(std::vector<ESortOrder> sortOrders, TCallback<TUUComparerSignature> cgComparator)
    : SortOrders_(std::move(sortOrders))
    , CGComparator_(std::move(cgComparator))
{ }

void TComparator::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SortOrders_);
}

int TComparator::GetLength() const
{
    return SortOrders_.size();
}

void TComparator::ValidateKey(const TKey& key) const
{
    YT_LOG_FATAL_IF(
        key.GetLength() != GetLength(),
        "Comparator is used with key of different length (Key: %v, Comparator: %v)",
        key,
        *this);
}

void TComparator::ValidateKeyBound(const TKeyBound& keyBound) const
{
    YT_LOG_FATAL_IF(
        static_cast<int>(keyBound.Prefix.GetCount()) > GetLength(),
        "Comparator is used with longer key bound (KeyBound: %v, Comparator: %v)",
        keyBound,
        *this);
}

int TComparator::CompareValues(int index, const TUnversionedValue& lhs, const TUnversionedValue& rhs) const
{
    int valueComparisonResult = CompareRowValues(lhs, rhs);

    if (SortOrders_[index] == ESortOrder::Descending) {
        valueComparisonResult = -valueComparisonResult;
    }

    return valueComparisonResult;
}

TKeyBound TComparator::StrongerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const
{
    YT_VERIFY(lhs);
    YT_VERIFY(rhs);

    YT_VERIFY(lhs.IsUpper == rhs.IsUpper);
    auto comparisonResult = CompareKeyBounds(lhs, rhs);
    if (lhs.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    return (comparisonResult <= 0) ? rhs : lhs;
}

void TComparator::ReplaceIfStrongerKeyBound(TKeyBound& lhs, const TKeyBound& rhs) const
{
    if (!lhs) {
        lhs = rhs;
        return;
    }

    if (!rhs) {
        return;
    }

    YT_VERIFY(lhs.IsUpper == rhs.IsUpper);
    auto comparisonResult = CompareKeyBounds(lhs, rhs);
    if (lhs.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    if (comparisonResult < 0) {
        lhs = rhs;
    }
}

void TComparator::ReplaceIfStrongerKeyBound(TOwningKeyBound& lhs, const TOwningKeyBound& rhs) const
{
    if (!lhs) {
        lhs = rhs;
        return;
    }

    if (!rhs) {
        return;
    }

    YT_VERIFY(lhs.IsUpper == rhs.IsUpper);
    auto comparisonResult = CompareKeyBounds(lhs, rhs);
    if (lhs.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    if (comparisonResult < 0) {
        lhs = rhs;
    }
}

TKeyBound TComparator::WeakerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const
{
    YT_VERIFY(lhs.IsUpper == rhs.IsUpper);
    auto comparisonResult = CompareKeyBounds(lhs, rhs);
    if (lhs.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    return (comparisonResult >= 0) ? rhs : lhs;
}

bool TComparator::IsRangeEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const
{
    YT_VERIFY(!lowerBound.IsUpper);
    YT_VERIFY(upperBound.IsUpper);
    return CompareKeyBounds(lowerBound, upperBound, /* lowerVsUpper */ 1) >= 0;
}

bool TComparator::IsInteriorEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const
{
    YT_VERIFY(!lowerBound.IsUpper);
    YT_VERIFY(upperBound.IsUpper);
    return IsRangeEmpty(lowerBound, upperBound) || TryAsSingletonKey(lowerBound, upperBound);
}

bool TComparator::TestKey(const TKey& key, const TKeyBound& keyBound) const
{
    ValidateKey(key);
    ValidateKeyBound(keyBound);

    int comparisonResult = 0;

    for (int index = 0; index < static_cast<int>(keyBound.Prefix.GetCount()); ++index) {
        const auto& keyValue = key[index];
        const auto& keyBoundValue = keyBound.Prefix[index];
        comparisonResult = CompareValues(index, keyValue, keyBoundValue);
        if (comparisonResult != 0) {
            break;
        }
    }

    if (keyBound.IsUpper) {
        comparisonResult = -comparisonResult;
    }

    // Now:
    // - comparisonResult > 0 means that key is strictly inside ray (i.e. test is positive);
    // - comparisonResult == 0 means that key starts with key bound prefix (i.e. we should consider inclusiveness);
    // - comparisonResult < 0 means that key is strictly outside ray (i.e. test is negative).

    return comparisonResult > 0 || (comparisonResult == 0 && keyBound.IsInclusive);
}

int TComparator::CompareKeyBounds(const TKeyBound& lhs, const TKeyBound& rhs, int lowerVsUpper) const
{
    ValidateKeyBound(lhs);
    ValidateKeyBound(rhs);

    int comparisonResult = 0;

    // In case when one key bound is a proper prefix of another, points to the shorter one.
    const TKeyBound* shorter = nullptr;

    for (int index = 0; ; ++index) {
        if (index >= static_cast<int>(lhs.Prefix.GetCount()) &&
            index >= static_cast<int>(rhs.Prefix.GetCount()))
        {
            // Prefixes coincide. Check if key bounds are indeed at the same point.
            {
                auto lhsInclusivenessAsUpper = (lhs.IsUpper && lhs.IsInclusive) || (!lhs.IsUpper && !lhs.IsInclusive);
                auto rhsInclusivenessAsUpper = (rhs.IsUpper && rhs.IsInclusive) || (!rhs.IsUpper && !rhs.IsInclusive);
                if (lhsInclusivenessAsUpper != rhsInclusivenessAsUpper) {
                    return lhsInclusivenessAsUpper - rhsInclusivenessAsUpper;
                }
            }

            // Ok, they are indeed at the same point. How do we break ties?
            if (lowerVsUpper == 0) {
                // We are asked not to break ties.
                return 0;
            }

            // Break ties using #upperFirst.
            comparisonResult = lhs.IsUpper - rhs.IsUpper;

            if (lowerVsUpper > 0) {
                comparisonResult = -comparisonResult;
            }
            return comparisonResult;
        } else if (index >= static_cast<int>(lhs.Prefix.GetCount())) {
            shorter = &lhs;
            break;
        } else if (index >= static_cast<int>(rhs.Prefix.GetCount())) {
            shorter = &rhs;
            break;
        } else {
            const auto& lhsValue = lhs.Prefix[index];
            const auto& rhsValue = rhs.Prefix[index];
            comparisonResult = CompareValues(index, lhsValue, rhsValue);
            if (comparisonResult != 0) {
                return comparisonResult;
            }
        }
    }
    YT_VERIFY(shorter);

    // By this moment, longer operand is strictly between shorter operand and toggleInclusiveness(shorter operand).
    // Thus we have to check if shorter operand is "largest" among itself and its toggleInclusiveness counterpart.
    if ((shorter->IsUpper && shorter->IsInclusive) || (!shorter->IsUpper && !shorter->IsInclusive)) {
        comparisonResult = -1;
    } else {
        comparisonResult = 1;
    }

    // By now comparisonResult expresses if longer < shorter. Now check which hand is actually shorter.
    if (shorter == &lhs) {
        comparisonResult = -comparisonResult;
    }

    return comparisonResult;
}

int TComparator::CompareKeys(const TKey& lhs, const TKey& rhs) const
{
    ValidateKey(lhs);
    ValidateKey(rhs);

    if (CGComparator_) {
        // Compare keys with code generated comparator.
        int comparisonResult = CGComparator_.Run(lhs.Begin(), rhs.Begin(), GetLength());

        if (comparisonResult == 0) {
            return comparisonResult;
        }

        int differentColumnIdx = abs(comparisonResult) - 1;
        int orderSign = SortOrders_[differentColumnIdx] == ESortOrder::Ascending ? +1 : -1;
        return comparisonResult * orderSign;
    }

    for (int index = 0; index < lhs.GetLength(); ++index) {
        auto valueComparisonResult = CompareValues(index, lhs[index], rhs[index]);
        if (valueComparisonResult != 0) {
            return valueComparisonResult;
        }
    }

    return 0;
}

std::optional<TKey> TComparator::TryAsSingletonKey(const TKeyBound& lowerBound, const TKeyBound& upperBound) const
{
    ValidateKeyBound(lowerBound);
    ValidateKeyBound(upperBound);
    YT_VERIFY(!lowerBound.IsUpper);
    YT_VERIFY(upperBound.IsUpper);

    if (static_cast<int>(lowerBound.Prefix.GetCount()) != GetLength() ||
        static_cast<int>(upperBound.Prefix.GetCount()) != GetLength())
    {
        return std::nullopt;
    }

    if (!lowerBound.IsInclusive || !upperBound.IsInclusive) {
        return std::nullopt;
    }

    for (int index = 0; index < static_cast<int>(lowerBound.Prefix.GetCount()); ++index) {
        if (CompareValues(index, lowerBound.Prefix[index], upperBound.Prefix[index]) != 0) {
            return std::nullopt;
        }
    }

    return TKey::FromRowUnchecked(lowerBound.Prefix);
}

TComparator TComparator::Trim(int keyColumnCount) const
{
    YT_VERIFY(keyColumnCount <= std::ssize(SortOrders_));

    auto sortOrders = SortOrders_;
    sortOrders.resize(keyColumnCount);
    return TComparator(std::move(sortOrders));
}

bool TComparator::HasDescendingSortOrder() const
{
    return std::find(SortOrders_.begin(), SortOrders_.end(), ESortOrder::Descending) != SortOrders_.end();
}

TComparator::operator bool() const
{
    return !SortOrders_.empty();
}

void FormatValue(TStringBuilderBase* builder, const TComparator& comparator, TStringBuf /* spec */)
{
    builder->AppendFormat("{Length: %v, SortOrders: ", comparator.GetLength());
    for (auto sortOrder : comparator.SortOrders()) {
        switch (sortOrder) {
            case ESortOrder::Ascending:
                builder->AppendChar('A');
                break;
            case ESortOrder::Descending:
                builder->AppendChar('D');
                break;
            default:
                YT_ABORT();
        }
    }
    builder->AppendChar('}');
}

void Serialize(const TComparator& comparator, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoListFor(comparator.SortOrders(), [&] (TFluentList fluent, ESortOrder sortOrder) {
            fluent.Item().Value(sortOrder);
        });
}

////////////////////////////////////////////////////////////////////////////////

int GetCompareSign(int value)
{
    return value != 0 ? value > 0 ? 1 : -1 : 0;
}

int ComparePrefix(const TUnversionedValue* lhs, const TUnversionedValue* rhs, int length)
{
    int index = 0;
    while (index < length) {
        int result = CompareRowValues(lhs[index], rhs[index]);
        ++index;
        if (result != 0) {
            return result > 0 ? index : -index;
        }
    }

    return 0;
}

int CompareKeys(TLegacyKey lhs, TLegacyKey rhs, TPrefixComparer prefixComparer)
{
    return CompareKeys(ToKeyRef(lhs), ToKeyRef(rhs), prefixComparer);
}

int CompareKeys(TLegacyKey lhs, TLegacyKey rhs, const TKeyComparer& prefixComparer)
{
    return CompareKeys(ToKeyRef(lhs), ToKeyRef(rhs), prefixComparer);
}

TKeyComparer::TKeyComparer(const TBase& base)
    : TBase(base)
{ }

TKeyComparer::TKeyComparer()
    : TBase(
        New<TCaller>(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            YT_CURRENT_SOURCE_LOCATION,
#endif
            nullptr,
            &ComparePrefix),
        &TCaller::StaticInvoke)
{ }

////////////////////////////////////////////////////////////////////////////////

TKeyRef ToKeyRef(TKey key)
{
    return key.Elements();
}

////////////////////////////////////////////////////////////////////////////////

TKeyBoundRef::TKeyBoundRef(
    TKeyRef base,
    bool inclusive,
    bool upper)
    : TKeyRef(base)
    , Inclusive(inclusive)
    , Upper(upper)
{ }

TKeyBoundRef ToKeyBoundRef(const TKeyBound& bound)
{
    return TKeyBoundRef(ToKeyRef(bound.Prefix), bound.IsInclusive, bound.IsUpper);
}

TKeyBoundRef ToKeyBoundRef(const TOwningKeyBound& bound)
{
    return TKeyBoundRef(ToKeyRef(bound.Prefix), bound.IsInclusive, bound.IsUpper);
}

std::pair<int, bool> GetBoundPrefixAndInclusiveness(TUnversionedRow row, bool isUpper, int keyLength);

TKeyBoundRef ToKeyBoundRef(TUnversionedRow row, bool upper, int keyLength)
{
    if (!row) {
        return TKeyBoundRef({}, /*inclusive*/ true, upper);
    }

    auto [prefixLength, inclusive] = GetBoundPrefixAndInclusiveness(row, upper, keyLength);
    return TKeyBoundRef(
        ToKeyRef(row, prefixLength),
        inclusive,
        upper);
}

////////////////////////////////////////////////////////////////////////////////

int TestComparisonResult(int result, TRange<ESortOrder> sortOrders, bool inclusive, bool upper)
{
    if (result != 0) {
        if (sortOrders[std::abs(result) - 1] == ESortOrder::Descending) {
            result = -result;
        }

        if (upper) {
            result = -result;
        }
    }

    return result > 0 || inclusive && result == 0;
}

int TestComparisonResult(int result, bool inclusive, bool upper)
{
    if (upper) {
        result = -result;
    }

    return result > 0 || inclusive && result == 0;
}

////////////////////////////////////////////////////////////////////////////////

int CompareWithWidening(
    TUnversionedValueRange keyPrefix,
    TUnversionedValueRange boundKey)
{
    return CompareWithWidening(keyPrefix, boundKey, ComparePrefix);
}

int TestKey(TUnversionedValueRange key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders)
{
    YT_VERIFY(bound.size() <= key.size());
    int result = ComparePrefix(key.begin(), bound.begin(), bound.size());

    return TestComparisonResult(result, sortOrders, bound.Inclusive, bound.Upper);
}

int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders)
{
    int result = CompareWithWidening(key, bound);
    return TestComparisonResult(result, sortOrders, bound.Inclusive, bound.Upper);
}

int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound)
{
    int result = CompareWithWidening(key, bound);
    return TestComparisonResult(result, bound.Inclusive, bound.Upper);
}

int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, TPrefixComparer prefixComparer)
{
    int result = CompareWithWidening(key, bound, prefixComparer);
    return TestComparisonResult(result, bound.Inclusive, bound.Upper);
}

int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, const TKeyComparer& prefixComparer)
{
    int result = CompareWithWidening(key, bound, prefixComparer);
    return TestComparisonResult(result, bound.Inclusive, bound.Upper);
}

int TestKeyWithWidening(TPrefixComparer prefixComparer, TUnversionedValueRange key, const TKeyBoundRef& bound)
{
    int result = CompareWithWidening(key, bound, prefixComparer);
    return TestComparisonResult(result, bound.Inclusive, bound.Upper);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, ESortOrder sortOrder, TStringBuf /* spec */)
{
    FormatEnum(builder, sortOrder, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
