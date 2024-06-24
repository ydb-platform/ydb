#pragma once

#include "public.h"

#include "key.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/library/codegen/caller.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESortOrder,
    ((Ascending)   (0))
    ((Descending)  (1))
);

////////////////////////////////////////////////////////////////////////////////

//! Class that encapsulates all necessary information for key comparison
//! and testing if key belongs to the ray defined by a key bound.
class TComparator
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<ESortOrder>, SortOrders);

public:
    TComparator() = default;
    explicit TComparator(std::vector<ESortOrder> sortOrders, TCallback<TUUComparerSignature> cgComparator = {});

    void Persist(const TPersistenceContext& context);

    //! Test if key #key belongs to the ray defined by #keyBound.
    bool TestKey(const TKey& key, const TKeyBound& keyBound) const;

    //! Compare key bounds according to their logical position on a line of all possible keys.
    //! If lhs and rhs belong to the same point, compare lower limit against upper limit as
    //! defined by #lowerVsUpperResult (i.e. if 0, lower == upper; if < 0, lower < upper; if > 0, lower > upper) .
    int CompareKeyBounds(const TKeyBound& lhs, const TKeyBound& rhs, int lowerVsUpperResult = 0) const;

    //! Compare two values belonging to the index #index of the key.
    int CompareValues(int index, const TUnversionedValue& lhs, const TUnversionedValue& rhs) const;

    //! Compare keys.
    int CompareKeys(const TKey& lhs, const TKey& rhs) const;

    //! Returns the strongest of two key bounds. Key bounds should be of same direction
    //! (but possibly of different inclusiveness).
    TKeyBound StrongerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Shorthand for #lhs = #StrongerKeyBound(#lhs, #rhs).
    void ReplaceIfStrongerKeyBound(TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Same as previous for owning key bounds.
    void ReplaceIfStrongerKeyBound(TOwningKeyBound& lhs, const TOwningKeyBound& rhs) const;

    //! Returns the weakest of two key bounds. Key bounds should be of same direction
    //! (but possibly of different inclusiveness).
    TKeyBound WeakerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Check if the range defined by two key bounds is empty.
    bool IsRangeEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const;

    //! Check if the range defined by two key bounds has empty interior, i.e. is empty or is a singleton key.
    bool IsInteriorEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const;

    //! Return length of the primary key to which this comparator corresponds.
    //! In particular, any key bound length passed as an argument must not exceed GetLength()
    //! and any key length should be equal to GetLength().
    int GetLength() const;

    //! If there exists such key K that #lhs == ">= K" and #rhs == "<= K", return it.
    std::optional<TKey> TryAsSingletonKey(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Returns a comparator that compares rows by first #keyColumnCount columns and ignores other.
    TComparator Trim(int keyColumnCount) const;

    //! Returns true if at least one column has descending sort order.
    bool HasDescendingSortOrder() const;

    //! Empty comparator is identified with an absence of comparator.
    //! This may be used instead of TComparator.
    explicit operator bool() const;

private:
    // Compiler generated comparer that is used in CompareKeys().
    TCallback<TUUComparerSignature> CGComparator_;

private:
    void ValidateKey(const TKey& key) const;
    void ValidateKeyBound(const TKeyBound& keyBound) const;
};

void FormatValue(TStringBuilderBase* builder, const TComparator& comparator, TStringBuf spec);

void Serialize(const TComparator& comparator, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

using TPrefixComparer = TUUComparerSignature;

////////////////////////////////////////////////////////////////////////////////

int GetCompareSign(int value);

//! Obeys the usual rule: the result's sign incidates the comparison outcome.
//! Also |abs(result) - 1| is equal to index of first non-equal component.
template <typename TComparer>
int CompareKeys(TUnversionedValueRange lhs, TUnversionedValueRange rhs, const TComparer& prefixComparer);
int ComparePrefix(const TUnversionedValue* lhs, const TUnversionedValue* rhs, int length);
int CompareKeys(TLegacyKey lhs, TLegacyKey rhs, TPrefixComparer prefixComparer);
int CompareKeys(TLegacyKey lhs, TLegacyKey rhs, const TKeyComparer& prefixComparer);

class TKeyComparer
    : public TCallback<TPrefixComparer>
{
public:
    using TBase = TCallback<TPrefixComparer>;
    using TBase::TBase;

    using TCaller = NCodegen::TCGCaller<TPrefixComparer>;

    TKeyComparer(const TBase& base);
    TKeyComparer();
};

////////////////////////////////////////////////////////////////////////////////

TKeyRef ToKeyRef(TUnversionedRow row);
TKeyRef ToKeyRef(TUnversionedRow row, int prefixLength);
TKeyRef ToKeyRef(TKey key);

////////////////////////////////////////////////////////////////////////////////

class TKeyBoundRef
    : public TKeyRef
{
public:
    bool Inclusive;
    bool Upper;

    TKeyBoundRef(
        TKeyRef base,
        bool inclusive = false,
        bool upper = false);
};

TKeyBoundRef ToKeyBoundRef(const TKeyBound& bound);
TKeyBoundRef ToKeyBoundRef(const TOwningKeyBound& bound);
TKeyBoundRef ToKeyBoundRef(TUnversionedRow row, bool upper, int keyLength);

////////////////////////////////////////////////////////////////////////////////

template <typename TComparer>
int CompareWithWidening(
    TUnversionedValueRange keyPrefix,
    TUnversionedValueRange boundKey,
    const TComparer& prefixComparer);
int CompareWithWidening(
    TUnversionedValueRange keyPrefix,
    TUnversionedValueRange boundKey);

int TestKey(TUnversionedValueRange key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders);
int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound);
int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, TPrefixComparer prefixComparer);
int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, const TKeyComparer& prefixComparer);
int TestKeyWithWidening(TUnversionedValueRange key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders);

int TestComparisonResult(int result, TRange<ESortOrder> sortOrders, bool inclusive, bool upper);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, ESortOrder sortOrder, TStringBuf /* spec */);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define COMPARATOR_INL_H_
#include "comparator-inl.h"
#undef COMPARATOR_INL_H_
