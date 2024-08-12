#include "key_bound.h"

#include "helpers.h"
#include "row_buffer.h"
#include "serialize.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NYson;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_FATAL below.
YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TableClientKey");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::pair<bool, bool> RelationToIsUpperAndIsInclusive(TStringBuf relation)
{
    if (relation == "<=") {
        return {/* isInclusive */ true, /* isUpper */ true};
    } else if (relation == ">=") {
        return {/* isInclusive */ true, /* isUpper */ false};
    } else if (relation == "<") {
        return {/* isInclusive */ false, /* isUpper */ true};
    } else if (relation == ">") {
        return {/* isInclusive */ false, /* isUpper */ false};
    } else {
        THROW_ERROR_EXCEPTION(
            "Error parsing relation literal %Qv; one of \"<=\", \">=\", \"<\", \">\" expected",
            relation);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRow(const TRow& row, bool isInclusive, bool isUpper)
{
    YT_VERIFY(row);

    ValidateValueTypes(row);
    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRow(TRow&& row, bool isInclusive, bool isUpper)
{
    YT_VERIFY(row);

    ValidateValueTypes(row);
    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRowUnchecked(const TRow& row, bool isInclusive, bool isUpper)
{
    YT_VERIFY(row);

#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key bound from row");
    }
#endif

    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRowUnchecked(TRow&& row, bool isInclusive, bool isUpper)
{
    YT_VERIFY(row);

#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key bound from row");
    }
#endif

    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::MakeUniversal(bool isUpper)
{
    return TKeyBoundImpl<TRow, TKeyBound>::FromRow(EmptyKey(), /* isInclusive */ true, isUpper);
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::MakeEmpty(bool isUpper)
{
    return TKeyBoundImpl<TRow, TKeyBound>::FromRow(EmptyKey(), /* isInclusive */ false, isUpper);
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::ValidateValueTypes(const TRow& row)
{
    YT_VERIFY(row);

    for (const auto& value : row) {
        ValidateDataValueType(value.Type);
    }
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::FormatValue(TStringBuilderBase* builder) const
{
    if (!Prefix) {
        builder->AppendChar('#');
    } else {
        builder->AppendChar(IsUpper ? '<' : '>');
        if (IsInclusive) {
            builder->AppendChar('=');
        }
        builder->AppendString(ToString(Prefix, /*valuesOnly*/ true));
    }
}

template <class TRow, class TKeyBound>
TKeyBoundImpl<TRow, TKeyBound>::operator bool() const
{
    return static_cast<bool>(Prefix);
}

template <class TRow, class TKeyBound>
bool TKeyBoundImpl<TRow, TKeyBound>::IsUniversal() const
{
    return IsInclusive && Prefix && Prefix.GetCount() == 0;
}

template <class TRow, class TKeyBound>
bool TKeyBoundImpl<TRow, TKeyBound>::IsEmpty() const
{
    return !IsInclusive && Prefix && Prefix.GetCount() == 0;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::Invert() const
{
    YT_VERIFY(Prefix);
    return TKeyBound::FromRowUnchecked(Prefix, !IsInclusive, !IsUpper);
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::ToggleInclusiveness() const
{
    YT_VERIFY(Prefix);
    return TKeyBound::FromRowUnchecked(Prefix, !IsInclusive, IsUpper);
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::UpperCounterpart() const
{
    YT_VERIFY(Prefix);
    return IsUpper ? *static_cast<const TKeyBound*>(this) : Invert();
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::LowerCounterpart() const
{
    YT_VERIFY(Prefix);
    return IsUpper ? Invert() : *static_cast<const TKeyBound*>(this);
}

template <class TRow, class TKeyBound>
TStringBuf TKeyBoundImpl<TRow, TKeyBound>::GetRelation() const
{
    if (IsUpper && IsInclusive) {
        return "<=";
    } else if (IsUpper && !IsInclusive) {
        return "<";
    } else if (!IsUpper && IsInclusive) {
        return ">=";
    } else if (!IsUpper && !IsInclusive) {
        return ">";
    } else {
        Y_UNREACHABLE();
    }
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Prefix);
    Persist(context, IsInclusive);
    Persist(context, IsUpper);
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::Serialize(IYsonConsumer* consumer) const
{
    if (*this) {
        BuildYsonFluently(consumer)
            .BeginList()
                .Item().Value(GetRelation())
                .Item().Value(Prefix)
            .EndList();
    } else {
        BuildYsonFluently(consumer)
            .Entity();
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TKeyBoundImpl<TUnversionedRow, TKeyBound>;
template class TKeyBoundImpl<TUnversionedOwningRow, TOwningKeyBound>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TOwningKeyBound::operator TKeyBound() const
{
    TKeyBound result;
    result.Prefix = Prefix;
    result.IsInclusive = IsInclusive;
    result.IsUpper = IsUpper;
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TOwningKeyBound& keyBound, TStringBuf /*spec*/)
{
    return keyBound.FormatValue(builder);
}

void PrintTo(const TOwningKeyBound& keyBound, ::std::ostream* os)
{
    *os << ToString(keyBound);
}

void Serialize(const TKeyBound& keyBound, IYsonConsumer* consumer)
{
    keyBound.Serialize(consumer);
}

////////////////////////////////////////////////////////////////////////////////

TOwningKeyBound TKeyBound::ToOwning() const
{
    TOwningKeyBound result;
    result.Prefix = TUnversionedOwningRow(Prefix);
    result.IsInclusive = IsInclusive;
    result.IsUpper = IsUpper;
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TKeyBound& keyBound, TStringBuf /*spec*/)
{
    return keyBound.FormatValue(builder);
}

void PrintTo(const TKeyBound& keyBound, ::std::ostream* os)
{
    *os << ToString(keyBound);
}

void Serialize(const TOwningKeyBound& keyBound, IYsonConsumer* consumer)
{
    keyBound.Serialize(consumer);
}

void Deserialize(TOwningKeyBound& keyBound, const NYTree::INodePtr& node)
{
    if (node->GetType() == ENodeType::Entity) {
        keyBound = TOwningKeyBound();
    } else if (node->GetType() == ENodeType::List) {
        auto listNode = node->AsList();
        if (listNode->GetChildCount() != 2) {
            THROW_ERROR_EXCEPTION(
                "Error parsing key bound: list node must have exactly two elements, "
                "first of which is a relation string literal, and second is a row; "
                "%v elements found",
                listNode->GetChildCount());
        }
        auto relationNode = listNode->GetChildOrThrow(0);
        auto rowNode = listNode->GetChildOrThrow(1);

        if (relationNode->GetType() != ENodeType::String) {
            THROW_ERROR_EXCEPTION(
                "Error parsing key bound: first element must be a string node; actual %Qv node",
                relationNode->GetType());
        }

        auto relation = relationNode->GetValue<TString>();
        auto [isInclusive, isUpper] = NDetail::RelationToIsUpperAndIsInclusive(relation);

        TUnversionedOwningRow row;
        Deserialize(row, rowNode);

        keyBound = TOwningKeyBound::FromRow(row, isInclusive, isUpper);
    } else {
        THROW_ERROR_EXCEPTION("Error parsing key bound: expected %Qlv node, actual %Qlv node",
            NYTree::ENodeType::List,
            node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator ==(const TKeyBound& lhs, const TKeyBound& rhs)
{
    return
        lhs.Prefix == rhs.Prefix &&
        lhs.IsInclusive == rhs.IsInclusive &&
        lhs.IsUpper == rhs.IsUpper;
}

bool operator ==(const TOwningKeyBound& lhs, const TOwningKeyBound& rhs)
{
    return static_cast<TKeyBound>(lhs) == static_cast<TKeyBound>(rhs);
}

////////////////////////////////////////////////////////////////////////////////

// Common implementation for owning case and non-owning case over row buffer.
// Returns pair {prefixKeyLength, inclusive flag} describing how to transform
// legacy key into key bound.
std::pair<int, bool> GetBoundPrefixAndInclusiveness(TUnversionedRow row, bool isUpper, int keyLength)
{
    YT_VERIFY(row);

    // Flag indicating that row starts with #keyLength non-sentinel values followed by at least one arbitrary value.
    bool isLongRow = false;

    // If row contains at least one sentinel on first #keyLength positions, type of leftmost of them.
    std::optional<EValueType> leftmostSentinelType;

    // Length of the longest prefix of row which is free of sentinels. Prefix length is limited by #keyLength.
    int prefixLength = 0;
    for (int index = 0; index < static_cast<int>(row.GetCount()) && index <= keyLength; ++index) {
        if (index == keyLength) {
            isLongRow = true;
            break;
        }
        if (row[index].Type != EValueType::Min && row[index].Type != EValueType::Max) {
            ++prefixLength;
        } else {
            leftmostSentinelType = row[index].Type;
            break;
        }
    }

    // When dealing with legacy rows, upper limit is always exclusive and lower limit is always inclusive.
    // We will call this kind of inclusiveness standard. This implies following cases for key bounds.
    //
    // (A) If row is long, upper limit will be inclusive and lower limit will be exclusive, i.e. inclusiveness is toggled.
    // (B) Otherwise, if row has exactly length of #keyLength and does not contain sentinels, inclusiveness is standard.
    //
    // Suppose none of (A) and (B) happened. We know that prefix is strictly shorter than #keyLength. If may or may not be
    // followed by a sentinel. Actually there is no difference if prefix is followed by Min or if it is not followed by sentinel.
    // To prove this fact, consider row R = prefix + [Min], length(R) < #keyLength and key K, length(K) == #keyLength.
    // It is easy to see that R is compared to K in exactly the same way as prefix is compared to K; this case
    // corresponds to a key bound with standard inclusiveness.
    //
    // Similar argument shows that if prefix is followed by Max, key bound inclusiveness should be toggled.
    //
    // So, we have only two more cases:
    //
    // (C) Otherwise, if prefix is followed by Min or no sentinel, inclusiveness is standard.
    // (D) Otherwise (prefix is followed by Max), inclusiveness is toggled.

    // Cases (A) and (D).
    bool toggleInclusiveness = isLongRow || leftmostSentinelType == EValueType::Max;

    bool isInclusive = (isUpper && toggleInclusiveness) || (!isUpper && !toggleInclusiveness);

    return {prefixLength, isInclusive};
}

TOwningKeyBound KeyBoundFromLegacyRow(TUnversionedRow row, bool isUpper, int keyLength)
{
    if (!row) {
        return TOwningKeyBound::MakeUniversal(isUpper);
    }

    auto [prefixLength, isInclusive] = GetBoundPrefixAndInclusiveness(row, isUpper, keyLength);
    return TOwningKeyBound::FromRow(
        TUnversionedOwningRow(row.FirstNElements(prefixLength)),
        isInclusive,
        isUpper);
}

TKeyBound KeyBoundFromLegacyRow(TUnversionedRow row, bool isUpper, int keyLength, const TRowBufferPtr& rowBuffer)
{
    if (!row) {
        return TKeyBound::MakeUniversal(isUpper);
    }

    auto [prefixLength, isInclusive] = GetBoundPrefixAndInclusiveness(row, isUpper, keyLength);
    YT_VERIFY(prefixLength <= static_cast<int>(row.GetCount()));
    row = rowBuffer->CaptureRow(MakeRange(row.Begin(), prefixLength));

    return TKeyBound::FromRow(
        row,
        isInclusive,
        isUpper);
}

TUnversionedOwningRow KeyBoundToLegacyRow(TKeyBound keyBound)
{
    if (!keyBound) {
        return TUnversionedOwningRow();
    }

    TUnversionedOwningRowBuilder builder;
    for (const auto& value : keyBound.Prefix) {
        builder.AddValue(value);
    }
    auto shouldAddMax = (keyBound.IsUpper && keyBound.IsInclusive) || (!keyBound.IsUpper && !keyBound.IsInclusive);
    if (shouldAddMax) {
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
    }
    return builder.FinishRow();
}

TUnversionedRow KeyBoundToLegacyRow(TKeyBound keyBound, const TRowBufferPtr& rowBuffer)
{
    if (!keyBound) {
        return TUnversionedRow();
    }

    auto shouldAddMax = (keyBound.IsUpper && keyBound.IsInclusive) || (!keyBound.IsUpper && !keyBound.IsInclusive);

    auto row = rowBuffer->AllocateUnversioned(keyBound.Prefix.GetCount() + (shouldAddMax ? 1 : 0));
    memcpy(row.Begin(), keyBound.Prefix.Begin(), sizeof(TUnversionedValue) * keyBound.Prefix.GetCount());
    if (shouldAddMax) {
        row[keyBound.Prefix.GetCount()] = MakeUnversionedSentinelValue(EValueType::Max, keyBound.Prefix.GetCount());
    }
    for (auto& value : row) {
        rowBuffer->CaptureValue(&value);
    }

    return row;
}

TKeyBound ShortenKeyBound(TKeyBound keyBound, int length, const TRowBufferPtr& rowBuffer)
{
    if (!keyBound) {
        return TKeyBound();
    }

    if (static_cast<int>(keyBound.Prefix.GetCount()) <= length) {
        // No need to change anything.
        return keyBound;
    }

    // If we do perform shortening, resulting key bound is going to be inclusive despite the original inclusiveness.

    auto result = TKeyBound::FromRowUnchecked(
        rowBuffer->CaptureRow(MakeRange(keyBound.Prefix.Begin(), length)),
        /* isInclusive */ true,
        keyBound.IsUpper);

    return result;
}

//! Owning version of #ShortenKeyBound.
TOwningKeyBound ShortenKeyBound(TOwningKeyBound keyBound, int length)
{
    if (keyBound.Prefix.GetCount() <= length) {
        // No need to change anything.
        return keyBound;
    }

    // If we do perform shortening, resulting key bound is going to be inclusive despite the original inclusiveness.

    return TOwningKeyBound::FromRowUnchecked(
        TUnversionedOwningRow(keyBound.Prefix.FirstNElements(length)),
        /* isInclusive */ true,
        keyBound.IsUpper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

size_t THash<NYT::NTableClient::TKeyBound>::operator()(const NYT::NTableClient::TKeyBound& keyBound) const
{
    using NYT::HashCombine;

    size_t result = 0;
    HashCombine(result, keyBound.Prefix);
    HashCombine(result, keyBound.IsInclusive);
    HashCombine(result, keyBound.IsUpper);

    return result;
}
