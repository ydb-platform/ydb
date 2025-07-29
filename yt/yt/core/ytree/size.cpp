#include "size.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/string/cast.h>

namespace NYT {

namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Use suffixes for 1000, ..., 1000 ** 6 and 1024, ..., 1024 ** 6.
// 1000 ** 6 < 1024 ** 6 = 2 ** 60 < std::numeric_limits<TSize::TUnderlying>::max().
constexpr int MaxMultiplierOrder = 6;
using TMultipliers = std::array<std::array<TSize::TUnderlying, MaxMultiplierOrder + 1>, 2>;

constexpr TMultipliers Multipliers = std::invoke([] {
    TMultipliers result;
    result[0][0] = 1;
    result[1][0] = 1;
    for (int i = 1; i <= MaxMultiplierOrder; ++i) {
        result[0][i] = result[0][i - 1] * 1000;
        result[1][i] = result[1][i - 1] * 1024;
    }
    return result;
});

TSize::TUnderlying DeserializeSizeWithSuffixesImpl(TStringBuf originalValue)
{
    TStringBuf value = originalValue;

    bool basedOnPowerOf2 = value.ChopSuffix("i");
    int order =
        value.ChopSuffix("K") ? 1 :
        value.ChopSuffix("M") ? 2 :
        value.ChopSuffix("G") ? 3 :
        value.ChopSuffix("T") ? 4 :
        value.ChopSuffix("P") ? 5 :
        value.ChopSuffix("E") ? 6 : 0;

    TSize::TUnderlying multiplier = Multipliers[static_cast<int>(basedOnPowerOf2)][order];
    TSize::TUnderlying result = FromString<TSize::TUnderlying>(value);

    bool tooLargeValue = result < 0
        ? result < std::numeric_limits<TSize::TUnderlying>::lowest() / multiplier
        : result > std::numeric_limits<TSize::TUnderlying>::max() / multiplier;
    THROW_ERROR_EXCEPTION_IF(tooLargeValue, "Cannot parse too large value %Qlv as 64-bit integral type", originalValue);

    return result * multiplier;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSize TSize::FromString(TStringBuf serializedValue)
{
    return TSize(DeserializeSizeWithSuffixesImpl(serializedValue));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSize& value, NYson::IYsonConsumer* consumer)
{
    Serialize(value.Underlying(), consumer);
}

void Deserialize(TSize& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Int64) {
        value = TSize(node->AsInt64()->GetValue());
    } else if (node->GetType() == ENodeType::Uint64) {
        value = TSize(CheckedIntegralCast<i64>(node->AsUint64()->GetValue()));
    } else if (node->GetType() == ENodeType::String) {
        value = TSize::FromString(node->AsString()->GetValue());
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TSize value from %Qlv",
            node->GetType());
    }
}

void Deserialize(TSize& value, NYson::TYsonPullParserCursor* cursor)
{
    if ((*cursor)->GetType() == NYson::EYsonItemType::Int64Value) {
        value = TSize((*cursor)->UncheckedAsInt64());
        cursor->Next();
    } else if ((*cursor)->GetType() == NYson::EYsonItemType::Uint64Value) {
        value = TSize(CheckedIntegralCast<i64>((*cursor)->UncheckedAsUint64()));
        cursor->Next();
    } else if ((*cursor)->GetType() == NYson::EYsonItemType::StringValue) {
        value = TSize::FromString((*cursor)->UncheckedAsString());
        cursor->Next();
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TSize value from %Qlv",
            (*cursor)->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
NYT::NYTree::TSize FromStringImpl<NYT::NYTree::TSize, char>(const char* data, size_t size)
{
    return NYT::NYTree::TSize::FromString(TStringBuf(data, size));
}

template<>
bool TryFromStringImpl<NYT::NYTree::TSize, char>(const char* data, size_t size, NYT::NYTree::TSize& value)
{
    try {
        value = NYT::NYTree::TSize::FromString(TStringBuf(data, size));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::NYTree::TSize>(IOutputStream& out, const NYT::NYTree::TSize& value) {
    out << value.Underlying();
}

////////////////////////////////////////////////////////////////////////////////
