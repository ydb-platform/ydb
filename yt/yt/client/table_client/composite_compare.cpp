#include "composite_compare.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token_writer.h>

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/farmhash/farm_hash.h>
#include <library/cpp/yt/logging/logger.h>

#include <util/stream/mem.h>

#include <cmath>

namespace NYT::NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "YsonCompositeCompare");

////////////////////////////////////////////////////////////////////////////////

namespace {

// This file implements comparison for composite and any values.
// Composite types that supports comparison are:
//   1. Optional
//   2. List
//   3. Tuple
//   4. Variant
//
// When we compare composite or any values we assume that they are well-formed YSON representations of same type supporting comparison.
// And we compare them in following manner:
//   1. We scan two values simultaneously and look at their yson tokens and find first mismatching token.
//   2. If one of the token is EndList (this only can happen if we parsing values of list type
//      and one list is shorter than another) that means that value containing EndList is less that other.
//   3. Otherwise if one of the values is Entity (other value have to be non null value) that means
//      that value containing Entity is less than other.
//   4. Otherwise if values have different types we compare them using EValueType order (via MapItemTypeToValueType)
//   5. Otherwise it's values of the same type and we can easily compare them.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECompareClass, ui32,
    ((Incomparable)(0))
    ((EndList)(1))
    ((Entity)(2))
    ((BeginValue)(3))
);

// Helper function for GetCompareClass
static constexpr ui32 SetMask(EYsonItemType type, ECompareClass compareClass)
{
    return static_cast<ui32>(compareClass) << (static_cast<ui32>(type) * 2);
}

static constexpr ECompareClass GetCompareClass(EYsonItemType type)
{
    // We have single integer where each pair of bits encodes map EYsonItemType -> ECompareClass.
    constexpr ui32 compareClassMask =
        SetMask(EYsonItemType::EndList, ECompareClass::EndList) |
        SetMask(EYsonItemType::EntityValue, ECompareClass::Entity) |
        SetMask(EYsonItemType::BeginList, ECompareClass::BeginValue) |
        SetMask(EYsonItemType::Int64Value, ECompareClass::BeginValue) |
        SetMask(EYsonItemType::Uint64Value, ECompareClass::BeginValue) |
        SetMask(EYsonItemType::DoubleValue, ECompareClass::BeginValue) |
        SetMask(EYsonItemType::BooleanValue, ECompareClass::BeginValue) |
        SetMask(EYsonItemType::StringValue, ECompareClass::BeginValue);
    static_assert(TEnumTraits<EYsonItemType>::GetDomainSize() * 2 <= 32);
    return static_cast<ECompareClass>(0x3u & (compareClassMask >> (static_cast<ui32>(type) * 2)));
}

template <typename T>
Y_FORCE_INLINE int ComparePrimitive(T lhs, T rhs)
{
    if (lhs == rhs) {
        return 0;
    } else if (lhs < rhs) {
        return -1;
    } else {
        return 1;
    }
}

template <>
Y_FORCE_INLINE int ComparePrimitive<double>(double lhs, double rhs)
{
    if (lhs < rhs) {
        return -1;
    } else if (lhs > rhs) {
        return 1;
    } else if (std::isnan(lhs)) {
        if (std::isnan(rhs)) {
            return 0;
        }
        return 1;
    } else if (std::isnan(rhs)) {
        return -1;
    } else {
        return 0;
    }
}

[[noreturn]] static void ThrowIncomparableYsonToken(EYsonItemType tokenType)
{
    THROW_ERROR_EXCEPTION("Incomparable YSON token %Qlv",
        tokenType);
}

Y_FORCE_INLINE static int GetSign(int x)
{
    return static_cast<int>(0 < x) - static_cast<int>(0 > x);
}

Y_FORCE_INLINE static EValueType MapItemTypeToValueType(EYsonItemType itemType)
{
    static const TEnumIndexedArray<EYsonItemType, EValueType> mapping = {
        {EYsonItemType::EndOfStream, EValueType::Min},
        {EYsonItemType::BeginMap, EValueType::Min},
        {EYsonItemType::EndMap, EValueType::Min},
        {EYsonItemType::BeginAttributes, EValueType::Min},
        {EYsonItemType::EndAttributes, EValueType::Min},
        {EYsonItemType::BeginList, EValueType::Any},
        {EYsonItemType::EndList, EValueType::Min},
        {EYsonItemType::EntityValue, EValueType::Min},
        {EYsonItemType::BooleanValue, EValueType::Boolean},
        {EYsonItemType::Int64Value, EValueType::Int64},
        {EYsonItemType::Uint64Value, EValueType::Uint64},
        {EYsonItemType::DoubleValue, EValueType::Double},
        {EYsonItemType::StringValue, EValueType::String},
    };
    auto valueType = mapping[itemType];
    if (valueType == EValueType::Min) {
        ThrowIncomparableYsonToken(itemType);
    }
    return valueType;
}

Y_FORCE_INLINE static int CompareYsonItems(const TYsonItem& lhs, const TYsonItem& rhs)
{
    if (lhs.GetType() == rhs.GetType()) {
        switch (lhs.GetType()) {
            case EYsonItemType::EndOfStream:
            case EYsonItemType::BeginList:
            case EYsonItemType::EndList:
            case EYsonItemType::EntityValue:
                return 0;
            case EYsonItemType::Int64Value:
                return ComparePrimitive(lhs.UncheckedAsInt64(), rhs.UncheckedAsInt64());
            case EYsonItemType::Uint64Value:
                return ComparePrimitive(lhs.UncheckedAsUint64(), rhs.UncheckedAsUint64());
            case EYsonItemType::DoubleValue:
                return ComparePrimitive(lhs.UncheckedAsDouble(), rhs.UncheckedAsDouble());
            case EYsonItemType::BooleanValue:
                return ComparePrimitive(lhs.UncheckedAsBoolean(), rhs.UncheckedAsBoolean());
            case EYsonItemType::StringValue:
                return GetSign(TString::compare(lhs.UncheckedAsString(), rhs.UncheckedAsString()));

            case EYsonItemType::BeginMap:
            case EYsonItemType::EndMap:
            case EYsonItemType::BeginAttributes:
            case EYsonItemType::EndAttributes:
                ThrowIncomparableYsonToken(lhs.GetType());
        }
        YT_ABORT();
    }

    const auto lhsClass = GetCompareClass(lhs.GetType());
    const auto rhsClass = GetCompareClass(rhs.GetType());

    if (lhsClass == ECompareClass::Incomparable) {
        ThrowIncomparableYsonToken(lhs.GetType());
    }
    if (rhsClass == ECompareClass::Incomparable) {
        ThrowIncomparableYsonToken(rhs.GetType());
    }

    if (lhsClass == ECompareClass::BeginValue && rhsClass == ECompareClass::BeginValue) {
        return static_cast<int>(MapItemTypeToValueType(lhs.GetType())) - static_cast<int>(MapItemTypeToValueType(rhs.GetType()));
    }
    return ComparePrimitive(static_cast<ui32>(lhsClass), static_cast<ui32>(rhsClass));
}

// Returns the minimum binary size needed to represent a potentially truncated version of the this item.
// We should not rely on the return value for Map or Attribute-related items, there is no special handling for them.
i64 GetMinResultingSize(const TYsonItem& item, bool isInsideList)
{
    // These bytes were already accounted when handling the corresponding BeginList.
    if (item.GetType() == EYsonItemType::EndList) {
        return 0;
    }

    auto resultingSize = item.GetBinarySize();

    // Strings can be truncated, so we only count the remaining bytes for now.
    // In practice that is the single flag byte and the length of the string as a varint.
    if (item.GetType() == EYsonItemType::StringValue) {
        resultingSize -= item.UncheckedAsString().size();
        YT_VERIFY(resultingSize >= 0);
    }

    // Accounting for EndList, since we need to accomodate the list ending within the size limit.
    if (item.GetType() == EYsonItemType::BeginList) {
        resultingSize += TYsonItem::Simple(EYsonItemType::EndList).GetBinarySize();
    }

    // All items inside any enclosing list are currently required to be followed by an item separator, which has the size of one byte.
    if (isInsideList) {
        resultingSize += 1;
    }

    return resultingSize;
}

} // namespace

int CompareDoubleValues(double lhs, double rhs)
{
    if (lhs < rhs) {
        return -1;
    } else if (lhs > rhs) {
        return +1;
    } else if (std::isnan(lhs)) {
        if (std::isnan(rhs)) {
            return 0;
        } else {
            return 1;
        }
    } else if (std::isnan(rhs)) {
        return -1;
    }

    return 0;
}

int CompareYsonValues(TYsonStringBuf lhs, TYsonStringBuf rhs)
{
    YT_ASSERT(lhs.GetType() == EYsonType::Node);
    YT_ASSERT(rhs.GetType() == EYsonType::Node);

    TMemoryInput lhsIn(lhs.AsStringBuf());
    TMemoryInput rhsIn(rhs.AsStringBuf());

    TYsonPullParser lhsParser(&lhsIn, EYsonType::Node);
    TYsonPullParser rhsParser(&rhsIn, EYsonType::Node);

    for (;;) {
        const auto lhsItem = lhsParser.Next();
        const auto rhsItem = rhsParser.Next();

        auto res = CompareYsonItems(lhsItem, rhsItem);
        if (res != 0) {
            return res;
        } else if (lhsItem.GetType() == EYsonItemType::EndOfStream) {
            return 0;
        }
        Y_ASSERT(lhsItem.GetType() != EYsonItemType::EndOfStream &&
            rhsItem.GetType() != EYsonItemType::EndOfStream);
    }
}

TFingerprint CompositeFarmHash(TYsonStringBuf value)
{
    YT_ASSERT(value.GetType() == EYsonType::Node);

    TMemoryInput in(value.AsStringBuf());
    TYsonPullParser parser(&in, EYsonType::Node);

    auto throwUnexpectedYsonToken = [] (const TYsonItem& item) {
        THROW_ERROR_EXCEPTION("Unexpected YSON token %Qlv in composite value", item.GetType());
    };

    TFingerprint result;

    auto item = parser.Next();
    switch (item.GetType()) {
        case EYsonItemType::BeginAttributes:
        case EYsonItemType::EndAttributes:
        case EYsonItemType::BeginMap:
        case EYsonItemType::EndMap:
            throwUnexpectedYsonToken(item);
            [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
        case EYsonItemType::BeginList:
            result = FarmFingerprint('[');
            break;
        case EYsonItemType::EndList:
            result = FarmFingerprint(']');
            break;
        case EYsonItemType::EntityValue:
            result = FarmFingerprint(0);
            break;
        case EYsonItemType::Int64Value:
            result = FarmFingerprint(item.UncheckedAsInt64());
            break;
        case EYsonItemType::Uint64Value:
            result = FarmFingerprint(item.UncheckedAsUint64());
            break;
        case EYsonItemType::DoubleValue: {
            // NB. We cannot compute hash of double
            // So we replicate logic of  FarmFingerprint(const TUnversionedValue& ) here
            // and cast double to ui64
            result = FarmFingerprint(BitCast<ui64>(item.UncheckedAsDouble()));
            break;
        }
        case EYsonItemType::BooleanValue:
            result = FarmFingerprint(item.UncheckedAsBoolean());
            break;
        case EYsonItemType::StringValue: {
            auto string = item.UncheckedAsString();
            result = FarmFingerprint(FarmFingerprint(string.Data(), string.size()));
            break;
        }
        case EYsonItemType::EndOfStream:
            // Invalid yson, parser should have thrown.
            Y_ABORT();
    }

    for (;;) {
        item = parser.Next();
        switch (item.GetType()) {
            case EYsonItemType::BeginAttributes:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::BeginMap:
            case EYsonItemType::EndMap:
                throwUnexpectedYsonToken(item);
                continue;
            case EYsonItemType::BeginList:
                result = FarmFingerprint(result, '[');
                continue;
            case EYsonItemType::EndList:
                result = FarmFingerprint(result, ']');
                continue;
            case EYsonItemType::EntityValue:
                result = FarmFingerprint(result, 0);
                continue;
            case EYsonItemType::Int64Value:
                result = FarmFingerprint(item.UncheckedAsInt64(), result);
                continue;
            case EYsonItemType::Uint64Value:
                result = FarmFingerprint(result, item.UncheckedAsUint64());
                continue;
            case EYsonItemType::DoubleValue:
                // NB. see comment above.
                result = FarmFingerprint(BitCast<ui64>(item.UncheckedAsDouble()));
                continue;
            case EYsonItemType::BooleanValue:
                result = FarmFingerprint(result, item.UncheckedAsBoolean());
                continue;
            case EYsonItemType::StringValue: {
                auto string = item.UncheckedAsString();
                result = FarmFingerprint(result, FarmFingerprint(string.Data(), string.size()));
                continue;
            }
            case EYsonItemType::EndOfStream:
                return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TYsonString> TruncateYsonValue(TYsonStringBuf originalYson, i64 size)
{
    YT_VERIFY(originalYson.GetType() == EYsonType::Node);

    YT_VERIFY(size >= 0);
    if (!size) {
        return {};
    }

    TMemoryInput valueIn(originalYson.AsStringBuf());
    TYsonPullParser valueParser(&valueIn, EYsonType::Node);

    TString truncatedYson;
    TStringOutput output(truncatedYson);
    output.Reserve(std::min(size, std::ssize(originalYson.AsStringBuf())));
    TCheckedInDebugYsonTokenWriter writer(&output);

    i64 unclosedListCount = 0;
    bool emptyResult = true;

    for (auto remainingBytes = size;;) {
        const auto item = valueParser.Next();

        // We don't handle the case of the last EndList, since the function below returns 0 for EndList anyway.
        bool isInsideList = unclosedListCount > 0;
        auto resultingItemSize = GetMinResultingSize(item, isInsideList);

        if (resultingItemSize > remainingBytes) {
            break;
        }

        bool isEof = false;

        switch (item.GetType()) {
            case EYsonItemType::BeginList:
                ++unclosedListCount;
                writer.WriteBeginList();
                break;
            case EYsonItemType::EndList:
                --unclosedListCount;
                writer.WriteEndList();
                break;
            case EYsonItemType::EntityValue:
                writer.WriteEntity();
                break;
            case EYsonItemType::Int64Value:
                writer.WriteBinaryInt64(item.UncheckedAsInt64());
                break;
            case EYsonItemType::Uint64Value:
                writer.WriteBinaryUint64(item.UncheckedAsUint64());
                break;
            case EYsonItemType::DoubleValue:
                writer.WriteBinaryDouble(item.UncheckedAsDouble());
                break;
            case EYsonItemType::BooleanValue:
                writer.WriteBinaryBoolean(item.UncheckedAsBoolean());
                break;
            case EYsonItemType::StringValue: {
                auto truncatedString = item.UncheckedAsString().Trunc(remainingBytes - resultingItemSize);
                writer.WriteBinaryString(truncatedString);
                // This is a slight overestimation, since a smaller string might have a shorter varint part storing its length.
                resultingItemSize += truncatedString.size();
                break;
            }
            // Maps and attributes are not comparable.
            // However, we can store everything up to this point into the truncated string.
            case EYsonItemType::BeginAttributes:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::BeginMap:
            case EYsonItemType::EndMap:
            case EYsonItemType::EndOfStream:
                isEof = true;
                break;
            default:
                YT_ABORT();
        }

        if (isEof) {
            break;
        }

        emptyResult = false;

        if (unclosedListCount && item.GetType() != EYsonItemType::BeginList) {
            writer.WriteItemSeparator();
        }

        remainingBytes -= resultingItemSize;
    }

    YT_VERIFY(unclosedListCount >= 0);
    while (unclosedListCount) {
        writer.WriteEndList();
        if (--unclosedListCount) {
            writer.WriteItemSeparator();
        }
    }

    if (emptyResult) {
        return {};
    } else {
        writer.Finish();
    }

    YT_LOG_ALERT_IF(
        std::ssize(truncatedYson) > size,
        "Composite YSON truncation increased the value's binary size (OriginalValue: %v, TruncatedValue: %v)",
        originalYson.AsStringBuf(),
        truncatedYson);

    return TYsonString(std::move(truncatedYson));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
