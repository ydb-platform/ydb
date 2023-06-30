#include "composite_compare.h"

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/stream/mem.h>

namespace NYT::NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

// This file implements comparison for composite values.
// Composite types that supports comparison are:
//   1. Optional
//   2. List
//   3. Tuple
//   4. Variant
//
// When we compare composite values we assume that they are well-formed yson representations of same type supporting comparison.
// And we compare them in following manner:
//   1. We scan two values simultaneously and look at their yson tokens and find first mismatching token.
//   2. If one of the token is EndList (this only can happen if we parsing values of list type
//      and one list is shorter than another) that means that value containing EndList is less that other.
//   3. Otherwise if one of the values is Entity (other value have to be non null value) that means
//      that value containing Entity is less than other.
//   4. Otherwise it's 2 values of the same type and we can easily compare them.
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
        THROW_ERROR_EXCEPTION("Incomparable scalar types %Qlv and %Qlv in YSON representation",
            lhs.GetType(),
            rhs.GetType());
    }
    return ComparePrimitive(static_cast<ui32>(lhsClass), static_cast<ui32>(rhsClass));
}

} // namespace

int CompareCompositeValues(TYsonStringBuf lhs, TYsonStringBuf rhs)
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
            Y_FAIL();
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

} // namespace NYT::NTableClient
