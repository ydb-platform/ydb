#include "unversioned_value.h"

#ifndef YT_COMPILING_UDF

#include "unversioned_row.h"
#include "composite_compare.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/compare.h>

#include <contrib/libs/xxhash/xxhash.h>

#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TUnversionedValue::AsStringBuf() const
{
    return TStringBuf(Data.String, Length);
}

TString TUnversionedValue::AsString() const
{
    return TString(Data.String, Length);
}

TFingerprint GetFarmFingerprint(const TUnversionedValue& value)
{
    auto type = value.Type;
    switch (type) {
        case EValueType::String:
            return NYT::FarmFingerprint(value.Data.String, value.Length);

        case EValueType::Int64:
            // NB: We use BitCast here instead of std::bit_cast for supporting build with C++17.
            return NYT::FarmFingerprint(BitCast<ui64>(value.Data.Int64));

        case EValueType::Uint64:
            return NYT::FarmFingerprint(value.Data.Uint64);

        case EValueType::Double:
            // NB: We use BitCast here instead of std::bit_cast for supporting build with C++17.
            return NYT::FarmFingerprint(BitCast<ui64>(value.Data.Double));

        case EValueType::Boolean:
            return NYT::FarmFingerprint(static_cast<ui64>(value.Data.Boolean));

        case EValueType::Null:
            return NYT::FarmFingerprint(0);

        case EValueType::Composite:
        case EValueType::Any:
            return CompositeFarmHash(NYson::TYsonStringBuf(value.AsStringBuf()));

        default:
#ifdef YT_COMPILING_UDF
            YT_ABORT();
#else
            THROW_ERROR_EXCEPTION(
                EErrorCode::UnhashableType,
                "Cannot hash values of type %Qlv; only scalar types are allowed for key columns",
                type)
                << TErrorAttribute("value", value);
#endif
    }
}

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TUnversionedValue& value, ::std::ostream* os)
{
    *os << ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendWithCut(TStringBuilderBase* builder, TStringBuf string)
{
    constexpr auto Cutoff = 128;
    if (string.size() <= 2 * Cutoff + 3) {
        builder->AppendString(string);
    } else {
        builder->AppendString(string.substr(0, Cutoff));
        builder->AppendString("...");
        builder->AppendString(string.substr(string.size() - Cutoff, Cutoff));
    }
}

} // namespace

void FormatValue(TStringBuilderBase* builder, const TUnversionedValue& value, TStringBuf format)
{
    using NTableClient::EValueFlags;
    using NTableClient::EValueType;

    bool noFlags = false;
    for (char c : format) {
        noFlags |= c == 'k';
    }

    if (!noFlags) {
        if (Any(value.Flags & EValueFlags::Aggregate)) {
            builder->AppendChar('%');
        }
        if (Any(value.Flags & EValueFlags::Hunk)) {
            builder->AppendChar('&');
        }
        builder->AppendFormat("%v#", value.Id);
    }
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            builder->AppendFormat("<%v>", value.Type);
            break;

        case EValueType::Int64:
            builder->AppendFormat("%v", value.Data.Int64);
            break;

        case EValueType::Uint64:
            builder->AppendFormat("%vu", value.Data.Uint64);
            break;

        case EValueType::Double:
            builder->AppendFormat("%v", value.Data.Double);
            break;

        case EValueType::Boolean:
            builder->AppendFormat("%v", value.Data.Boolean);
            break;

        case EValueType::String: {
            builder->AppendChar('"');
            AppendWithCut(builder, value.AsStringBuf());
            builder->AppendChar('"');
            break;
        }

        case EValueType::Any:
        case EValueType::Composite: {
            if (value.Type == EValueType::Composite) {
                // ermolovd@ says "composites" are comparable, in contrast to "any".
                builder->AppendString("><");
            }

            auto compositeString = ConvertToYsonString(
                NYson::TYsonString(value.AsString()),
                NYson::EYsonFormat::Text);

            AppendWithCut(builder, compositeString.AsStringBuf());
            break;
        }
    }
}

TString ToString(const TUnversionedValue& value, bool valueOnly)
{
    return ToStringViaBuilder(value, valueOnly ? "k" : "");
}

////////////////////////////////////////////////////////////////////////////////

size_t TDefaultUnversionedValueHash::operator()(const TUnversionedValue& value) const
{
    return GetFarmFingerprint(value);
}

bool TDefaultUnversionedValueEqual::operator()(const TUnversionedValue& lhs, const TUnversionedValue& rhs) const
{
    return lhs == rhs;
}

size_t TBitwiseUnversionedValueHash::operator()(const TUnversionedValue& value) const
{
    static_assert(sizeof(value.Id) + sizeof(value.Type) + sizeof(value.Flags) <= sizeof(size_t));

    size_t result = value.Id |
        (static_cast<size_t>(value.Type) << (sizeof(value.Id) * 8)) |
        (static_cast<size_t>(value.Flags) << ((sizeof(value.Id) + sizeof(value.Type)) * 8));

    switch (value.Type) {
        case EValueType::Int64:
            result ^= SplitMix64(value.Data.Int64);
            break;
        case EValueType::Uint64:
            result ^= SplitMix64(value.Data.Uint64);
            break;
        case EValueType::Double:
            // In a bitwise hash, no double normalization is performed, WYSIWYG.
            result ^= SplitMix64(BitCast<ui64>(value.Data.Double));
            break;
        case EValueType::Boolean:
            result ^= SplitMix64(static_cast<ui64>(value.Data.Boolean));
            break;
        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            // XXH3 is properly mixed out-of-the-box.
            result ^= XXH3_64bits(value.Data.String, value.Length);
            break;
        default:
            break;
    }
    return result;
}

bool TBitwiseUnversionedValueEqual::operator()(const TUnversionedValue& lhs, const TUnversionedValue& rhs) const
{
    if (lhs.Id != rhs.Id) {
        return false;
    }
    if (lhs.Flags != rhs.Flags) {
        return false;
    }
    if (lhs.Type != rhs.Type) {
        return false;
    }
    switch (lhs.Type) {
        case EValueType::Int64:
            return lhs.Data.Int64 == rhs.Data.Int64;
        case EValueType::Uint64:
            return lhs.Data.Uint64 == rhs.Data.Uint64;
        case EValueType::Double:
            return NaNSafeTernaryCompare(lhs.Data.Double, rhs.Data.Double) == 0;
        case EValueType::Boolean:
            return lhs.Data.Boolean == rhs.Data.Boolean;
        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            if (lhs.Length != rhs.Length) {
                return false;
            }
            return ::memcmp(lhs.Data.String, rhs.Data.String, lhs.Length) == 0;
        default:
            return true;
    }
}

void TBitwiseUnversionedValueEqual::FormatDiff(
    TStringBuilderBase* builder,
    const TUnversionedValue& lhs,
    const TUnversionedValue& rhs)
{
    if (lhs.Id != rhs.Id) {
        builder->AppendFormat("Value id mismatch: %v vs %v\n",
            lhs.Id,
            rhs.Id);
        return;
    }
    if (lhs.Flags != rhs.Flags) {
        builder->AppendFormat("Value flags mismatch: %v vs %v\n",
            lhs.Flags,
            rhs.Flags);
        return;
    }
    if (lhs.Type != rhs.Type) {
        builder->AppendFormat("Value type mismatch: %v vs %v\n",
            lhs.Type,
            rhs.Type);
        return;
    }
    switch (lhs.Type) {
        case EValueType::Int64:
            if (lhs.Data.Int64 != rhs.Data.Int64) {
                builder->AppendFormat("\"int64\" value mismatch: %v vs %v\n",
                    lhs.Data.Int64,
                    rhs.Data.Int64);
            }
            break;
        case EValueType::Uint64:
            if (lhs.Data.Uint64 != rhs.Data.Uint64) {
                builder->AppendFormat("\"uint64\" value mismatch: %v vs %v\n",
                    lhs.Data.Uint64,
                    rhs.Data.Uint64);
            }
            break;
        case EValueType::Double:
            if (lhs.Data.Double != rhs.Data.Double) {
                builder->AppendFormat("\"double\" value mismatch: %v vs %v\n",
                    lhs.Data.Double,
                    rhs.Data.Double);
            }
            break;
        case EValueType::Boolean:
            if (lhs.Data.Boolean != rhs.Data.Boolean) {
                builder->AppendFormat("\"boolean\" value mismatch: %v vs %v\n",
                    lhs.Data.Boolean,
                    rhs.Data.Boolean);
            }
            break;
        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            if (lhs.Length != rhs.Length) {
                builder->AppendFormat("%Qlv value length mismatch: %v vs %v\n",
                    lhs.Type,
                    lhs.Length,
                    rhs.Length);
                break;
            }
            if (::memcmp(lhs.Data.String, rhs.Data.String, lhs.Length) != 0) {
                builder->AppendFormat("%Qlv value mismatch: %v vs %v\n",
                    lhs.Type,
                    DumpRangeToHex(TRef::FromStringBuf(lhs.AsStringBuf())),
                    DumpRangeToHex(TRef::FromStringBuf(rhs.AsStringBuf())));
            }
            break;
        default:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
