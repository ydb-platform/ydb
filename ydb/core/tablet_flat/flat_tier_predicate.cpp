#include "flat_tier_predicate.h"

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/cast.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

#include <type_traits>

namespace NKikimr {
namespace NTable {

namespace {

    using EUnit = NKikimrSchemeOp::TTTLSettings::EUnit;

    constexpr i64 MicrosPerMilli = 1000;
    constexpr i64 MicrosPerSecond = 1000 * MicrosPerMilli;
    constexpr i64 MicrosPerDay = 86400 * MicrosPerSecond;

    enum class EPgKind {
        Unsupported,
        Date,
        Timestamp,
        Int4,
        Int8,
    };

    // Postgres dates and timestamps count from 2000-01-01
    constexpr i64 PgEpochDays = 10957;
    constexpr i64 PgEpochMicros = PgEpochDays * MicrosPerDay;

    // Pg columns hold the Postgres binary (send/recv) format: the supported
    // types are big-endian integers of a fixed width, so they are compared
    // as integers without decoding them through Postgres (which allocates)
    template <typename T>
    T ReadPgInteger(const TCell& cell) {
        Y_ENSURE(cell.Size() == sizeof(T), "Unexpected Pg value size " << cell.Size());
        using TUnsigned = std::make_unsigned_t<T>;
        return static_cast<T>(InetToHost(ReadUnaligned<TUnsigned>(cell.Data())));
    }

    // Stable PostgreSQL type oids (see catalog/pg_type_d.h)
    constexpr ui32 PgInt8Oid = 20;
    constexpr ui32 PgInt4Oid = 23;
    constexpr ui32 PgDateOid = 1082;
    constexpr ui32 PgTimestampOid = 1114;

    EPgKind GetPgKind(const NScheme::TTypeInfo& type) {
        switch (NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc())) {
            case PgDateOid:
                return EPgKind::Date;
            case PgTimestampOid:
                return EPgKind::Timestamp;
            case PgInt4Oid:
                return EPgKind::Int4;
            case PgInt8Oid:
                return EPgKind::Int8;
            default:
                return EPgKind::Unsupported;
        }
    }

    bool IsUnitRequired(const NScheme::TTypeInfo& type) {
        switch (type.GetTypeId()) {
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
            case NScheme::NTypeIds::DyNumber:
                return true;
            case NScheme::NTypeIds::Pg: {
                const auto kind = GetPgKind(type);
                return kind == EPgKind::Int4 || kind == EPgKind::Int8;
            }
            default:
                return false;
        }
    }

    bool IsSupportedType(const NScheme::TTypeInfo& type) {
        switch (type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
            case NScheme::NTypeIds::Datetime:
            case NScheme::NTypeIds::Timestamp:
            case NScheme::NTypeIds::Date32:
            case NScheme::NTypeIds::Datetime64:
            case NScheme::NTypeIds::Timestamp64:
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
            case NScheme::NTypeIds::DyNumber:
                return true;
            case NScheme::NTypeIds::Pg:
                return GetPgKind(type) != EPgKind::Unsupported;
            default:
                return false;
        }
    }

    bool IsKnownUnit(EUnit unit) {
        switch (unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
            case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                return true;
            default:
                return false;
        }
    }

    i64 UnitMicros(EUnit unit) {
        switch (unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                return MicrosPerSecond;
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                return MicrosPerMilli;
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                return 1;
            default:
                Y_ENSURE(false, "Unexpected unit " << static_cast<ui32>(unit));
        }
    }

    i64 FloorDiv(i64 value, i64 divisor) {
        i64 result = value / divisor;
        if (value % divisor != 0 && value < 0) {
            --result;
        }
        return result;
    }

    i64 CeilDiv(i64 value, i64 divisor) {
        return FloorDiv(value, divisor) + (value % divisor != 0 ? 1 : 0);
    }

    // An integer wider than i64/ui64 (sign + magnitude), with infinities
    struct TWide {
        int Inf = 0; // -1 for minus infinity, +1 for plus infinity
        bool Neg = false;
        ui64 Abs = 0;

        static TWide Signed(i64 value) {
            TWide result;
            result.Neg = value < 0;
            result.Abs = value < 0 ? static_cast<ui64>(-(value + 1)) + 1 : static_cast<ui64>(value);
            return result;
        }

        static TWide Unsigned(ui64 value) {
            TWide result;
            result.Abs = value;
            return result;
        }

        static TWide Infinity(int sign) {
            TWide result;
            result.Inf = sign;
            return result;
        }

        i64 AsSigned() const {
            Y_ENSURE(!Inf);
            return Neg ? -static_cast<i64>(Abs - 1) - 1 : static_cast<i64>(Abs);
        }

        friend int CompareWide(const TWide& a, const TWide& b) {
            if (a.Inf != b.Inf) {
                return a.Inf < b.Inf ? -1 : 1;
            }
            if (a.Inf) {
                return 0;
            }
            if (a.Neg != b.Neg) {
                return a.Neg ? -1 : 1;
            }
            if (a.Abs == b.Abs) {
                return 0;
            }
            return (a.Abs < b.Abs) != a.Neg ? -1 : 1;
        }
    };

    // How a column value maps to a moment in time
    enum class EScale {
        // instant(value) = value * unit
        Multiple,
        // instant(value) = floor(value / 1000) microseconds (Uint columns in nanoseconds, like TTL)
        NanosWholeMicros,
        // instant(value) = value nanoseconds (Pg integer columns in nanoseconds, like TTL)
        NanosExact,
    };

    // The smallest (>=) or the largest (<=, <) integer column value satisfying
    // `instant(value) cmp micros`, as a wide integer that may exceed the column range
    TWide IntegerBound(i64 micros, bool lessOrEqual, EScale scale, i64 unitMicros) {
        switch (scale) {
            case EScale::Multiple:
                // value * unit <= micros  <=>  value <= floor(micros / unit)
                // value * unit <  micros  <=>  value <  ceil(micros / unit)
                // value * unit >= micros  <=>  value >= ceil(micros / unit)
                return TWide::Signed(lessOrEqual ? FloorDiv(micros, unitMicros) : CeilDiv(micros, unitMicros));
            case EScale::NanosWholeMicros:
            case EScale::NanosExact: {
                // value / 1000 <= micros  <=>  value <= micros * 1000 + 999
                const ui64 extra = scale == EScale::NanosWholeMicros && lessOrEqual ? 999 : 0;
                if (micros >= 0) {
                    const ui64 us = static_cast<ui64>(micros);
                    if (us > (Max<ui64>() - extra) / 1000) {
                        return TWide::Infinity(+1);
                    }
                    return TWide::Unsigned(us * 1000 + extra);
                }
                if (micros < Min<i64>() / 1000) {
                    return TWide::Infinity(-1);
                }
                return TWide::Signed(micros * 1000 + static_cast<i64>(extra));
            }
        }
        Y_ENSURE(false, "Unreachable");
    }

    // Threshold as a decimal number in the given unit, as row table TTL does for
    // serialized (DyNumber, Pg integer) columns: seconds and milliseconds are
    // rounded down, nanoseconds are exact microseconds * 1000
    TString MicrosToUnitFloorString(i64 micros, EUnit unit) {
        if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
            // Appending zeros avoids overflow of micros * 1000
            return ToString(micros) + "000";
        }
        return ToString(FloorDiv(micros, UnitMicros(unit)));
    }

    // Exact decimal number of units, e.g. 1.5 seconds for 1500000 microseconds
    TString MicrosToUnitExactString(i64 micros, EUnit unit) {
        if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS
            || unit == NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS)
        {
            return MicrosToUnitFloorString(micros, unit);
        }

        const ui64 divisor = static_cast<ui64>(UnitMicros(unit));
        const TWide wide = TWide::Signed(micros);
        TString result = wide.Neg ? "-" : "";
        result += ToString(wide.Abs / divisor);
        if (const ui64 fraction = wide.Abs % divisor) {
            const size_t digits = ToString(divisor).size() - 1;
            TString text = ToString(fraction);
            text = TString(digits - text.size(), '0') + text;
            while (text.back() == '0') {
                text.pop_back();
            }
            result += "." + text;
        }
        return result;
    }

    // DyNumber binary representation is ordered bytewise, except that negative
    // numbers store inverted digits without a terminator: when one negative
    // mantissa is a prefix of another, the shorter one is the greater number
    // (as if it was padded with inverted zero digits)
    int CompareDyNumbers(TStringBuf left, TStringBuf right) {
        const size_t common = Min(left.size(), right.size());
        if (const int result = left.Head(common).compare(right.Head(common))) {
            return result;
        }
        if (left.size() == right.size()) {
            return 0;
        }
        const bool negative = !left.empty() && left[0] == '\x00';
        const bool leftShorter = left.size() < right.size();
        return leftShorter != negative ? -1 : 1;
    }

    bool IsRelative(const TTierBound& bound) {
        return std::holds_alternative<TTierRelativeBound>(bound);
    }

    // Absolute range as [Lo, HiExclusive), Nothing() means infinity
    struct TAbsoluteSpan {
        TMaybe<i64> Lo;
        TMaybe<i64> HiExclusive;

        explicit TAbsoluteSpan(const TTierRange& range) {
            if (range.From) {
                Lo = std::get<TTierAbsoluteBound>(*range.From).Micros;
            }
            if (range.To) {
                const i64 to = std::get<TTierAbsoluteBound>(*range.To).Micros;
                if (!range.ToInclusive) {
                    HiExclusive = to;
                } else if (to != Max<i64>()) {
                    HiExclusive = to + 1;
                }
            }
        }

        bool Overlaps(const TAbsoluteSpan& other) const {
            auto below = [](const TMaybe<i64>& lo, const TMaybe<i64>& hiExclusive) {
                return !lo || !hiExclusive || *lo < *hiExclusive;
            };
            return below(Lo, other.HiExclusive) && below(other.Lo, HiExclusive);
        }
    };

} // namespace

    void TTierPredicate::Validate(const TTieringPolicy& policy) {
        std::optional<bool> relative;
        for (const auto& rule : policy.Rules) {
            Y_ENSURE(rule.TierId != Max<ui32>(), "Invalid tier id");
            Y_ENSURE(!rule.Ranges.empty(), "Tiering rule for tier " << rule.TierId << " has no ranges");

            for (const auto& range : rule.Ranges) {
                Y_ENSURE(range.From || range.To, "Tiering rule for tier " << rule.TierId << " has an unbounded range");
                for (const auto* bound : {&range.From, &range.To}) {
                    if (*bound) {
                        Y_ENSURE(!relative || *relative == IsRelative(**bound),
                            "Relative and absolute tiering bounds cannot be mixed in one policy");
                        relative = IsRelative(**bound);
                    }
                }
            }
        }

        if (relative.value_or(true)) {
            // Relative rules come only from Interval(...): a single inclusive upper bound,
            // ordered from the coldest tier to the warmest, so offsets must strictly decrease
            TMaybe<TDuration> prevOffset;
            for (const auto& rule : policy.Rules) {
                const bool interval = rule.Ranges.size() == 1
                    && !rule.Ranges[0].From && rule.Ranges[0].To && rule.Ranges[0].ToInclusive;
                Y_ENSURE(interval, "Relative tiering rule for tier " << rule.TierId
                    << " must be a single inclusive upper bound");
                const auto& bound = std::get<TTierRelativeBound>(*rule.Ranges[0].To);
                Y_ENSURE(!prevOffset || bound.Offset < *prevOffset,
                    "Tiering rules must be ordered from the coldest tier to the warmest: offset "
                    << bound.Offset << " for tier " << rule.TierId << " follows offset " << *prevOffset);
                prevOffset = bound.Offset;
            }
        } else {
            for (const auto& rule : policy.Rules) {
                for (const auto& range : rule.Ranges) {
                    if (range.From && range.To) {
                        const i64 from = std::get<TTierAbsoluteBound>(*range.From).Micros;
                        const i64 to = std::get<TTierAbsoluteBound>(*range.To).Micros;
                        Y_ENSURE(from < to || (range.ToInclusive && from == to),
                            "Empty tiering range for tier " << rule.TierId);
                    }
                }
            }

            // Ranges of different rules must not overlap, so the rule order does not matter
            for (size_t i = 0; i < policy.Rules.size(); ++i) {
                for (size_t j = i + 1; j < policy.Rules.size(); ++j) {
                    for (const auto& left : policy.Rules[i].Ranges) {
                        for (const auto& right : policy.Rules[j].Ranges) {
                            Y_ENSURE(!TAbsoluteSpan(left).Overlaps(TAbsoluteSpan(right)),
                                "Tiering ranges of tiers " << policy.Rules[i].TierId
                                << " and " << policy.Rules[j].TierId << " overlap");
                        }
                    }
                }
            }
        }
    }

    TTierPredicate::TTierPredicate(const TTieringPolicy& policy, TInstant now, const TRowScheme& scheme)
        : Policy(policy)
    {
        Validate(Policy);

        // Column position may change between schemes (e.g. after a column is dropped),
        // so a predicate is valid only for the scheme it was created with
        const auto* info = scheme.ColInfo(Policy.ColumnTag);
        Y_ENSURE(info, "Tiering column " << Policy.ColumnTag << " is not found");
        Y_ENSURE(IsSupportedType(info->TypeInfo), "Unsupported tiering column type " << info->TypeInfo.GetTypeId());
        Y_ENSURE(!IsUnitRequired(info->TypeInfo) || IsKnownUnit(Policy.Unit),
            "Tiering column of type " << info->TypeInfo.GetTypeId() << " requires a unit");

        Type = info->TypeInfo;
        Pos = info->Pos;
        PgTypeId = Type.GetTypeId() == NScheme::NTypeIds::Pg ? NPg::PgTypeIdFromTypeDesc(Type.GetPgTypeDesc()) : 0;

        const i64 nowUs = static_cast<i64>(Min(now.MicroSeconds(), static_cast<ui64>(Max<i64>())));

        Compiled.reserve(Policy.Rules.size());

        for (const auto& rule : Policy.Rules) {
            auto& ranges = Compiled.emplace_back();
            ranges.reserve(rule.Ranges.size());
            for (const auto& range : rule.Ranges) {
                auto& compiled = ranges.emplace_back();
                if (range.From) {
                    compiled.From = MakeThreshold(ToMicros(*range.From, nowUs), ECmp::GreaterOrEqual);
                }
                if (range.To) {
                    compiled.To = MakeThreshold(ToMicros(*range.To, nowUs),
                        range.ToInclusive ? ECmp::LessOrEqual : ECmp::Less);
                }
            }
        }
    }

    TVector<ui32> TTierPredicate::GetUnrepresentableTierIds() const {
        auto unrepresentable = [](const std::optional<TThreshold>& threshold) {
            return threshold && threshold->Kind == TThreshold::EKind::Unrepresentable;
        };

        TVector<ui32> result;
        for (size_t i = 0; i < Policy.Rules.size(); ++i) {
            for (const auto& range : Compiled[i]) {
                if (unrepresentable(range.From) || unrepresentable(range.To)) {
                    result.push_back(Policy.Rules[i].TierId);
                    break;
                }
            }
        }
        return result;
    }

    i64 TTierPredicate::ToMicros(const TTierBound& bound, i64 nowUs) const {
        if (const auto* relative = std::get_if<TTierRelativeBound>(&bound)) {
            const i64 offsetUs = static_cast<i64>(Min(relative->Offset.MicroSeconds(), static_cast<ui64>(Max<i64>())));
            // Both operands are non-negative, so there is no overflow
            return nowUs - offsetUs;
        }
        return std::get<TTierAbsoluteBound>(bound).Micros;
    }

    TTierPredicate::TThreshold TTierPredicate::MakeThreshold(i64 micros, ECmp cmp) const {
        using EKind = TThreshold::EKind;

        TThreshold result;
        result.Cmp = cmp;
        const bool lessOrEqual = cmp == ECmp::LessOrEqual;

        // Integer columns: clamp the bound to the column range
        auto setInteger = [&](TWide bound, TWide min, TWide max, bool isSigned) {
            const int toMin = CompareWide(bound, min);
            const int toMax = CompareWide(bound, max);
            switch (cmp) {
                case ECmp::LessOrEqual:
                    result.Kind = toMax >= 0 ? EKind::AlwaysMatch : toMin < 0 ? EKind::NeverMatch : EKind::Value;
                    break;
                case ECmp::Less:
                    result.Kind = toMax > 0 ? EKind::AlwaysMatch : toMin <= 0 ? EKind::NeverMatch : EKind::Value;
                    break;
                case ECmp::GreaterOrEqual:
                    result.Kind = toMin <= 0 ? EKind::AlwaysMatch : toMax > 0 ? EKind::NeverMatch : EKind::Value;
                    break;
            }
            if (result.Kind == EKind::Value) {
                if (isSigned) {
                    result.Signed = bound.AsSigned();
                } else {
                    result.Unsigned = bound.Abs;
                }
            }
        };

        auto setUnsigned = [&](TWide bound, ui64 max) {
            setInteger(bound, TWide::Unsigned(0), TWide::Unsigned(max), false);
        };

        auto setSigned = [&](TWide bound, i64 min, i64 max) {
            setInteger(bound, TWide::Signed(min), TWide::Signed(max), true);
        };

        auto setSerialized = [&](TMaybe<TString> serialized) {
            Y_DEBUG_ABORT_UNLESS(serialized, "Cannot convert tiering threshold for column type %d",
                static_cast<int>(Type.GetTypeId()));
            if (serialized) {
                result.Kind = EKind::Value;
                result.Serialized = std::move(*serialized);
            } else {
                result.Kind = EKind::Unrepresentable;
            }
        };

        auto unitBound = [&](EScale nanosScale) {
            if (Policy.Unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
                return IntegerBound(micros, lessOrEqual, nanosScale, 1);
            }
            return IntegerBound(micros, lessOrEqual, EScale::Multiple, UnitMicros(Policy.Unit));
        };

        switch (Type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
                setUnsigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, MicrosPerDay), Max<ui16>());
                break;
            case NScheme::NTypeIds::Datetime:
                setUnsigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, MicrosPerSecond), Max<ui32>());
                break;
            case NScheme::NTypeIds::Timestamp:
                setUnsigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, 1), Max<ui64>());
                break;
            case NScheme::NTypeIds::Date32:
                setSigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, MicrosPerDay), Min<i32>(), Max<i32>());
                break;
            case NScheme::NTypeIds::Datetime64:
                setSigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, MicrosPerSecond), Min<i64>(), Max<i64>());
                break;
            case NScheme::NTypeIds::Timestamp64:
                setSigned(IntegerBound(micros, lessOrEqual, EScale::Multiple, 1), Min<i64>(), Max<i64>());
                break;
            case NScheme::NTypeIds::Uint32:
                setUnsigned(unitBound(EScale::NanosWholeMicros), Max<ui32>());
                break;
            case NScheme::NTypeIds::Uint64:
                setUnsigned(unitBound(EScale::NanosWholeMicros), Max<ui64>());
                break;
            case NScheme::NTypeIds::DyNumber:
                // DyNumber has arbitrary precision, so any bound is representable. An inclusive
                // upper bound keeps TTL rounding, other bounds are exact (values may be fractional).
                setSerialized(NDyNumber::ParseDyNumberString(lessOrEqual
                    ? MicrosToUnitFloorString(micros, Policy.Unit)
                    : MicrosToUnitExactString(micros, Policy.Unit)));
                break;
            case NScheme::NTypeIds::Pg:
                switch (GetPgKind(Type)) {
                    case EPgKind::Date: {
                        // A date denotes the start of its day, Postgres counts days from 2000-01-01
                        const i64 days = lessOrEqual ? FloorDiv(micros, MicrosPerDay) : CeilDiv(micros, MicrosPerDay);
                        setSigned(TWide::Signed(days - PgEpochDays), Min<i32>(), Max<i32>());
                        break;
                    }
                    case EPgKind::Timestamp:
                        // Postgres counts microseconds from 2000-01-01
                        if (micros < Min<i64>() + PgEpochMicros) {
                            setSigned(TWide::Infinity(-1), Min<i64>(), Max<i64>());
                        } else {
                            setSigned(TWide::Signed(micros - PgEpochMicros), Min<i64>(), Max<i64>());
                        }
                        break;
                    case EPgKind::Int4:
                    case EPgKind::Int8: {
                        const bool int4 = GetPgKind(Type) == EPgKind::Int4;
                        // A bound outside of the integer range is not an error:
                        // all values are either below or above it
                        setSigned(unitBound(EScale::NanosExact),
                            int4 ? Min<i32>() : Min<i64>(), int4 ? Max<i32>() : Max<i64>());
                        break;
                    }
                    case EPgKind::Unsupported:
                        Y_ENSURE(false, "Unreachable");
                }
                break;
            default:
                Y_ENSURE(false, "Unreachable");
        }

        return result;
    }

    int TTierPredicate::Compare(const TCell& cell, const TThreshold& threshold) const {
        auto compare = [](auto value, auto bound) {
            return value < bound ? -1 : value > bound ? 1 : 0;
        };

        switch (Type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
                return compare(ui64(cell.AsValue<ui16>()), threshold.Unsigned);
            case NScheme::NTypeIds::Datetime:
            case NScheme::NTypeIds::Uint32:
                return compare(ui64(cell.AsValue<ui32>()), threshold.Unsigned);
            case NScheme::NTypeIds::Timestamp:
            case NScheme::NTypeIds::Uint64:
                return compare(cell.AsValue<ui64>(), threshold.Unsigned);
            case NScheme::NTypeIds::Date32:
                return compare(i64(cell.AsValue<i32>()), threshold.Signed);
            case NScheme::NTypeIds::Datetime64:
            case NScheme::NTypeIds::Timestamp64:
                return compare(cell.AsValue<i64>(), threshold.Signed);
            case NScheme::NTypeIds::DyNumber:
                return CompareDyNumbers(cell.AsBuf(), threshold.Serialized);
            case NScheme::NTypeIds::Pg:
                switch (PgTypeId) {
                    case PgDateOid:
                    case PgInt4Oid:
                        return compare(i64(ReadPgInteger<i32>(cell)), threshold.Signed);
                    case PgTimestampOid:
                    case PgInt8Oid:
                        return compare(ReadPgInteger<i64>(cell), threshold.Signed);
                    default:
                        Y_ENSURE(false, "Unreachable");
                }
            default:
                Y_ENSURE(false, "Unreachable");
        }
    }

    bool TTierPredicate::Matches(const TCell& cell, const TThreshold& threshold) const {
        switch (threshold.Kind) {
            case TThreshold::EKind::Value:
                break;
            case TThreshold::EKind::AlwaysMatch:
                return true;
            case TThreshold::EKind::NeverMatch:
            case TThreshold::EKind::Unrepresentable:
                return false;
        }

        const int result = Compare(cell, threshold);
        switch (threshold.Cmp) {
            case ECmp::Less:
                return result < 0;
            case ECmp::LessOrEqual:
                return result <= 0;
            case ECmp::GreaterOrEqual:
                return result >= 0;
        }
        Y_ENSURE(false, "Unreachable");
    }

    TTierDecision TTierPredicate::Evaluate(const TRowState& row) const {
        Y_ENSURE(Pos < row.Size());

        if (row != ERowOp::Upsert && row != ERowOp::Reset) {
            // Erase markers and absent rows carry no values
            return TTierDecision::Undecided();
        }

        const TCellOp op = row.GetCellOp(Pos);
        if (ELargeObj(op) != ELargeObj::Inline || ECellOp(op) != ECellOp::Set) {
            // Value is unknown (Empty), null, reset to default or not materialized
            return TTierDecision::Undecided();
        }

        const TCell& cell = row.Get(Pos);
        if (cell.IsNull()) {
            return TTierDecision::Undecided();
        }

        for (size_t i = 0; i < Policy.Rules.size(); ++i) {
            for (const auto& range : Compiled[i]) {
                if ((!range.From || Matches(cell, *range.From)) && (!range.To || Matches(cell, *range.To))) {
                    return TTierDecision::Tier(Policy.Rules[i].TierId);
                }
            }
        }

        return TTierDecision::Tier(DefaultTierId);
    }

}
}
