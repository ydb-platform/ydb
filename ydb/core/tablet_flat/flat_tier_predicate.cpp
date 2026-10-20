#include "flat_tier_predicate.h"

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/cast.h>
#include <util/system/byteorder.h>

#include <algorithm>
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

    i64 FloorDiv(i64 value, i64 divisor) {
        i64 result = value / divisor;
        if (value % divisor != 0 && value < 0) {
            --result;
        }
        return result;
    }

    // Threshold (in microseconds since epoch) converted to a value in the given unit,
    // rounded down, so that `value <= result` iff `instant(value) <= threshold`
    i64 MicrosToUnitFloor(i64 thresholdUs, EUnit unit) {
        switch (unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                return FloorDiv(thresholdUs, MicrosPerSecond);
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                return FloorDiv(thresholdUs, MicrosPerMilli);
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                return thresholdUs;
            default:
                Y_ENSURE(false, "Unexpected unit " << static_cast<ui32>(unit));
        }
    }

    // Threshold as a decimal number in the given unit, as row table TTL does for
    // serialized (DyNumber, Pg integer) columns: seconds and milliseconds are
    // rounded down, nanoseconds are exact microseconds * 1000
    TString MicrosToUnitString(i64 thresholdUs, EUnit unit) {
        if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
            // Appending zeros avoids overflow of thresholdUs * 1000
            return thresholdUs == 0 ? TString("0") : ToString(thresholdUs) + "000";
        }
        return ToString(MicrosToUnitFloor(thresholdUs, unit));
    }

} // namespace

    TTierPredicate::TTierPredicate(const TTieringPolicy& policy)
        : Policy(policy)
    {
        // Rules go from the coldest tier to the warmest, so relative offsets must strictly decrease
        TMaybe<TDuration> prevOffset;
        for (const auto& rule : Policy.Rules) {
            Y_ENSURE(rule.TierId != Max<ui32>(), "Invalid tier id");

            const auto& bound = std::get<TTierRelativeBound>(rule.Before);
            Y_ENSURE(!prevOffset || bound.Offset < *prevOffset,
                "Tiering rules must be ordered from the coldest tier to the warmest: offset "
                << bound.Offset << " for tier " << rule.TierId << " follows offset " << *prevOffset);
            prevOffset = bound.Offset;
        }
    }

    void TTierPredicate::Bind(TInstant now, const TRowScheme& scheme) {
        Bound = false;

        // Column position may change between schemes (e.g. after a column is dropped),
        // so it is resolved on every bind
        const auto* info = scheme.ColInfo(Policy.ColumnTag);
        Y_ENSURE(info, "Tiering column " << Policy.ColumnTag << " is not found");
        Y_ENSURE(IsSupportedType(info->TypeInfo), "Unsupported tiering column type " << info->TypeInfo.GetTypeId());
        Y_ENSURE(!IsUnitRequired(info->TypeInfo) || IsKnownUnit(Policy.Unit),
            "Tiering column of type " << info->TypeInfo.GetTypeId() << " requires a unit");

        Type = info->TypeInfo;
        Pos = info->Pos;
        TypeOrder = NScheme::TTypeInfoOrder(Type);

        const i64 nowUs = static_cast<i64>(Min(now.MicroSeconds(), static_cast<ui64>(Max<i64>())));

        Thresholds.clear();
        Thresholds.reserve(Policy.Rules.size());

        for (const auto& rule : Policy.Rules) {
            const auto& bound = std::get<TTierRelativeBound>(rule.Before);
            const i64 offsetUs = static_cast<i64>(Min(bound.Offset.MicroSeconds(), static_cast<ui64>(Max<i64>())));
            // Both operands are non-negative, so there is no overflow
            Thresholds.push_back(MakeThreshold(nowUs - offsetUs));
        }

        Bound = true;
    }

    TVector<ui32> TTierPredicate::GetUnrepresentableTierIds() const {
        Y_ENSURE(Bound);

        TVector<ui32> result;
        for (size_t i = 0; i < Policy.Rules.size(); ++i) {
            if (Thresholds[i].Kind == TThreshold::EKind::Unrepresentable) {
                result.push_back(Policy.Rules[i].TierId);
            }
        }
        return result;
    }

    TTierPredicate::TThreshold TTierPredicate::MakeThreshold(i64 thresholdUs) const {
        using EKind = TThreshold::EKind;

        TThreshold result;

        // The threshold is kept in the native cell representation of the column type
        auto setRaw = [&](auto value) {
            result.Kind = EKind::Value;
            result.Raw = TString(reinterpret_cast<const char*>(&value), sizeof(value));
        };

        // Clamps to the column range: values above the range match everything,
        // values below the range match nothing
        auto setUnsigned = [&]<typename T>(T, i64 value) {
            if (value < 0) {
                result.Kind = EKind::NeverMatch;
            } else if (static_cast<ui64>(value) > Max<T>()) {
                result.Kind = EKind::AlwaysMatch;
            } else {
                setRaw(static_cast<T>(value));
            }
        };

        auto setSigned = [&]<typename T>(T, i64 value) {
            if (value > Max<T>()) {
                result.Kind = EKind::AlwaysMatch;
            } else if (value < Min<T>()) {
                result.Kind = EKind::NeverMatch;
            } else {
                setRaw(static_cast<T>(value));
            }
        };

        // Postgres binary (send/recv) representation of fixed-width types is big-endian
        auto setPg = [&]<typename T>(T, i64 value) {
            result.Kind = EKind::Value;
            const auto raw = HostToInet(static_cast<std::make_unsigned_t<T>>(static_cast<T>(value)));
            result.Raw = TString(reinterpret_cast<const char*>(&raw), sizeof(raw));
        };

        auto setSerialized = [&](TMaybe<TString> serialized) {
            Y_DEBUG_ABORT_UNLESS(serialized, "Cannot convert tiering threshold for column type %d",
                static_cast<int>(Type.GetTypeId()));
            if (serialized) {
                result.Kind = EKind::Value;
                result.Raw = std::move(*serialized);
            }
        };

        switch (Type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
                setUnsigned(ui16(), FloorDiv(thresholdUs, MicrosPerDay));
                break;
            case NScheme::NTypeIds::Datetime:
                setUnsigned(ui32(), FloorDiv(thresholdUs, MicrosPerSecond));
                break;
            case NScheme::NTypeIds::Timestamp:
                setUnsigned(ui64(), thresholdUs);
                break;
            case NScheme::NTypeIds::Date32:
                setSigned(i32(), FloorDiv(thresholdUs, MicrosPerDay));
                break;
            case NScheme::NTypeIds::Datetime64:
                setSigned(i64(), FloorDiv(thresholdUs, MicrosPerSecond));
                break;
            case NScheme::NTypeIds::Timestamp64:
                setSigned(i64(), thresholdUs);
                break;
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64: {
                const ui64 typeMax = Type.GetTypeId() == NScheme::NTypeIds::Uint32 ? Max<ui32>() : Max<ui64>();
                ui64 value;
                if (Policy.Unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
                    // TTL compares whole microseconds: value / 1000 <= threshold
                    if (thresholdUs < 0) {
                        result.Kind = EKind::NeverMatch;
                        break;
                    }
                    const ui64 us = static_cast<ui64>(thresholdUs);
                    value = us > (Max<ui64>() - 999) / 1000 ? Max<ui64>() : us * 1000 + 999;
                } else {
                    const i64 units = MicrosToUnitFloor(thresholdUs, Policy.Unit);
                    if (units < 0) {
                        result.Kind = EKind::NeverMatch;
                        break;
                    }
                    value = static_cast<ui64>(units);
                }
                if (value >= typeMax) {
                    result.Kind = EKind::AlwaysMatch;
                } else if (Type.GetTypeId() == NScheme::NTypeIds::Uint32) {
                    setRaw(static_cast<ui32>(value));
                } else {
                    setRaw(value);
                }
                break;
            }
            case NScheme::NTypeIds::DyNumber:
                // DyNumber has arbitrary precision, so any threshold is representable
                setSerialized(NDyNumber::ParseDyNumberString(MicrosToUnitString(thresholdUs, Policy.Unit)));
                break;
            case NScheme::NTypeIds::Pg:
                switch (GetPgKind(Type)) {
                    // Postgres stores -infinity and infinity as the minimum and the maximum
                    // of the type, so a threshold below the type range saturates to the
                    // minimum (only -infinity matches) instead of matching nothing
                    case EPgKind::Date: {
                        // A date denotes the start of its day
                        const i64 days = FloorDiv(thresholdUs, MicrosPerDay) - PgEpochDays;
                        if (days > Max<i32>()) {
                            result.Kind = EKind::AlwaysMatch;
                        } else {
                            setPg(i32(), std::max<i64>(days, Min<i32>()));
                        }
                        break;
                    }
                    case EPgKind::Timestamp:
                        if (thresholdUs < Min<i64>() + PgEpochMicros) {
                            setPg(i64(), Min<i64>());
                        } else {
                            setPg(i64(), thresholdUs - PgEpochMicros);
                        }
                        break;
                    case EPgKind::Int4:
                    case EPgKind::Int8: {
                        // A threshold outside of the integer range is not an error:
                        // all values are either below or above it
                        const i64 typeMax = GetPgKind(Type) == EPgKind::Int4 ? Max<i32>() : Max<i64>();
                        const i64 typeMin = GetPgKind(Type) == EPgKind::Int4 ? Min<i32>() : Min<i64>();
                        i64 value;
                        if (Policy.Unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
                            if (thresholdUs > typeMax / 1000) {
                                result.Kind = EKind::AlwaysMatch;
                                break;
                            }
                            if (thresholdUs < typeMin / 1000) {
                                result.Kind = EKind::NeverMatch;
                                break;
                            }
                            value = thresholdUs * 1000;
                        } else {
                            value = MicrosToUnitFloor(thresholdUs, Policy.Unit);
                        }
                        if (value > typeMax) {
                            result.Kind = EKind::AlwaysMatch;
                        } else if (value < typeMin) {
                            result.Kind = EKind::NeverMatch;
                        } else if (GetPgKind(Type) == EPgKind::Int4) {
                            setPg(i32(), value);
                        } else {
                            setPg(i64(), value);
                        }
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

        // TODO(https://github.com/ydb-platform/ydb/issues/56230): Pg cells are compared via
        // PgNativeBinaryCompare, which allocates on every call.
        // TODO(https://github.com/ydb-platform/ydb/issues/56231): DyNumber cells are compared
        // bytewise, which mis-orders negative numbers with a common mantissa prefix.
        const TCell bound(threshold.Raw.data(), threshold.Raw.size());
        return CompareTypedCells(cell, bound, TypeOrder) <= 0;
    }

    TTierDecision TTierPredicate::Evaluate(const TRowState& row) const {
        Y_ENSURE(Bound, "Tier predicate must be bound before evaluation");
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
            if (Matches(cell, Thresholds[i])) {
                return TTierDecision::Tier(Policy.Rules[i].TierId);
            }
        }

        return TTierDecision::Tier(DefaultTierId);
    }

}
}
