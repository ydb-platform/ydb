#include "flat_tier_predicate.h"

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/cast.h>

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

    // Value used by row table TTL for serialized (DyNumber, Pg integer) columns
    ui64 InstantValue(TInstant instant, EUnit unit) {
        switch (unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                return instant.Seconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                return instant.MilliSeconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                return instant.MicroSeconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                return instant.NanoSeconds();
            default:
                Y_ENSURE(false, "Unexpected unit " << static_cast<ui32>(unit));
        }
    }

} // namespace

    TTierPredicate::TTierPredicate(const TTieringPolicy& policy, const TRowScheme& scheme)
        : Policy(policy)
        , Type([&] {
            const auto* info = scheme.ColInfo(policy.ColumnTag);
            Y_ENSURE(info, "Tiering column " << policy.ColumnTag << " is not found");
            return info->TypeInfo;
        }())
        , Pos(scheme.ColInfo(policy.ColumnTag)->Pos)
    {
        Y_ENSURE(IsSupportedType(Type), "Unsupported tiering column type " << Type.GetTypeId());
        Y_ENSURE(!IsUnitRequired(Type) || IsKnownUnit(Policy.Unit),
            "Tiering column of type " << Type.GetTypeId() << " requires a unit");

        for (const auto& rule : Policy.Rules) {
            Y_ENSURE(rule.TierId != Max<ui32>(), "Invalid tier id");
        }
    }

    void TTierPredicate::Bind(TInstant now) {
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

    TTierPredicate::TThreshold TTierPredicate::MakeThreshold(i64 thresholdUs) const {
        TThreshold result;

        auto setUnsigned = [&](i64 value) {
            // Unsigned columns cannot hold values below zero, nothing matches
            if (value >= 0) {
                result.Valid = true;
                result.Unsigned = static_cast<ui64>(value);
            }
        };

        auto setSigned = [&](i64 value) {
            result.Valid = true;
            result.Signed = value;
        };

        switch (Type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
                setUnsigned(FloorDiv(thresholdUs, MicrosPerDay));
                break;
            case NScheme::NTypeIds::Datetime:
                setUnsigned(FloorDiv(thresholdUs, MicrosPerSecond));
                break;
            case NScheme::NTypeIds::Timestamp:
                setUnsigned(thresholdUs);
                break;
            case NScheme::NTypeIds::Date32:
                setSigned(FloorDiv(thresholdUs, MicrosPerDay));
                break;
            case NScheme::NTypeIds::Datetime64:
                setSigned(FloorDiv(thresholdUs, MicrosPerSecond));
                break;
            case NScheme::NTypeIds::Timestamp64:
                setSigned(thresholdUs);
                break;
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
                if (Policy.Unit == NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS) {
                    // TTL compares whole microseconds: value / 1000 <= threshold
                    if (thresholdUs >= 0) {
                        result.Valid = true;
                        const ui64 us = static_cast<ui64>(thresholdUs);
                        result.Unsigned = us > (Max<ui64>() - 999) / 1000 ? Max<ui64>() : us * 1000 + 999;
                    }
                } else {
                    setUnsigned(MicrosToUnitFloor(thresholdUs, Policy.Unit));
                }
                break;
            case NScheme::NTypeIds::DyNumber: {
                // Serialized thresholds are not defined before the epoch
                if (thresholdUs < 0) {
                    break;
                }
                const auto instant = TInstant::MicroSeconds(thresholdUs);
                if (auto serialized = NDyNumber::ParseDyNumberString(ToString(InstantValue(instant, Policy.Unit)))) {
                    result.Valid = true;
                    result.Serialized = std::move(*serialized);
                }
                break;
            }
            case NScheme::NTypeIds::Pg: {
                if (thresholdUs < 0) {
                    break;
                }
                const auto instant = TInstant::MicroSeconds(thresholdUs);
                TString text;
                switch (GetPgKind(Type)) {
                    case EPgKind::Date:
                    case EPgKind::Timestamp:
                        text = instant.ToString();
                        break;
                    case EPgKind::Int4:
                    case EPgKind::Int8:
                        text = ToString(InstantValue(instant, Policy.Unit));
                        break;
                    case EPgKind::Unsupported:
                        Y_ENSURE(false, "Unreachable");
                }
                auto converted = NPg::PgNativeBinaryFromNativeText(text, Type.GetPgTypeDesc());
                if (!converted.Error) {
                    result.Valid = true;
                    result.Serialized = std::move(converted.Str);
                }
                break;
            }
            default:
                Y_ENSURE(false, "Unreachable");
        }

        return result;
    }

    bool TTierPredicate::Matches(const TCell& cell, const TThreshold& threshold) const {
        if (!threshold.Valid) {
            return false;
        }

        switch (Type.GetTypeId()) {
            case NScheme::NTypeIds::Date:
                return cell.AsValue<ui16>() <= threshold.Unsigned;
            case NScheme::NTypeIds::Datetime:
            case NScheme::NTypeIds::Uint32:
                return cell.AsValue<ui32>() <= threshold.Unsigned;
            case NScheme::NTypeIds::Timestamp:
            case NScheme::NTypeIds::Uint64:
                return cell.AsValue<ui64>() <= threshold.Unsigned;
            case NScheme::NTypeIds::Date32:
                return cell.AsValue<i32>() <= threshold.Signed;
            case NScheme::NTypeIds::Datetime64:
            case NScheme::NTypeIds::Timestamp64:
                return cell.AsValue<i64>() <= threshold.Signed;
            case NScheme::NTypeIds::DyNumber:
                return cell.AsBuf() <= TStringBuf(threshold.Serialized);
            case NScheme::NTypeIds::Pg:
                return NPg::PgNativeBinaryCompare(
                    cell.Data(), cell.Size(),
                    threshold.Serialized.data(), threshold.Serialized.size(),
                    Type.GetPgTypeDesc()) <= 0;
            default:
                Y_ENSURE(false, "Unreachable");
        }
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
