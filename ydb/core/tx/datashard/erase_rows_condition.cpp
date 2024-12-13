#include "erase_rows_condition.h"

#include <yql/essentials/types/dynumber/dynumber.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/datetime/base.h>
#include <util/string/cast.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr {
namespace NDataShard {

class TExpirationCondition: public IEraseRowsCondition {
    using EUnit = NKikimrSchemeOp::TTTLSettings::EUnit;

    static TMaybe<ui64> InstantValue(TInstant instant, EUnit unit) {
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
            return Nothing();
        }
    }

    TMaybe<TString> GetWallClockDyNumber() const {
        const auto instantValue = InstantValue(WallClockInstant, Unit);
        if (!instantValue) {
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Unsupported unit: " << static_cast<ui32>(Unit));
            CannotSerialize = true;
            return Nothing();
        }

        const auto strInstant = ToString(*instantValue);
        WallClockSerialized = NDyNumber::ParseDyNumberString(strInstant);
        if (!WallClockSerialized) {
            CannotSerialize = true;
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Cannot parse DyNumber from: " << strInstant.Quote());
        }

        return WallClockSerialized;
    }

    void ParsePgFromText(const TString& value) const {
        const auto& result = NPg::PgNativeBinaryFromNativeText(value, Type.GetPgTypeDesc());
        if (result.Error) {
            CannotSerialize = true;
            LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Cannot create PG native binary from: " << value.Quote());
        } else {
            WallClockSerialized = std::move(result.Str);
        }
    }

    TMaybe<TString> GetWallClockPg() const {
        switch (NPg::PgTypeIdFromTypeDesc(Type.GetPgTypeDesc())) {
            case DATEOID:
            case TIMESTAMPOID: {
                const auto& wallClockIsoString = WallClockInstant.ToString();
                ParsePgFromText(wallClockIsoString);
                break;
            }
            case INT4OID:
            case INT8OID: {
                const auto instantValue = InstantValue(WallClockInstant, Unit);
                if (!instantValue) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                        "Unsupported unit: " << static_cast<ui32>(Unit));
                    CannotSerialize = true;
                    return Nothing();
                }
                const auto strInstant = ToString(*instantValue);
                ParsePgFromText(strInstant);
                break;
            }
            default:
                CannotSerialize = true;
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Unsupported PG type");
        }
        return WallClockSerialized;
    }

    TMaybe<TString> GetWallClockSerialized() const {
        if (WallClockSerialized) {
            return WallClockSerialized;
        }

        if (CannotSerialize) {
            return Nothing();
        }

        switch (Type.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber:
            return GetWallClockDyNumber();
        case NScheme::NTypeIds::Pg:
            return GetWallClockPg();
        default:
            Y_ABORT("Unreachable");
        }
    }

    bool CheckUi64(ui64 value) const {
        switch (Type.GetTypeId()) {
        // 'date-type column' mode
        case NScheme::NTypeIds::Date:
            return TInstant::Days(value) <= WallClockInstant;
        case NScheme::NTypeIds::Datetime:
            return TInstant::Seconds(value) <= WallClockInstant;
        case NScheme::NTypeIds::Timestamp:
            return TInstant::MicroSeconds(value) <= WallClockInstant;
        // 'value since epoch' mode
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Uint64:
            switch (Unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                return TInstant::Seconds(value) <= WallClockInstant;
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                return TInstant::MilliSeconds(value) <= WallClockInstant;
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                return TInstant::MicroSeconds(value) <= WallClockInstant;
            case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                return TInstant::MicroSeconds(value / 1000) <= WallClockInstant;
            default:
                LOG_CRIT_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                    "Unsupported unit: " << static_cast<ui32>(Unit));
                return false;
            }
        default:
            Y_ABORT("Unreachable");
        }
    }

    bool CheckI64(i64 value) const {
        
        // Dates before 1970 are deleted by TTL
        if (value < 0)
            return true;

        switch (Type.GetTypeId()) {
        // 'big date-type column' mode
        case NScheme::NTypeIds::Date32:
            return TInstant::Days(value) <= WallClockInstant;
        case NScheme::NTypeIds::Datetime64:
            return TInstant::Seconds(value) <= WallClockInstant;
        case NScheme::NTypeIds::Timestamp64:
            return TInstant::MicroSeconds(value) <= WallClockInstant;
        default:
            Y_ABORT("Unreachable");
        }
    }

    bool CheckSerialized(TStringBuf value) const {
        if (const auto& wallClockSerialized = GetWallClockSerialized()) {
            switch (Type.GetTypeId()) {
            // 'value since epoch' mode
            case NScheme::NTypeIds::DyNumber:
                return value <= *wallClockSerialized;
            case NScheme::NTypeIds::Pg: {
                int result = NPg::PgNativeBinaryCompare(
                    value.data(), value.size(),
                    wallClockSerialized->data(), wallClockSerialized->size(),
                    Type.GetPgTypeDesc());
                return result <= 0;
            }
            default:
                Y_ABORT("Unreachable");
            }
        } else {
            return false;
        }
    }

public:
    explicit TExpirationCondition(NTable::TTag columnId, ui64 wallClockTimestamp, EUnit unit)
        : ColumnId(columnId)
        , WallClockInstant(TInstant::FromValue(wallClockTimestamp))
        , Unit(unit)
        , CannotSerialize(false)
        , Pos(Max<NTable::TPos>())
    {
    }

    explicit TExpirationCondition(const NKikimrTxDataShard::TExpirationCondition& proto)
        : TExpirationCondition(proto.GetColumnId(), proto.GetWallClockTimestamp(), proto.GetColumnUnit())
    {
    }

    void AddToRequest(NKikimrTxDataShard::TEvEraseRowsRequest& request) const override {
        auto& proto = *request.MutableExpiration();
        proto.SetColumnId(ColumnId);
        proto.SetWallClockTimestamp(WallClockInstant.GetValue());
        proto.SetColumnUnit(Unit);
    }

    void Prepare(TIntrusiveConstPtr<NTable::TRowScheme> scheme, TMaybe<NTable::TPos> remapPos) override {
        const auto* columnInfo = scheme->ColInfo(ColumnId);
        Y_ABORT_UNLESS(columnInfo);

        Pos = remapPos.GetOrElse(columnInfo->Pos);
        Y_ABORT_UNLESS(Pos < scheme->Tags().size());

        Type = columnInfo->TypeInfo;
    }

    bool Check(const NTable::TRowState& row) const override {
        Y_ABORT_UNLESS(Pos != Max<NTable::TPos>());
        Y_ABORT_UNLESS(Pos < row.Size());

        const auto& cell = row.Get(Pos);
        if (cell.IsNull()) {
            return false;
        }

        switch (Type.GetTypeId()) {
        case NScheme::NTypeIds::Date:
            return CheckUi64(cell.AsValue<ui16>());
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Uint32:
            return CheckUi64(cell.AsValue<ui32>());
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Uint64:
            return CheckUi64(cell.AsValue<ui64>());
        case NScheme::NTypeIds::Date32:
            return CheckI64(cell.AsValue<i32>());
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
            return CheckI64(cell.AsValue<i64>());
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::Pg:
            return CheckSerialized(cell.AsBuf());
        default:
            return false;
        }
    }

    TVector<NTable::TTag> Tags() const override {
        return {ColumnId};
    }

private:
    const NTable::TTag ColumnId;
    const TInstant WallClockInstant;
    const EUnit Unit;

    mutable TMaybe<TString> WallClockSerialized;
    mutable bool CannotSerialize;

    NTable::TPos Pos;
    NScheme::TTypeInfo Type;

}; // TExpirationCondition

template <typename TProto>
IEraseRowsCondition* CreateEraseRowsConditionImpl(const TProto& proto) {
    switch (proto.GetConditionCase()) {
        case TProto::kExpiration:
            return new TExpirationCondition(proto.GetExpiration());
        default:
            return nullptr;
    }
}

IEraseRowsCondition* CreateEraseRowsCondition(const NKikimrTxDataShard::TEvEraseRowsRequest& request) {
    return CreateEraseRowsConditionImpl(request);
}

IEraseRowsCondition* CreateEraseRowsCondition(const NKikimrTxDataShard::TEvConditionalEraseRowsRequest& request) {
    return CreateEraseRowsConditionImpl(request);
}

} // NDataShard
} // NKikimr
