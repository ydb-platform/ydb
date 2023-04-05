#include "schemeshard_info_types.h"
#include "schemeshard_olap_types.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {
namespace NSchemeShard {

// Helper accessors for OLTP and OLAP tables that use different TColumn's
namespace {
    inline
    bool IsDropped(const TOlapSchema::TColumn& col) {
        Y_UNUSED(col);
        return false;
    }

    inline
    ui32 GetType(const TOlapSchema::TColumn& col) {
        Y_VERIFY(col.GetType().GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
        return col.GetType().GetTypeId();
    }

    inline
    bool IsDropped(const TTableInfo::TColumn& col) {
        return col.IsDropped();
    }

    inline
    ui32 GetType(const TTableInfo::TColumn& col) {
        Y_VERIFY(col.PType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
        return col.PType.GetTypeId();
    }
}

template <class TColumn>
bool ValidateUnit(const TColumn& column, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr) {
    switch (GetType(column)) {
    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
        if (unit != NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) {
            errStr = "To enable TTL on date type column 'DateTypeColumnModeSettings' should be specified";
            return false;
        }
        break;
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::DyNumber:
        if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) {
            errStr = "To enable TTL on integral type column 'ValueSinceUnixEpochModeSettings' should be specified";
            return false;
        }
        break;
    default:
        errStr = "Unsupported column type";
        return false;
    }
    return true;
}

bool ValidateTtlSettings(const NKikimrSchemeOp::TTTLSettings& ttl,
    const THashMap<ui32, TTableInfo::TColumn>& sourceColumns,
    const THashMap<ui32, TTableInfo::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    const TSubDomainInfo& subDomain, TString& errStr)
{
    using TTtlProto = NKikimrSchemeOp::TTTLSettings;

    switch (ttl.GetStatusCase()) {
    case TTtlProto::kEnabled: {
        const auto& enabled = ttl.GetEnabled();
        const TString colName = enabled.GetColumnName();

        auto it = colName2Id.find(colName);
        if (it == colName2Id.end()) {
            errStr = Sprintf("Cannot enable TTL on unknown column: '%s'", colName.data());
            return false;
        }

        const TTableInfo::TColumn* column = nullptr;
        const ui32 colId = it->second;
        if (alterColumns.contains(colId)) {
            column = &alterColumns.at(colId);
        } else if (sourceColumns.contains(colId)) {
            column = &sourceColumns.at(colId);
        } else {
            Y_VERIFY("Unknown column");
        }

        if (IsDropped(*column)) {
            errStr = Sprintf("Cannot enable TTL on dropped column: '%s'", colName.data());
            return false;
        }

        const auto unit = enabled.GetColumnUnit();
        if (!ValidateUnit(*column, unit, errStr)) {
            return false;
        }

        if (enabled.HasSysSettings()) {
            const auto& sys = enabled.GetSysSettings();
            if (TDuration::FromValue(sys.GetRunInterval()) < subDomain.GetTtlMinRunInterval()) {
                errStr = Sprintf("TTL run interval cannot be less than limit: %" PRIu64, subDomain.GetTtlMinRunInterval().Seconds());
                return false;
            }
        }
        break;
    }

    case TTtlProto::kDisabled:
        break;

    default:
        errStr = "TTL status must be specified";
        return false;
    }

    return true;
}

static bool ValidateColumnTableTtl(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl,
    const THashMap<ui32, TOlapSchema::TColumn>& sourceColumns,
    const THashMap<ui32, TOlapSchema::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    IErrorCollector& errors)
{
    const TString colName = ttl.GetColumnName();

    auto it = colName2Id.find(colName);
    if (it == colName2Id.end()) {
        errors.AddError(Sprintf("Cannot enable TTL on unknown column: '%s'", colName.data()));
        return false;
    }

    const TOlapSchema::TColumn* column = nullptr;
    const ui32 colId = it->second;
    if (alterColumns.contains(colId)) {
        column = &alterColumns.at(colId);
    } else if (sourceColumns.contains(colId)) {
        column = &sourceColumns.at(colId);
    } else {
        Y_VERIFY("Unknown column");
    }

    if (IsDropped(*column)) {
        errors.AddError(Sprintf("Cannot enable TTL on dropped column: '%s'", colName.data()));
        return false;
    }

    if (ttl.HasExpireAfterBytes()) {
        errors.AddError("TTL with eviction by size is not supported yet");
        return false;
    }

    if (!ttl.HasExpireAfterSeconds()) {
        errors.AddError("TTL without eviction time");
        return false;
    }

    auto unit = ttl.GetColumnUnit();

    switch (GetType(*column)) {
        case NScheme::NTypeIds::DyNumber:
            errors.AddError("Unsupported column type for TTL in column tables");
            return false;
        default:
            break;
    }

    TString errStr;
    if (!ValidateUnit(*column, unit, errStr)) {
        errors.AddError(errStr);
        return false;
    }
    return true;
}

bool TOlapSchema::ValidateTtlSettings(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl, IErrorCollector& errors) const {
    using TTtlProto = NKikimrSchemeOp::TColumnDataLifeCycle;

    switch (ttl.GetStatusCase()) {
        case TTtlProto::kEnabled:
            return ValidateColumnTableTtl(ttl.GetEnabled(), {}, Columns, ColumnsByName, errors);
        case TTtlProto::kDisabled:
        default:
            break;
    }

    return true;
}

}}
