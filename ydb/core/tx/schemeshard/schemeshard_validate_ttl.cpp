#include "schemeshard_info_types.h"

#include "common/validation.h"
#include "olap/columns/schema.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {
static inline bool IsDropped(const TTableInfo::TColumn& col) {
    return col.IsDropped();
}

static inline ui32 GetType(const TTableInfo::TColumn& col) {
    Y_ABORT_UNLESS(col.PType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
    return col.PType.GetTypeId();
}

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
            Y_ABORT_UNLESS("Unknown column");
        }

        if (IsDropped(*column)) {
            errStr = Sprintf("Cannot enable TTL on dropped column: '%s'", colName.data());
            return false;
        }

        const auto unit = enabled.GetColumnUnit();
        if (!NValidation::TTTLValidator::ValidateUnit(GetType(*column), unit, errStr)) {
            return false;
        }

        const TInstant now = TInstant::Now();
        if (enabled.GetExpireAfterSeconds() > now.Seconds()) {
            errStr = Sprintf("TTL should be less than %" PRIu64 " seconds (%" PRIu64 " days, %" PRIu64 " years). The ttl behaviour is undefined before 1970.", now.Seconds(), now.Days(), now.Days() / 365);
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

}}
