#include "validation.h"

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <util/string/builder.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr::NSchemeShard::NValidation {

bool TTTLValidator::ValidateUnit(const NScheme::TTypeInfo columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr) {
    switch (columnType.GetTypeId()) {
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Date32:
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
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
        case NScheme::NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(columnType.GetPgTypeDesc())) {
                case DATEOID:
                case TIMESTAMPOID:
                    if (unit != NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) {
                        errStr = "To enable TTL on date PG type column 'DateTypeColumnModeSettings' should be specified";
                        return false;
                    }
                    break;
                case INT4OID:
                case INT8OID:
                    if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) {
                        errStr = "To enable TTL on integral PG type column 'ValueSinceUnixEpochModeSettings' should be specified";
                        return false;
                    }
                    break;
                default:
                    errStr = "Unsupported column type";
                    return false;
            }
            break;
        default:
            errStr = "Unsupported column type";
            return false;
    }
    return true;
}

bool TTTLValidator::ValidateTiers(const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTTLSettings_TTier>& tiers, TString& errStr) {
    for (i64 i = 0; i < tiers.size(); ++i) {
        const auto& tier = tiers[i];
        if (!tier.HasApplyAfterSeconds()) {
            errStr = TStringBuilder() << "Tier " << i << ": missing ApplyAfterSeconds";
            return false;
        }
        if (i != 0 && tier.GetApplyAfterSeconds() <= tiers[i - 1].GetApplyAfterSeconds()) {
            errStr = TStringBuilder() << "Tiers in the sequence must have increasing ApplyAfterSeconds: "
                                      << tiers[i - 1].GetApplyAfterSeconds() << " (tier " << i - 1
                                      << ") >= " << tier.GetApplyAfterSeconds() << " (tier " << i << ")";
            return false;
        }
        switch (tier.GetActionCase()) {
            case NKikimrSchemeOp::TTTLSettings_TTier::kDelete:
                if (i + 1 != tiers.size()) {
                    errStr = TStringBuilder() << "Tier " << i << ": only the last tier in TTL settings can have Delete action";
                    return false;
                }
                break;
            case NKikimrSchemeOp::TTTLSettings_TTier::kEvictToExternalStorage:
                break;
            case NKikimrSchemeOp::TTTLSettings_TTier::ACTION_NOT_SET:
                errStr = TStringBuilder() << "Tier " << i << ": missing Action";
                return false;
        }
    }
    return true;
}

}
