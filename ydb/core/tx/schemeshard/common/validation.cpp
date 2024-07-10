#include "validation.h"

namespace NKikimr::NSchemeShard::NValidation {

bool TTTLValidator::ValidateUnit(const NScheme::TTypeId columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr) {
    switch (columnType) {
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
        default:
            errStr = "Unsupported column type";
            return false;
    }
    return true;
}

}