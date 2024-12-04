#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard::NValidation {

class TTTLValidator {
public:
// <<<<<<< HEAD
//     static bool ValidateUnit(const NScheme::TTypeId columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr);
//     static bool ValidateTiers(const NKikimrSchemeOp::TTTLSettings::TEnabled ttlSettings, TString& errStr);
// =======
//     static bool ValidateUnit(const NScheme::TTypeInfo columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr);
//     static bool ValidateTiers(const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTTLSettings_TTier>& tiers, TString& errStr);

// private:
// >>>>>>> b2da93a482 (configure tiering on CS via ttl (#12095))
    static bool ValidateUnit(const NScheme::TTypeId columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr);
    static bool ValidateTiers(const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTTLSettings_TTier>& tiers, TString& errStr);

};
}
