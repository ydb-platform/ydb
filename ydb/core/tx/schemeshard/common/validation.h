#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard::NValidation {

class TTTLValidator {
public:
    static bool ValidateUnit(const NScheme::TTypeInfo columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr);
    static bool ValidateTiers(const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTTLSettings_TTier>& tiers, TString& errStr);

private:
};
}
