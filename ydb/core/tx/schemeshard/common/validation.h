#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard::NValidation {

class TTTLValidator {
public:
    static bool ValidateUnit(const NScheme::TTypeId columnType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& errStr);
};
}