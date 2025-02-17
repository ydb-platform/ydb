#include "udf_validate.h"
#include <util/string/join.h>
#include <util/generic/yexception.h>

namespace NYql {
namespace NUdf {

#define SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val: return TStringBuf(#name);

TString ValidateModeAvailables() {
    return Join(", ",
        ValidateModeAsStr(EValidateMode::None),
        ValidateModeAsStr(EValidateMode::Lazy),
        ValidateModeAsStr(EValidateMode::Greedy)
    );
}

TStringBuf ValidateModeAsStr(EValidateMode validateMode) {
    switch (static_cast<int>(validateMode)) {
        UDF_VALIDATE_MODE(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

EValidateMode ValidateModeByStr(const TString& validateModeStr) {
    const TString lowerValidateModeStr = to_lower(validateModeStr);
    for (auto val = EValidateMode::None; val < EValidateMode::Max; val = static_cast<EValidateMode>(static_cast<ui8>(val) + 1)) {
        if (lowerValidateModeStr == to_lower(TString(ValidateModeAsStr(val)))) {
            return val;
        }
    }
    ythrow yexception() << "Unknown udf validate mode: " << validateModeStr;
}

TStringBuf ValidatePolicyAsStr(EValidatePolicy validatePolicy) {
    switch (static_cast<int>(validatePolicy)) {
        UDF_VALIDATE_POLICY(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

EValidatePolicy ValidatePolicyByStr(const TString& validatePolicyStr) {
    const TString lowerValidatePolicyStr = to_lower(validatePolicyStr);
    for (auto val = EValidatePolicy::Fail; val < EValidatePolicy::Max; val = static_cast<EValidatePolicy>(static_cast<ui8>(val) + 1)) {
        if (lowerValidatePolicyStr == to_lower(TString(ValidatePolicyAsStr(val)))) {
            return val;
        }
    }
    ythrow yexception() << "Unknown udf validate policy: " << validatePolicyStr;
}

} // namspace NUdf
} // namspace NYql
