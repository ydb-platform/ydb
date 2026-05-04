#include "udf_validate.h"
#include <util/string/join.h>
#include <util/generic/yexception.h>

namespace NYql::NUdf {

#define SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val:                              \
        return TStringBuf(#name);

TString ValidateModeAvailables() {
    return Join(", ",
                ValidateModeAsStr(EValidateMode::None),
                ValidateModeAsStr(EValidateMode::Lazy),
                ValidateModeAsStr(EValidateMode::Greedy));
}

TStringBuf ValidateModeAsStr(EValidateMode validateMode) {
    switch (static_cast<int>(validateMode)) {
        UDF_VALIDATE_MODE(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

EValidateMode ValidateModeByStr(const TString& verifyModeStr) {
    const TString lowerVerifyModeStr = to_lower(verifyModeStr);
    for (auto val = EValidateMode::None; val < EValidateMode::Max; val = static_cast<EValidateMode>(static_cast<ui8>(val) + 1)) {
        if (lowerVerifyModeStr == to_lower(TString(ValidateModeAsStr(val)))) {
            return val;
        }
    }
    ythrow yexception() << "Unknown udf validate mode: " << verifyModeStr;
}

TStringBuf ValidatePolicyAsStr(EValidatePolicy verifyPolicy) {
    switch (static_cast<int>(verifyPolicy)) {
        UDF_VALIDATE_POLICY(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

EValidatePolicy ValidatePolicyByStr(const TString& verifyPolicy) {
    const TString lowerVerifyPolicy = to_lower(verifyPolicy);
    for (auto val = EValidatePolicy::Fail; val < EValidatePolicy::Max; val = static_cast<EValidatePolicy>(static_cast<ui8>(val) + 1)) {
        if (lowerVerifyPolicy == to_lower(TString(ValidatePolicyAsStr(val)))) {
            return val;
        }
    }
    ythrow yexception() << "Unknown udf validate policy: " << verifyPolicy;
}

EValidateDatumMode ToDatumValidateMode(EValidateMode validateMode) {
    switch (validateMode) {
        case EValidateMode::None:
            return EValidateDatumMode::None;
        case EValidateMode::Lazy:
            return EValidateDatumMode::Cheap;
        case EValidateMode::Greedy:
        case EValidateMode::Max:
            return EValidateDatumMode::Expensive;
    }
}
} // namespace NYql::NUdf
