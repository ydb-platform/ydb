#include <ydb/core/protos/auth.pb.h>
#include <vector>
#include <util/generic/string.h>
#include "validators.h"


namespace NKikimr::NConfig {
namespace {

EValidationResult ValidatePasswordComplexitySettings(const NKikimrProto::TPasswordComplexitySettings& passwordComplexitySettings, std::vector<TString>&msg) {
    size_t minCountOfRequiredChars = passwordComplexitySettings.GetMinLowerCaseCount() +
                                     passwordComplexitySettings.GetMinUpperCaseCount() +
                                     passwordComplexitySettings.GetMinNumbersCount() +
                                     passwordComplexitySettings.GetMinSpecialCharsCount();
    if (passwordComplexitySettings.GetMinLength() < minCountOfRequiredChars) {
        msg = std::vector<TString>{"password_complexity_settings: Min length of password cannot be less than "
                                   "total min counts of lower case chars, upper case chars, numbers and special chars"};
        return EValidationResult::Error;
    }
    return EValidationResult::Ok;
}

} // namespace

EValidationResult ValidateAuthConfig(const NKikimrProto::TAuthConfig& authConfig, std::vector<TString>& msg) {
    EValidationResult validatePasswordComplexitySettingsResult = ValidatePasswordComplexitySettings(authConfig.GetPasswordComplexitySettings(), msg);
    if (validatePasswordComplexitySettingsResult == EValidationResult::Error) {
        return EValidationResult::Error;
    }
    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }
    return EValidationResult::Ok;
}

} // NKikimr::NConfig
