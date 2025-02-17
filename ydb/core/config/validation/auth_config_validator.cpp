#include <ydb/core/protos/auth.pb.h>
#include <vector>
#include <util/generic/string.h>
#include "validators.h"


namespace NKikimr::NConfig {
namespace {

EValidationResult ValidatePasswordComplexity(const NKikimrProto::TPasswordComplexity& passwordComplexity, std::vector<TString>&msg) {
    size_t minCountOfRequiredChars = passwordComplexity.GetMinLowerCaseCount() +
                                     passwordComplexity.GetMinUpperCaseCount() +
                                     passwordComplexity.GetMinNumbersCount() +
                                     passwordComplexity.GetMinSpecialCharsCount();
    if (passwordComplexity.GetMinLength() < minCountOfRequiredChars) {
        msg = std::vector<TString>{"password_complexity: Min length of password cannot be less than "
                                   "total min counts of lower case chars, upper case chars, numbers and special chars"};
        return EValidationResult::Error;
    }
    return EValidationResult::Ok;
}

} // namespace

EValidationResult ValidateAuthConfig(const NKikimrProto::TAuthConfig& authConfig, std::vector<TString>& msg) {
    EValidationResult validatePasswordComplexityResult = ValidatePasswordComplexity(authConfig.GetPasswordComplexity(), msg);
    if (validatePasswordComplexityResult == EValidationResult::Error) {
        return EValidationResult::Error;
    }
    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }
    return EValidationResult::Ok;
}

} // NKikimr::NConfig
