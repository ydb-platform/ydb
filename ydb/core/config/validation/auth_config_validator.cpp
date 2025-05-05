#include <ydb/core/protos/auth.pb.h>
#include <vector>
#include <util/generic/string.h>
#include <util/datetime/base.h>
#include "validators.h"


namespace NKikimr::NConfig {
namespace {

EValidationResult ValidatePasswordComplexity(const NKikimrProto::TPasswordComplexity& passwordComplexity, std::vector<TString>& msg) {
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

EValidationResult ValidateAccountLockout(const NKikimrProto::TAccountLockout& accountLockout, std::vector<TString>& msg) {
    TDuration attemptResetDuration;
    if (TDuration::TryParse(accountLockout.GetAttemptResetDuration(), attemptResetDuration)) {
        return EValidationResult::Ok;
    }
    msg = std::vector<TString>{"account_lockout: Cannot parse attempt reset duration"};
    return EValidationResult::Error;
}

} // namespace

EValidationResult ValidateAuthConfig(const NKikimrProto::TAuthConfig& authConfig, std::vector<TString>& msg) {
    if (authConfig.HasPasswordComplexity()) {
        EValidationResult validateResult = ValidatePasswordComplexity(authConfig.GetPasswordComplexity(), msg);
        if (validateResult == EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }

    if (authConfig.HasAccountLockout()) {
        EValidationResult validateResult = ValidateAccountLockout(authConfig.GetAccountLockout(), msg);
        if (validateResult == EValidationResult::Error) {
            return EValidationResult::Error;
        }
    }

    if (msg.size() > 0) {
        return EValidationResult::Warn;
    }
    return EValidationResult::Ok;
}

} // NKikimr::NConfig
