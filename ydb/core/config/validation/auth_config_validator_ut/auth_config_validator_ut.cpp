#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/auth.pb.h>
#include <vector>

using namespace NKikimr::NConfig;

Y_UNIT_TEST_SUITE(AuthConfigValidation) {
    Y_UNIT_TEST(AcceptValidPasswordComplexity) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TPasswordComplexity* validPasswordComplexity = authConfig.MutablePasswordComplexity();

        validPasswordComplexity->SetMinLength(8);
        validPasswordComplexity->SetMinLowerCaseCount(2);
        validPasswordComplexity->SetMinUpperCaseCount(2);
        validPasswordComplexity->SetMinNumbersCount(2);
        validPasswordComplexity->SetMinSpecialCharsCount(2);

        std::vector<TString> error;
        EValidationResult result = ValidateAuthConfig(authConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), error.front());
    }

    Y_UNIT_TEST(CannotAcceptInvalidPasswordComplexity) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TPasswordComplexity* invalidPasswordComplexity = authConfig.MutablePasswordComplexity();

        // 8 < 2 + 2 + 2 + 3
        invalidPasswordComplexity->SetMinLength(8);
        invalidPasswordComplexity->SetMinLowerCaseCount(2);
        invalidPasswordComplexity->SetMinUpperCaseCount(2);
        invalidPasswordComplexity->SetMinNumbersCount(2);
        invalidPasswordComplexity->SetMinSpecialCharsCount(3);

        std::vector<TString> error;
        EValidationResult result = ValidateAuthConfig(authConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "password_complexity: Min length of password cannot be less than "
                                                 "total min counts of lower case chars, upper case chars, numbers and special chars");
    }

    Y_UNIT_TEST(AcceptValidAccountLockoutConfig) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TAccountLockout* validAccountLockoutConfig = authConfig.MutableAccountLockout();

        {
            validAccountLockoutConfig->SetAttemptResetDuration("12h");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
            UNIT_ASSERT_C(error.empty(), error.front());
        }

        {
            validAccountLockoutConfig->SetAttemptResetDuration("5m");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
            UNIT_ASSERT_C(error.empty(), error.front());
        }

        {
            validAccountLockoutConfig->SetAttemptResetDuration("5s");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
            UNIT_ASSERT_C(error.empty(), error.front());
        }
    }

    Y_UNIT_TEST(CannotAcceptInvalidAccountLockoutConfig) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TAccountLockout* invalidAccountLockoutConfig = authConfig.MutableAccountLockout();

        {
            invalidAccountLockoutConfig->SetAttemptResetDuration("h");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
            UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(error.front(), "account_lockout: Cannot parse attempt reset duration");
        }

        {
            invalidAccountLockoutConfig->SetAttemptResetDuration("");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
            UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(error.front(), "account_lockout: Cannot parse attempt reset duration");
        }

        {
            invalidAccountLockoutConfig->SetAttemptResetDuration("12hhh");

            std::vector<TString> error;
            EValidationResult result = ValidateAuthConfig(authConfig, error);
            UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
            UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(error.front(), "account_lockout: Cannot parse attempt reset duration");
        }
    }
}
