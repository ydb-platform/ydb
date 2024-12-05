#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/auth.pb.h>
#include <vector>

using namespace NKikimr::NConfig;

Y_UNIT_TEST_SUITE(AuthConfigValidation) {
    Y_UNIT_TEST(AcceptValidPasswordComplexitySettings) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TPasswordComplexitySettings* validPasswordComplexitySettings = authConfig.MutablePasswordComplexitySettings();

        validPasswordComplexitySettings->SetMinLength(8);
        validPasswordComplexitySettings->SetMinLowerCaseCount(2);
        validPasswordComplexitySettings->SetMinUpperCaseCount(2);
        validPasswordComplexitySettings->SetMinNumbersCount(2);
        validPasswordComplexitySettings->SetMinSpecialCharsCount(2);

        std::vector<TString> error;
        EValidationResult result = ValidateAuthConfig(authConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Ok);
        UNIT_ASSERT_C(error.empty(), "Should not be errors");
    }

    Y_UNIT_TEST(CannotAcceptInvalidPasswordComplexitySettings) {
        NKikimrProto::TAuthConfig authConfig;
        NKikimrProto::TPasswordComplexitySettings* invalidPasswordComplexitySettings = authConfig.MutablePasswordComplexitySettings();

        // 8 < 2 + 2 + 2 + 3
        invalidPasswordComplexitySettings->SetMinLength(8);
        invalidPasswordComplexitySettings->SetMinLowerCaseCount(2);
        invalidPasswordComplexitySettings->SetMinUpperCaseCount(2);
        invalidPasswordComplexitySettings->SetMinNumbersCount(2);
        invalidPasswordComplexitySettings->SetMinSpecialCharsCount(3);

        std::vector<TString> error;
        EValidationResult result = ValidateAuthConfig(authConfig, error);
        UNIT_ASSERT_EQUAL(result, EValidationResult::Error);
        UNIT_ASSERT_VALUES_EQUAL(error.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(error.front(), "password_complexity_settings: Min length of password cannot be less than "
                                                 "total min counts of lower case chars, upper case chars, numbers and special chars");
    }
}
