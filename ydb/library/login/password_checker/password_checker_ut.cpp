#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include "password_checker.h"

using namespace NLogin;

Y_UNIT_TEST_SUITE(PasswordChecker) {

    Y_UNIT_TEST(CheckCorrectPasswordWithMaxComplexity) {
        TPasswordComplexity passwordComplexity({
            .MinLength = 8,
            .MinLowerCaseCount = 2,
            .MinUpperCaseCount = 2,
            .MinNumbersCount = 2,
            .MinSpecialCharsCount = 2,
            .SpecialChars = TPasswordComplexity::VALID_SPECIAL_CHARS,
            .CanContainUsername = false
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "qwer%Bs7*S4";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(result.Success, result.Error);
        UNIT_ASSERT(result.Error.empty());
    }

    Y_UNIT_TEST(CannotAcceptTooShortPassword) {
        TPasswordComplexity passwordComplexity({.MinLength = 8});
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "abcd"; // Short password
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password is too short");
    }

    Y_UNIT_TEST(PasswordCannotContainUsername) {
        TPasswordComplexity passwordComplexity({.CanContainUsername = false});
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "123testuserqqq";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password must not contain user name");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutLowerCaseCharacters) {
        TPasswordComplexity passwordComplexity({
            .MinLowerCaseCount = 4
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "12345$*QWERTY";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, TStringBuilder() << "Incorrect password format: should contain at least "
                                                                 << passwordComplexity.MinLowerCaseCount
                                                                 << " lower case character");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutUpperCaseCharacters) {
        TPasswordComplexity passwordComplexity({
            .MinUpperCaseCount = 4
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "12345$*qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, TStringBuilder() << "Incorrect password format: should contain at least "
                                                                 << passwordComplexity.MinUpperCaseCount
                                                                 << " upper case character");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutNumbers) {
        TPasswordComplexity passwordComplexity({
            .MinNumbersCount = 4
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "ASDF$*qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, TStringBuilder() << "Incorrect password format: should contain at least "
                                                                 << passwordComplexity.MinNumbersCount
                                                                 << " number");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutSpecialCharacters) {
        TPasswordComplexity passwordComplexity({
            .MinSpecialCharsCount = 4
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "ASDF42qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, TStringBuilder() << "Incorrect password format: should contain at least "
                                                                 << passwordComplexity.MinSpecialCharsCount
                                                                 << " special character");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithInvalidCharacters) {
        TPasswordComplexity passwordComplexity;
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "ASDF42*qwerty~~"; // ~ is invalid character
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password contains unacceptable characters");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutLowerCaseAndSpecialCharacters) {
        TPasswordComplexity passwordComplexity({
            .MinLowerCaseCount = 2, .MinSpecialCharsCount = 2
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "ASDF42Q6S7D8";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, TStringBuilder() << "Incorrect password format: should contain at least "
                                                                 << passwordComplexity.MinLowerCaseCount
                                                                 << " lower case character, should contain at least "
                                                                 << passwordComplexity.MinSpecialCharsCount
                                                                 << " special character");
    }

    Y_UNIT_TEST(AcceptPasswordWithCustomSpecialCharactersList) {
        TString customSpecialCharacters = "!&*"; // Only 3 special symbols are accepted
        TPasswordComplexity passwordComplexity({
            .MinSpecialCharsCount = 2,
            .SpecialChars = customSpecialCharacters
        });
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString correctPassword = "pass45!WOR*d";
        TPasswordChecker::TResult result = passwordChecker.Check(username, correctPassword);
        UNIT_ASSERT_C(result.Success, result.Error);
        UNIT_ASSERT(result.Error.empty());

        result = passwordChecker.Check(username, "pass45!WOR$d"); // '$' incorrect symbol
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password contains unacceptable characters");
    }

    Y_UNIT_TEST(AcceptEmptyPassword) {
        TPasswordComplexity passwordComplexity({
            .MinLength = 0
        }); // Enable empty password by set MinLength as 0
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(result.Success, result.Error);
        UNIT_ASSERT(result.Error.empty());
    }

    Y_UNIT_TEST(CannotAcceptEmptyPassword) {
        TPasswordComplexity passwordComplexity({
            .MinLength = 8
        }); // Disable empty password, min length is 8
        TPasswordChecker passwordChecker(passwordComplexity);
        TString username = "testuser";
        TString password = "";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password is too short");
    }

}
