#include <library/cpp/testing/unittest/registar.h>
#include <unordered_set>
#include "password_checker.h"

using namespace NLogin;

Y_UNIT_TEST_SUITE(PasswordChecker) {

    Y_UNIT_TEST(CheckCorrectPassword) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        // Correct password has:
        //  lower case
        //  upper case
        //  numbers
        //  special symbols from list !@#$%^&*()_+{}|<>?=
        //  length from 8 to 16 characters
        TString password = "qwer%Bs7*S";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(result.Success, "Must not be error");
        UNIT_ASSERT(result.Error.empty());
    }

    Y_UNIT_TEST(CannotAcceptTooShortPassword) {
        TPasswordCheckParameters checkParameters({.MinPasswordLength = 8});
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "abcd"; // Short password
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password is too short");
    }

    Y_UNIT_TEST(CannotAcceptTooLongPassword) {
        TPasswordCheckParameters checkParameters({.MaxPasswordLength = 10});
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "abcd123456789"; // Long password
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password is too long");
    }

    Y_UNIT_TEST(PasswordCannotContainUsername) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "123testuserqqq";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password must not contain user name");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutLowerCaseCharacters) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "12345$*QWERTY";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Incorrect password format: lower case character is missing");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutUpperCaseCharacters) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "12345$*qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Incorrect password format: upper case character is missing");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutNumbers) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "ASDF$*qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Incorrect password format: number is missing");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutSpecialSymbols) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "ASDF42qwerty";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Incorrect password format: special character is missing");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithInvalidCharacters) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "ASDF42*qwerty~~";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password contains unacceptable characters");
    }

    Y_UNIT_TEST(CannotAcceptPasswordWithoutLowerCaseAndSpecialSymbols) {
        TPasswordCheckParameters checkParameters;
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "ASDF42Q6S7D8";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Incorrect password format: lower case character is missing, special character is missing");
    }

    Y_UNIT_TEST(AcceptPasswordWithCustomSpecialSymbolsList) {
        std::unordered_set<char> customSpecialSymbols {'!', '&', '*'}; // Only 3 special symbols are accepted
        TPasswordCheckParameters checkParameters({.SpecialSymbols = customSpecialSymbols});
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString correctPassword = "pass45!WOR*d";
        TPasswordChecker::TResult result = passwordChecker.Check(username, correctPassword);
        UNIT_ASSERT_C(result.Success, "Must not be error");
        UNIT_ASSERT(result.Error.empty());

        result = passwordChecker.Check(username, "pass45!WOR$d"); // '$' incorrect symbol
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password contains unacceptable characters");
    }

    Y_UNIT_TEST(AcceptEmptyPasswordByEnableSpecialFlag) {
        TPasswordCheckParameters checkParameters({.EnableEmptyPassword = true}); // Enable empty password, min length is 8
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(result.Success, "Must not be error");
        UNIT_ASSERT(result.Error.empty());
    }

    Y_UNIT_TEST(AcceptEmptyPasswordBySetMinPasswordLengthAsZero) {
        TPasswordCheckParameters checkParameters({.MinPasswordLength = 0, .EnableEmptyPassword = false}); // Enable empty password by set MinPasswordLength as 0
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(result.Success, "Must not be error");
        UNIT_ASSERT(result.Error.empty());
    }

    Y_UNIT_TEST(CannotAcceptEmptyPassword) {
        TPasswordCheckParameters checkParameters({.EnableEmptyPassword = false}); // Disable empty password, min length is 8
        TPasswordChecker passwordChecker(checkParameters);
        TString username = "testuser";
        TString password = "";
        TPasswordChecker::TResult result = passwordChecker.Check(username, password);
        UNIT_ASSERT_C(!result.Success, "Must be error");
        UNIT_ASSERT_STRINGS_EQUAL(result.Error, "Password is too short");
    }

}
