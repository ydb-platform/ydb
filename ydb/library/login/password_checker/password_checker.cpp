#include <cctype>
#include <util/string/builder.h>
#include "password_checker.h"

namespace NLogin {

TPasswordComplexity::TPasswordComplexity()
    : SpecialChars(VALID_SPECIAL_CHARS)
{}

TPasswordComplexity::TPasswordComplexity(const TInitializer& initializer)
    : MinLength(initializer.MinLength)
    , MinLowerCaseCount(initializer.MinLowerCaseCount)
    , MinUpperCaseCount(initializer.MinUpperCaseCount)
    , MinNumbersCount(initializer.MinNumbersCount)
    , MinSpecialCharsCount(initializer.MinSpecialCharsCount)
    , CanContainUsername(initializer.CanContainUsername)
{
    if (initializer.SpecialChars.empty()) {
        SpecialChars.insert(VALID_SPECIAL_CHARS.begin(), VALID_SPECIAL_CHARS.end());
    } else {
        for (const char ch : initializer.SpecialChars) {
            if (VALID_SPECIAL_CHARS.contains(ch)) {
                SpecialChars.insert(ch);
            }
        }
    }
}

bool TPasswordComplexity::IsSpecialCharValid(char ch) const {
    return SpecialChars.contains(ch);
}

const std::unordered_set<char> TPasswordComplexity::VALID_SPECIAL_CHARS {'!', '@', '#', '$', '%', '^', '&',
                                                                         '*', '(', ')', '_', '+', '{',
                                                                         '}', '|', '<', '>', '?', '='};

TPasswordChecker::TComplexityState::TComplexityState(const TPasswordComplexity& passwordComplexity)
    : PasswordComplexity(passwordComplexity)
{}

void TPasswordChecker::TComplexityState::IncLowerCaseCount() {
    ++LowerCaseCount;
}

void TPasswordChecker::TComplexityState::IncUpperCaseCount() {
    ++UpperCaseCount;
}

void TPasswordChecker::TComplexityState::IncNumbersCount() {
    ++NumbersCount;
}

void TPasswordChecker::TComplexityState::IncSpecialCharsCount() {
    ++SpecialCharsCount;
}

bool TPasswordChecker::TComplexityState::CheckLowerCaseCount() const {
    return LowerCaseCount >= PasswordComplexity.MinLowerCaseCount;
}

bool TPasswordChecker::TComplexityState::CheckUpperCaseCount() const {
    return UpperCaseCount >= PasswordComplexity.MinUpperCaseCount;
}

bool TPasswordChecker::TComplexityState::CheckNumbersCount() const {
    return NumbersCount >= PasswordComplexity.MinNumbersCount;
}

bool TPasswordChecker::TComplexityState::CheckSpecialCharsCount() const {
    return SpecialCharsCount >= PasswordComplexity.MinSpecialCharsCount;
}

TPasswordChecker::TPasswordChecker(const TPasswordComplexity& passwordComplexity)
    : PasswordComplexity(passwordComplexity)
{}

TPasswordChecker::TResult TPasswordChecker::Check(const TString& username, const TString& password) const {
    if (password.empty() && PasswordComplexity.MinLength == 0) {
        return {.Success = true};
    }
    if (password.length() < PasswordComplexity.MinLength) {
        return {.Success = false, .Error = "Password is too short"};
    }
    if (!PasswordComplexity.CanContainUsername && password.Contains(username)) {
        return {.Success = false, .Error = "Password must not contain user name"};
    }

    TComplexityState complexityState(PasswordComplexity);
    for (const char& ch : password) {
        if (std::islower(static_cast<unsigned char>(ch))) {
            complexityState.IncLowerCaseCount();
        } else if (std::isupper(static_cast<unsigned char>(ch))) {
            complexityState.IncUpperCaseCount();
        } else if (std::isdigit(static_cast<unsigned char>(ch))) {
            complexityState.IncNumbersCount();
        } else if (PasswordComplexity.IsSpecialCharValid(ch)) {
            complexityState.IncSpecialCharsCount();
        } else {
            return {.Success = false, .Error = "Password contains unacceptable characters"};
        }
    }

    TStringBuilder errorMessage;
    errorMessage << "Incorrect password format: ";
    bool hasError = false;
    if (!complexityState.CheckLowerCaseCount()) {
        errorMessage << "should contain at least " << PasswordComplexity.MinLowerCaseCount << " lower case character";
        hasError = true;
    }
    if (!complexityState.CheckUpperCaseCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << PasswordComplexity.MinUpperCaseCount << " upper case character";
        hasError = true;
    }
    if (!complexityState.CheckNumbersCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << PasswordComplexity.MinNumbersCount << " number";
        hasError = true;
    }
    if (!complexityState.CheckSpecialCharsCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << PasswordComplexity.MinSpecialCharsCount << " special character";
        hasError = true;
    }

    if (hasError) {
        return {.Success = false, .Error = errorMessage};
    }
    return {.Success = true};
}

void TPasswordChecker::Update(const TPasswordComplexity& passwordComplexity) {
    PasswordComplexity = passwordComplexity;
}

} // NLogin
