#include <cctype>
#include <util/string/builder.h>
#include "password_checker.h"

namespace NLogin {

TPasswordComplexitySettings::TPasswordComplexitySettings()
    : SpecialChars(VALID_SPECIAL_CHARS.cbegin(), VALID_SPECIAL_CHARS.cend())
{}

TPasswordComplexitySettings::TPasswordComplexitySettings(const TInitializer& initializer)
    : MinLength(initializer.MinLength)
    , MinLowerCaseCount(initializer.MinLowerCaseCount)
    , MinUpperCaseCount(initializer.MinUpperCaseCount)
    , MinNumbersCount(initializer.MinNumbersCount)
    , MinSpecialCharsCount(initializer.MinSpecialCharsCount)
    , CanContainUsername(initializer.CanContainUsername)
{
    static const std::unordered_set<char> validSpecialChars(VALID_SPECIAL_CHARS.cbegin(), VALID_SPECIAL_CHARS.cend());
    for (const char ch : initializer.SpecialChars) {
        if (validSpecialChars.contains(ch)) {
            SpecialChars.insert(ch);
        }
    }
}

bool TPasswordComplexitySettings::IsSpecialCharValid(char ch) const {
    return SpecialChars.contains(ch);
}

const TString TPasswordComplexitySettings::VALID_SPECIAL_CHARS = "!@#$%^&*()_+{}|<>?=";

TPasswordChecker::TComplexityState::TComplexityState(const TPasswordComplexitySettings& complexitySettings)
    : ComplexitySettings(complexitySettings)
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
    return LowerCaseCount >= ComplexitySettings.MinLowerCaseCount;
}

bool TPasswordChecker::TComplexityState::CheckUpperCaseCount() const {
    return UpperCaseCount >= ComplexitySettings.MinUpperCaseCount;
}

bool TPasswordChecker::TComplexityState::CheckNumbersCount() const {
    return NumbersCount >= ComplexitySettings.MinNumbersCount;
}

bool TPasswordChecker::TComplexityState::CheckSpecialCharsCount() const {
    return SpecialCharsCount >= ComplexitySettings.MinSpecialCharsCount;
}

TPasswordChecker::TPasswordChecker(const TPasswordComplexitySettings& complexitySettings)
    : ComplexitySettings(complexitySettings)
{}

TPasswordChecker::TResult TPasswordChecker::Check(const TString& username, const TString& password) const {
    if (password.empty() && ComplexitySettings.MinLength == 0) {
        return {.Success = true};
    }
    if (password.length() < ComplexitySettings.MinLength) {
        return {.Success = false, .Error = "Password is too short"};
    }
    if (!ComplexitySettings.CanContainUsername && password.Contains(username)) {
        return {.Success = false, .Error = "Password must not contain user name"};
    }

    TComplexityState complexityState(ComplexitySettings);
    for (const char& ch : password) {
        if (std::islower(static_cast<unsigned char>(ch))) {
            complexityState.IncLowerCaseCount();
        } else if (std::isupper(static_cast<unsigned char>(ch))) {
            complexityState.IncUpperCaseCount();
        } else if (std::isdigit(static_cast<unsigned char>(ch))) {
            complexityState.IncNumbersCount();
        } else if (ComplexitySettings.IsSpecialCharValid(ch)) {
            complexityState.IncSpecialCharsCount();
        } else {
            return {.Success = false, .Error = "Password contains unacceptable characters"};
        }
    }

    TStringBuilder errorMessage;
    errorMessage << "Incorrect password format: ";
    bool hasError = false;
    if (!complexityState.CheckLowerCaseCount()) {
        errorMessage << "should contain at least " << ComplexitySettings.MinLowerCaseCount << " lower case character";
        hasError = true;
    }
    if (!complexityState.CheckUpperCaseCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << ComplexitySettings.MinUpperCaseCount << " upper case character";
        hasError = true;
    }
    if (!complexityState.CheckNumbersCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << ComplexitySettings.MinNumbersCount << " number";
        hasError = true;
    }
    if (!complexityState.CheckSpecialCharsCount()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "should contain at least " << ComplexitySettings.MinSpecialCharsCount << " special character";
        hasError = true;
    }

    if (hasError) {
        return {.Success = false, .Error = errorMessage};
    }
    return {.Success = true};
}

void TPasswordChecker::Update(const TPasswordComplexitySettings& complexitySettings) {
    ComplexitySettings = complexitySettings;
}

} // NLogin
