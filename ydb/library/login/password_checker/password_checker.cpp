#include <cctype>
#include <util/string/builder.h>
#include "password_checker.h"

namespace NLogin {

TPasswordCheckParameters::TPasswordCheckParameters()
    : SpecialSymbols(VALID_SPECIAL_SYMBOLS)
{}

TPasswordCheckParameters::TPasswordCheckParameters(const TInitializer& initializer)
    : MinPasswordLength(initializer.MinPasswordLength)
    , MaxPasswordLength(initializer.MaxPasswordLength)
    , NeedLowerCase(initializer.NeedLowerCase)
    , NeedUpperCase(initializer.NeedUpperCase)
    , NeedNumbers(initializer.NeedNumbers)
    , NeedSpecialSymbols(initializer.NeedSpecialSymbols)
{
    for (const char symbol : initializer.SpecialSymbols) {
        if (VALID_SPECIAL_SYMBOLS.contains(symbol)) {
            SpecialSymbols.insert(symbol);
        }
    }
}

ui32 TPasswordCheckParameters::GetMinPasswordLength() const {
    return MinPasswordLength;
}

ui32 TPasswordCheckParameters::GetMaxPasswordLength() const {
    return MaxPasswordLength;
}

bool TPasswordCheckParameters::NeedLowerCaseUse() const {
    return NeedLowerCase;
}

bool TPasswordCheckParameters::NeedUpperCaseUse() const {
    return NeedUpperCase;
}

bool TPasswordCheckParameters::NeedNumbersUse() const {
    return NeedNumbers;
}

bool TPasswordCheckParameters::NeedSpecialSymbolsUse() const {
    return NeedSpecialSymbols;
}

bool TPasswordCheckParameters::IsSpecialSymbolValid(char symbol) const {
    return SpecialSymbols.contains(symbol);
}

void TPasswordCheckParameters::SetMinPasswordLength(ui32 length) {
    MinPasswordLength = length;
}

void TPasswordCheckParameters::SetMaxPasswordLength(ui32 length) {
    MaxPasswordLength = length;
}

void TPasswordCheckParameters::SetLowerCaseUse(bool flag) {
    NeedLowerCase = flag;
}

void TPasswordCheckParameters::SetUpperCaseUse(bool flag) {
    NeedUpperCase = flag;
}

void TPasswordCheckParameters::SetNumbersUse(bool flag) {
    NeedNumbers = flag;
}

void TPasswordCheckParameters::SetSpecialSymbolsUse(bool flag) {
    NeedSpecialSymbols = flag;
}

void TPasswordCheckParameters::SetSpecialSymbols(const TString& specialSymbols) {
    SpecialSymbols.clear();
    for (const char symbol : specialSymbols) {
        if (VALID_SPECIAL_SYMBOLS.contains(symbol)) {
            SpecialSymbols.insert(symbol);
        }
    }
}

const std::unordered_set<char> TPasswordCheckParameters::VALID_SPECIAL_SYMBOLS = {'!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '{', '}', '|', '<', '>', '?', '='};

void TPasswordChecker::TFlagsStore::SetLowerCase(bool flag) {
    LowerCase = flag;
}

void TPasswordChecker::TFlagsStore::SetUpperCase(bool flag) {
    UpperCase = flag;
}

void TPasswordChecker::TFlagsStore::SetNumber(bool flag) {
    Number = flag;
}

void TPasswordChecker::TFlagsStore::SetSpecialSymbol(bool flag) {
    SpecialSymbol = flag;
}

bool TPasswordChecker::TFlagsStore::HasLowerCase() const {
    return LowerCase;
}

bool TPasswordChecker::TFlagsStore::HasUpperCase() const {
    return UpperCase;
}

bool TPasswordChecker::TFlagsStore::HasNumber() const {
    return Number;
}

bool TPasswordChecker::TFlagsStore::HasSpecialSymbol() const {
    return SpecialSymbol;
}

TPasswordChecker::TPasswordChecker(const TPasswordCheckParameters& checkParameters)
    : CheckParameters(checkParameters)
{}

TPasswordChecker::TResult TPasswordChecker::Check(const TString& username, const TString& password) const {
    if (password.empty() && CheckParameters.GetMinPasswordLength() == 0) {
        return {.Success = true};
    }
    if (password.length() < CheckParameters.GetMinPasswordLength()) {
        return {.Success = false, .Error = "Password is too short"};
    }
    if (password.length() > CheckParameters.GetMaxPasswordLength()) {
        return {.Success = false, .Error = "Password is too long"};
    }
    if (password.Contains(username)) {
        return {.Success = false, .Error = "Password must not contain user name"};
    }

    TFlagsStore passwordFlags;
    for (const char& symbol : password) {
        if (std::islower(static_cast<unsigned char>(symbol))) {
            passwordFlags.SetLowerCase(true);
        } else if (std::isupper(static_cast<unsigned char>(symbol))) {
            passwordFlags.SetUpperCase(true);
        } else if (std::isdigit(static_cast<unsigned char>(symbol))) {
            passwordFlags.SetNumber(true);
        } else if (CheckParameters.IsSpecialSymbolValid(symbol)) {
            passwordFlags.SetSpecialSymbol(true);
        } else {
            return {.Success = false, .Error = "Password contains unacceptable characters"};
        }
    }

    TStringBuilder errorMessage;
    errorMessage << "Incorrect password format: ";
    bool hasError = false;
    if (!passwordFlags.HasLowerCase() && CheckParameters.NeedLowerCaseUse()) {
        errorMessage << "lower case character is missing";
        hasError = true;
    }
    if (!passwordFlags.HasUpperCase() && CheckParameters.NeedUpperCaseUse()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "upper case character is missing";
        hasError = true;
    }
    if (!passwordFlags.HasNumber() && CheckParameters.NeedNumbersUse()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "number is missing";
        hasError = true;
    }
    if (!passwordFlags.HasSpecialSymbol() && CheckParameters.NeedSpecialSymbolsUse()) {
        if (hasError) {
            errorMessage << ", ";
        }
        errorMessage << "special character is missing";
        hasError = true;
    }

    if (hasError) {
        return {.Success = false, .Error = errorMessage};
    }
    return {.Success = true};
}

void TPasswordChecker::Update(const TPasswordCheckParameters& checkParameters) {
    CheckParameters = checkParameters;
}

} // NLogin
