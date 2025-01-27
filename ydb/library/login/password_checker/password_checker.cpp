#include <cctype>
#include <util/string/builder.h>
#include "password_checker.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>

#include <format>

namespace NLogin {

TPasswordComplexity::TPasswordComplexity()
    : SpecialChars(VALID_SPECIAL_CHARS.cbegin(), VALID_SPECIAL_CHARS.cend())
{}

TPasswordComplexity::TPasswordComplexity(const TInitializer& initializer)
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

bool TPasswordComplexity::IsSpecialCharValid(char ch) const {
    return SpecialChars.contains(ch);
}

const TString TPasswordComplexity::VALID_SPECIAL_CHARS = "!@#$%^&*()_+{}|<>?=";

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

bool TPasswordChecker::IsBase64(const std::string& value) {
    std::unordered_set<char> Base64Symbols;
    auto add = [&Base64Symbols](char l, char r) -> void {
        for (char c = l; c <= r; c++) {
            Base64Symbols.insert(c);
        }
    };

    add('A', 'Z');
    add('a', 'z');
    add('0', '9');
    add('+', '+');
    add('=', '=');
    add('/', '/');

    for (auto c : value) {
        if (!Base64Symbols.contains(c)) {
            return false;
        }
    }

    return true;
}

TPasswordChecker::TResult TPasswordChecker::CheckSyntaxOfHash(const TString& hash) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(hash, &json)) {
        return {.Success = false, .Error = "Cannot parse hash value; it should be in JSON-format"};
    }

    if (json.GetType() != NJson::JSON_MAP
        || json.GetMap().size() != 3
        || !json.Has("type")
        || !json.Has("salt")
        || !json.Has("hash")
        || json["type"].GetType() != NJson::JSON_STRING
        || json["salt"].GetType() != NJson::JSON_STRING
        || json["hash"].GetType() != NJson::JSON_STRING
    ) {
        return {.Success = false,
                .Error = "There should be strictly three fields here: salt, hash and type"};
    }

    if (json["type"].GetStringRobust() != "argon2id") {
        return {.Success = false,
                .Error = "Field \'type\' must be equal \"argon2id\""};
    }

    const auto& hashField = json["hash"].GetStringRobust();
    const auto& saltField = json["salt"].GetStringRobust();

    if (hashField.size() != HashSizeBase64) {
        std::string error = std::format("Length of field \'hash\' is {}, but it must be equal {}", hashField.size(), HashSizeBase64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (saltField.size() != SaltSizeBase64) {
        std::string error = std::format("Length of field \'salt\' is {}, but it must be equal {}", saltField.size(), SaltSizeBase64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (!TPasswordChecker::IsBase64(hashField)) {
        return {.Success = false,
                .Error = "Field \'hash\' must be in base64 format"};
    }

    if (!TPasswordChecker::IsBase64(saltField)) {
        return {.Success = false,
                .Error = "Field \'salt\' must be in base64 format"};
    }

    return {.Success = true};
};

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
