#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <unordered_set>

namespace NLogin {

class TPasswordComplexity {
public:
    struct TInitializer {
        size_t MinLength = 0;
        size_t MinLowerCaseCount = 0;
        size_t MinUpperCaseCount = 0;
        size_t MinNumbersCount = 0;
        size_t MinSpecialCharsCount = 0;
        TString SpecialChars;
        bool CanContainUsername = false;
    };

    static const std::unordered_set<char> VALID_SPECIAL_CHARS;

    size_t MinLength = 0;
    size_t MinLowerCaseCount = 0;
    size_t MinUpperCaseCount = 0;
    size_t MinNumbersCount = 0;
    size_t MinSpecialCharsCount = 0;
    std::unordered_set<char> SpecialChars;
    bool CanContainUsername = false;

    TPasswordComplexity();
    TPasswordComplexity(const TInitializer& initializer);

    bool IsSpecialCharValid(char ch) const;
};

class TPasswordChecker {
public:
    struct TResult {
        bool Success = true;
        TString Error;
    };

private:
    class TComplexityState {
    private:
        size_t LowerCaseCount = 0;
        size_t UpperCaseCount = 0;
        size_t NumbersCount = 0;
        size_t SpecialCharsCount = 0;

        const TPasswordComplexity& PasswordComplexity;

    public:
        TComplexityState(const TPasswordComplexity& passwordComplexity);

        void IncLowerCaseCount();
        void IncUpperCaseCount();
        void IncNumbersCount();
        void IncSpecialCharsCount();

        bool CheckLowerCaseCount() const;
        bool CheckUpperCaseCount() const;
        bool CheckNumbersCount() const;
        bool CheckSpecialCharsCount() const;
    };

private:
    TPasswordComplexity PasswordComplexity;

public:
    TPasswordChecker(const TPasswordComplexity& passwordComplexity);
    TResult Check(const TString& username, const TString& password) const;
    void Update(const TPasswordComplexity& passwordComplexity);
};

} // NLogin
