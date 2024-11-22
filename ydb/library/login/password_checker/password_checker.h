#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <unordered_set>

namespace NLogin {

class TPasswordCheckParameters {
public:
    struct TInitializer {
        ui32 MinPasswordLength = 8;
        ui32 MaxPasswordLength = 15;
        bool NeedLowerCase = true;
        bool NeedUpperCase = true;
        bool NeedNumbers = true;
        bool NeedSpecialSymbols = true;
        std::unordered_set<char> SpecialSymbols = VALID_SPECIAL_SYMBOLS;
        bool EnableEmptyPassword = true;
    };

private:
    static const std::unordered_set<char> VALID_SPECIAL_SYMBOLS;

    ui32 MinPasswordLength = 8;
    ui32 MaxPasswordLength = 15;
    bool NeedLowerCase = true;
    bool NeedUpperCase = true;
    bool NeedNumbers = true;
    bool NeedSpecialSymbols = true;
    std::unordered_set<char> SpecialSymbols;
    bool EnableEmptyPassword = true;

public:
    TPasswordCheckParameters();
    TPasswordCheckParameters(const TInitializer& initializer);

    ui32 GetMinPasswordLength() const;
    ui32 GetMaxPasswordLength() const;
    bool NeedLowerCaseUse() const;
    bool NeedUpperCaseUse() const;
    bool NeedNumbersUse() const;
    bool NeedSpecialSymbolsUse() const;
    bool IsEmptyPasswordEnable() const;
    bool IsSpecialSymbolValid(char symbol) const;
};

class TPasswordChecker {
public:
    struct TResult {
        bool Success = true;
        TString Error;
    };

private:
    class TFlagsStore {
    private:
        bool LowerCase = false;
        bool UpperCase = false;
        bool Number = false;
        bool SpecialSymbol = false;

    public:
        void SetLowerCase(bool flag);
        void SetUpperCase(bool flag);
        void SetNumber(bool flag);
        void SetSpecialSymbol(bool flag);

        bool HasLowerCase() const;
        bool HasUpperCase() const;
        bool HasNumber() const;
        bool HasSpecialSymbol() const;
    };

private:
    TPasswordCheckParameters CheckParameters;

public:
    TPasswordChecker(const TPasswordCheckParameters& checkParameters);
    TResult Check(const TString& username, const TString& password) const;
};

} // NLogin
