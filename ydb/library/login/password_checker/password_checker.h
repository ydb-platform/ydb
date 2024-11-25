#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <limits>
#include <unordered_set>

namespace NLogin {

class TPasswordCheckParameters {
public:
    struct TInitializer {
        ui32 MinPasswordLength = 0;
        ui32 MaxPasswordLength = std::numeric_limits<ui32>::max();
        bool NeedLowerCase = false;
        bool NeedUpperCase = false;
        bool NeedNumbers = false;
        bool NeedSpecialSymbols = false;
        TString SpecialSymbols;
    };

    static const std::unordered_set<char> VALID_SPECIAL_SYMBOLS;

private:
    ui32 MinPasswordLength = 0;
    ui32 MaxPasswordLength = std::numeric_limits<ui32>::max();
    bool NeedLowerCase = false;
    bool NeedUpperCase = false;
    bool NeedNumbers = false;
    bool NeedSpecialSymbols = false;
    std::unordered_set<char> SpecialSymbols;

public:
    TPasswordCheckParameters();
    TPasswordCheckParameters(const TInitializer& initializer);

    ui32 GetMinPasswordLength() const;
    ui32 GetMaxPasswordLength() const;
    bool NeedLowerCaseUse() const;
    bool NeedUpperCaseUse() const;
    bool NeedNumbersUse() const;
    bool NeedSpecialSymbolsUse() const;
    bool IsSpecialSymbolValid(char symbol) const;

    void SetMinPasswordLength(ui32 length);
    void SetMaxPasswordLength(ui32 length);
    void SetLowerCaseUse(bool flag);
    void SetUpperCaseUse(bool flag);
    void SetNumbersUse(bool flag);
    void SetSpecialSymbolsUse(bool flag);
    void SetSpecialSymbols(const TString& specialSymbols);
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
    void Update(const TPasswordCheckParameters& checkParameters);
};

} // NLogin
