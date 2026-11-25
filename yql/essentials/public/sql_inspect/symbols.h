#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <variant>

namespace NYql::NSqlInspect {

enum class ESymbolType {
    Import,
    Export,
    Parameter,
};

struct TSymbolSettings {
    bool Ansi = false;
    // Currently does not restrict accepted syntax: v1 parsing is language-version independent.
    TLangVersion LangVer = MinLangVersion;
    // Unset selects all types; an empty set selects none. SQL is always parsed.
    TMaybe<TSet<ESymbolType>> Filter;
};

struct TImportSymbols {
    TString Name;
    // Unquoted path components, excluding the leading-dot marker.
    TVector<TString> Module;
    bool IsAbsolute = false;
};

struct TExportSymbol {
    TString Name;
};

struct TParameterSymbol {
    TString Name;
};

struct TSymbols {
    TVector<TImportSymbols> Imports;
    TVector<TExportSymbol> Exports;
    TVector<TParameterSymbol> Parameters;
};

using TSymbolsResult = std::variant<TIssues, TSymbols>;

// Names are unquoted and exclude '$'. Imports contain original names, never aliases.
// Parameters come only from DECLARE statements, not variable definitions or references.
// Imports (including nested imports) are unique and sorted by (IsAbsolute, Module, Name).
// Exports and parameters are unique and sorted by Name (case-sensitive).
// Only syntax is checked: modules and symbol definitions are not resolved.
TSymbolsResult GetSymbols(const TString& sql, const TSymbolSettings& settings = {});

} // namespace NYql::NSqlInspect
