#pragma once

#include <yql/essentials/sql/v1/lexer/regex/generic.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

namespace NSQLHighlight {

enum class EUnitKind {
    Keyword,
    Punctuation,
    QuotedIdentifier,
    BindParameterIdentifier,
    OptionIdentifier,
    TypeIdentifier,
    FunctionIdentifier,
    Identifier,
    Literal,
    StringLiteral,
    Comment,
    Whitespace,
    Error,
};

struct TRangePattern {
    static constexpr const char* EmbeddedPythonBegin = "@@#py";
    static constexpr const char* EmbeddedJavaScriptBegin = "@@//js";

    TString BeginPlain;
    TString EndPlain;
    TMaybe<TString> EscapeRegex;
};

// Range patterns are expected to be matched before others.
struct TUnit {
    EUnitKind Kind;
    TVector<TRangePattern> RangePatterns;
    TVector<NSQLTranslationV1::TRegexPattern> Patterns;
    TMaybe<TVector<NSQLTranslationV1::TRegexPattern>> PatternsANSI;
    bool IsPlain = true;
    bool IsCodeGenExcluded = false;
};

struct THighlighting {
    TString Name = "YQL";
    TString Extension = "yql";
    TVector<TUnit> Units;
};

THighlighting MakeHighlighting();

THighlighting MakeHighlighting(const NSQLReflect::TLexerGrammar& grammar);

} // namespace NSQLHighlight
