#pragma once

#include <yql/essentials/sql/v1/lexer/regex/generic.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

// TODO(vityaman): Migrate YDB to corrected version
#define BindParamterIdentifier BindParameterIdentifier // NOLINT

namespace NSQLHighlight {

    enum class EUnitKind {
        Keyword,
        Punctuation,
        QuotedIdentifier,
        BindParameterIdentifier,
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
        TString Begin;
        TString End;
    };

    struct TUnit {
        EUnitKind Kind;
        TVector<NSQLTranslationV1::TRegexPattern> Patterns;
        TMaybe<TVector<NSQLTranslationV1::TRegexPattern>> PatternsANSI;
        TMaybe<TRangePattern> RangePattern;
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
