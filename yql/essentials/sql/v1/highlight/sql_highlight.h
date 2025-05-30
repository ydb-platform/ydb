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
        BindParamterIdentifier,
        TypeIdentifier,
        FunctionIdentifier,
        Identifier,
        Literal,
        StringLiteral,
        Comment,
        Whitespace,
        Error,
    };

    struct TUnit {
        EUnitKind Kind;
        TVector<NSQLTranslationV1::TRegexPattern> Patterns;
        TMaybe<TVector<NSQLTranslationV1::TRegexPattern>> PatternsANSI;
    };

    struct THighlighting {
        TVector<TUnit> Units;
    };

    THighlighting MakeHighlighting();

    THighlighting MakeHighlighting(const NSQLReflect::TLexerGrammar& grammar);

} // namespace NSQLHighlight
