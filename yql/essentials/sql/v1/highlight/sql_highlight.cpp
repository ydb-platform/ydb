#include "sql_highlight.h"

#include <yql/essentials/sql/v1/lexer/regex/regex.h>

#include <contrib/libs/re2/re2/re2.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NSQLHighlight {

    using NSQLTranslationV1::Merged;
    using NSQLTranslationV1::TRegexPattern;

    struct Syntax {
        const NSQLReflect::TLexerGrammar* Grammar;
        THashMap<TString, TString> RegexesDefault;
        THashMap<TString, TString> RegexesANSI;

        TString Concat(const TVector<TStringBuf>& names) {
            TString concat;
            for (const auto& name : names) {
                concat += Get(name);
            }
            return concat;
        }

        TString Get(const TStringBuf name, bool ansi = false) const {
            if (Grammar->PunctuationNames.contains(name)) {
                return RE2::QuoteMeta(Grammar->BlockByName.at(name));
            }
            if (ansi) {
                return RegexesANSI.at(name);
            }
            return RegexesDefault.at(name);
        }
    };

    NSQLTranslationV1::TRegexPattern CaseInsensitive(TStringBuf text) {
        return {
            .Body = TString(text),
            .IsCaseInsensitive = true,
        };
    }

    template <EUnitKind K>
    TUnit MakeUnit(Syntax& syntax);

    template <>
    TUnit MakeUnit<EUnitKind::Keyword>(Syntax& s) {
        using NSQLReflect::TLexerGrammar;

        TUnit unit = {.Kind = EUnitKind::Keyword};
        for (const auto& keyword : s.Grammar->KeywordNames) {
            const TStringBuf content = TLexerGrammar::KeywordBlockByName(keyword);
            unit.Patterns.push_back(CaseInsensitive(content));
        }

        unit.Patterns = {Merged(std::move(unit.Patterns))};
        return unit;
    }

    template <>
    TUnit MakeUnit<EUnitKind::Punctuation>(Syntax& s) {
        TUnit unit = {.Kind = EUnitKind::Punctuation};
        for (const auto& name : s.Grammar->PunctuationNames) {
            const TString content = s.Get(name);
            unit.Patterns.push_back({content});
        }

        unit.Patterns = {Merged(std::move(unit.Patterns))};
        unit.IsPlain = false;
        unit.IsCodeGenExcluded = true;
        return unit;
    }

    template <>
    TUnit MakeUnit<EUnitKind::QuotedIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::QuotedIdentifier,
            .Patterns = {
                {s.Get("ID_QUOTED")},
            },
            .IsPlain = false,
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::BindParameterIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::BindParameterIdentifier,
            .Patterns = {
                {s.Concat({"DOLLAR", "ID_PLAIN"})},
            },
            .IsPlain = false,
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::TypeIdentifier>(Syntax& s) {
        TVector<NSQLTranslationV1::TRegexPattern> types;
        NJson::TJsonValue json = NJson::ReadJsonFastTree(NResource::Find("types.json"));
        for (const NJson::TJsonValue& value : json.GetArraySafe()) {
            types.emplace_back(CaseInsensitive(value["name"].GetStringSafe()));
        }

        return {
            .Kind = EUnitKind::TypeIdentifier,
            .Patterns = {
                {s.Get("ID_PLAIN"), s.Get("LESS")},
                {Merged(std::move(types))},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::FunctionIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::FunctionIdentifier,
            .Patterns = {
                {s.Concat({"ID_PLAIN", "NAMESPACE", "ID_PLAIN"})},
                {s.Get("ID_PLAIN"), s.Get("LPAREN")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Identifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::Identifier,
            .Patterns = {
                {s.Get("ID_PLAIN")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Literal>(Syntax& s) {
        return {
            .Kind = EUnitKind::Literal,
            .Patterns = {
                {s.Get("REAL")},
                {s.Get("INTEGER_VALUE")},
                {s.Get("DIGITS")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::StringLiteral>(Syntax& s) {
        return {
            .Kind = EUnitKind::StringLiteral,
            .Patterns = {{s.Get("STRING_VALUE")}},
            .PatternsANSI = TVector<TRegexPattern>{
                TRegexPattern{s.Get("STRING_VALUE", /* ansi = */ true)},
            },
            .RangePattern = TRangePattern{
                .Begin = "@@",
                .End = "@@",
            },
            .IsPlain = false,
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Comment>(Syntax& s) {
        return {
            .Kind = EUnitKind::Comment,
            .Patterns = {{s.Get("COMMENT")}},
            .PatternsANSI = Nothing(),
            .RangePattern = TRangePattern{
                .Begin = R"re(/\*)re",
                .End = R"re(\*/)re",
            },
            .IsPlain = false,
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Whitespace>(Syntax& s) {
        return {
            .Kind = EUnitKind::Whitespace,
            .Patterns = {
                {s.Get("WS")},
            },
            .IsPlain = false,
            .IsCodeGenExcluded = true,
        };
    }

    Syntax MakeSyntax(const NSQLReflect::TLexerGrammar& grammar) {
        using NSQLTranslationV1::MakeRegexByOtherName;

        Syntax syntax;
        syntax.Grammar = &grammar;
        for (auto& [k, v] : MakeRegexByOtherName(*syntax.Grammar, /* ansi = */ false)) {
            syntax.RegexesDefault.emplace(std::move(k), std::move(v));
        }
        for (auto& [k, v] : MakeRegexByOtherName(*syntax.Grammar, /* ansi = */ true)) {
            syntax.RegexesANSI.emplace(std::move(k), std::move(v));
        }
        return syntax;
    }

    THighlighting MakeHighlighting() {
        return MakeHighlighting(NSQLReflect::LoadLexerGrammar());
    }

    THighlighting MakeHighlighting(const NSQLReflect::TLexerGrammar& grammar) {
        Syntax s = MakeSyntax(grammar);

        THighlighting h;
        h.Units.emplace_back(MakeUnit<EUnitKind::Comment>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Punctuation>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::FunctionIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::TypeIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Keyword>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::QuotedIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::BindParameterIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Identifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Literal>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::StringLiteral>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Whitespace>(s));

        return h;
    }

} // namespace NSQLHighlight

template <>
void Out<NSQLHighlight::EUnitKind>(IOutputStream& out, NSQLHighlight::EUnitKind kind) {
    switch (kind) {
        case NSQLHighlight::EUnitKind::Keyword:
            out << "keyword";
            break;
        case NSQLHighlight::EUnitKind::Punctuation:
            out << "punctuation";
            break;
        case NSQLHighlight::EUnitKind::QuotedIdentifier:
            out << "quoted-identifier";
            break;
        case NSQLHighlight::EUnitKind::BindParameterIdentifier:
            out << "bind-parameter-identifier";
            break;
        case NSQLHighlight::EUnitKind::TypeIdentifier:
            out << "type-identifier";
            break;
        case NSQLHighlight::EUnitKind::FunctionIdentifier:
            out << "function-identifier";
            break;
        case NSQLHighlight::EUnitKind::Identifier:
            out << "identifier";
            break;
        case NSQLHighlight::EUnitKind::Literal:
            out << "literal";
            break;
        case NSQLHighlight::EUnitKind::StringLiteral:
            out << "string-literal";
            break;
        case NSQLHighlight::EUnitKind::Comment:
            out << "comment";
            break;
        case NSQLHighlight::EUnitKind::Whitespace:
            out << "ws";
            break;
        case NSQLHighlight::EUnitKind::Error:
            out << "error";
            break;
    }
}
