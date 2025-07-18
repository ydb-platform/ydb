#include "generate_vim.h"

#include "generate.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/string/builder.h>

#include <ranges>

namespace NSQLHighlight {

    namespace {

        TString ToVim(TString regex) {
            static RE2 LikelyUnquotedLParen(R"((^|[^\\])(\())");
            static RE2 LikelyNonGreedyMatch(R"re((^|[^\\])(\*\?))re");

            // We can leave some capturing groups in case `\\\\(`,
            // but it is okay as the goal is to meet the Vim limit.

            YQL_ENSURE(!regex.Contains(R"(\\*?)"), "" << regex);

            RE2::GlobalReplace(&regex, LikelyUnquotedLParen, R"(\1%()");
            RE2::GlobalReplace(&regex, LikelyNonGreedyMatch, R"re(\1{-})re");

            return regex;
        }

        TString ToVim(EUnitKind kind, const NSQLTranslationV1::TRegexPattern& pattern) {
            TStringBuilder vim;

            vim << R"(")";
            vim << R"(\v)";

            if (IsPlain(kind)) {
                vim << R"(<)";
            }

            if (pattern.IsCaseInsensitive) {
                vim << R"(\c)";
            }

            vim << "(" << ToVim(pattern.Body) << ")";

            if (!pattern.After.empty()) {
                vim << "(" << ToVim(pattern.After) << ")@=";
            }

            if (IsPlain(kind)) {
                vim << R"(>)";
            }

            vim << R"(")";

            return vim;
        }

        TString ToVimName(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Keyword:
                    return "yqlKeyword";
                case EUnitKind::Punctuation:
                    return "yqlPunctuation";
                case EUnitKind::QuotedIdentifier:
                    return "yqlQuotedIdentifier";
                case EUnitKind::BindParameterIdentifier:
                    return "yqlBindParameterIdentifier";
                case EUnitKind::TypeIdentifier:
                    return "yqlTypeIdentifier";
                case EUnitKind::FunctionIdentifier:
                    return "yqlFunctionIdentifier";
                case EUnitKind::Identifier:
                    return "yqlIdentifier";
                case EUnitKind::Literal:
                    return "yqlLiteral";
                case EUnitKind::StringLiteral:
                    return "yqlStringLiteral";
                case EUnitKind::Comment:
                    return "yqlComment";
                case EUnitKind::Whitespace:
                    return "yqlWhitespace";
                case EUnitKind::Error:
                    return "yqlError";
            }
        }

        void PrintRules(IOutputStream& out, const TUnit& unit) {
            TString name = ToVimName(unit.Kind);
            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                out << "syn match " << ToVimName(unit.Kind) << " "
                    << ToVim(unit.Kind, pattern) << '\n';
            }
        }

        TVector<TStringBuf> ToVimGroups(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Keyword:
                    return {"Keyword"};
                case EUnitKind::Punctuation:
                    return {"Operator"};
                case EUnitKind::QuotedIdentifier:
                    return {"Special", "Underlined"};
                case EUnitKind::BindParameterIdentifier:
                    return {"Identifier"};
                case EUnitKind::TypeIdentifier:
                    return {"Type"};
                case EUnitKind::FunctionIdentifier:
                    return {"Function"};
                case EUnitKind::Identifier:
                    return {"Identifier"};
                case EUnitKind::Literal:
                    return {"Number"};
                case EUnitKind::StringLiteral:
                    return {"String"};
                case EUnitKind::Comment:
                    return {"Comment"};
                case EUnitKind::Whitespace:
                    return {};
                case EUnitKind::Error:
                    return {};
            }
        }

    } // namespace

    void GenerateVim(IOutputStream& out, const THighlighting& highlighting) {
        const auto units = std::ranges::reverse_view(highlighting.Units);

        out << "if exists(\"b:current_syntax\")" << '\n';
        out << "  finish" << '\n';
        out << "endif" << '\n';
        out << '\n';

        for (const TUnit& unit : units) {
            if (IsIgnored(unit.Kind)) {
                continue;
            }

            PrintRules(out, unit);
        }

        out << '\n';

        for (const TUnit& unit : units) {
            for (TStringBuf group : ToVimGroups(unit.Kind)) {
                out << "highlight default link " << ToVimName(unit.Kind) << " " << group << '\n';
            }
        }

        out << '\n';

        out << "let b:current_syntax = \"yql\"" << '\n';
        out.Flush();
    }

} // namespace NSQLHighlight
