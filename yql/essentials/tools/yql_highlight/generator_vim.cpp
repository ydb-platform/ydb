#include "generator_vim.h"

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

        TString ToVim(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
            TStringBuilder vim;

            vim << R"(")";
            vim << R"(\v)";

            if (unit.IsPlain) {
                vim << R"(<)";
            }

            if (pattern.IsCaseInsensitive) {
                vim << R"(\c)";
            }

            if (!pattern.Before.empty()) {
                vim << "(" << ToVim(pattern.Before) << ")@<=";
            }

            vim << "(" << ToVim(pattern.Body) << ")";

            if (!pattern.After.empty()) {
                vim << "(" << ToVim(pattern.After) << ")@=";
            }

            if (unit.IsPlain) {
                vim << R"(>)";
            }

            vim << R"(")";

            // Prevent a range pattern conflict
            if (unit.RangePattern) {
                SubstGlobal(vim, "|\\n", "");
            }

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

        TString VimRangeEscaped(TString range) {
            SubstGlobal(range, "*", "\\*");
            return range;
        }

        void PrintRules(IOutputStream& out, const TUnit& unit) {
            TString name = ToVimName(unit.Kind);
            for (const auto& pattern : std::ranges::reverse_view(unit.Patterns)) {
                out << "syn match " << ToVimName(unit.Kind) << " "
                    << ToVim(unit, pattern) << '\n';
            }
            if (auto range = unit.RangePattern) {
                out << "syntax region " << name << "Multiline" << " "
                    << "start=\"" << VimRangeEscaped(range->Begin) << "\" "
                    << "end=\"" << VimRangeEscaped(range->End) << "\""
                    << '\n';
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
                    return {"Define"};
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
        out << "if exists(\"b:current_syntax\")" << '\n';
        out << "  finish" << '\n';
        out << "endif" << '\n';
        out << '\n';

        for (const TUnit& unit : std::ranges::reverse_view(highlighting.Units)) {
            if (unit.IsCodeGenExcluded) {
                continue;
            }

            PrintRules(out, unit);
        }

        out << '\n';

        for (const TUnit& unit : std::ranges::reverse_view(highlighting.Units)) {
            TString name = ToVimName(unit.Kind);
            for (TStringBuf group : ToVimGroups(unit.Kind)) {
                out << "highlight default link " << name << "Multiline" << " " << group << '\n';
                out << "highlight default link " << name << " " << group << '\n';
            }
        }

        out << '\n';

        out << "let b:current_syntax = \"" << highlighting.Extension << "\"" << '\n';
        out.Flush();
    }

    IGenerator::TPtr MakeVimGenerator() {
        return MakeOnlyFileGenerator(GenerateVim);
    }

} // namespace NSQLHighlight
