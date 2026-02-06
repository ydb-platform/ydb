#include "check_lexers.h"

#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/regex/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/core/issue/yql_issue.h>

#include <util/string/builder.h>

namespace NSQLTranslationV1 {

namespace {

std::tuple<
    bool,
    TVector<NSQLTranslation::TParsedToken>,
    NYql::TIssues>
Tokenize(NSQLTranslation::ILexer& lexer, const TString& query) {
    bool isOk;
    TVector<NSQLTranslation::TParsedToken> tokens;
    NYql::TIssues issues;

    isOk = lexer.Tokenize(query, "", [&](auto token) {
        tokens.emplace_back(std::move(token));
    }, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS);

    return {isOk, std::move(tokens), std::move(issues)};
}

NYql::TIssuePtr Group(TString name, bool isOk, const NYql::TIssues& issues) {
    TString message = name + " is " + (isOk ? "OK" : "FAIL");

    NYql::TIssuePtr grouping = new NYql::TIssue(std::move(message));
    for (const NYql::TIssue& issue : issues) {
        grouping->AddSubIssue(new NYql::TIssue(issue));
    }

    return grouping;
}

} // namespace

bool CheckLexers(NYql::TPosition pos, const TString& query, NYql::TIssues& issues) {
    NSQLTranslationV1::TLexers lexers;
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)) {
        return false;
    }

    if (settings.PgParser) {
        return true;
    }

    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    auto lexerMain = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, NSQLTranslationV1::ELexerFlavor::Default);
    auto lexerPure = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, NSQLTranslationV1::ELexerFlavor::Pure);
    auto lexerRegex = NSQLTranslationV1::MakeRegexLexerFactory(settings.AnsiLexer)->MakeLexer();

    auto [isMainOk, mainTokens, mainIssues] = Tokenize(*lexerMain, query);
    auto [isPureOk, pureTokens, pureIssues] = Tokenize(*lexerPure, query);
    auto [isRegexOk, regexTokens, regexIssues] = Tokenize(*lexerRegex, query);

    if (!(isMainOk == isPureOk && isPureOk == isRegexOk)) {
        NYql::TIssue issue(pos, "Main, Pure, Lexer statuses differ");
        issue.AddSubIssue(Group("Main", isMainOk, std::move(mainIssues)));
        issue.AddSubIssue(Group("Pure", isPureOk, std::move(pureIssues)));
        issue.AddSubIssue(Group("Regex", isRegexOk, std::move(regexIssues)));
        return false;
    }

    if (!isMainOk) { // The lexer behaviour on invalid input is implementation defined
        return true;
    }

    bool hasErrors = false;
    auto check = [&](const char* name, const TVector<NSQLTranslation::TParsedToken>& otherTokens) {
        if (mainTokens.size() != otherTokens.size()) {
            hasErrors = true;
            issues.AddIssue(NYql::TIssue(pos, TStringBuilder() << "Mismatch token count, main: " << mainTokens.size() << ", " << name << ": " << otherTokens.size() << "\n"));
        }

        TStringBuilder textBuilder;

        for (size_t i = 0; i < Min(mainTokens.size(), otherTokens.size()); ++i) {
            if (mainTokens[i].Name != otherTokens[i].Name || mainTokens[i].Content != otherTokens[i].Content) {
                hasErrors = true;
                TStringBuilder err;
                err << "Mismatch token #" << i << ", main: " << mainTokens[i].Name << ":" << mainTokens[i].Content
                    << ", " << name << ": " << otherTokens[i].Name << ":" << otherTokens[i].Content << "\n";
                err << "Text sample: [";
                TString text = textBuilder;
                constexpr size_t LexerContextSample = 50;
                err << text.substr(text.size() >= LexerContextSample ? text.size() - LexerContextSample : 0u, LexerContextSample);
                err << "]\n";
                issues.AddIssue(NYql::TIssue(pos, err));
                break;
            }

            textBuilder << mainTokens[i].Content;
        }
    };

    check("pure", pureTokens);
    check("regex", regexTokens);
    return !hasErrors;
}

} // namespace NSQLTranslationV1
