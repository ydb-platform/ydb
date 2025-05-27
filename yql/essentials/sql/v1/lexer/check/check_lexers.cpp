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

bool CheckLexers(NYql::TPosition pos, const TString& query, NYql::TIssues& issues) {
    NSQLTranslationV1::TLexers lexers;
    NSQLTranslation::TTranslationSettings settings;
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)) {
        return false;
    }

    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    auto lexerMain = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true, NSQLTranslationV1::ELexerFlavor::Default);
    auto lexerPure = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true, NSQLTranslationV1::ELexerFlavor::Pure);
    auto lexerRegex = NSQLTranslationV1::MakeRegexLexerFactory(settings.AnsiLexer)->MakeLexer();
    TVector<NSQLTranslation::TParsedToken> mainTokens;
    if (!lexerMain->Tokenize(query, "", [&](auto token) { mainTokens.push_back(token);}, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return false;
    }

    TVector<NSQLTranslation::TParsedToken> pureTokens;
    if (!lexerPure->Tokenize(query, "", [&](auto token) { pureTokens.push_back(token);}, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return false;
    }

    TVector<NSQLTranslation::TParsedToken> regexTokens;
    if (!lexerRegex->Tokenize(query, "", [&](auto token) { regexTokens.push_back(token);}, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return false;
    }

    bool hasErrors = false;
    auto check = [&](const char* name, const TVector<NSQLTranslation::TParsedToken>& otherTokens) {
        if (mainTokens.size() != otherTokens.size()) {
            hasErrors = true;
            issues.AddIssue(NYql::TIssue(pos, TStringBuilder () << "Mismatch token count, main: " <<
                mainTokens.size() << ", " << name << ": " << otherTokens.size() << "\n"));
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

}
