#include "check_state.h"

#include "settings.h"
#include "utils.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>
#include <yql/essentials/parser/pg_wrapper/arena_ctx.h>
#include <yql/essentials/parser/lexer_common/hints.h>

namespace NYql::NFastCheck {

TCheckState::TCheckState(const TChecksRequest& request)
    : Request_(request)
{
}

TCheckState::~TCheckState() = default;

ESyntax TCheckState::GetEffectiveSyntax() {
    if (!ParsedSettingsCache_) {
        TParsedSettingsCache result;
        result.Success = NSQLTranslation::ParseTranslationSettingsFromComments(
            Request_.Program,
            result.Settings,
            result.Issues);
        ParsedSettingsCache_ = result;
    }

    if (ParsedSettingsCache_->Settings.HasPgParser) {
        return ESyntax::PG;
    }

    return Request_.Syntax;
}

bool TCheckState::CheckLexer(TIssues& issues) {
    if (LexerCache_) {
        const auto& cached = *LexerCache_;
        issues.AddIssues(cached.Issues);
        return cached.Success;
    }

    TLexerResult result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildLexerSettings(Request_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Success = false;
        LexerCache_ = result;
        issues.AddIssues(result.Issues);
        return false;
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer);

    result.Success = NSQLTranslation::CollectSqlHints(
        *lexer,
        Request_.Program,
        Request_.File,
        Request_.File,
        result.Hints,
        result.Issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        /*utf8Aware=*/true);

    LexerCache_ = result;
    issues.AddIssues(result.Issues);
    return result.Success;
}

google::protobuf::Message* TCheckState::ParseSql(TIssues& issues) {
    if (ParserCache_) {
        const auto& cached = *ParserCache_;
        issues.AddIssues(cached.Issues);
        return cached.Msg;
    }

    if (!CheckLexer(issues)) {
        return nullptr;
    }

    TParserResult result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildSqlParsingSettings(Request_, &Arena_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Msg = nullptr;
        ParserCache_ = result;
        issues.AddIssues(result.Issues);
        return nullptr;
    }

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    result.Msg = NSQLTranslationV1::SqlAST(
        parsers,
        Request_.Program,
        Request_.File,
        result.Issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        settings.AnsiLexer,
        true,
        &Arena_);

    ParserCache_ = result;
    issues.AddIssues(result.Issues);
    return result.Msg;
}

const TAstParseResult* TCheckState::TranslateSql(TIssues& issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        issues.AddIssues(cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildSqlTranslationSettings(Request_, &Arena_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        issues.AddIssues(TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    if (!CheckLexer(result.Issues)) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        issues.AddIssues(TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    auto* protoAst = ParseSql(result.Issues);
    if (!protoAst) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        issues.AddIssues(TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    result.Result = NSQLTranslationV1::SqlASTToYql(
        lexers,
        parsers,
        Request_.Program,
        *protoAst,
        LexerCache_->Hints,
        settings);
    result.Issues.AddIssues(result.Result.Issues);

    TranslateCache_ = std::move(result);

    const auto& cached = *TranslateCache_;
    issues.AddIssues(cached.Issues);
    return &cached.Result;
}

const NYql::TPGParseResult* TCheckState::ParsePg(TIssues& issues) {
    if (PgParserCache_) {
        const auto& cached = *PgParserCache_;
        issues.AddIssues(cached.Issues);
        return &cached.Result;
    }

    TPgParserResult result;
    NYql::PGParse(Request_.Program, result.Result);

    PgParserCache_ = std::move(result);
    issues.AddIssues(PgParserCache_->Issues);
    return &PgParserCache_->Result;
}

const TAstParseResult* TCheckState::TranslatePg(TIssues& issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        issues.AddIssues(cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;

    NSQLTranslation::TTranslationSettings settings;
    BuildPgTranslationSettings(Request_, &Arena_, settings);

    const auto* pgResult = ParsePg(result.Issues);
    if (!pgResult) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        issues.AddIssues(TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    result.Result = NSQLTranslationPG::PGToYql(*pgResult, Request_.Program, settings);
    result.Issues.AddIssues(result.Result.Issues);

    TranslateCache_ = std::move(result);

    const auto& cached = *TranslateCache_;
    issues.AddIssues(cached.Issues);
    return &cached.Result;
}

const TAstParseResult* TCheckState::ParseSExpr(TIssues& issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        issues.AddIssues(cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;
    result.Result = ParseAst(Request_.Program);
    result.Issues.AddIssues(result.Result.Issues);

    TranslateCache_ = std::move(result);

    const auto& cached = *TranslateCache_;
    issues.AddIssues(cached.Issues);
    return &cached.Result;
}

const TAstParseResult* TCheckState::TranslateSExpr(TIssues& issues) {
    return ParseSExpr(issues);
}

} // namespace NYql::NFastCheck
