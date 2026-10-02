#include "check_state.h"

#include "settings.h"
#include "utils.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>
#include <yql/essentials/parser/pg_wrapper/arena_ctx.h>
#include <yql/essentials/parser/lexer_common/hints.h>

namespace NYql::NFastCheck {

namespace {

class TPGParseEvents final: public IPGParseEvents {
public:
    explicit TPGParseEvents(TIssues* issues = nullptr)
        : Issues_(issues)
    {
    }

    bool IsSuccessful() const {
        return IsSuccessful_;
    }

    void OnResult(const List* raw) final {
        Y_UNUSED(raw);
        IsSuccessful_ = true;
    }

    void OnError(const TIssue& issue) final {
        if (Issues_) {
            Issues_->AddIssue(issue);
        }
    }

private:
    bool IsSuccessful_ = false;
    TIssues* Issues_;
};

bool IsOk(const TPGParseResult& result, TIssues* issues = nullptr) {
    TPGParseEvents v(issues);
    result.Visit(v);
    return v.IsSuccessful();
}

void AddIssues(TIssues* output, const TIssues& issues) {
    if (output) {
        output->AddIssues(issues);
    }
}

} // namespace

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

bool TCheckState::CheckLexer(TIssues* issues) {
    if (LexerCache_) {
        const auto& cached = *LexerCache_;
        AddIssues(issues, cached.Issues);
        return cached.Success;
    }

    TLexerResult result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildLexerSettings(Request_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Success = false;
        LexerCache_ = result;
        AddIssues(issues, result.Issues);
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
    AddIssues(issues, result.Issues);
    return result.Success;
}

google::protobuf::Message* TCheckState::ParseSql(TIssues* issues) {
    if (ParserCache_) {
        const auto& cached = *ParserCache_;
        AddIssues(issues, cached.Issues);
        return cached.Msg;
    }

    if (!CheckLexer(Request_.SuppressPrerequisiteIssues ? nullptr : issues)) {
        return nullptr;
    }

    TParserResult result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildSqlParsingSettings(Request_, &Arena_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Msg = nullptr;
        ParserCache_ = result;
        AddIssues(issues, result.Issues);
        return nullptr;
    }

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            settings.MaxParseTreeDepth),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            settings.MaxParseTreeDepth),
    };

    result.Msg = NSQLTranslationV1::SqlAST(
        parsers,
        Request_.Program,
        Request_.File,
        result.Issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        settings.AnsiLexer,
        /*antlr4=*/true,
        &Arena_);

    ParserCache_ = result;
    AddIssues(issues, result.Issues);
    return result.Msg;
}

const TAstParseResult* TCheckState::TranslateSql(TIssues* issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        AddIssues(issues, cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;

    NSQLTranslation::TTranslationSettings settings;
    if (!BuildSqlTranslationSettings(Request_, &Arena_, settings, result.Issues, ParsedSettingsCache_)) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        AddIssues(issues, TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    TIssues* issueSink = Request_.SuppressPrerequisiteIssues ? nullptr : &result.Issues;

    if (!CheckLexer(issueSink)) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        AddIssues(issues, TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    auto* protoAst = ParseSql(issueSink);
    if (!protoAst) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        AddIssues(issues, TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            settings.MaxParseTreeDepth),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false,
            settings.MaxParseTreeDepth),
    };

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
    AddIssues(issues, cached.Issues);
    return &cached.Result;
}

const NYql::TPGParseResult* TCheckState::ParsePg(TIssues* issues) {
    if (PgParserCache_) {
        const auto& cached = *PgParserCache_;
        AddIssues(issues, cached.Issues);
        return IsOk(cached.Result) ? &cached.Result : nullptr;
    }

    TPgParserResult result;
    NYql::PGParse(Request_.Program, result.Result);
    const bool isOk = IsOk(result.Result, &result.Issues);

    PgParserCache_ = std::move(result);
    AddIssues(issues, PgParserCache_->Issues);
    return isOk ? &PgParserCache_->Result : nullptr;
}

const TAstParseResult* TCheckState::TranslatePg(TIssues* issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        AddIssues(issues, cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;

    NSQLTranslation::TTranslationSettings settings;
    BuildPgTranslationSettings(Request_, &Arena_, settings);

    const auto* pgResult = ParsePg(Request_.SuppressPrerequisiteIssues ? nullptr : &result.Issues);
    if (!pgResult) {
        result.Result = TAstParseResult();
        TranslateCache_ = std::move(result);
        AddIssues(issues, TranslateCache_->Issues);
        return &TranslateCache_->Result;
    }

    result.Result = NSQLTranslationPG::PGToYql(*pgResult, Request_.Program, settings);
    result.Issues.AddIssues(result.Result.Issues);

    TranslateCache_ = std::move(result);

    const auto& cached = *TranslateCache_;
    AddIssues(issues, cached.Issues);
    return &cached.Result;
}

const TAstParseResult* TCheckState::ParseSExpr(TIssues* issues) {
    if (TranslateCache_) {
        const auto& cached = *TranslateCache_;
        AddIssues(issues, cached.Issues);
        return &cached.Result;
    }

    TAstResultCache result;
    result.Result = ParseAst(Request_.Program);
    result.Issues.AddIssues(result.Result.Issues);

    TranslateCache_ = std::move(result);

    const auto& cached = *TranslateCache_;
    AddIssues(issues, cached.Issues);
    return &cached.Result;
}

const TAstParseResult* TCheckState::TranslateSExpr(TIssues* issues) {
    return ParseSExpr(Request_.SuppressPrerequisiteIssues ? nullptr : issues);
}

} // namespace NYql::NFastCheck
