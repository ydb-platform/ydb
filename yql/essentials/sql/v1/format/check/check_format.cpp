#include "check_format.h"

#include <util/string/builder.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/statement.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NSQLFormat {

bool IsQueryTextMayPresent(const google::protobuf::Message* message) {
    const auto& root = static_cast<const NSQLTranslationV1::TSQLv1ParserAST&>(*message);
    auto statementNames = NSQLTranslationV1::StatementNames(root.GetRule_sql_query());
    return AnyOf(statementNames, [](const auto& x) {
        return x.Internal == "CreateView";
    });
}

google::protobuf::Message* SqlToProtoAst(
    const NSQLTranslationV1::TParsers& parsers,
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    NSQLTranslation::TTranslationSettings& effectiveSettings,
    NYql::TIssues& issues)
{
    effectiveSettings = settings;
    if (!NSQLTranslation::ParseTranslationSettings(query, effectiveSettings, issues)) {
        return nullptr;
    }

    return NSQLTranslationV1::SqlAST(
        parsers, query, effectiveSettings.File, issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        effectiveSettings.AnsiLexer, effectiveSettings.Arena);
}

NYql::TAstParseResult ProtoAstToAst(
    const NSQLTranslationV1::TLexers& lexers,
    const NSQLTranslationV1::TParsers& parsers,
    const TString& query,
    const google::protobuf::Message* protoAst,
    const NSQLTranslation::TTranslationSettings& effectiveSettings)
{
    NSQLTranslation::TTranslators translators(
        /* V0 = */ nullptr,
        MakeTranslator(lexers, parsers),
        /* PG = */ nullptr);

    auto lexer = MakeLexer(lexers, effectiveSettings.AnsiLexer);
    YQL_ENSURE(lexer);

    NSQLTranslation::TSQLHints hints;
    if (NYql::TAstParseResult res; !CollectSqlHints(
            *lexer,
            query,
            effectiveSettings.File,
            effectiveSettings.File,
            hints,
            res.Issues,
            effectiveSettings.MaxErrors,
            /*utf8Aware=*/true))
    {
        return res;
    }

    return NSQLTranslation::SqlASTToYql(
        translators, query, *protoAst, hints, effectiveSettings);
}

bool ValidateAST(
    const TString& originalQuery,
    const NYql::TAstNode* originalAst,
    const TString& formatted,
    const NSQLTranslation::TTranslationSettings& settings,
    const NSQLTranslationV1::TLexers& lexers,
    const NSQLTranslationV1::TParsers& parsers,
    NYql::TIssues& issues)
{
    NSQLTranslation::TTranslationSettings formattedEffectiveSettings;
    const auto* formattedProtoAst = SqlToProtoAst(
        parsers, formatted, settings, formattedEffectiveSettings, issues);
    if (!formattedProtoAst) {
        return false;
    }

    NYql::TAstParseResult formattedRes = ProtoAstToAst(
        lexers, parsers, formatted, formattedProtoAst, formattedEffectiveSettings);

    const NYql::TAstNode* formattedYQLs = formattedRes.Root;
    const NYql::TAstNode* expectedYQLs = originalAst;

    const bool formattedIsOk = static_cast<bool>(formattedYQLs);
    const bool expectedIsOk = static_cast<bool>(expectedYQLs);
    if (expectedIsOk != formattedIsOk) {
        issues.AddIssue(
            TStringBuilder()
            << "Formatter changed semantics: "
            << "original was " << (expectedIsOk ? "OK" : "BAD")
            << ", but "
            << "formatted is " << (formattedIsOk ? "OK" : "BAD"));
        return false;
    }

    if (IsQueryTextMayPresent(formattedProtoAst)) {
        Y_UNUSED(originalQuery);
        return true; // TODO(YQL-21134): normalize embedded query text;
    }

    if (expectedYQLs && formattedYQLs) {
        const auto printFlags = NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote;

        TStringStream formattedYQLsText;
        formattedYQLs->PrettyPrintTo(formattedYQLsText, printFlags);

        TStringStream expectedYQLsText;
        expectedYQLs->PrettyPrintTo(expectedYQLsText, printFlags);

        if (expectedYQLsText.Str() != formattedYQLsText.Str()) {
            issues.AddIssue("Source query's AST and formatted query's AST are not same");
            return false;
        }
    }

    return true;
}

bool ValidateConvergence(
    const ISqlFormatter::TPtr& formatter,
    const TString& formatted,
    NYql::TIssues& issues,
    EConvergenceRequirement convergence)
{
    switch (convergence) {
        case EConvergenceRequirement::None: {
            return true;
        }
        case EConvergenceRequirement::Triple: {
            TString formatted2;
            if (!formatter->Format(formatted, formatted2, issues)) {
                return false;
            }

            TString formatted3;
            if (!formatter->Format(formatted2, formatted3, issues)) {
                return false;
            }

            if (formatted2 != formatted3) {
                issues.AddIssue(
                    TStringBuilder()
                    << "Triple formatting check failed. "
                    << "Formatting a doubly formatted query yielded a different result.");
                return false;
            }

            return true;
        }
        case EConvergenceRequirement::Double: {
            TString formatted2;
            if (!formatter->Format(formatted, formatted2, issues)) {
                return false;
            }

            if (formatted != formatted2) {
                issues.AddIssue(
                    TStringBuilder()
                    << "Double formatting check failed. "
                    << "Formatting an already formatted query yielded a different result. "
                    << "Add /* skip double format */ to suppress");
                return false;
            }

            return true;
        }
    }
}

TMaybe<TString> CheckedFormat(
    const TString& query,
    const NYql::TAstNode* ast,
    const NSQLTranslation::TTranslationSettings& settings,
    NYql::TIssues& issues,
    EConvergenceRequirement convergence)
{
    if (NSQLTranslation::TTranslationSettings effective;
        !NSQLTranslation::ParseTranslationSettings(query, effective, issues))
    {
        return Nothing();
    } else if (effective.PgParser) {
        return query;
    }

    NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
    };

    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);

    TString formatted;
    if (!formatter->Format(query, formatted, issues)) {
        return Nothing();
    }

    if (!ValidateAST(query, ast, formatted, settings, lexers, parsers, issues)) {
        return Nothing();
    }

    if (!ValidateConvergence(formatter, formatted, issues, convergence)) {
        return Nothing();
    }

    return formatted;
}

} // namespace NSQLFormat
