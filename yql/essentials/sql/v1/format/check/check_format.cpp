#include "check_format.h"

#include "ast.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/statement.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/sql.h>

#include <yql/essentials/utils/yql_panic.h>

#include <util/string/builder.h>

namespace NSQLFormat {

bool ValidateAST(
    const TString& original,
    TMaybe<const NYql::TAstNode*> originalRes,
    const TString& formatted,
    const NSQLTranslation::TTranslationSettings& settings,
    const NSQLTranslationV1::TLexers& lexers,
    const NSQLTranslationV1::TParsers& parsers,
    NYql::TIssues& issues)
{
    NYql::TAstParseResult originalResHolder;
    if (!originalRes.Defined()) {
        originalResHolder = NSQLTranslationV1::SqlToYql(lexers, parsers, original, settings);
        originalRes = originalResHolder.Root;
    }

    const NYql::TAstNode* expectedYQLs = *originalRes;

    NYql::TAstParseResult formattedRes = NSQLTranslationV1::SqlToYql(lexers, parsers, formatted, settings);
    const NYql::TAstNode* formattedYQLs = formattedRes.Root;

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

    if (expectedYQLs && formattedYQLs) {
        TMaybe<bool> areEqual = AreAstEqual(expectedYQLs, formattedYQLs);
        if (areEqual && !*areEqual) {
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
    TMaybe<const NYql::TAstNode*> ast,
    NSQLTranslation::TTranslationSettings settings,
    NYql::TIssues& issues,
    EConvergenceRequirement convergence)
{
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)) {
        return Nothing();
    } else if (settings.PgParser) {
        return query;
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
