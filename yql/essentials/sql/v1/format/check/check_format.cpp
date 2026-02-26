#include "check_format.h"

#include <util/string/builder.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NSQLFormat {

bool Validate(
    const NYql::TAstParseResult& original,
    const NYql::TAstParseResult& formatted,
    NYql::TIssues& issues)
{
    if (original.IsOk() != formatted.IsOk()) {
        issues.AddIssue(
            TStringBuilder()
            << "Formatter changed semantics: "
            << "original was " << (original.IsOk() ? "OK" : "BAD")
            << ", but "
            << "formatted is " << (original.IsOk() ? "OK" : "BAD"));
        return false;
    }

    return true;
}

TMaybe<TString> CheckedFormat(
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    NYql::TIssues& issues,
    bool isIdempotencyChecked)
{
    NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
    };

    NSQLTranslation::TTranslators translators(
        /* V0 = */ nullptr,
        MakeTranslator(lexers, parsers),
        /* PG = */ nullptr);

    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);

    TString formatted;
    if (!formatter->Format(query, formatted, issues)) {
        return Nothing();
    }

    NYql::TAstParseResult expectedYQLs = NSQLTranslation::SqlToYql(translators, query, settings);
    NYql::TAstParseResult formattedYQLs = NSQLTranslation::SqlToYql(translators, formatted, settings);
    if (!Validate(expectedYQLs, formattedYQLs, issues)) {
        return Nothing();
    }

    if (expectedYQLs.IsOk() && formattedYQLs.IsOk()) {
        const auto printFlags = NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote;

        TStringStream formattedYQLsText;
        formattedYQLs.Root->PrettyPrintTo(formattedYQLsText, printFlags);

        TStringStream expectedYQLsText;
        expectedYQLs.Root->PrettyPrintTo(expectedYQLsText, printFlags);

        if (expectedYQLsText.Str() != formattedYQLsText.Str()) {
            issues.AddIssue("Source query's AST and formatted query's AST are not same");
            return Nothing();
        }
    }

    if (isIdempotencyChecked) {
        TString formatted2;
        if (!formatter->Format(formatted, formatted2, issues)) {
            return Nothing();
        }

        if (formatted != formatted2) {
            issues.AddIssue(
                TStringBuilder()
                << "Formatting an already formatted query yielded a different result. "
                << "Add /* skip double format */ to suppress");
            return Nothing();
        }
    }

    return formatted;
}

} // namespace NSQLFormat
