#include "sql_format.h"

#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

namespace NSQLFormat {

bool SqlFormatSimple(const TString& query, TString& formattedQuery, TString& error) {
    NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };

    NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
            /*isAmbiguityError=*/false,
            /*isAmbiguityDebugging=*/false),
    };

    return SqlFormatSimple(lexers, parsers, query, formattedQuery, error);
}

} // namespace NSQLFormat
