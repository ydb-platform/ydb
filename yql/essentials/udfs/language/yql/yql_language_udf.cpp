#include <yql/essentials/public/udf/udf_helpers.h>

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

using namespace NYql;
using namespace NKikimr::NUdf;
using namespace NSQLTranslation;

SIMPLE_UDF(TObfuscate, TOptional<char*>(TAutoMap<char*>)) {
    using namespace NSQLFormat;
    try {
        const auto sqlRef = args[0].AsStringRef();
        TString formattedQuery;
        NYql::TIssues issues;
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        if (!MakeSqlFormatter(lexers, parsers, settings)->Format(TString(sqlRef), formattedQuery, issues, EFormatMode::Obfuscate)) {
            return {};
        }

        return valueBuilder->NewString(formattedQuery);
    } catch (const yexception&) {
        return {};
    }
}

SIMPLE_MODULE(TYqlLangModule,
    TObfuscate
);

REGISTER_MODULES(TYqlLangModule);
