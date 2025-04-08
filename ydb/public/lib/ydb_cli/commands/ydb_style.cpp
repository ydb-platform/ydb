#include "ydb_style.h"

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

namespace NYdb::NConsoleClient {

    TCommandStyle::TCommandStyle()
        : TYdbCommand("style", {}, "Run SQL styler")
    {
    }

    void TCommandStyle::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.NeedToConnect = false;
        config.SetFreeArgsNum(0);
    }

    void TCommandStyle::Parse(TConfig& config) {
        TClientCommand::Parse(config);
    }

    int TCommandStyle::Run(TConfig&) {
        NSQLTranslationV1::TLexers lexers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
        };

        NSQLTranslationV1::TParsers parsers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
        };

        IInputStream& input = Cin;
        TString text = input.ReadAll();

        TString formatted;
        TString error;
        if (!NSQLFormat::SqlFormatSimple(lexers, parsers, text, formatted, error)) {
            Cerr << "Error: " << error << Endl;
            return EXIT_FAILURE;
        }

        Cout << formatted;
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient
