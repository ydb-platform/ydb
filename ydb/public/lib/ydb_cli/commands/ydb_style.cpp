#include "ydb_style.h"

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <util/system/file.h>

namespace NYdb::NConsoleClient {

    TCommandStyle::TCommandStyle()
        : TYdbCommand("style", {}, "Run SQL styler")
    {
    }

    void TCommandStyle::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.NeedToConnect = false;
        config.SetFreeArgsMin(0);
        SetFreeArgTitle(0, "<path>", "File or directory path");
    }

    void TCommandStyle::Parse(TConfig& config) {
        TClientCommand::Parse(config);
        for (auto& path : config.ParseResult->GetFreeArgs()) {
            Paths.emplace_back(std::move(path));
        }
    }

    int TCommandStyle::Run(TConfig&) try {
        if (Paths.empty()) {
            return RunStd();
        }
        for (const auto& path : Paths) {
            FormatDEntry(path);
        }
        return EXIT_SUCCESS;
    } catch (const yexception& e) {
        return EXIT_FAILURE;
    }

    int TCommandStyle::RunStd() {
        TString formatted, error;
        if (!Format(Cin, formatted, error)) {
            Cerr << "Error: " << error << Endl;
            return EXIT_FAILURE;
        } else {
            Cout << formatted;
            return EXIT_SUCCESS;
        }
    }

    void TCommandStyle::FormatDEntry(const TFsPath& path, bool isSkippingNoSql) {
        if (!path.Exists()) {
            ythrow yexception() << "path '" << path << "' does not exist";
        }

        if (path.IsFile()) {
            if (isSkippingNoSql && !path.GetName().EndsWith(".sql")) {
                return;
            }

            FormatFile(path);
        } else if (path.IsDirectory() && !path.IsSymlink()) {
            FormatDirectory(path);
        } else {
            ythrow yexception() << "unexpected path state: is not a file and is not a directory";
        }
    }

    void TCommandStyle::FormatFile(const TFsPath& path) {
        Y_ENSURE(path.IsFile());

        TUnbufferedFileInput input(path);
        TString formatted, error;
        if (!Format(input, formatted, error)) {
            ythrow yexception() << "styling a file '" << path << "' failed: " << error;
        }
        Y_UNUSED(std::move(input));

        TFile file(path.GetPath(), EOpenModeFlag::RdWr);
        file.Write(formatted.data(), formatted.size());

        Cerr << "Formatted the file '" << path << "'" << Endl;
    }

    void TCommandStyle::FormatDirectory(const TFsPath& path) {
        Y_ENSURE(path.IsDirectory());

        TVector<TFsPath> children;
        path.List(children);

        for (const auto& path : children) {
            FormatDEntry(path, /* isSkippingNoSql = */ true);
        }
    }

    bool TCommandStyle::Format(IInputStream& input, TString& formatted, TString& error) {
        NSQLTranslationV1::TLexers lexers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
        };

        NSQLTranslationV1::TParsers parsers = {
            .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
            .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
        };

        TString text = input.ReadAll();
        return NSQLFormat::SqlFormatSimple(lexers, parsers, text, formatted, error);
    }

} // namespace NYdb::NConsoleClient
