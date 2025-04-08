#include "ydb_style.h"

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <library/cpp/diff/diff.h>

#include <util/system/file.h>

namespace NYdb::NConsoleClient {

    // FIXME: copy-pasted from the 'library/cpp/testing/unittest/registar.cpp'
    struct TDiffColorizer {
        NColorizer::TColors Colors;
        bool Reverse = false;

        explicit TDiffColorizer(bool reverse = false)
            : Reverse(reverse)
        {
        }

        TString Special(TStringBuf str) const {
            return ToString(Colors.YellowColor()) + str;
        }

        TString Common(TArrayRef<const char> str) const {
            return ToString(Colors.OldColor()) + TString(str.begin(), str.end());
        }

        TString Left(TArrayRef<const char> str) const {
            return ToString(GetLeftColor()) + TString(str.begin(), str.end());
        }

        TString Right(TArrayRef<const char> str) const {
            return ToString(GetRightColor()) + TString(str.begin(), str.end());
        }

        TStringBuf GetLeftColor() const {
            return Reverse ? Colors.RedColor() : Colors.GreenColor();
        }

        TStringBuf GetRightColor() const {
            return Reverse ? Colors.GreenColor() : Colors.RedColor();
        }
    };

    TCommandStyle::TCommandStyle()
        : TYdbCommand("style", {}, "Run SQL styler")
    {
    }

    void TCommandStyle::Config(TConfig& config) {
        TYdbCommand::Config(config);

        config.NeedToConnect = false;

        config.Opts
            ->AddLongOption("check", "Do not style inplace, just check")
            .StoreTrue(&IsChecking);

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
        Cout << "Error: " << e.AsStrBuf().After(':').After(':').Skip(1) << Endl;
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

        if (IsChecking) {
            Cerr << "Checked";
        } else {
            Cerr << "Formatted";
        }
        Cerr << " the file '" << path << "'" << Endl;
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
        if (!NSQLFormat::SqlFormatSimple(lexers, parsers, text, formatted, error)) {
            return false;
        }

        if (IsChecking && text != formatted) {
            TStringStream message;
            message << "file is not properly formatted\n";

            TVector<NDiff::TChunk<char>> diff;
            NDiff::InlineDiff(diff, formatted, text);
            NDiff::PrintChunks(message, TDiffColorizer(), diff);

            error = message.Str();

            return false;
        }

        return true;
    }

} // namespace NYdb::NConsoleClient
