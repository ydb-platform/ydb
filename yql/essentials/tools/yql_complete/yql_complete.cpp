#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/static/frequency.h>
#include <yql/essentials/sql/v1/complete/name/static/ranking.h>
#include <yql/essentials/sql/v1/complete/name/static/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/stream/file.h>

NSQLComplete::TFrequencyData LoadFrequencyDataFromFile(TString filepath) {
    TString text = TUnbufferedFileInput(filepath).ReadAll();
    return NSQLComplete::ParseJsonFrequencyData(text);
}

NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers, ansi, /* antlr4 = */ true,
            NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

int Run(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TString freqFileName;
    TMaybe<ui64> pos;
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('f', "freq", "frequences file").StoreResult(&freqFileName);
    opts.AddLongOption('p', "pos", "position").StoreResult(&pos);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    THolder<TUnbufferedFileInput> inFile;
    if (!inFileName.empty()) {
        inFile.Reset(new TUnbufferedFileInput(inFileName));
    }
    IInputStream& in = inFile ? *inFile.Get() : Cin;
    auto queryString = in.ReadAll();

    NSQLComplete::IRanking::TPtr ranking;
    if (freqFileName.empty()) {
        ranking = NSQLComplete::MakeDefaultRanking();
    } else {
        auto freq = LoadFrequencyDataFromFile(freqFileName);
        ranking = NSQLComplete::MakeDefaultRanking(std::move(freq));
    }
    auto engine = NSQLComplete::MakeSqlCompletionEngine(
        MakePureLexerSupplier(),
        NSQLComplete::MakeStaticNameService(
            NSQLComplete::MakeDefaultNameSet(),
            std::move(ranking)));

    NSQLComplete::TCompletionInput input;
    input.Text = queryString;
    if (pos) {
        input.CursorPosition = *pos;
    } else {
        input.CursorPosition = queryString.size();
    }

    auto output = engine->Complete(input);
    for (const auto& c : output.Candidates) {
        Cout << "[" << c.Kind << "] " << c.Content << "\n";
    }

    return 0;
}

int main(int argc, char* argv[]) {
    try {
        return Run(argc, argv);
    } catch (const yexception& e) {
        Cerr << "Caught exception:" << e.what() << Endl;
        return 1;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
