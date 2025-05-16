#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <yql/essentials/utils/utf8.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/charset/utf8.h>
#include <util/stream/file.h>

NSQLComplete::TFrequencyData LoadFrequencyDataFromFile(TString filepath) {
    TString text = TUnbufferedFileInput(filepath).ReadAll();
    return NSQLComplete::Pruned(NSQLComplete::ParseJsonFrequencyData(text));
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

size_t UTF8PositionToBytes(const TStringBuf text, size_t position) {
    const TStringBuf substr = SubstrUTF8(text, position, text.length());
    return substr.begin() - text.begin();
}

int Run(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TString inQueryText;
    TString freqFileName;
    TMaybe<ui64> pos;
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('q', "query", "input query text").RequiredArgument("query").StoreResult(&inQueryText);
    opts.AddLongOption('f', "freq", "frequences file").StoreResult(&freqFileName);
    opts.AddLongOption('p', "pos", "position").StoreResult(&pos);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (res.Has("input") && res.Has("query")) {
        ythrow yexception() << "use either 'input' or 'query', not both";
    }

    TString queryString;
    if (res.Has("query")) {
        queryString = std::move(inQueryText);
    } else {
        THolder<TUnbufferedFileInput> inFile;
        if (!inFileName.empty()) {
            inFile.Reset(new TUnbufferedFileInput(inFileName));
        }
        IInputStream& in = inFile ? *inFile.Get() : Cin;
        queryString = in.ReadAll();
    }

    NSQLComplete::TFrequencyData frequency;
    if (freqFileName.empty()) {
        frequency = NSQLComplete::LoadFrequencyData();
    } else {
        frequency = LoadFrequencyDataFromFile(freqFileName);
    }

    auto engine = NSQLComplete::MakeSqlCompletionEngine(
        MakePureLexerSupplier(),
        NSQLComplete::MakeStaticNameService(
            NSQLComplete::LoadDefaultNameSet(), 
            std::move(frequency)));

    NSQLComplete::TCompletionInput input;

    input.Text = queryString;
    if (!NYql::IsUtf8(input.Text)) {
        ythrow yexception() << "provided input is not UTF encoded";
    }

    if (pos) {
        input.CursorPosition = UTF8PositionToBytes(input.Text, *pos);
    } else if (Count(input.Text, '#') == 1) {
        Cerr << "Note: found an only '#', setting the cursor position\n";
        input.CursorPosition = input.Text.find('#');
    } else if (Count(input.Text, '#') >= 2) {
        Cerr << "Note: found multiple '#', defaulting the cursor position\n";
        input.CursorPosition = queryString.size();
    } else {
        input.CursorPosition = queryString.size();
    }

    auto output = engine->CompleteAsync(input).ExtractValueSync();
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
