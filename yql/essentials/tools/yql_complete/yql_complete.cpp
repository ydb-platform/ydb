#include <yql/essentials/sql/v1/complete/sql_complete.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/stream/file.h>

int Run(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TMaybe<ui64> pos;
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
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
    auto engine = NSQLComplete::MakeSqlCompletionEngine();
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
