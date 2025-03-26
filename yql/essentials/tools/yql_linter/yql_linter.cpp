#include <yql/essentials/public/fastcheck/linter.h>
#include <yql/essentials/utils/tty.h>

#include <library/cpp/getopt/last_getopt.h>
#include <google/protobuf/arena.h>

#include <library/cpp/colorizer/output.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/file.h>

int Run(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString inFileName;
    TString queryString;
    TString checks;
    THashMap<TString, TString> clusterMapping;
    TString modeStr = "Default";
    TString syntaxStr = "YQL";
    TString clusterModeStr = "Many";
    TString clusterSystem;

    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('v', "verbose", "show lint issues").NoArgument();
    opts.AddLongOption("list-checks", "list all enabled checks and exit").NoArgument();
    opts.AddLongOption("checks", "comma-separated list of globs with optional '-' prefix").StoreResult(&checks);
    opts.AddLongOption('C', "cluster", "cluster to service mapping").RequiredArgument("name@service")
        .KVHandler([&](TString cluster, TString provider) {
            if (cluster.empty() || provider.empty()) {
                throw yexception() << "Incorrect service mapping, expected form cluster@provider, e.g. plato@yt";
            }
            clusterMapping[cluster] = provider;
        }, '@');

    opts.AddLongOption('m', "mode", "query mode, allowed values: " + GetEnumAllNames<NYql::NFastCheck::EMode>()).StoreResult(&modeStr);
    opts.AddLongOption('s', "syntax", "query syntax, allowed values: " + GetEnumAllNames<NYql::NFastCheck::ESyntax>()).StoreResult(&syntaxStr);
    opts.AddLongOption("cluster-mode", "cluster mode, allowed values: " + GetEnumAllNames<NYql::NFastCheck::EClusterMode>()).StoreResult(&clusterModeStr);
    opts.AddLongOption("cluster-system", "cluster system").StoreResult(&clusterSystem);
    opts.AddLongOption("ansi-lexer", "use ansi lexer").NoArgument();
    opts.AddLongOption("no-colors", "disable colors for output").NoArgument();
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    const bool colorize = !res.Has("no-colors") && IsTty(NYql::EStdStream::Out);
    NYql::NFastCheck::TChecksRequest checkReq;
    if (res.Has("checks")) {
        checkReq.Filters = NYql::NFastCheck::ParseChecks(checks);
    }

    if (res.Has("list-checks")) {
        for (const auto& c : NYql::NFastCheck::ListChecks(checkReq.Filters)) {
            if (colorize) {
                Cout << NColorizer::LightCyan();
            }

            Cout << c;
            if (colorize) {
                Cout << NColorizer::Old();
            }

            Cout << '\n';
        }

        return 0;
    }

    THolder<TUnbufferedFileInput> inFile;
    if (!inFileName.empty()) {
        inFile.Reset(new TUnbufferedFileInput(inFileName));
    }
    IInputStream& in = inFile ? *inFile.Get() : Cin;

    queryString = in.ReadAll();
    int errors = 0;
    TString queryFile("query");

    checkReq.IsAnsiLexer = res.Has("ansi-lexer");
    checkReq.Program = queryString;
    checkReq.Syntax = NYql::NFastCheck::ESyntax::YQL;
    checkReq.ClusterMapping = clusterMapping;
    checkReq.Mode =  FromString<NYql::NFastCheck::EMode>(modeStr);
    checkReq.Syntax =  FromString<NYql::NFastCheck::ESyntax>(syntaxStr);
    checkReq.ClusterMode = FromString<NYql::NFastCheck::EClusterMode>(clusterModeStr);
    checkReq.ClusterSystem = clusterSystem;
    auto checkResp = NYql::NFastCheck::RunChecks(checkReq);
    for (const auto& c : checkResp.Checks) {
        if (!c.Success) {
            errors = 1;
        }

        if (colorize) {
            Cout << NColorizer::LightCyan();
        }

        Cout << c.CheckName << " ";
        if (colorize) {
            Cout << (c.Success ? NColorizer::Green() : NColorizer::LightRed());
        }

        Cout << (c.Success ? "PASSED" : "FAIL");
        if (colorize) {
            Cout << NColorizer::Old();
        }

        Cout << "\n";
        if (res.Has("verbose")) {
            c.Issues.PrintWithProgramTo(Cout, inFileName, queryString, colorize);
        }
    }

    return errors;
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
