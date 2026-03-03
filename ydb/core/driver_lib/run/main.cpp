#include "main.h"

// add support for CLI utils
#include <ydb/core/driver_lib/cli_utils/cli.h>

// add support for running kikimr node
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/driver_lib/run/config_parser.h>
#include <ydb/core/driver_lib/run/run.h>

// allocator info
#include <library/cpp/malloc/api/malloc.h>

// compatibility info
#include <ydb/core/driver_lib/version/version.h>

// backtrace formatting
#include <ydb/core/base/backtrace.h>

#ifndef _win_
#include <sys/mman.h>
#endif

#include <library/cpp/svnversion/svnversion.h>

#include <util/string/ascii.h>

namespace NKikimr {

int MainRun(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories) {
#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TKikimrRunner::SetSignalHandlers();
    Cout << "Starting Kikimr" << Endl;
    Cout << GetProgramSvnVersion() << Endl;

    TIntrusivePtr<TKikimrRunner> runner = TKikimrRunner::CreateKikimrRunner(runConfig, std::move(factories));
    if (runner) {
        runner->KikimrStart();
        runner->BusyLoop();
        // exit busy loop by a signal
        Cout << "Shutting Kikimr down" << Endl;
        runner->KikimrStop(false);
    }

    return 0;
}

void PrintAllocatorInfoAndExit() {
    Cout << "linked with malloc: " << NMalloc::MallocInfo().Name << Endl;
    exit(0);
}

void PrintCompatibilityInfoAndExit() {
    TString compatibilityInfo(CompatibilityInfo.PrintHumanReadable());
    Cout << compatibilityInfo;
    if (!compatibilityInfo.EndsWith("\n")) {
        Cout << Endl;
    }
    exit(0);
}

int Main(int argc, char **argv, std::shared_ptr<TModuleFactories> factories) {
#ifndef _win_
    mlockall(MCL_CURRENT);
#endif
    using namespace NLastGetopt;

    EnableYDBBacktraceFormat();

    NKikimrConfig::TAppConfig appConfig;
    TKikimrRunConfig runConfig(appConfig);
    TRunCommandConfigParser configParser(runConfig);

    // Minimal outer parser:
    // - Handles --allocator-info / --compatibility-info (immediate exit)
    // - Registers "run" command's global opts (--cluster-name, --log-level, etc.)
    // - Allows unknown options to pass through to the command tree
    // - Uses REQUIRE_ORDER to stop at first non-option argument
    TOpts opts;
    configParser.SetupGlobalOpts(opts);
    opts.AddLongOption(0, "allocator-info", "Print the name of allocator linked to the binary and exit")
        .NoArgument().Handler(&PrintAllocatorInfoAndExit);
    opts.AddLongOption(0, "compatibility-info", "Print compatibility info of this binary and exit")
        .NoArgument().Handler(&PrintCompatibilityInfoAndExit);
    opts.AllowUnknownCharOptions_ = true;
    opts.AllowUnknownLongOptions_ = true;
    opts.SetFreeArgsMin(0);
    opts.ArgPermutation_ = REQUIRE_ORDER;

    TOptsParseResult res(&opts, argc, argv);
    size_t freeArgsPos = res.GetFreeArgsPos();

    // Legacy "run" command: uses its own config parser path
    if (freeArgsPos < (size_t)argc && AsciiCompareIgnoreCase(argv[freeArgsPos], "run") == 0) {
        configParser.ParseGlobalOpts(res);
        configParser.ParseRunOpts(argc - freeArgsPos, argv + freeArgsPos);
        configParser.ApplyParsedOptions();
        return MainRun(runConfig, std::move(factories));
    }

    // All other commands go through the unified command tree
    return NDriverClient::NewClient(argc, argv, std::move(factories));
}

} // NKikimr

namespace {
std::terminate_handler defaultTerminateHandler;
}

void KikimrTerminateHandler() {
    Cerr << "======= terminate() call stack ========\n";
    FormatBackTrace(&Cerr);
    if (auto backtrace = TBackTrace::FromCurrentException(); backtrace.size() > 0) {
        Cerr << "======== exception call stack =========\n";
        backtrace.PrintTo(Cerr);
    }
    Cerr << "=======================================\n";

    auto oldHandler = defaultTerminateHandler;
    if (oldHandler)
        oldHandler();
    else
        abort();
}

void SetupTerminateHandler() {
    defaultTerminateHandler = std::get_terminate();
    std::set_terminate(KikimrTerminateHandler);
}

int ParameterizedMain(int argc, char **argv, std::shared_ptr<NKikimr::TModuleFactories> factories) {
    try {
        return NKikimr::Main(argc, argv, std::move(factories));
    }
    catch (const NYdb::NConsoleClient::TMisuseException& e) {
        Cerr << e.what() << Endl;
        Cerr << "Try \"--help\" option for more info." << Endl;
        return 1;
    }
    catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
    catch (const std::runtime_error& se) {
        Cerr << "Caught runtime error: " << se.what() << Endl;
        return 1;
    }
}
