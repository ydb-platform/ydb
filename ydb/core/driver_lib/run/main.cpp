#include "main.h"
#include "driver.h"

// add support for base utils
#include <ydb/core/driver_lib/base_utils/format_info.h>
#include <ydb/core/driver_lib/base_utils/format_util.h>
#include <ydb/core/driver_lib/base_utils/node_by_host.h>

// add support for CLI utils
#include <ydb/core/driver_lib/cli_utils/cli.h>

// add support for running kikimr node
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/driver_lib/run/config_parser.h>
#include <ydb/core/driver_lib/run/run.h>

// allocator info
#include <library/cpp/malloc/api/malloc.h>

#ifndef _win_
#include <sys/mman.h>
#endif

namespace NKikimr {

int MainRun(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories) {
#ifdef _win32_
    WSADATA dummy;
    WSAStartup(MAKEWORD(2, 2), &dummy);
#endif

    TKikimrRunner::SetSignalHandlers();
    Cout << "Starting Kikimr r" << GetArcadiaLastChange()
         << " built by " << GetProgramBuildUser() << Endl;

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

    int Main(int argc, char **argv, std::shared_ptr<TModuleFactories> factories) {
#ifndef _win_
        mlockall(MCL_CURRENT);
#endif
        using namespace NLastGetopt;
        using TDriverModeParser = TCliCommands<EDriverMode>;

        NKikimrConfig::TAppConfig appConfig;
        TCommandConfig cmdConf;
        TKikimrRunConfig runConfig(appConfig);

        TRunCommandConfigParser configParser(runConfig);

        TOpts opts = TOpts::Default();
        opts.SetTitle("YDB client/server binary");

        configParser.SetupGlobalOpts(opts);
        NMsgBusProxy::TMsgBusClientConfig mbusConfig;
        mbusConfig.ConfigureLastGetopt(opts, "mb-");
        NDriverClient::HideOptions(opts);
        opts.AddLongOption('s', "server", "Server address to connect (default $KIKIMR_SERVER)").RequiredArgument("ADDR[:NUM]");
        opts.AddLongOption('k', "token", "Security token").RequiredArgument("TOKEN");
        opts.AddLongOption('f', "token-file", "Security token file").RequiredArgument("PATH");
        opts.AddLongOption('d', "dump", "Dump requests to error log").NoArgument();
        opts.AddLongOption('t', "time", "Show request execution time").NoArgument();
        opts.AddLongOption('o', "progress", "Show progress of long requests").NoArgument();
        opts.AddLongOption(0,  "allocator-info", "Print the name of allocator linked to the binary and exit")
                .NoArgument().Handler(&PrintAllocatorInfoAndExit);
        opts.SetFreeArgsMin(1);
        opts.SetFreeArgTitle(0, "<command>", TDriverModeParser::CommandsCsv());
        opts.SetCmdLineDescr(NDriverClient::NewClientCommandsDescription(factories));

        opts.AddHelpOption('h');
        opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;

        TOptsParseResult res(&opts, argc, argv);

        size_t freeArgsPos = res.GetFreeArgsPos();
        argc -= freeArgsPos;
        argv += freeArgsPos;

        EDriverMode mode = TDriverModeParser::ParseCommand(*argv);

        if (mode == EDM_NO) {
            fprintf(stderr, "Unknown command '%s'\n\n", *argv);
            opts.PrintUsage(TString(""));
            exit(1);
        }

        configParser.ParseGlobalOpts(res);

        switch (mode) {
        case EDM_RUN:
        {
            configParser.ParseRunOpts(argc, argv);
            configParser.ApplyParsedOptions();
            return MainRun(runConfig, factories);
        }
        case EDM_ADMIN:
        case EDM_DB:
        case EDM_TABLET:
        case EDM_DEBUG:
        case EDM_BS:
        case EDM_BLOBSTORAGE:
        case EDM_SERVER:
        case EDM_CMS:
        case EDM_DISCOVERY:
        case EDM_WHOAMI:
            return NDriverClient::NewClient(argc + freeArgsPos, argv - freeArgsPos, factories);
        case EDM_FORMAT_INFO:
            return MainFormatInfo(cmdConf, argc, argv);
        case EDM_FORMAT_UTIL:
            return MainFormatUtil(cmdConf, argc, argv);
        case EDM_NODE_BY_HOST:
            return MainNodeByHost(cmdConf, argc, argv);
        case EDM_SCHEME_INITROOT:
            return NDriverClient::SchemeInitRoot(cmdConf, argc, argv);
        case EDM_COMPILE_AND_EXEC_MINIKQL:
            return NDriverClient::CompileAndExecMiniKQL(cmdConf, argc, argv);
        case EDM_TRACE:
            return NDriverClient::MessageBusTrace(cmdConf, argc, argv);
        case EDM_KEYVALUE_REQUEST:
            return NDriverClient::KeyValueRequest(cmdConf, argc, argv);
        case EDM_PERSQUEUE_REQUEST:
            return NDriverClient::PersQueueRequest(cmdConf, argc, argv);
        case EDM_PERSQUEUE_STRESS:
            return NDriverClient::PersQueueStress(cmdConf, argc, argv);
        case EDM_PERSQUEUE_DISCOVER_CLUSTERS:
            return NDriverClient::PersQueueDiscoverClustersRequest(cmdConf, argc, argv);
        case EDM_LOAD_REQUEST:
            return NDriverClient::LoadRequest(cmdConf, argc, argv);
        case EDM_DS_LOAD_REQUEST:
            return NDriverClient::DsLoadRequest(cmdConf, argc, argv);
        case EDM_ACTORSYS_PERFTEST:
            return NDriverClient::ActorsysPerfTest(cmdConf, argc, argv);
        default:
            Y_FAIL("Not Happens");
        }
    }
} // NKikimr

namespace {
std::terminate_handler defaultTerminateHandler;
}

void KikimrTerminateHandler() {
    Cerr << "======= terminate() call stack ========\n";
    FormatBackTrace(&Cerr);
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
}

