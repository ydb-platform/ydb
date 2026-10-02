#include "main.h"

#include <ydb/core/driver_lib/cli_utils/cli.h>
#include <ydb/core/base/backtrace.h>

#ifndef _win_
#include <sys/mman.h>
#endif


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
#ifndef _win_
        mlockall(MCL_CURRENT);
#endif
        NKikimr::EnableYDBBacktraceFormat();
        return NKikimr::NDriverClient::NewClient(argc, argv, std::move(factories));
    }
    catch (const NYdb::NConsoleClient::TMisuseException& e) {
        Cerr << e.what() << Endl;
        Cerr << "Try \"--help\" option for more info." << Endl;
        return 1;
    }
    catch (const NLastGetopt::TUsageException& e) {
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
