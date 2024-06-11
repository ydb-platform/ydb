#include "queue.h"
#include "worker.h"

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/monlib/service/monservice.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/datetime.h>


int main(int argc, char* argv[]) {
    //NLWTrace::StartLwtraceFromEnv();
    //signal(SIGPIPE, SIG_IGN);
    TString configPath;
    int monPort = 7777;
    int lwtraceThreadLogSize = 60000;
    bool disableLWTrace = false;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption(0, "mon-port", "port of monitoring service")
        .RequiredArgument("port")
        .StoreResult(&monPort, monPort);
    opts.AddLongOption('f', "file", "filepath to config")
        .Required()
        .RequiredArgument("file")
        .StoreResult(&configPath, configPath);
    opts.AddLongOption("lwtrace-thread-log-size", "thread log size")
        .RequiredArgument("size")
        .StoreResult(&lwtraceThreadLogSize, lwtraceThreadLogSize);
    opts.AddLongOption('L', "disable-lwtrace", "disable lwtrace")
        .NoArgument()
        .SetFlag(&disableLWTrace);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

}