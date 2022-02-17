#include "secondary_index.h"

#include <util/system/env.h>

using namespace NLastGetopt;
using namespace NYdb;

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
    TOpts opts = TOpts::Default();

    TString endpoint;
    TString database;
    TString prefix;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "prefix", "Base prefix for tables").Optional().RequiredArgument("PATH")
        .StoreResult(&prefix);
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgTitle(0, "<COMMAND>", GetCmdList());
    opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;

    TOptsParseResult res(&opts, argc, argv);
    size_t freeArgsPos = res.GetFreeArgsPos();
    argc -= freeArgsPos;
    argv += freeArgsPos;

    ECmd cmd = ParseCmd(*argv);
    if (cmd == ECmd::NONE) {
        Cerr << "Unsupported command '" << *argv << "'" << Endl;
        return 1;
    }

    if (prefix.empty()) {
        prefix = database;
    }

    auto config = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));
    TDriver driver(config);

    try {
        switch (cmd) {
            case ECmd::NONE:
                break;
            case ECmd::CREATE_TABLES:
                return RunCreateTables(driver, prefix, argc, argv);
            case ECmd::DROP_TABLES:
                return RunDropTables(driver, prefix, argc, argv);
            case ECmd::UPDATE_VIEWS:
                return RunUpdateViews(driver, prefix, argc, argv);
            case ECmd::LIST_SERIES:
                return RunListSeries(driver, prefix, argc, argv);
            case ECmd::GENERATE_SERIES:
                return RunGenerateSeries(driver, prefix, argc, argv);
            case ECmd::DELETE_SERIES:
                return RunDeleteSeries(driver, prefix, argc, argv);
        }
    } catch (const TYdbErrorException& e) {
        Cerr << "Execution failed: " << e << Endl;
        return 1;
    }

    Y_UNREACHABLE();
    return 1;
}
