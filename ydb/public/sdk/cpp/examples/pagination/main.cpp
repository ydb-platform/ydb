#include "pagination.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/system/env.h>

using namespace NLastGetopt;
using namespace NYdb;

void StopHandler(int) {
    exit(1);
}

int main(int argc, char** argv) {
    TOpts opts = TOpts::Default();

    TString endpoint;
    TString database;
    TString path;
    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('p', "path", "Base path for tables").Optional().RequiredArgument("PATH")
        .StoreResult(&path);

    signal(SIGINT, &StopHandler);
    signal(SIGTERM, &StopHandler);

    TOptsParseResult res(&opts, argc, argv);

    if (path.empty()) {
        path = database;
    }

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));
    TDriver driver(driverConfig);

    if (!Run(driver, path)) {
        return 2;
    }

    return 0;
}
