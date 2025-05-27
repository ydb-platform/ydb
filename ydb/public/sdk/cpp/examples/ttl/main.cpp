#include "ttl.h"

#include <library/cpp/getopt/last_getopt.h>

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NStatusHelpers;

void StopHandler(int) {
    exit(1);
}

int main(int argc, char** argv) {
    TOpts opts = TOpts::Default();

    std::string endpoint;
    std::string database;
    std::string path;
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
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");
    TDriver driver(driverConfig);

    if (!Run(driver, path)) {
        return 2;
    }

    return 0;
}
