#include "basic_example.h"

#include <library/cpp/getopt/last_getopt.h>

#include <cstdlib>
#include <fstream>

using namespace NLastGetopt;
using namespace NYdb;

void StopHandler(int) {
    exit(1);
}

std::string ReadFile(const std::string& filename) {
    std::ifstream input(filename);
    std::stringstream data;
    data << input.rdbuf();
    return data.str();
}

int main(int argc, char** argv) {
    TOpts opts = TOpts::Default();

    std::string endpoint;
    std::string database;
    std::string certPath;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
        .StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database name").Required().RequiredArgument("PATH")
        .StoreResult(&database);
    opts.AddLongOption('c', "cert", "Certificate path to use secure connection").Optional().RequiredArgument("PATH")
        .StoreResult(&certPath);

    signal(SIGINT, &StopHandler);
    signal(SIGTERM, &StopHandler);

    TOptsParseResult res(&opts, argc, argv);

    auto driverConfig = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    if (!certPath.empty()) {
        std::string cert = ReadFile(certPath);
        driverConfig.UseSecureConnection(cert);
    }

    TDriver driver(driverConfig);

    if (!Run(driver)) {
        driver.Stop(true);
        return 2;
    }

    driver.Stop(true);
    return 0;
}
