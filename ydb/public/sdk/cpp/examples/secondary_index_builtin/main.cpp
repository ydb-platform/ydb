#include "secondary_index.h"

#include <util/system/env.h>

using namespace NLastGetopt;
using namespace NYdb;

int main(int argc, char** argv) {

    TString endpoint;
    TString database;
    TString command;

    TOpts opts = TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH").StoreResult(&database);
    opts.AddLongOption('c', "command", "execute command").Required().RequiredArgument("TYPE").StoreResult(&command);

    opts.SetFreeArgsMin(0);
    opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;
    opts.AllowUnknownLongOptions_ = true;

    TOptsParseResult result(&opts, argc, argv);

    size_t freeArgPos = result.GetFreeArgsPos() - 1;
    argc -= freeArgPos;
    argv += freeArgPos;

    TCommand cmd = Parse(command.c_str());

    if (cmd == TCommand::NONE) {
        Cerr << "Unsupported command: " << command << Endl;
        return 1;
    }

    auto config = TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));

    TDriver driver(config);

    try {
        switch (cmd) {
            case TCommand::CREATE:
                return Create(driver, database);
            case TCommand::INSERT:
                return Insert(driver, database);
            case TCommand::SELECT:
                return Select(driver, database, argc, argv);
            case TCommand::SELECT_JOIN:
                return SelectJoin(driver, database, argc, argv);
            case TCommand::DROP:
                return Drop(driver, database);
            case TCommand::NONE:
                return 1;
        }

    } catch (const TYdbErrorException& e) {
        Cerr << "Execution failed: " << e << Endl;
        return 1;
    }
}
