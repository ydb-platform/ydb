#include "vector_index.h"

#include <util/system/env.h>
#include <util/stream/file.h>

using namespace NLastGetopt;
using namespace NYdb;

int main(int argc, char** argv) {
    TString endpoint;
    TString command;
    TOptions options;

    TOpts opts = TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("PATH").StoreResult(&options.Database);
    opts.AddLongOption('c', "command", "execute command").Required().RequiredArgument("COMMAND").StoreResult(&command);
    opts.AddLongOption("table", "table name").Required().RequiredArgument("TABLE").StoreResult(&options.Table);
    opts.AddLongOption("index_type", "index type").Required().RequiredArgument("TYPE").StoreResult(&options.IndexType);
    opts.AddLongOption("index_quantizer", "index quantizer").Required().RequiredArgument("QUANTIZER").StoreResult(&options.IndexQuantizer);
    opts.AddLongOption("primary_key", "primary key column").Required().RequiredArgument("PK").StoreResult(&options.PrimaryKey);
    opts.AddLongOption("embedding", "embedding column").Required().RequiredArgument("EMBEDDING").StoreResult(&options.Embedding);
    opts.AddLongOption("distance", "distance function").Required().RequiredArgument("DISTANCE").StoreResult(&options.Distance);
    opts.AddLongOption("top_k", "count of top").Required().RequiredArgument("TOPK").StoreResult(&options.TopK);
    opts.AddLongOption("data", "list of columns to read").Required().RequiredArgument("DATA").StoreResult(&options.Data);

    opts.SetFreeArgsMin(0);
    TOptsParseResult result(&opts, argc, argv);

    ECommand cmd = Parse(command);

    if (cmd == ECommand::None) {
        Cerr << "Unsupported command: " << command << Endl;
        return 1;
    }

    auto config = TDriverConfig()
                      .SetEndpoint(endpoint)
                      .SetDatabase(options.Database)
                      .SetAuthToken(GetEnv("YDB_TOKEN"));

    TDriver driver(config);

    try {
        switch (cmd) {
            case ECommand::CreateIndex:
                return CreateIndex(driver, options);
            case ECommand::UpdateIndex:
                return UpdateIndex(driver, options);
            case ECommand::TopK:
                return TopK(driver, options);
            default:
                break;
        }
    } catch (const TVectorException& e) {
        Cerr << "Execution failed: " << e << Endl;
    }
    return 1;
}
