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
    opts.AddLongOption('c', "command", "execute command: [Create|Drop|Build|Recreate]Index or TopK (used for search, read request)").Required().RequiredArgument("COMMAND").StoreResult(&command);
    opts.AddLongOption("table", "table name").Required().RequiredArgument("TABLE").StoreResult(&options.Table);
    opts.AddLongOption("index_type", "index type: [flat]").Required().RequiredArgument("TYPE").StoreResult(&options.IndexType);
    opts.AddLongOption("index_quantizer", "index quantizer: [none|int8|uint8|bit]").Required().RequiredArgument("QUANTIZER").StoreResult(&options.IndexQuantizer);
    opts.AddLongOption("primary_key", "primary key column").Required().RequiredArgument("PK").StoreResult(&options.PrimaryKey);
    opts.AddLongOption("embedding", "embedding (vector) column").Required().RequiredArgument("EMBEDDING").StoreResult(&options.Embedding);
    opts.AddLongOption("distance", "distance function: [Cosine|Euclidean|Manhattan]Distance").Required().RequiredArgument("DISTANCE").StoreResult(&options.Distance);
    opts.AddLongOption("rows", "count of rows in table, used only for [Build|Recreate]Index commands").Required().RequiredArgument("ROWS").StoreResult(&options.Rows);
    opts.AddLongOption("top_k", "count of rows in top, used only for TopK command").Required().RequiredArgument("TOPK").StoreResult(&options.TopK);
    opts.AddLongOption("data", "list of columns to read, used only for TopK command").Required().RequiredArgument("DATA").StoreResult(&options.Data);
    opts.AddLongOption("target", "file with target vector, used only for TopK command").Required().RequiredArgument("TARGET").StoreResult(&options.Target);

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
            case ECommand::DropIndex:
                return DropIndex(driver, options);
            case ECommand::CreateIndex:
                return CreateIndex(driver, options);
            case ECommand::BuildIndex:
                return BuildIndex(driver, options);
            case ECommand::RecreateIndex:
                if (auto r = DropIndex(driver, options); r != 0) {
                    return r;
                }
                if (auto r = CreateIndex(driver, options); r != 0) {
                    return r;
                }
                return BuildIndex(driver, options);
            case ECommand::TopK:
                return TopK(driver, options);
            default:
                break;
        }
    } catch (const std::exception& e) {
        Cerr << "Execution failed: " << e.what() << Endl;
    }
    return 1;
}
