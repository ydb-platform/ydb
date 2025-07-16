#include "vector_index.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/helpers/helpers.h>


void PrintResults(const std::vector<TResultItem>& items)
{
    if (items.empty()) {
        std::cout << "No items found" << std::endl;
        return;
    }

    for (const auto& item : items) {
        std::cout << "[score=" << item.Score << "] " << item.Id << ": " << item.Document << std::endl;
    }
}

void VectorExample(
    const std::string& endpoint,
    const std::string& database,
    const std::string& tableName,
    const std::string& indexName)
{
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);

    try {
        DropVectorTable(client, tableName);
        CreateVectorTable(client, tableName);
        std::vector<TItem> items = {
            {.Id = "1", .Document = "document 1", .Embedding = {0.98, 0.1, 0.01}},
            {.Id = "2", .Document = "document 2", .Embedding = {1.0, 0.05, 0.05}},
            {.Id = "3", .Document = "document 3", .Embedding = {0.9, 0.1, 0.1}},
            {.Id = "4", .Document = "document 4", .Embedding = {0.03, 0.0, 0.99}},
            {.Id = "5", .Document = "document 5", .Embedding = {0.0, 0.0, 0.99}},
            {.Id = "6", .Document = "document 6", .Embedding = {0.0, 0.02, 1.0}},
            {.Id = "7", .Document = "document 7", .Embedding = {0.0, 1.05, 0.05}},
            {.Id = "8", .Document = "document 8", .Embedding = {0.02, 0.98, 0.1}},
            {.Id = "9", .Document = "document 9", .Embedding = {0.0, 1.0, 0.05}},
        };
        InsertItems(client, tableName, items);
        PrintResults(SearchItems(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3));
        AddIndex(driver, client, database, tableName, indexName, "similarity=cosine", 3, 1, 3);
        PrintResults(SearchItems(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3, indexName));
    } catch (const std::exception& e) {
        std::cerr << "Execution failed: " << e.what() << std::endl;
    }

    driver.Stop(true);
}

int main(int argc, char** argv) {
    std::string endpoint;
    std::string database;
    std::string tableName;
    std::string indexName;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("DATABASE").StoreResult(&database);
    opts.AddLongOption("table", "table name").Required().RequiredArgument("TABLE").StoreResult(&tableName);
    opts.AddLongOption("index", "index name").Required().RequiredArgument("INDEX").StoreResult(&indexName);

    opts.SetFreeArgsMin(0);
    NLastGetopt::TOptsParseResult result(&opts, argc, argv);

    VectorExample(endpoint, database, tableName, indexName);
    return 0;
}
