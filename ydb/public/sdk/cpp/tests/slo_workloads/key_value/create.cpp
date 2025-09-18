#include "key_value.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NYdb;
using namespace NYdb::NTable;


namespace {
    void CreateTable(TTableClient& client, const std::string& prefix) {
        RetryBackoff(client, 5, [prefix](TSession session) {
            auto desc = TTableBuilder()
                .AddNullableColumn("object_id_key", EPrimitiveType::Uint32)
                .AddNullableColumn("object_id", EPrimitiveType::Uint32)
                .AddNullableColumn("timestamp", EPrimitiveType::Uint64)
                .AddNullableColumn("payload", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumns({ "object_id_key", "object_id" })
                .Build();

            auto tableSettings = TCreateTableSettings()
                .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(PartitionsCount))
                .CancelAfter(DefaultReactionTime)
                .ClientTimeout(DefaultReactionTime + TDuration::Seconds(5));

            return session.CreateTable(
                JoinPath(prefix, TableName)
                , std::move(desc)
                , std::move(tableSettings)
            ).ExtractValueSync();
        });
    }
} //namespace

int CreateTable(TDatabaseOptions& dbOptions) {
    TTableClient client(dbOptions.Driver);
    CreateTable(client, dbOptions.Prefix);
    Cout << "Table created." << Endl;
    return EXIT_SUCCESS;
}
