#include "hive_metastore_client.h"

namespace NKikimr::NExternalSource {

THiveMetastoreClient::THiveMetastoreClient(const TString& host, int32_t port) 
    : Socket(new apache::thrift::transport::TSocket(host, port))
    , Transport(new apache::thrift::transport::TBufferedTransport(Socket))
    , Protocol(new apache::thrift::protocol::TBinaryProtocol(Transport))
    , Client(new Apache::Hadoop::Hive::ThriftHiveMetastoreClient(Protocol)) {
    Transport->open();
    // It's very important to keep here the only 1 thread
    ThreadPool.Start(1);
}

template<typename TResultValue>
NThreading::TFuture<TResultValue> THiveMetastoreClient::RunOperation(const std::function<TResultValue()>& function) {
    NThreading::TPromise<TResultValue> promise = NThreading::NewPromise<TResultValue>();
    Y_ABORT_UNLESS(ThreadPool.Add(MakeThrFuncObj([promise, function, transport=Transport]() mutable {
        try {
            if constexpr (std::is_void_v<TResultValue>) {
                function();
                promise.SetValue();
            } else {
                promise.SetValue(function());  
            }       
        } catch (const apache::thrift::transport::TTransportException&) {
            transport->close();
            transport->open();
            promise.SetException(std::current_exception());
        } catch (...) {
            promise.SetException(std::current_exception());
        }
    })));
    return promise.GetFuture();
}

NThreading::TFuture<void> THiveMetastoreClient::CreateDatabase(const Apache::Hadoop::Hive::Database& database) {
    return RunOperation<void>([client=Client, database]() {
        client->create_database(database);
    });
}

NThreading::TFuture<Apache::Hadoop::Hive::Database> THiveMetastoreClient::GetDatabase(const TString& name) {
    return RunOperation<Apache::Hadoop::Hive::Database>([client=Client, name]()  {
        Apache::Hadoop::Hive::Database database;
        client->get_database(database, name);
        return database;
    });
}

NThreading::TFuture<void> THiveMetastoreClient::CreateTable(const Apache::Hadoop::Hive::Table& table) {
    return RunOperation<void>([client=Client, table]() {
        client->create_table(table);
    });
}

NThreading::TFuture<std::vector<std::string>> THiveMetastoreClient::GetAllDatabases() {
    return RunOperation<std::vector<std::string>>([client=Client]() {
        std::vector<std::string> databases;
        client->get_all_databases(databases);
        return databases;
    });
}

NThreading::TFuture<Apache::Hadoop::Hive::Table> THiveMetastoreClient::GetTable(const TString& databaseName, const TString& tableName) {
    return RunOperation<Apache::Hadoop::Hive::Table>([client=Client, databaseName, tableName]()  {
        Apache::Hadoop::Hive::Table table;
        client->get_table(table, databaseName, tableName);
        return table;
    });
}

NThreading::TFuture<std::vector<std::string>> THiveMetastoreClient::GetAllTables(const TString& databaseName) {
    return RunOperation<std::vector<std::string>>([client=Client, databaseName]()  {
        std::vector<std::string> tables;
        client->get_all_tables(tables, databaseName);
        return tables;
    });
}

NThreading::TFuture<void> THiveMetastoreClient::UpdateTableColumnStatistics(const Apache::Hadoop::Hive::ColumnStatistics& columnStatistics) {
    return RunOperation<void>([client=Client, columnStatistics]() {
        client->update_table_column_statistics(columnStatistics);
    });
}

NThreading::TFuture<Apache::Hadoop::Hive::TableStatsResult> THiveMetastoreClient::GetTableStatistics(const Apache::Hadoop::Hive::TableStatsRequest& request) {
    return RunOperation<Apache::Hadoop::Hive::TableStatsResult>([client=Client, request]()  {
        Apache::Hadoop::Hive::TableStatsResult result;
        client->get_table_statistics_req(result, request);
        return result;
    });
}

NThreading::TFuture<Apache::Hadoop::Hive::Partition> THiveMetastoreClient::AddPartition(const Apache::Hadoop::Hive::Partition& partition) {
    return RunOperation<Apache::Hadoop::Hive::Partition>([client=Client, partition]()  {
        Apache::Hadoop::Hive::Partition result;
        client->add_partition(result, partition);
        return result;
    });
}

NThreading::TFuture<void> THiveMetastoreClient::DropPartition(const TString& databaseName, const TString& tableName, const std::vector<std::string>& partitionValues, bool deleteData) {
    return RunOperation<void>([client=Client, databaseName, tableName, partitionValues, deleteData]() {
        client->drop_partition(databaseName, tableName, partitionValues, deleteData);
    });
}

NThreading::TFuture<std::vector<Apache::Hadoop::Hive::Partition>> THiveMetastoreClient::GetPartitionsByFilter(const TString& databaseName, const TString& tableName, const TString& filter, int16_t maxPartitions) {
    return RunOperation<std::vector<Apache::Hadoop::Hive::Partition>>([client=Client, databaseName, tableName, filter, maxPartitions]()  {
        std::vector<Apache::Hadoop::Hive::Partition> partitions;
        client->get_partitions_by_filter(partitions, databaseName, tableName, filter, maxPartitions);
        return partitions;
    });
}

NThreading::TFuture<std::string> THiveMetastoreClient::GetConfigValue(const std::string& name, const std::string& defaultValue) {
    return RunOperation<std::string>([client=Client, name, defaultValue]()  {
        std::string result;
        client->get_config_value(result, name, defaultValue);
        return result;
    });
}

THiveMetastoreClient::~THiveMetastoreClient() {
    ThreadPool.Stop();
    Transport->close();
}

}
