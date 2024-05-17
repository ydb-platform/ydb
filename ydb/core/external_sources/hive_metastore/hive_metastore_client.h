#include <ydb/core/external_sources/hive_metastore/gen-cpp/ThriftHiveMetastore.h>

#include <contrib/restricted/thrift/thrift/protocol/TBinaryProtocol.h>
#include <contrib/restricted/thrift/thrift/transport/TSocket.h>
#include <contrib/restricted/thrift/thrift/transport/TTransportUtils.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/thread/pool.h>

namespace NKikimr::NExternalSource {

struct THiveMetastoreClient : public TThrRefBase {
public:
    THiveMetastoreClient(const TString& host, int32_t port);

    NThreading::TFuture<void> CreateDatabase(const Apache::Hadoop::Hive::Database& database);
    NThreading::TFuture<Apache::Hadoop::Hive::Database> GetDatabase(const TString& name);

    NThreading::TFuture<void> CreateTable(const Apache::Hadoop::Hive::Table& table);
    NThreading::TFuture<Apache::Hadoop::Hive::Table> GetTable(const TString& databaseName, const TString& tableName);

    NThreading::TFuture<void> UpdateTableColumnStatistics(const Apache::Hadoop::Hive::ColumnStatistics& columnStatistics);
    NThreading::TFuture<Apache::Hadoop::Hive::TableStatsResult> GetTableStatistics(const Apache::Hadoop::Hive::TableStatsRequest& request);

    NThreading::TFuture<Apache::Hadoop::Hive::Partition> AddPartition(const Apache::Hadoop::Hive::Partition& partition);
    NThreading::TFuture<void> DropPartition(const TString& databaseName, const TString& tableName, const std::vector<std::string>& partitionValues, bool deleteData = false);
    NThreading::TFuture<std::vector<Apache::Hadoop::Hive::Partition>> GetPartitionsByFilter(const TString& databaseName, const TString& tableName, const TString& filter, int16_t maxPartitions = -1);

    NThreading::TFuture<std::string> GetConfigValue(const std::string& name, const std::string& defaultValue = {});

    ~THiveMetastoreClient();

private:
    template<typename TResultValue>
    NThreading::TFuture<TResultValue> RunOperation(const std::function<TResultValue()>& function);

private:
    std::shared_ptr<apache::thrift::protocol::TTransport> Socket;
    std::shared_ptr<apache::thrift::protocol::TTransport> Transport;
    std::shared_ptr<apache::thrift::protocol::TProtocol> Protocol;
    std::shared_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> Client;
    TThreadPool ThreadPool;
};

}