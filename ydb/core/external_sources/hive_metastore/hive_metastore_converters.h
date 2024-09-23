#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/core/external_sources/hive_metastore/hive_metastore_native/gen-cpp/ThriftHiveMetastore.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NExternalSource {

struct THiveMetastoreConverters {
    struct TStatistics {
        TMaybe<int64_t> Rows;
        TMaybe<int64_t> Size;
    };

    static TString GetFormat(const Apache::Hadoop::Hive::Table& table);
    static TString GetCompression(const Apache::Hadoop::Hive::Table& table);
    static TString GetLocation(const Apache::Hadoop::Hive::Table& table);
    static std::vector<Ydb::Column> GetColumns(const Apache::Hadoop::Hive::Table& table);
    static std::vector<TString> GetPartitionedColumns(const Apache::Hadoop::Hive::Table& table);
    static TString GetPartitionsFilter(const NYql::NConnector::NApi::TPredicate& predicate);
    static TStatistics GetStatistics(const Apache::Hadoop::Hive::TableStatsResult& statistics);
    static TStatistics GetStatistics(const std::vector<Apache::Hadoop::Hive::Partition>& partitions);
};

}
