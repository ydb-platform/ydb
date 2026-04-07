#pragma once

#include <ydb/public/lib/ydb_cli/common/formats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

namespace NYdb::NConsoleClient {

struct TDescribeOptions {
    bool ShowPermissions = false;
    bool ShowKeyShardBoundaries = false;
    bool ShowStats = false;
    bool ShowPartitionStats = false;
    TString Database;
};

class TDescribeLogic {
public:
    TDescribeLogic(TDriver driver, IOutputStream& out);

    IOutputStream& GetOutputStream() const { return Out; }

    int Describe(const TString& path, const TDescribeOptions& options, EDataFormat format = EDataFormat::Pretty);

private:
    int DescribePath(const TString& path, const TDescribeOptions& options, EDataFormat format);
    int PrintPathResponse(const TString& path, const NScheme::TDescribePathResult& result, const TDescribeOptions& options, EDataFormat format);
    int DescribeEntryDefault(NScheme::TSchemeEntry entry, const TDescribeOptions& options);

    int DescribeTable(const TString& path, const TDescribeOptions& options, EDataFormat format);
    int DescribeColumnTable(const TString& path, const TDescribeOptions& options, EDataFormat format);
    int PrintTableResponsePretty(const NTable::TTableDescription& tableDescription, const TDescribeOptions& options) const;
    void WarnAboutTableOptions(const TString& path, const TDescribeOptions& options, EDataFormat format);

    int DescribeTopic(const TString& path, const TDescribeOptions& options, EDataFormat format);
    int PrintTopicResponsePretty(const NYdb::NTopic::TTopicDescription& settings, const TDescribeOptions& options) const;

    int DescribeCoordinationNode(const TString& path, EDataFormat format);
    int PrintCoordinationNodeResponsePretty(const NYdb::NCoordination::TNodeDescription& result) const;

    int DescribeReplication(const TString& path, const TDescribeOptions& options, EDataFormat format);
    int PrintReplicationResponsePretty(const NYdb::NReplication::TDescribeReplicationResult& result, const TDescribeOptions& options) const;

    int DescribeTransfer(const TString& path, EDataFormat format);
    int PrintTransferResponsePretty(const NYdb::NReplication::TDescribeTransferResult& result) const;

    int DescribeView(const TString& path);

    int DescribeExternalDataSource(const TString& path, EDataFormat format);
    int PrintExternalDataSourceResponsePretty(const NYdb::NTable::TExternalDataSourceDescription& result) const;

    int DescribeExternalTable(const TString& path, EDataFormat format);
    int PrintExternalTableResponsePretty(const NYdb::NTable::TExternalTableDescription& result) const;

    int DescribeSystemView(const TString& path, EDataFormat format);
    int PrintSystemViewResponsePretty(const NYdb::NTable::TSystemViewDescription& result) const;

    int TryTopicConsumerDescribeOrFail(const TString& path, const NScheme::TDescribePathResult& result, const TDescribeOptions& options, EDataFormat format);
    std::pair<TString, TString> ParseTopicConsumer(const TString& path) const;
    int PrintConsumerResponsePretty(const NYdb::NTopic::TConsumerDescription& description, const TDescribeOptions& options) const;

    template<typename TDescriptionType>
    void PrintPermissionsIfNeeded(const TDescriptionType& description, const TDescribeOptions& options) const;

private:
    TDriver Driver;
    IOutputStream& Out;
};

} // namespace NYdb::NConsoleClient
