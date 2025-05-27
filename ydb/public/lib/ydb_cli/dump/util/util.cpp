#include "util.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NYdb::NDump {

using namespace NScheme;
using namespace NTable;
using namespace NCms;

TStatus DescribeTable(TTableClient& tableClient, const TString& path, TMaybe<TTableDescription>& out) {
    auto func = [&path, &out](TSession session) {
        auto settings = TDescribeTableSettings().WithKeyShardBoundary(true);
        auto result = session.DescribeTable(path, settings).ExtractValueSync();

        if (result.IsSuccess()) {
            out = result.GetTableDescription();
        }

        return result;
    };

    return tableClient.RetryOperationSync(func, TRetryOperationSettings().Idempotent(true));
}

TStatus DescribeExternalDataSource(TTableClient& client, const TString& path, Ydb::Table::DescribeExternalDataSourceResult& out) {
    auto status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.DescribeExternalDataSource(path).ExtractValueSync();
        if (result.IsSuccess()) {
            out = TProtoAccessor::GetProto(result.GetExternalDataSourceDescription());
        }
        return result;
    });
    return status;
}

TStatus DescribeReplication(NReplication::TReplicationClient& client, const TString& path, TMaybe<NReplication::TReplicationDescription>& out) {
    out.Clear();

    auto status = NConsoleClient::RetryFunction([&]() {
        return client.DescribeReplication(path).ExtractValueSync();
    });
    if (status.IsSuccess()) {
        out = status.GetReplicationDescription();
    }
    return status;
}

TDescribePathResult DescribePath(TSchemeClient& schemeClient, const TString& path, const TDescribePathSettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TDescribePathResult {
        return schemeClient.DescribePath(path, settings).ExtractValueSync();
    });
}

TStatus MakeDirectory(TSchemeClient& schemeClient, const TString& path, const TMakeDirectorySettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TStatus {
        return schemeClient.MakeDirectory(path, settings).ExtractValueSync();
    });
}

TStatus ModifyPermissions(TSchemeClient& schemeClient, const TString& path, const TModifyPermissionsSettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TStatus {
        return schemeClient.ModifyPermissions(path, settings).ExtractValueSync();
    });
}

TListDirectoryResult ListDirectory(TSchemeClient& schemeClient, const TString& path, const TListDirectorySettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TListDirectoryResult {
        return schemeClient.ListDirectory(path, settings).ExtractValueSync();
    });
}

TListDatabasesResult ListDatabases(TCmsClient& cmsClient, const TListDatabasesSettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TListDatabasesResult {
        return cmsClient.ListDatabases(settings).ExtractValueSync();
    });
}

TGetDatabaseStatusResult GetDatabaseStatus(TCmsClient& cmsClient, const std::string& path, const TGetDatabaseStatusSettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TGetDatabaseStatusResult {
        return cmsClient.GetDatabaseStatus(path, settings).ExtractValueSync();
    });
}

TStatus CreateDatabase(TCmsClient& cmsClient, const std::string& path, const TCreateDatabaseSettings& settings) {
    return NConsoleClient::RetryFunction([&]() -> TStatus {
        return cmsClient.CreateDatabase(path, settings).ExtractValueSync();
    });
}

} // NYdb::NDump
