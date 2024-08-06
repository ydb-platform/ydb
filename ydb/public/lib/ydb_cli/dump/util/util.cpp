#include "util.h"

#include <ydb/public/lib/ydb_cli/common/retry_func.h>

namespace NYdb::NDump {

using namespace NScheme;
using namespace NTable;

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

}
