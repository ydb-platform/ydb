#include "util.h"

#include <ydb/public/lib/ydb_cli/common/recursive_list.h>

#include <util/folder/path.h>

namespace NYdb {
namespace NDump {

using namespace NScheme;
using namespace NTable;

namespace {

// function must return TStatus or derived struct
// function must be re-invokable
template <typename TFunction>
decltype(auto) RetryFunction(
        const TFunction& func,
        const ui32 maxRetries = 10,
        TDuration retrySleep = TDuration::MilliSeconds(500)) {

    for (ui32 retryNumber = 0; retryNumber <= maxRetries; ++retryNumber) {
        auto result = func();

        if (result.IsSuccess()) {
            return result;
        }

        if (retryNumber == maxRetries) {
            return result;
        }

        switch (result.GetStatus()) {
            case EStatus::ABORTED:
                break;

            case EStatus::UNAVAILABLE:
            case EStatus::OVERLOADED:
            case EStatus::TRANSPORT_UNAVAILABLE:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED: {
                ExponentialBackoff(retrySleep);
                break;
            }
            default:
                return result;
        }

    }
    // unreachable
    return func();
}

} // anonymous

TStatus DescribeTable(TTableClient& tableClient, const TString& path, TMaybe<TTableDescription>& out) {
    auto func = [&path, &out](TSession session) {
        auto settings = TDescribeTableSettings().WithKeyShardBoundary(true);
        auto result = session.DescribeTable(path, settings).GetValueSync();

        if (result.IsSuccess()) {
            out = result.GetTableDescription();
        }

        return result;
    };

    return tableClient.RetryOperationSync(func, TRetryOperationSettings().Idempotent(true));
}

void ExponentialBackoff(TDuration& sleep, TDuration max) {
    Sleep(sleep);
    sleep = Min(sleep * 2, max);
}

TDescribePathResult DescribePath(TSchemeClient& schemeClient, const TString& path, const TDescribePathSettings& settings) {
    return RetryFunction([&]() -> TDescribePathResult {
        return schemeClient.DescribePath(path, settings).GetValueSync();
    });
}

TStatus MakeDirectory(TSchemeClient& schemeClient, const TString& path, const TMakeDirectorySettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return schemeClient.MakeDirectory(path, settings).GetValueSync();
    });
}

TStatus RemoveDirectory(TSchemeClient& schemeClient, const TString& path, const TRemoveDirectorySettings& settings) {
    return RetryFunction([&]() -> TStatus {
        return schemeClient.RemoveDirectory(path, settings).GetValueSync();
    });
}

TStatus RemoveDirectoryRecursive(
        TTableClient& tableClient,
        TSchemeClient& schemeClient,
        const TString& path,
        const TRemoveDirectorySettings& settings,
        bool removeSelf) {

    auto recursiveListResult = NConsoleClient::RecursiveList(schemeClient, path, {}, removeSelf);
    if (!recursiveListResult.Status.IsSuccess()) {
        return recursiveListResult.Status;
    }

    // output order is: Root, Recursive(children)...
    // we need to reverse it to delete recursively
    for (auto it = recursiveListResult.Entries.rbegin(); it != recursiveListResult.Entries.rend(); ++it) {
        const auto& entry = *it;
        switch (entry.Type) {
            case ESchemeEntryType::Directory: {
                auto result = RemoveDirectory(schemeClient, entry.Name, settings);
                if (!result.IsSuccess()) {
                    return result;
                }
                break;
            }
            case ESchemeEntryType::Table: {
                auto result = tableClient.RetryOperationSync([path = entry.Name](TSession session) {
                    return session.DropTable(path).GetValueSync();
                });
                if (!result.IsSuccess()) {
                    return result;
                }
                break;
            }
            default:
                return TStatus(EStatus::BAD_REQUEST, {});
        }
    }

    return TStatus(EStatus::SUCCESS, {});
}

} // NDump
} // NYdb
