#include "operation_helpers.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>

#include <util/string/builder.h>

#include <util/system/mutex.h>
#include <util/system/rwlock.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

ui64 RoundUpFileSize(ui64 size)
{
    constexpr ui64 roundUpTo = 4ull << 10;
    return (size + roundUpTo - 1) & ~(roundUpTo - 1);
}

bool UseLocalModeOptimization(const TClientContext& context, const IClientRetryPolicyPtr& clientRetryPolicy)
{
    if (!context.Config->EnableLocalModeOptimization) {
        return false;
    }

    static THashMap<TString, bool> localModeMap;
    static TRWMutex mutex;

    {
        TReadGuard guard(mutex);
        auto it = localModeMap.find(context.ServerName);
        if (it != localModeMap.end()) {
            return it->second;
        }
    }

    bool isLocalMode = false;
    TString localModeAttr("//sys/@local_mode_fqdn");
    // We don't want to pollute logs with errors about failed request,
    // so we check if path exists before getting it.
    if (NRawClient::Exists(clientRetryPolicy->CreatePolicyForGenericRequest(),
            context,
            TTransactionId(),
            localModeAttr,
            TExistsOptions().ReadFrom(EMasterReadKind::Cache)))
    {
        auto fqdnNode = NRawClient::TryGet(
            clientRetryPolicy->CreatePolicyForGenericRequest(),
            context,
            TTransactionId(),
            localModeAttr,
            TGetOptions().ReadFrom(EMasterReadKind::Cache));
        if (!fqdnNode.IsUndefined()) {
            auto fqdn = fqdnNode.AsString();
            isLocalMode = (fqdn == TProcessState::Get()->FqdnHostName);
            YT_LOG_DEBUG("Checking local mode; LocalModeFqdn: %v FqdnHostName: %v IsLocalMode: %v",
                fqdn,
                TProcessState::Get()->FqdnHostName,
                isLocalMode ? "true" : "false");
        }
    }

    {
        TWriteGuard guard(mutex);
        localModeMap[context.ServerName] = isLocalMode;
    }

    return isLocalMode;
}

TString GetOperationWebInterfaceUrl(TStringBuf serverName, TOperationId operationId)
{
    serverName.ChopSuffix(":80");
    serverName.ChopSuffix(".yt.yandex-team.ru");
    serverName.ChopSuffix(".yt.yandex.net");
    return ::TStringBuilder() << "https://yt.yandex-team.ru/" << serverName <<
        "/operations/" << GetGuidAsString(operationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
