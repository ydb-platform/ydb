#include "requests.h"

#include "context.h"
#include "host_manager.h"
#include "retry_request.h"

#include <yt/cpp/mapreduce/client/transaction.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/node_builder.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/generic/buffer.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool ParseBoolFromResponse(const TString& response)
{
    return GetBool(NodeFromYsonString(response));
}

TGUID ParseGuidFromResponse(const TString& response)
{
    auto node = NodeFromYsonString(response);
    return GetGuid(node.AsString());
}

////////////////////////////////////////////////////////////////////////////////

TString GetProxyForHeavyRequest(const TClientContext& context)
{
    if (!context.Config->UseHosts) {
        return context.ProxyAddress ? *context.ProxyAddress : context.ServerName;
    }

    return NPrivate::THostManager::Get().GetProxyForHeavyRequest(context);
}

void LogRequestError(
    const TString& requestId,
    const THttpHeader& header,
    const TString& message,
    const TString& attemptDescription)
{
    YT_LOG_ERROR("RSP %v - %v - %v - %v - X-YT-Parameters: %v",
        requestId,
        header.GetUrl(),
        message,
        attemptDescription,
        NodeToYsonString(header.GetParameters()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
