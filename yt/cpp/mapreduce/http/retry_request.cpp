#include "retry_request.h"

#include "context.h"
#include "helpers.h"
#include "http_client.h"
#include "requests.h"

#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static NHttpClient::IHttpResponsePtr Request(
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TString& requestId,
    const TRequestConfig& config)
{
    TString hostName;
    if (config.IsHeavy) {
        hostName = GetProxyForHeavyRequest(context);
    } else {
        hostName = context.ServerName;
    }

    UpdateHeaderForProxyIfNeed(hostName, context, header);

    auto url = GetFullUrlForProxy(hostName, context, header);

    return context.HttpClient->Request(url, requestId, config.HttpConfig, header, body);
}

static NHttpClient::IHttpRequestPtr StartRequest(
    const TClientContext& context,
    THttpHeader& header,
    const TString& requestId,
    const TRequestConfig& config)
{
    TString hostName;
    if (config.IsHeavy) {
        hostName = GetProxyForHeavyRequest(context);
    } else {
        hostName = context.ServerName;
    }

    UpdateHeaderForProxyIfNeed(hostName, context, header);

    auto url = GetFullUrlForProxy(hostName, context, header);

    return context.HttpClient->StartRequest(url, requestId, config.HttpConfig, header);
}

NHttpClient::IHttpResponsePtr RequestWithoutRetry(
    const TClientContext& context,
    TMutationId& mutationId,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    if (header.HasMutationId()) {
        if (mutationId.IsEmpty()) {
            header.RemoveParameter("retry");
            mutationId = header.AddMutationId();
        } else {
            header.AddParameter("retry", true, /*overwrite*/ true);
            header.SetMutationId(mutationId);
        }
    }
    auto requestId = CreateGuidAsString();
    return Request(context, header, body, requestId, config);
}

NHttpClient::IHttpRequestPtr StartRequestWithoutRetry(
    const TClientContext& context,
    THttpHeader& header,
    const TRequestConfig& config)
{
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    auto requestId = CreateGuidAsString();
    return StartRequest(context, header, requestId, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
