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


TResponseInfo RetryRequestWithPolicy(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    UpdateHeaderForProxyIfNeed(context.ServerName, context, header);

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    bool useMutationId = header.HasMutationId();
    bool retryWithSameMutationId = false;

    if (!retryPolicy) {
        retryPolicy = CreateDefaultRequestRetryPolicy(context.Config);
    }

    while (true) {
        auto requestId = CreateGuidAsString();
        try {
            retryPolicy->NotifyNewAttempt();

            if (useMutationId) {
                if (retryWithSameMutationId) {
                    header.AddParameter("retry", true, /* overwrite = */ true);
                } else {
                    header.RemoveParameter("retry");
                    header.AddMutationId();
                }
            }

            auto response = Request(context, header, body, requestId, config);
            return TResponseInfo{
                .RequestId = response->GetRequestId(),
                .Response = response->GetResponse(),
                .HttpCode = response->GetStatusCode(),
            };
        } catch (const TErrorResponse& e) {
            LogRequestError(requestId, header, e.what(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = e.IsTransportError();

            if (!IsRetriable(e)) {
                throw;
            }

            auto maybeRetryTimeout = retryPolicy->OnRetriableError(e);
            if (maybeRetryTimeout) {
                TWaitProxy::Get()->Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        } catch (const std::exception& e) {
            LogRequestError(requestId, header, e.what(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = true;

            if (!IsRetriable(e)) {
                throw;
            }

            auto maybeRetryTimeout = retryPolicy->OnGenericError(e);
            if (maybeRetryTimeout) {
                TWaitProxy::Get()->Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        }
    }

    Y_ABORT("Retries must have either succeeded or thrown an exception");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
