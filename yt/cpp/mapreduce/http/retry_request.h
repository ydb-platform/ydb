#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/http/http_client.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/fwd.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TResponseInfo
{
    TString RequestId;
    TString Response;
    int HttpCode = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestConfig
{
    NHttpClient::THttpConfig HttpConfig;
    bool IsHeavy = false;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResult>
TResult RequestWithRetry(
    IRequestRetryPolicyPtr retryPolicy,
    std::function<TResult(TMutationId&)> func)
{
    bool useSameMutationId = false;
    TMutationId mutationId;

    while (true) {
        try {
            retryPolicy->NotifyNewAttempt();
            if constexpr (std::is_same_v<TResult, void>) {
                func(mutationId);
                return;
            } else {
                return func(mutationId);
            }
        } catch (const TErrorResponse& e) {
            YT_LOG_ERROR("Retry failed %v - %v",
                e.GetError().GetMessage(),
                retryPolicy->GetAttemptDescription());

            useSameMutationId = e.IsTransportError();

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
            YT_LOG_ERROR("Retry failed %v - %v",
                e.what(),
                retryPolicy->GetAttemptDescription());

            useSameMutationId = true;

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
        if (!useSameMutationId) {
            mutationId = {};
        }
    }
}

// Retry request with given `header' and `body' using `retryPolicy'.
// If `retryPolicy == nullptr' use default, currently `TAttemptLimitedRetryPolicy(TConfig::Get()->RetryCount)`.
TResponseInfo RetryRequestWithPolicy(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body = {},
    const TRequestConfig& config = TRequestConfig());

NHttpClient::IHttpResponsePtr RequestWithoutRetry(
    const TClientContext& context,
    TMutationId& mutationId,
    THttpHeader& header,
    TMaybe<TStringBuf> body = {},
    const TRequestConfig& config = TRequestConfig());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
