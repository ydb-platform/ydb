#pragma once

#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/client/client.h>

#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

namespace NYT::NDetail {

template <typename TResult>
TResult RetryTransactionWithPolicy(
    const TClientBasePtr& client,
    std::function<TResult(ITransactionPtr)> func,
    IRequestRetryPolicyPtr retryPolicy)
{
    if (!retryPolicy) {
        retryPolicy = CreateDefaultRequestRetryPolicy(client->GetContext().Config);
    }

    while (true) {
        try {
            retryPolicy->NotifyNewAttempt();
            auto transaction = client->StartTransaction(TStartTransactionOptions());
            if constexpr (std::is_same<TResult, void>::value) {
                func(transaction);
                transaction->Commit();
                return;
            } else {
                auto result = func(transaction);
                transaction->Commit();
                return result;
            }
        } catch (const TErrorResponse& e) {
            YT_LOG_ERROR("Retry failed %v - %v",
                e.GetError().GetMessage(),
                retryPolicy->GetAttemptDescription());

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
}

} // namespace NYT::NDetail
