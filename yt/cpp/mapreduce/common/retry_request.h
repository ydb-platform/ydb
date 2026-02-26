#pragma once

#include "expected_error_guard.h"
#include "retry_lib.h"
#include "wait_proxy.h"

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/fwd.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/generic/guid.h>

namespace NYT::NDetail {

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
            // NB(achains): Do not log expected error in stderr.
            if (TExpectedErrorGuard::IsErrorExpected(e)) {
                YT_LOG_INFO("Received expected error, retry failed %v - %v",
                    e.GetError().GetMessage(),
                    retryPolicy->GetAttemptDescription());
            } else {
                YT_LOG_ERROR("Retry failed %v - %v",
                    e.GetError().GetMessage(),
                    retryPolicy->GetAttemptDescription());
            }

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
