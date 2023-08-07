#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// IRequestRetryPolicy class controls retries of single request.
class IRequestRetryPolicy
    : public virtual TThrRefBase
{
public:
    // Helper function that returns text description of current attempt, e.g.
    //   "attempt 3 / 10"
    // used in logs.
    virtual TString GetAttemptDescription() const = 0;

    // Library code calls this function before any request attempt.
    virtual void NotifyNewAttempt() = 0;

    // OnRetriableError is called whenever client gets YT error that can be retried (e.g. operation limit exceeded).
    // OnGenericError is called whenever request failed due to generic error like network error.
    //
    // Both methods must return nothing if policy doesn't want to retry this error.
    // Otherwise method should return backoff time.
    virtual TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) = 0;
    virtual TMaybe<TDuration> OnGenericError(const std::exception& e) = 0;

    // OnIgnoredError is called whenever client gets an error but is going to ignore it.
    virtual void OnIgnoredError(const TErrorResponse& /*e*/) = 0;
};
using IRequestRetryPolicyPtr = ::TIntrusivePtr<IRequestRetryPolicy>;

////////////////////////////////////////////////////////////////////////////////

// IClientRetryPolicy controls creation of policies for individual requests.
class IClientRetryPolicy
    : public virtual TThrRefBase
{
public:
    virtual IRequestRetryPolicyPtr CreatePolicyForGenericRequest() = 0;
    virtual IRequestRetryPolicyPtr CreatePolicyForStartOperationRequest() = 0;
};


////////////////////////////////////////////////////////////////////////////////

class TAttemptLimitedRetryPolicy
    : public IRequestRetryPolicy
{
public:
    explicit TAttemptLimitedRetryPolicy(ui32 attemptLimit, const TConfigPtr& config);

    void NotifyNewAttempt() override;

    TMaybe<TDuration> OnGenericError(const std::exception& e) override;
    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override;
    void OnIgnoredError(const TErrorResponse& e) override;
    TString GetAttemptDescription() const override;

    bool IsAttemptLimitExceeded() const;

protected:
    const TConfigPtr Config_;

private:
    const ui32 AttemptLimit_;
    ui32 Attempt_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestRetryPolicyPtr CreateDefaultRequestRetryPolicy(const TConfigPtr& config);
IClientRetryPolicyPtr CreateDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider, const TConfigPtr& config);
IRetryConfigProviderPtr CreateDefaultRetryConfigProvider();

////////////////////////////////////////////////////////////////////////////////

// Check if error returned by YT can be retried
bool IsRetriable(const TErrorResponse& errorResponse);
bool IsRetriable(const std::exception& ex);

// Get backoff duration for errors returned by YT.
TDuration GetBackoffDuration(const TErrorResponse& errorResponse, const TConfigPtr& config);

// Get backoff duration for errors that are not TErrorResponse.
TDuration GetBackoffDuration(const std::exception& error, const TConfigPtr& config);
TDuration GetBackoffDuration(const TConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
