#include "retry_lib.h"

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/retry_policy.h>

#include <util/string/builder.h>
#include <util/generic/set.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAttemptLimitedRetryPolicy::TAttemptLimitedRetryPolicy(ui32 attemptLimit, const TConfigPtr& config)
    : Config_(config)
    , AttemptLimit_(attemptLimit)
{ }

void TAttemptLimitedRetryPolicy::NotifyNewAttempt()
{
    ++Attempt_;
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::OnGenericError(const std::exception& e)
{
    if (IsAttemptLimitExceeded()) {
        return Nothing();
    }
    return GetBackoffDuration(e, Config_);
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::OnRetriableError(const TErrorResponse& e)
{
    if (IsAttemptLimitExceeded()) {
        return Nothing();
    }
    return GetBackoffDuration(e, Config_);
}

void TAttemptLimitedRetryPolicy::OnIgnoredError(const TErrorResponse& /*e*/)
{
    --Attempt_;
}

TString TAttemptLimitedRetryPolicy::GetAttemptDescription() const
{
    return ::TStringBuilder() << "attempt " << Attempt_ << " of " << AttemptLimit_;
}

bool TAttemptLimitedRetryPolicy::IsAttemptLimitExceeded() const
{
    return Attempt_ >= AttemptLimit_;
}
////////////////////////////////////////////////////////////////////////////////

class TTimeLimitedRetryPolicy
    : public IRequestRetryPolicy
{
public:
    TTimeLimitedRetryPolicy(IRequestRetryPolicyPtr retryPolicy, TDuration timeout)
        : RetryPolicy_(retryPolicy)
        , Deadline_(TInstant::Now() + timeout)
        , Timeout_(timeout)
    { }
    void NotifyNewAttempt() override
    {
        if (TInstant::Now() >= Deadline_) {
            ythrow TRequestRetriesTimeout() << "retry timeout exceeded (timeout: " << Timeout_ << ")";
        }
        RetryPolicy_->NotifyNewAttempt();
    }

    TMaybe<TDuration> OnGenericError(const std::exception& e) override
    {
        return RetryPolicy_->OnGenericError(e);
    }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        return RetryPolicy_->OnRetriableError(e);
    }

    void OnIgnoredError(const TErrorResponse& e) override
    {
        return RetryPolicy_->OnIgnoredError(e);
    }

    TString GetAttemptDescription() const override
    {
        return RetryPolicy_->GetAttemptDescription();
    }

private:
    const IRequestRetryPolicyPtr RetryPolicy_;
    const TInstant Deadline_;
    const TDuration Timeout_;
};

////////////////////////////////////////////////////////////////////////////////

class TDefaultClientRetryPolicy
    : public IClientRetryPolicy
{
public:
    explicit TDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider, const TConfigPtr& config)
        : RetryConfigProvider_(std::move(retryConfigProvider))
        , Config_(config)
    { }

    IRequestRetryPolicyPtr CreatePolicyForGenericRequest() override
    {
        return Wrap(CreateDefaultRequestRetryPolicy(Config_));
    }

    IRequestRetryPolicyPtr CreatePolicyForStartOperationRequest() override
    {
        return Wrap(MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(Config_->StartOperationRetryCount), Config_));
    }

    IRequestRetryPolicyPtr Wrap(IRequestRetryPolicyPtr basePolicy)
    {
        auto config = RetryConfigProvider_->CreateRetryConfig();
        if (config.RetriesTimeLimit < TDuration::Max()) {
            return ::MakeIntrusive<TTimeLimitedRetryPolicy>(std::move(basePolicy), config.RetriesTimeLimit);
        }
        return basePolicy;
    }

private:
    IRetryConfigProviderPtr RetryConfigProvider_;
    const TConfigPtr Config_;
};

class TDefaultRetryConfigProvider
    : public IRetryConfigProvider
{
public:
    TRetryConfig CreateRetryConfig() override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestRetryPolicyPtr CreateDefaultRequestRetryPolicy(const TConfigPtr& config)
{
    return MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(config->RetryCount), config);
}

IClientRetryPolicyPtr CreateDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider, const TConfigPtr& config)
{
    return MakeIntrusive<TDefaultClientRetryPolicy>(std::move(retryConfigProvider), config);
}
IRetryConfigProviderPtr CreateDefaultRetryConfigProvider()
{
    return MakeIntrusive<TDefaultRetryConfigProvider>();
}

////////////////////////////////////////////////////////////////////////////////

static bool IsChunkError(int code)
{
    return code / 100 == 7;
}

// Check whether:
// 1) codes contain at least one chunk error AND
// 2) codes don't contain non-retriable chunk errors.
static bool IsRetriableChunkError(const TSet<int>& codes)
{
    using namespace NClusterErrorCodes;
    auto isChunkError = false;
    for (auto code : codes) {
        switch (code) {
            case NChunkClient::SessionAlreadyExists:
            case NChunkClient::ChunkAlreadyExists:
            case NChunkClient::WindowError:
            case NChunkClient::BlockContentMismatch:
            case NChunkClient::InvalidBlockChecksum:
            case NChunkClient::BlockOutOfRange:
            case NChunkClient::MissingExtension:
            case NChunkClient::NoSuchBlock:
            case NChunkClient::NoSuchChunk:
            case NChunkClient::NoSuchChunkList:
            case NChunkClient::NoSuchChunkTree:
            case NChunkClient::NoSuchChunkView:
            case NChunkClient::NoSuchMedium:
                return false;
            default:
                isChunkError |= IsChunkError(code);
                break;
        }
    }
    return isChunkError;
}

static TMaybe<TDuration> TryGetBackoffDuration(const TErrorResponse& errorResponse, const TConfigPtr& config)
{
    int httpCode = errorResponse.GetHttpCode();
    if (httpCode / 100 != 4 && !errorResponse.IsFromTrailers()) {
        return config->RetryInterval;
    }

    auto allCodes = errorResponse.GetError().GetAllErrorCodes();
    using namespace NClusterErrorCodes;
    if (httpCode == 429
        || allCodes.count(NSecurityClient::RequestQueueSizeLimitExceeded)
        || allCodes.count(NRpc::RequestQueueSizeLimitExceeded))
    {
        // request rate limit exceeded
        return config->RateLimitExceededRetryInterval;
    }
    if (errorResponse.IsConcurrentOperationsLimitReached()) {
        // limit for the number of concurrent operations exceeded
        return config->StartOperationRetryInterval;
    }
    if (IsRetriableChunkError(allCodes)) {
        // chunk client errors
        return config->ChunkErrorsRetryInterval;
    }
    for (auto code : {
        NRpc::TransportError,
        NRpc::Unavailable,
        NApi::RetriableArchiveError,
        NSequoiaClient::SequoiaRetriableError,
        Canceled,
    }) {
        if (allCodes.contains(code)) {
            return config->RetryInterval;
        }
    }
    return Nothing();
}

TDuration GetBackoffDuration(const TErrorResponse& errorResponse, const TConfigPtr& config)
{
    return TryGetBackoffDuration(errorResponse, config).GetOrElse(config->RetryInterval);
}

bool IsRetriable(const TErrorResponse& errorResponse)
{
    // Retriability of an error doesn't depend on config, so just use global one.
    return TryGetBackoffDuration(errorResponse, TConfig::Get()).Defined();
}

bool IsRetriable(const std::exception& ex)
{
    if (dynamic_cast<const TRequestRetriesTimeout*>(&ex)) {
        return false;
    }
    return true;
}

TDuration GetBackoffDuration(const std::exception& /*error*/, const TConfigPtr& config)
{
    return GetBackoffDuration(config);
}

TDuration GetBackoffDuration(const TConfigPtr& config)
{
    return config->RetryInterval;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
