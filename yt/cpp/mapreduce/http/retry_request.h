#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/http/http_client.h>

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

// Retry request with given `header' and `body' using `retryPolicy'.
// If `retryPolicy == nullptr' use default, currently `TAttemptLimitedRetryPolicy(TConfig::Get()->RetryCount)`.
TResponseInfo RetryRequestWithPolicy(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body = {},
    const TRequestConfig& config = TRequestConfig());

TResponseInfo RequestWithoutRetry(
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body = {},
    const TRequestConfig& config = TRequestConfig());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
