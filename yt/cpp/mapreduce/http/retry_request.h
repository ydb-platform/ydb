#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/http/http_client.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/generic/maybe.h>

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

NHttpClient::IHttpResponsePtr RequestWithoutRetry(
    const TClientContext& context,
    TMutationId& mutationId,
    THttpHeader& header,
    TMaybe<TStringBuf> body = {},
    const TRequestConfig& config = {});

NHttpClient::IHttpRequestPtr StartRequestWithoutRetry(
    const TClientContext& context,
    THttpHeader& header,
    const TRequestConfig& config = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
