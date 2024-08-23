#pragma once

#include "fwd.h"

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/datetime/base.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <util/stream/fwd.h>

#include <memory>

namespace NYT::NHttpClient {

////////////////////////////////////////////////////////////////////////////////

struct THttpConfig
{
    TDuration SocketTimeout = TDuration::Zero();
};

////////////////////////////////////////////////////////////////////////////////

class IHttpResponse
{
public:
    virtual ~IHttpResponse() = default;

    virtual int GetStatusCode() = 0;
    virtual IInputStream* GetResponseStream() = 0;
    virtual TString GetResponse() = 0;
    virtual TString GetRequestId() const = 0;
};

class IHttpRequest
{
public:
    virtual ~IHttpRequest() = default;

    virtual IOutputStream* GetStream() = 0;
    virtual IHttpResponsePtr Finish() = 0;
};


class IHttpClient
{
public:
    virtual ~IHttpClient() = default;

    virtual IHttpResponsePtr Request(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& header, TMaybe<TStringBuf> body = {}) = 0;

    virtual IHttpResponsePtr Request(const TString& url, const TString& requestId, const THttpHeader& header, TMaybe<TStringBuf> body = {})
    {
        return Request(url, requestId, /*config*/ {}, header, body);
    }

    virtual IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& header) = 0;

    virtual IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpHeader& header)
    {
        return StartRequest(url, requestId, /*config*/ {}, header);
    }
};

////////////////////////////////////////////////////////////////////////////////

IHttpClientPtr CreateDefaultHttpClient();

IHttpClientPtr CreateCoreHttpClient(bool useTLS, const TConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpClient
