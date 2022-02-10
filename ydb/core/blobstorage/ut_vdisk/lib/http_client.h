#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>

#include <library/cpp/http/io/stream.h>

// Simple http client based on library/cpp/http/simple.

class TNetworkAddress;
class IOutputStream;
class TSocket;

class THttpRequestException : public yexception {
private:
    int StatusCode;

public:
    THttpRequestException(int statusCode = 0);
    int GetStatusCode() const;
};

class THttpClient {
public:
    using THeaders = THashMap<TString, TString>;

public:
    THttpClient(const TString& host,
                ui32 port,
                TDuration socketTimeout = TDuration::Seconds(5),
                TDuration connectTimeout = TDuration::Seconds(30));

    virtual ~THttpClient();

    void SendHttpRequest(const TStringBuf relativeUrl,
                         const TString& body,
                         const TString& method,
                         IOutputStream* output,
                         const THeaders& headers = THeaders()) const;

private:
    void ReadAndTransferHttp(const TStringBuf relativeUrl, THttpInput& inp, IOutputStream* output) const;

    std::unique_ptr<TNetworkAddress> Resolve() const;
    std::unique_ptr<TSocket> Connect(TNetworkAddress& addr) const;
    void ProcessResponse(const TStringBuf relativeUrl, THttpInput& input, IOutputStream* output,
        const unsigned statusCode) const;

private:
    const TString Host;
    const ui32 Port;
    const TDuration SocketTimeout;
    const TDuration ConnectTimeout;
    bool IsHttps;
};
