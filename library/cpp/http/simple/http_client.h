#pragma once

#include "http_client_options.h"

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/network/socket.h>

#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/openssl/io/stream.h>

class TNetworkAddress;
class IOutputStream;
class TSocket;

namespace NPrivate {
    class THttpConnection;
}

/*!
 * HTTPS is supported in two modes.
 * HTTPS verification enabled by default in TKeepAliveHttpClient and disabled by default in TSimpleHttpClient.
 * HTTPS verification requires valid private certificate on server side and valid public certificate on client side.
 *
 * For client:
 * Uses builtin certs.
 * Also uses default CA path /etc/ssl/certs/ - can be provided with debian package: ca-certificates.deb.
 * It can be expanded with ENV: SSL_CERT_DIR.
 */

/*!
 * TKeepAliveHttpClient can keep connection alive with HTTP and HTTPS only if you use the same instance of class.
 * It closes connection on every socket/network error and throws error.
 * For example, HTTP code == 500 is NOT error - connection will be still open.
 * It is THREAD UNSAFE because it stores connection state in attributes.
 * If you need thread safe client, look at TSimpleHttpClient
 */

class TKeepAliveHttpClient {
public:
    using THeaders = THashMap<TString, TString>;
    using THttpCode = unsigned;

public:
    TKeepAliveHttpClient(const TString& host,
                         ui32 port,
                         TDuration socketTimeout = TDuration::Seconds(5),
                         TDuration connectTimeout = TDuration::Seconds(30));

    THttpCode DoGet(const TStringBuf relativeUrl,
                    IOutputStream* output = nullptr,
                    const THeaders& headers = THeaders(),
                    THttpHeaders* outHeaders = nullptr);

    // builds post request from headers and body
    THttpCode DoPost(const TStringBuf relativeUrl,
                     const TStringBuf body,
                     IOutputStream* output = nullptr,
                     const THeaders& headers = THeaders(),
                     THttpHeaders* outHeaders = nullptr);

    // builds request with any HTTP method from headers and body
    THttpCode DoRequest(const TStringBuf method,
                        const TStringBuf relativeUrl,
                        const TStringBuf body,
                        IOutputStream* output = nullptr,
                        const THeaders& inHeaders = THeaders(),
                        THttpHeaders* outHeaders = nullptr);

    // requires already well-formed request
    THttpCode DoRequestRaw(const TStringBuf raw,
                           IOutputStream* output = nullptr,
                           THttpHeaders* outHeaders = nullptr);

    void DisableVerificationForHttps();
    void SetClientCertificate(const TOpenSslClientIO::TOptions::TClientCert& options);

    void ResetConnection();

    const TString& GetHost() const {
        return Host;
    }

    ui32 GetPort() const {
        return Port;
    }

private:
    template <class T>
    THttpCode DoRequestReliable(const T& raw,
                                IOutputStream* output,
                                THttpHeaders* outHeaders);

    TVector<IOutputStream::TPart> FormRequest(TStringBuf method, const TStringBuf relativeUrl,
                                              TStringBuf body,
                                              const THeaders& headers, TStringBuf contentLength) const;

    THttpCode ReadAndTransferHttp(THttpInput& input, IOutputStream* output, THttpHeaders* outHeaders) const;

    bool CreateNewConnectionIfNeeded(); // Returns true if now we have a new connection.

private:
    using TVerifyCert = TOpenSslClientIO::TOptions::TVerifyCert;
    using TClientCert = TOpenSslClientIO::TOptions::TClientCert;

    const TString Host;
    const ui32 Port;
    const TDuration SocketTimeout;
    const TDuration ConnectTimeout;
    const bool IsHttps;

    THolder<NPrivate::THttpConnection> Connection;
    bool IsClosingRequired;
    TMaybe<TClientCert> ClientCertificate;
    TMaybe<TVerifyCert> HttpsVerification;

private:
    THttpInput* GetHttpInput();

    using TIfResponseRequired = std::function<bool(const THttpInput&)>;
    TIfResponseRequired IfResponseRequired;

    friend class TSimpleHttpClient;
    friend class TRedirectableHttpClient;
};

class THttpRequestException: public yexception {
private:
    int StatusCode;

public:
    THttpRequestException(int statusCode = 0);
    int GetStatusCode() const;
};

/*!
 * TSimpleHttpClient can NOT keep connection alive.
 * It closes connection after each request.
 * HTTP code < 200 || code >= 300 is error - exception will be thrown.
 * It is THREAD SAFE because it stores only consts.
 */

class TSimpleHttpClient {
protected:
    using TVerifyCert = TKeepAliveHttpClient::TVerifyCert;

    const TString Host;
    const ui32 Port;
    const TDuration SocketTimeout;
    const TDuration ConnectTimeout;
    bool HttpsVerification = false;

public:
    using THeaders = TKeepAliveHttpClient::THeaders;
    using TOptions = TSimpleHttpClientOptions;

public:
    explicit TSimpleHttpClient(const TOptions& options);

    TSimpleHttpClient(const TString& host, ui32 port,
                      TDuration socketTimeout = TDuration::Seconds(5), TDuration connectTimeout = TDuration::Seconds(30));

    void EnableVerificationForHttps();

    void DoGet(const TStringBuf relativeUrl, IOutputStream* output, const THeaders& headers = THeaders()) const;

    // builds post request from headers and body
    void DoPost(const TStringBuf relativeUrl, TStringBuf body, IOutputStream* output, const THeaders& headers = THeaders()) const;

    // requires already well-formed post request
    void DoPostRaw(const TStringBuf relativeUrl, TStringBuf rawRequest, IOutputStream* output) const;

    virtual ~TSimpleHttpClient();

private:
    TKeepAliveHttpClient CreateClient() const;

    virtual void PrepareClient(TKeepAliveHttpClient& cl) const;
    virtual void ProcessResponse(const TStringBuf relativeUrl, THttpInput& input, IOutputStream* output, const unsigned statusCode) const;
};

class TRedirectableHttpClient: public TSimpleHttpClient {
public:
    TRedirectableHttpClient(const TString& host, ui32 port, TDuration socketTimeout = TDuration::Seconds(5),
                            TDuration connectTimeout = TDuration::Seconds(30));

private:
    void PrepareClient(TKeepAliveHttpClient& cl) const override;
    void ProcessResponse(const TStringBuf relativeUrl, THttpInput& input, IOutputStream* output, const unsigned statusCode) const override;
};

namespace NPrivate {
    class THttpConnection {
    public:
        THttpConnection(const TString& host,
                        ui32 port,
                        TDuration sockTimeout,
                        TDuration connTimeout,
                        bool isHttps,
                        const TMaybe<TOpenSslClientIO::TOptions::TClientCert>& clientCert,
                        const TMaybe<TOpenSslClientIO::TOptions::TVerifyCert>& verifyCert);

        bool IsOk() const {
            return IsNotSocketClosedByOtherSide(Socket);
        }

        template <typename TContainer>
        void Write(const TContainer& request) {
            HttpOut->Write(request.data(), request.size());
            HttpIn = Ssl ? MakeHolder<THttpInput>(Ssl.Get())
                         : MakeHolder<THttpInput>(&SocketIn);
            HttpOut->Flush();
        }

        THttpInput* GetHttpInput() {
            return HttpIn.Get();
        }

    private:
        static TNetworkAddress Resolve(const TString& host, ui32 port);

        static TSocket Connect(TNetworkAddress& addr,
                               TDuration sockTimeout,
                               TDuration connTimeout,
                               const TString& host,
                               ui32 port);

    private:
        TNetworkAddress Addr;
        TSocket Socket;
        TSocketInput SocketIn;
        TSocketOutput SocketOut;
        THolder<TOpenSslClientIO> Ssl;
        THolder<THttpInput> HttpIn;
        THolder<THttpOutput> HttpOut;
    };
}

template <class T>
TKeepAliveHttpClient::THttpCode TKeepAliveHttpClient::DoRequestReliable(const T& raw,
                                                                        IOutputStream* output,
                                                                        THttpHeaders* outHeaders) {
    for (int i = 0; i < 2; ++i) {
        const bool haveNewConnection = CreateNewConnectionIfNeeded();
        const bool couldRetry = !haveNewConnection && i == 0; // Actually old connection could be already closed by server,
                                                              // so we should try one more time in this case.
        try {
            Connection->Write(raw);

            THttpCode code = ReadAndTransferHttp(*Connection->GetHttpInput(), output, outHeaders);
            if (!Connection->GetHttpInput()->IsKeepAlive()) {
                IsClosingRequired = true;
            }
            return code;
        } catch (const TSystemError& e) {
            Connection.Reset();
            if (!couldRetry || e.Status() != EPIPE) {
                throw;
            }
        } catch (const THttpReadException&) { // Actually old connection is already closed by server
            Connection.Reset();
            if (!couldRetry) {
                throw;
            }
        } catch (const std::exception&) {
            Connection.Reset();
            throw;
        }
    }
    Y_ABORT(); // We should never be here.
    return 0;
}
