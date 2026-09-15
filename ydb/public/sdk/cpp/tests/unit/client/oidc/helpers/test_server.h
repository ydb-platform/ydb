#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/mutex.h>

#include <condition_variable>
#include <deque>
#include <vector>

class TOidcTestServer: public THttpServer::ICallBack {
public:
    struct TReply {
        HttpCodes Status = HTTP_OK;
        TString Body;
    };

    struct TRequestInfo {
        TString Path;
        TString Method;
        TCgiParameters Form;
        TString Authorization;
    };

    TOidcTestServer();

    ~TOidcTestServer() override;

    std::string Issuer() const;

    NYdb::TOidcConfig ClientConfig() const;

    void Enqueue(TString body, HttpCodes status);

    void BlockTokenRepliesUntil(NThreading::TFuture<void> released);

    std::vector<TRequestInfo> Requests() const;

    size_t DiscoveryCount() const;

    bool WaitRequests(size_t count);

    class TRequest: public TRequestReplier {
    public:
        explicit TRequest(TOidcTestServer& server);

        bool DoReply(const TReplyParams& params) override;

        THolder<THttpServerConn> CreateHttpConnection(const TSocket& socket, size_t outputBuffer) override;

    private:
        TOidcTestServer& Server;
    };

    TClientRequest* CreateClient() override;

private:
    TPortManager Ports;
    THttpServer::TOptions Options;
    mutable TMutex Mutex;
    std::condition_variable_any Changed;
    std::deque<TReply> Replies;
    std::vector<TRequestInfo> Recorded;
    size_t Discoveries = 0;
    NThreading::TFuture<void> TokenReplyGate;
    THttpServer Server;
};
