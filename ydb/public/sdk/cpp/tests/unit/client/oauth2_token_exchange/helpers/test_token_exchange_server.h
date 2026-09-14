#pragma once
#include "jwt_check_helper.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/from_file.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/tests_data.h>

class TTestTokenExchangeServer: public THttpServer::ICallBack {
public:
    struct TCheck {
        bool ExpectRequest = true;
        HttpCodes StatusCode = HTTP_OK;
        TCgiParameters ExpectedInputParams;
        std::optional<TCgiParameters> InputParams;
        std::string Response;
        std::string ExpectedErrorPart;
        std::string Error;
        std::optional<TJwtCheck> SubjectJwtCheck;
        std::optional<TJwtCheck> ActorJwtCheck;

        void Check();

        void Reset() {
            Error.clear();
            InputParams = std::nullopt;
        }
    };

    class TRequest: public TRequestReplier {
    public:
        explicit TRequest(TTestTokenExchangeServer* server)
            : Server(server)
        {
        }

        bool DoReply(const TReplyParams& params) override;

    public:
        TTestTokenExchangeServer* Server = nullptr;
    };

    TTestTokenExchangeServer()
        : HttpOptions(PortManager.GetPort())
        , HttpServer(this, HttpOptions)
    {
        HttpServer.Start();
    }

    TClientRequest* CreateClient() override {
        return new TRequest(this);
    }

    std::string GetEndpoint() const {
        return TStringBuilder() << "http://localhost:" << HttpOptions.Port << "/exchange/token";
    }

    void Run(const std::function<void()>& f, bool checkExpectations = true);
    void Run(const NYdb::TOauth2TokenExchangeParams& params, const std::string& expectedToken = {}, bool checkExpectations = true);

    void RunFromConfig(const std::string& fileName, const std::string& expectedToken = {}, bool checkExpectations = true, const std::string& explicitTokenEndpoint = {});

    void CheckExpectations() {
        with_lock (Lock) {
            Check.Check();
        }
    }

    void WithLock(const std::function<void()>& f) {
        with_lock (Lock) {
            f();
        }
    }

public:
    TAdaptiveLock Lock;
    TPortManager PortManager;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;
    TCheck Check;
};
