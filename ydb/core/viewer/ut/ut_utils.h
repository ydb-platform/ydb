#pragma once

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/monlib/service/service.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NViewerTests {

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;
    TString PostContent;

    THttpRequest(HTTP_METHOD method)
        : Method(method)
    {}

    ~THttpRequest() {}

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return PostContent;
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

void WaitForHttpReady(TKeepAliveHttpClient& client);

} // namespace NKikimr::NViewerTests

