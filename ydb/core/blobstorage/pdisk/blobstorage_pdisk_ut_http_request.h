#pragma once
#include "defs.h"

namespace NKikimr {

struct THttpRequestMock : NMonitoring::IHttpRequest {
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;
    TString Path;

    ~THttpRequestMock() {}

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return Path.c_str();
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TStringBuf();
    }

    HTTP_METHOD GetMethod() const override {
        return HTTP_METHOD_UNDEFINED;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

} // NKikimr
