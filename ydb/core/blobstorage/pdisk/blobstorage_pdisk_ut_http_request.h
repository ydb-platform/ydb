#pragma once
#include "defs.h"

namespace NKikimr {

struct THttpRequest : NMonitoring::IHttpRequest {
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

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
        return TString();
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
