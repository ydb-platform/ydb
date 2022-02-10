#include "mon_service_http_request.h"
#include "monservice.h"

using namespace NMonitoring;

IMonHttpRequest::~IMonHttpRequest() {
}

TMonService2HttpRequest::~TMonService2HttpRequest() {
}

TString TMonService2HttpRequest::GetServiceTitle() const {
    return MonService->GetTitle();
}

IOutputStream& TMonService2HttpRequest::Output() {
    return *Out;
}

HTTP_METHOD TMonService2HttpRequest::GetMethod() const {
    return HttpRequest->GetMethod();
}

TStringBuf TMonService2HttpRequest::GetPathInfo() const {
    return PathInfo;
}

TStringBuf TMonService2HttpRequest::GetPath() const {
    return HttpRequest->GetPath();
}

TStringBuf TMonService2HttpRequest::GetUri() const {
    return HttpRequest->GetURI();
}

const TCgiParameters& TMonService2HttpRequest::GetParams() const {
    return HttpRequest->GetParams();
}

const TCgiParameters& TMonService2HttpRequest::GetPostParams() const {
    return HttpRequest->GetPostParams();
}

TStringBuf TMonService2HttpRequest::GetHeader(TStringBuf name) const {
    const THttpHeaders& headers = HttpRequest->GetHeaders();
    const THttpInputHeader* header = headers.FindHeader(name);
    if (header != nullptr) {
        return header->Value();
    }
    return TStringBuf();
}

const THttpHeaders& TMonService2HttpRequest::GetHeaders() const {
    return HttpRequest->GetHeaders();
}

TString TMonService2HttpRequest::GetRemoteAddr() const {
    return HttpRequest->GetRemoteAddr();
}

TStringBuf TMonService2HttpRequest::GetCookie(TStringBuf name) const {
    TStringBuf cookie = GetHeader("Cookie");
    size_t size = cookie.size();
    size_t start = 0;
    while (start < size) {
        size_t semicolon = cookie.find(';', start);
        auto pair = cookie.substr(start, semicolon - start);
        if (!pair.empty()) {
            size_t equal = pair.find('=');
            if (equal != TStringBuf::npos) {
                auto cookieName = pair.substr(0, equal);
                if (cookieName == name) {
                    size_t valueStart = equal + 1;
                    auto cookieValue = pair.substr(valueStart, semicolon - valueStart);
                    return cookieValue;
                }
            }
            start = semicolon;
            while (start < size && (cookie[start] == ' ' || cookie[start] == ';')) {
                ++start;
            }
        }
    }
    return TStringBuf();
}
