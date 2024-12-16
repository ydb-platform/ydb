#include "test_http_server.h"

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/server/http_ex.h>

#include <util/generic/yexception.h>

namespace NYql {

class TTestHttpServer::TImpl : public THttpServer::ICallBack {
    class TRequestProcessor : public THttpClientRequestEx {
    public:
        explicit TRequestProcessor(TImpl* parent)
            : Parent_(parent)
        {
            Y_UNUSED(Parent_);
        }

        bool Reply(void* /*tsr*/) override {
            if (!ProcessHeaders()) {
                return true;
            }

            if (!RequestString.StartsWith("GET ")) {
                return true;
            }

            TRequest r;
            for (auto& p : ParsedHeaders) {
                if (p.first == "Authorization" && p.second.StartsWith("OAuth ")) {
                    r.OAuthToken = p.second.substr(strlen("OAuth "));
                    continue;
                }

                if (p.first == "If-None-Match") {
                    r.IfNoneMatch = p.second;
                    continue;
                }

                if (p.first == "If-Modified-Since") {
                    r.IfModifiedSince = p.second;
                    continue;
                }
            }

            auto reply = Parent_->ProcessNextRequest(r);

            switch (reply.Code) {
            case HTTP_OK:
                Output() << "HTTP/1.1 200 Ok\r\n";
                break;

            case HTTP_NOT_MODIFIED:
                Output() << "HTTP/1.1 304 Not modified\r\n";
                break;

            case HTTP_FORBIDDEN:
                Output() << "HTTP/1.1 403 Forbidden\r\n";
                break;

            default:
                return true;
            }

            if (reply.ETag) {
                Output() << "ETag: " + reply.ETag + "\r\n";
            }

            if (reply.LastModified) {
                Output() << "Last-Modified: " + reply.LastModified + "\r\n";
            }

            if (reply.Content || reply.ContentLength) {
                const int length = reply.ContentLength.GetOrElse(reply.Content.length());
                Output() << "Content-Length: " << length << "\r\n";
            }

            Output() << "\r\n";
            if (reply.Content) {
                Output() << reply.Content;
            }

            Output().Finish();

            return true;
        }

    private:
        TImpl* Parent_ = nullptr;
    };

public:
    explicit TImpl(int port)
        : HttpServer_(this, THttpServer::TOptions(port))
        , Port_(port)
    {

    }

    TClientRequest* CreateClient() override {
        return new TRequestProcessor(this);
    }

    void Start() {
        Y_ENSURE(HttpServer_.Start());
    }

    void Stop() {
        HttpServer_.Stop();
    }

    TString GetUrl() const {
        return "http://localhost:" + ToString(Port_);
    }

    void SetRequestHandler(TRequestHandler handler) {
        RequestHandler_ = std::move(handler);
    }

private:
    TReply ProcessNextRequest(const TRequest& request) {
        return RequestHandler_(request);
    }

private:
    THttpServer HttpServer_;
    const int Port_;
    TRequestHandler RequestHandler_;
};

TTestHttpServer::TTestHttpServer(int port)
    : Impl_(new TImpl(port)) {
}

TTestHttpServer::~TTestHttpServer() {
    Impl_->Stop();
}

void TTestHttpServer::Start() {
    Impl_->Start();
}

TString TTestHttpServer::GetUrl() const {
    return Impl_->GetUrl();
}

void TTestHttpServer::SetRequestHandler(TRequestHandler handler) {
    return Impl_->SetRequestHandler(std::move(handler));
}

}
