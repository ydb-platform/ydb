#pragma once

#include <library/cpp/http/misc/httpcodes.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <functional>

namespace NYql {

class TTestHttpServer {
public:
    struct TReply {
        int Code = 0;
        TString ETag;
        TString LastModified;
        TString Content;
        TMaybe<int> ContentLength;

        static TReply Ok(const TString& content, TMaybe<int> contentLength = {}) {
            TReply r;
            r.Code = HTTP_OK;
            r.Content = content;
            r.ContentLength = contentLength;
            return r;
        }

        static TReply OkETag(const TString& content, const TString& etag, TMaybe<int> contentLength = {}) {
            TReply r = Ok(content, contentLength);
            r.ETag = etag;
            return r;
        }

        static TReply OkLastModified(const TString& content, const TString& lastModified, TMaybe<int> contentLength = {}) {
            TReply r = Ok(content, contentLength);
            r.LastModified = lastModified;
            return r;
        }

        static TReply NotModified(const TString& etag = {}, const TString& lastModified = {}) {
            TReply r;
            r.Code = HTTP_NOT_MODIFIED;
            r.ETag = etag;
            r.LastModified = lastModified;
            return r;
        }

        static TReply Forbidden() {
            TReply r;
            r.Code = HTTP_FORBIDDEN;
            return r;
        }
    };

    struct TRequest {
        TString OAuthToken;
        TString IfNoneMatch;
        TString IfModifiedSince;
    };

    typedef std::function<TReply(const TRequest& request)> TRequestHandler;

public:
    explicit TTestHttpServer(int port);
    ~TTestHttpServer();
    void Start();
    TString GetUrl() const;
    void SetRequestHandler(TRequestHandler handler);

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

}
