#include "http_proxy.h"
#include "http_static.h"
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/resource/resource.h>
#include <util/folder/path.h>
#include <util/stream/file.h>

namespace NHttp {

class THttpStaticContentHandler : public NActors::TActor<THttpStaticContentHandler> {
public:
    using TBase = NActors::TActor<THttpStaticContentHandler>;
    const TFsPath URL;
    const TFsPath FilePath;
    const TFsPath ResourcePath;
    TUrlAdapter UrlAdapter;

    THttpStaticContentHandler(const TString& url, const TString& filePath, const TString& resourcePath, TUrlAdapter&& urlAdapter)
        : TBase(&THttpStaticContentHandler::StateWork)
        , URL(url)
        , FilePath(filePath)
        , ResourcePath(resourcePath)
        , UrlAdapter(std::move(urlAdapter))
    {}

    static constexpr char ActorName[] = "HTTP_STATIC_ACTOR";

    static TInstant GetCompileTime() {
        tm compileTime = {};
        strptime(__DATE__ " " __TIME__, "%B %d %Y %H:%M:%S", &compileTime);
        return TInstant::Seconds(mktime(&compileTime));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        THttpOutgoingResponsePtr response;
        if (event->Get()->Request->Method != "GET") {
            response = event->Get()->Request->CreateResponseBadRequest("Wrong request");
            Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return;
        }
        TFsPath url(event->Get()->Request->URL.Before('?'));
        if (!url.IsAbsolute()) {
            response = event->Get()->Request->CreateResponseBadRequest("Completely wrong URL");
            Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return;
        }
        if (UrlAdapter) {
            UrlAdapter(url);
        }
        url = url.RelativeTo(URL);
        try {
            // TODO: caching?
            TString contentType = mimetypeByExt(url.GetName().c_str());
            TString data;
            TFileStat filestat;
            TFsPath resourcename(ResourcePath / url);
            if (NResource::FindExact(resourcename.GetPath(), &data)) {
                static TInstant compileTime(GetCompileTime());
                filestat.MTime = compileTime.Seconds();
            } else {
                TFsPath filename(FilePath / url);
                if (!filename.IsSubpathOf(FilePath) && filename != FilePath) {
                    response = event->Get()->Request->CreateResponseBadRequest("Wrong URL");
                    Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                    return;
                }
                if (filename.Stat(filestat) && filestat.IsFile()) {
                    data = TUnbufferedFileInput(filename).ReadAll();
                }
            }
            if (!filestat.IsNull()) {
                response = event->Get()->Request->CreateResponseOK(data, contentType, TInstant::Seconds(filestat.MTime));
            } else {
                response = event->Get()->Request->CreateResponseNotFound("File not found");
            }
        }
        catch (const yexception&) {
            response = event->Get()->Request->CreateResponseServiceUnavailable("Not available");
        }
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

NActors::IActor* CreateHttpStaticContentHandler(const TString& url, const TString& filePath, const TString& resourcePath, TUrlAdapter&& urlAdapter) {
    return new THttpStaticContentHandler(url, filePath, resourcePath, std::move(urlAdapter));
}

NActors::IActor* CreateHttpStaticContentHandler(const TString& url, const TString& filePath, const TString& resourcePath, const TString& index) {
    return CreateHttpStaticContentHandler(url, filePath, resourcePath, [index](TFsPath& url) {
        if (url.GetPath().EndsWith('/') && index) {
            url /= index;
        }
    });
}

}
