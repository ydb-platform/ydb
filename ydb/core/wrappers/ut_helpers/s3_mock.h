#pragma once

#include <library/cpp/http/server/http.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NWrappers {
namespace NTestHelpers {

class TS3Mock: public THttpServer::ICallBack {
public:
    struct TSettings {
        THttpServer::TOptions HttpOptions;
        bool CorruptETags;
        bool RejectUploadParts;

        TSettings();
        explicit TSettings(ui16 port);

        TSettings& WithHttpOptions(const THttpServer::TOptions& opts);
        TSettings& WithCorruptETags(bool value);
        TSettings& WithRejectUploadParts(bool value);

    }; // TSettings

private:
    class TRequest: public TRequestReplier {
        enum class EMethod {
            NotImplemented,
            Head,
            Get,
            Put,
            Post,
            Patch,
            Delete,
        };

        static EMethod ParseMethod(const char* str);
        static bool TryParseRange(TStringBuf str, std::pair<ui32, ui32>& range);

        bool HttpBadRequest(const TReplyParams& params, const TString& error = {});
        bool HttpNotFound(const TReplyParams& params, const TString& errorCode = {});
        bool HttpNotImplemented(const TReplyParams& params);
        void MaybeContinue(const TReplyParams& params);
        bool HttpServeRead(const TReplyParams& params, EMethod method, const TStringBuf path);
        bool HttpServeWrite(const TReplyParams& params, TStringBuf path, const TCgiParameters& queryParams);
        bool HttpServeAction(const TReplyParams& params, EMethod method, TStringBuf path, const TCgiParameters& queryParams);

    public:
        explicit TRequest(TS3Mock* parent);

        bool DoReply(const TReplyParams& params) override;

    private:
        TS3Mock* const Parent;

    }; // TRequest

public:
    explicit TS3Mock(const TSettings& settings = {});
    explicit TS3Mock(THashMap<TString, TString>&& data, const TSettings& settings = {});
    explicit TS3Mock(const THashMap<TString, TString>& data, const TSettings& settings = {});

    TClientRequest* CreateClient();
    bool Start();
    const char* GetError();

    const THashMap<TString, TString>& GetData() const { return Data; }

private:
    const TSettings Settings;
    THashMap<TString, TString> Data;

    int NextUploadId = 1;
    THashMap<std::pair<TString, TString>, TVector<TString>> MultipartUploads;
    THttpServer HttpServer;

}; // TS3Mock

} // NTestHelpers
} // NWrappers
} // NKikimr
