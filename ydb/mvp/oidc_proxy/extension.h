#pragma once

#include "cracked_page.h"
#include "openid_connect.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/queue.h>
#include <utility>

namespace NMVP::NOIDC {

class IExtension;

struct TProxiedResponseParams {
    NHttp::THttpIncomingRequestPtr Request;
    THolder<TCrackedPage> ProtectedPage;
    TString ResponseError;
    NHttp::THttpIncomingResponsePtr OriginalResponse;
    THolder<NHttp::THeadersBuilder> HeadersOverride;

private:
    std::optional<TString> StatusOverride;
    std::optional<TString> MessageOverride;
    std::optional<TString> BodyOverride;

public:
    void SetOriginalResponse(NHttp::THttpIncomingResponsePtr response) {
        OriginalResponse = std::move(response);
        HeadersOverride = MakeHolder<NHttp::THeadersBuilder>();
        if (OriginalResponse) {
            auto headers = NHttp::THeaders(OriginalResponse->Headers);
            for (const auto& header : headers.Headers) {
                HeadersOverride->Set(header.first, header.second);
            }
        }
    }

    void OverrideStatus(TString status) {
        StatusOverride = std::move(status);
    }

    TStringBuf GetStatus() const {
        if (StatusOverride.has_value()) {
            return TStringBuf(*StatusOverride);
        }
        return OriginalResponse ? TStringBuf(OriginalResponse->Status) : TStringBuf();
    }

    void OverrideMessage(TString message) {
        MessageOverride = std::move(message);
    }

    TStringBuf GetMessage() const {
        if (MessageOverride.has_value()) {
            return TStringBuf(*MessageOverride);
        }
        return OriginalResponse ? TStringBuf(OriginalResponse->Message) : TStringBuf();
    }

    void OverrideBody(TString body) {
        BodyOverride = std::move(body);
    }

    TStringBuf GetBody() const {
        if (BodyOverride.has_value()) {
            Cerr << "iiii GetBody override" << Endl;
            return TStringBuf(*BodyOverride);
        }
        Cerr << "iiii GetBody original" << Endl;
        return OriginalResponse ? TStringBuf(OriginalResponse->Body) : TStringBuf();
    }
};

struct TExtensionsSteps : public TQueue<std::unique_ptr<IExtension>> {
    std::unique_ptr<IExtension> Next();
};

struct TExtensionContext : public TThrRefBase {
    TActorId Sender;
    TExtensionsSteps Steps;
    THolder<TProxiedResponseParams> Params;

    void Reply(NHttp::THttpOutgoingResponsePtr httpResponse);
    void Reply();
    void Continue();
};

class IExtension {
public:
    virtual ~IExtension() = default;
    virtual void Execute(TIntrusivePtr<TExtensionContext> ctx) = 0;
};

} // NMVP::NOIDC
