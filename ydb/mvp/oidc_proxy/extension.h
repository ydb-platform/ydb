#pragma once

#include "cracked_page.h"
#include "openid_connect.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/queue.h>

namespace NMVP::NOIDC {

class IExtension;

struct TProxiedResponseParams {
    NHttp::THttpIncomingRequestPtr Request;
    THolder<TCrackedPage> ProtectedPage;
    TString ResponseError;
    THolder<NHttp::THeadersBuilder> HeadersOverride;

private:
    std::optional<TString> StatusOverride;
    std::optional<TString> MessageOverride;
    std::optional<TString> BodyOverride;

    NHttp::THttpIncomingResponsePtr Response;

    TStringBuf GetOverride(const std::optional<TString>& override, TStringBuf original) const {
        if (override.has_value()) {
            return TStringBuf(*override);
        }
        return original;
    }

public:
    void SetOriginalResponse(NHttp::THttpIncomingResponsePtr response) {
        Response = std::move(response);
        HeadersOverride = MakeHolder<NHttp::THeadersBuilder>();
        if (Response) {
            auto headers = NHttp::THeaders(Response->Headers);
            for (const auto& header : headers.Headers) {
                HeadersOverride->Set(header.first, header.second);
            }
        }
    }

    void OverrideStatus(TString&& status) {
        StatusOverride.emplace(std::move(status));
    }

    void OverrideMessage(TString&& message) {
        MessageOverride.emplace(std::move(message));
    }

    void OverrideBody(TString&& body) {
        BodyOverride.emplace(std::move(body));
    }

    TStringBuf GetStatusOverride() const {
        return GetOverride(StatusOverride, Response ? Response->Status : TStringBuf());
    }

    TStringBuf GetMessageOverride() const {
        return GetOverride(MessageOverride, Response ? Response->Message : TStringBuf());
    }

    TStringBuf GetBodyOverride() const {
        return GetOverride(BodyOverride, Response ? Response->Body : TStringBuf());
    }
};


struct TExtensionsSteps : public TQueue<std::unique_ptr<IExtension>> {
    std::unique_ptr<IExtension> Next();
};

struct TExtensionContext : public TThrRefBase {
    TActorId Sender;
    TExtensionsSteps Steps;
    TProxiedResponseParams Params;

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
