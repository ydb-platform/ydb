#pragma once

#include "cracked_page.h"
#include "openid_connect.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/queue.h>

namespace NMVP::NOIDC {

class TExtension;

struct TProxiedResponseParams {
    NHttp::THttpIncomingRequestPtr Request;
    THolder<TCrackedPage> ProtectedPage;
    TString ResponseError;

    TString StatusOverride;
    TString MessageOverride;
    TString BodyOverride;
    THolder<NHttp::THeadersBuilder> HeadersOverride;
};

struct TExtensionsSteps: public TQueue<std::unique_ptr<TExtension>> {
    std::unique_ptr<TExtension> Next();
};

struct TExtensionContext : public TThrRefBase {
    TActorId Sender;
    TExtensionsSteps Steps;
    THolder<TProxiedResponseParams> Params;
};

class TExtension {
public:
    virtual ~TExtension() = default;
    virtual void Execute(TIntrusivePtr<TExtensionContext> ctx) = 0;
};

class TExtensionWorker {
protected:
    const TOpenIdConnectSettings Settings;
    TIntrusivePtr<TExtensionContext> Context;

    void Reply(NHttp::THttpOutgoingResponsePtr httpResponse);
    void Reply();
    void Continue();

public:
    TExtensionWorker(const TOpenIdConnectSettings& settings)
        : Settings(settings)
    {}
};

} // NMVP::NOIDC
