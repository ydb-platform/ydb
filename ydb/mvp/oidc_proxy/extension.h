#pragma once

#include "cracked_page.h"
#include "openid_connect.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/queue.h>

namespace NMVP::NOIDC {

class IExtension;

struct TProxiedResponseParams {
    NHttp::THttpIncomingRequestPtr Request;
    THolder<TCrackedPage> ProtectedPage;
    TString ResponseError;

    TString StatusOverride;
    TString MessageOverride;
    TString BodyOverride;
    THolder<NHttp::THeadersBuilder> HeadersOverride;
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
