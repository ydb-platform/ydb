#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <util/generic/map.h>

#include <memory>

namespace NMVP {

struct TMetaCapabilities {
    TMap<TString, ui32> Versions;

    void AddCapability(const TString& name, ui32 version = 1) {
        Versions[name] = version;
    }

    NJson::TJsonValue ToJson() const {
        NJson::TJsonValue capabilities(NJson::JSON_MAP);
        for (const auto& [name, version] : Versions) {
            capabilities[name] = version;
        }
        return capabilities;
    }
};

class THandlerActorMetaCapabilities : public NActors::TActor<THandlerActorMetaCapabilities> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCapabilities>;

    explicit THandlerActorMetaCapabilities(std::shared_ptr<const TMetaCapabilities> capabilities)
        : TBase(&THandlerActorMetaCapabilities::StateWork)
        , Capabilities(std::move(capabilities))
    {}

    void Handle(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue responseBody(NJson::JSON_MAP);
        responseBody["Capabilities"] = Capabilities->ToJson();

        auto response = event->Get()->Request->CreateResponseOK(
            NJson::WriteJson(responseBody, false, true),
            "application/json; charset=utf-8"
        );
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }

private:
    const std::shared_ptr<const TMetaCapabilities> Capabilities;
};

} // namespace NMVP
