#pragma once

#include "meta_settings.h"

#include <ydb/mvp/meta/support_links/entities.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NMVP {

inline NJson::TJsonValue BuildSupportLinksEntityConfigJson(bool configured) {
    NJson::TJsonValue entityJson(NJson::JSON_MAP);
    entityJson["configured"] = configured;
    return entityJson;
}

inline NJson::TJsonValue BuildSupportLinksConfigJson(const TSupportLinksSettings& settings) {
    NJson::TJsonValue entitiesJson(NJson::JSON_MAP);
    for (const auto entityType : GetAllSupportLinksEntityTypes()) {
        entitiesJson[TString(GetSupportLinksEntityName(entityType))] =
            BuildSupportLinksEntityConfigJson(!GetSupportLinksEntityConfigs(settings, entityType).empty());
    }

    NJson::TJsonValue responseBody(NJson::JSON_MAP);
    responseBody["entities"] = std::move(entitiesJson);
    return responseBody;
}

class THandlerActorMetaSupportLinksConfig : public NActors::TActor<THandlerActorMetaSupportLinksConfig> {
public:
    using TBase = NActors::TActor<THandlerActorMetaSupportLinksConfig>;

    explicit THandlerActorMetaSupportLinksConfig(const TSupportLinksSettings& supportLinksSettings)
        : TBase(&THandlerActorMetaSupportLinksConfig::StateWork)
        , SupportLinksSettings(supportLinksSettings)
    {}

    void Handle(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event, const NActors::TActorContext& ctx) {
        auto response = event->Get()->Request->CreateResponseOK(
            NJson::WriteJson(BuildSupportLinksConfigJson(SupportLinksSettings), false, true),
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
    const TSupportLinksSettings SupportLinksSettings;
};

} // namespace NMVP
