#pragma once
#include "json_handlers.h"
#include "viewer.h"
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonConfig : public TActorBootstrapped<TJsonConfig> {
    using TBase = TActorBootstrapped<TJsonConfig>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonConfig(IViewer *viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TKikimrRunConfig& kikimrRunConfig = Viewer->GetKikimrRunConfig();
        TStringStream json;
        auto config = kikimrRunConfig.AppConfig;
        config.MutableNameserviceConfig()->ClearClusterUUID();
        config.MutableNameserviceConfig()->ClearAcceptUUID();
        config.ClearAuthConfig();
        TProtoToJson::ProtoToJson(json, config);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Configuration",
            .Description = "Returns configuration",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrConfig::TAppConfig>());
        return yaml;
    }
};

}
