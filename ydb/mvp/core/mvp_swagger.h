#pragma once
#include <util/generic/hash_set.h>
#include <util/string/join.h>
#include <util/stream/file.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/resource/resource.h>
#include <ydb/core/viewer/json/json.h>
#include "core_ydbc_impl.h"
#include <ydb/public/api/client/yc_private/ydb/v1/database_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/backup_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/quota_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/console_service.grpc.pb.h>
#include <filesystem>

namespace NMVP {

class THandlerActorMvpSwagger : public NActors::TActor<THandlerActorMvpSwagger> {
public:
    using TBase = NActors::TActor<THandlerActorMvpSwagger>;

    THandlerActorMvpSwagger()
        : TBase(&THandlerActorMvpSwagger::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root["swagger"] = "2.0";
        root["basePath"] = "/";
        root["schemes"].AppendValue("http");
        NJson::TJsonValue& bearerAuth = root["securityDefinitions"]["Bearer"];
        bearerAuth["type"] = "apiKey";
        bearerAuth["name"] = "Authorization";
        bearerAuth["in"] = "header";
        root["produces"].AppendValue("application/json");
        {
            NJson::TJsonValue& info = root["info"];
            info["version"] = "1.0.0";
            info["title"] = "MVP";
            info["description"] = "Multi-Viewer Proxy";
        }

        auto response = event->Get()->Request->CreateResponseOK(NJson::WriteJson(root, false, true), "application/json; charset=utf-8");
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

private:
    // Gets API method JSON scheme listed in content folder ya.make RESOURCES section
    static NJson::TJsonValue GetJsonScheme(const TFsPath& name) {
        static const TFsPath resourcesBasePath = "/mvp/content/api/schemes/";
        const TString resourcePath = (resourcesBasePath / name).GetPath();
        TString scheme;
        if (!NResource::FindExact(resourcePath, &scheme)) {
            ythrow yexception() << "API method JSON scheme '" << resourcePath << "' is not found in resources";
        }
        NJson::TJsonValue value;
        NJson::ReadJsonTree(scheme, &value, true);
        return value;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
