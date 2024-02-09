#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

class THandlerActorYdbcLocations : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcLocations> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcLocations>;
    const TMap<TString, TYdbcLocation> Locations;

    THandlerActorYdbcLocations(const TMap<TString, TYdbcLocation>& locations)
        : TBase(&THandlerActorYdbcLocations::StateWork)
        , Locations(locations)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        for (const auto& pr : Locations) {
            NJson::TJsonValue& jsonLocation = root.AppendValue(NJson::TJsonValue());
            jsonLocation["name"] = pr.first;
            jsonLocation["environment"] = pr.second.Environment;
            jsonLocation["endpoint"] = pr.second.GetEndpoint("cluster-api");
            jsonLocation["root"] = pr.second.RootDomain;
            if (pr.second.NotificationsEnvironmentId != 0) {
                jsonLocation["notificationsEnvironmentId"] = pr.second.NotificationsEnvironmentId;
            }
            if (pr.second.Disabled) {
                jsonLocation["disabled"] = true;
            }
            NJson::TJsonValue& dc = jsonLocation["dc"];
            dc.SetType(NJson::JSON_ARRAY);
            for (TStringBuf c : pr.second.DataCenters) {
                dc.AppendValue(c);
            }
        }
        TString body(NJson::WriteJson(root, false));
        auto response = event->Get()->Request->CreateResponseOK(body, "application/json; charset=utf-8");
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
