#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include "mvp.h"
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorMetaCpDatabasesVerboseGET : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesVerboseGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesVerboseGET>;
    const TYdbLocation& Location;
    NActors::TActorId HttpProxyId;
    NHttp::THttpOutgoingRequestPtr MetaCpDatabasesRequest;
    NJson::TJsonValue CpDatabasesInfo;
    NJson::TJsonValue ClusterInfo;
    TRequest Request;
    ui32 Requests = 0;

    THandlerActorMetaCpDatabasesVerboseGET(
        const NActors::TActorId& httpProxyId,
        const TYdbLocation& location,
        const NActors::TActorId& sender,
        const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , HttpProxyId(httpProxyId)
        , Request(sender, request)
    {
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        Requests = 2;
        TString clusterName(Request.Parameters["cluster_name"]);
        NHttp::THttpOutgoingRequestPtr httpRequests[] = {
            NHttp::THttpOutgoingRequest::CreateRequestGet(
                InstanceMVP->GetAppropriateEndpoint(Request.Request) + "/meta/cp_databases?cluster_name=" + clusterName),
            NHttp::THttpOutgoingRequest::CreateRequestGet(
                InstanceMVP->GetAppropriateEndpoint(Request.Request) + "/meta/cluster?name=" + clusterName)
        };
        for (const auto & httpRequest: httpRequests) {
            if (TYdbLocation::GetUserToken()) {
                httpRequest->Set("Authorization", TYdbLocation::GetUserToken());
            }
            THolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> request = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
            request->Timeout = TDuration::Seconds(30);
            ctx.Send(HttpProxyId, request.Release());
        }
        MetaCpDatabasesRequest = httpRequests[0];
        Become(&THandlerActorMetaCpDatabasesVerboseGET::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    static TJsonMapper MapDatabase(const NJson::TJsonValue& databaseInfo) {
        return [databaseInfo](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            context.Stop = true;
            NJson::TJsonValue root;
            NJson::TJsonValue& databases = root["databases"];
            NJson::TJsonValue& elem = databases.AppendValue(databaseInfo);
            NJson::TJsonValue& cloudInfo = elem["CloudInfo"];
            if (input.Has("cloud")) {
                cloudInfo = std::move(input["cloud"]);
            } else if (input.Has("error")) {
                cloudInfo = std::move(input);
            }
            return root;
        };
    }

    static TErrorHandler Error() {
        return [](const TString& error, TStringBuf body, TStringBuf contentType) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& errorField = root["error"];
            errorField = NJson::TJsonValue(error);
            Y_UNUSED(body);
            Y_UNUSED(contentType);
            return root;
        };
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message = event->Get()->Response->Message;

        if (event->Get()->Error.empty() && status == "200") {
            if (MetaCpDatabasesRequest == event->Get()->Request) {
                NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &CpDatabasesInfo);
            } else {
                NJson::TJsonValue json;
                NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &json);
                if (json.Has("cluster")) {
                    ClusterInfo = std::move(json["cluster"]);
                }
            }
            if (--Requests == 0) {
                ReplyAndDie(ctx);
            }
        } else {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponse(status, message)));
            Die(ctx);
        }
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        TJsonMergeRules rules;
        TVector<TJsonMergePeer> peers;
        NJson::TJsonValue* jsonClusterLocation;
        NJson::TJsonValue fallback;
        fallback["databases"].SetType(NJson::JSON_ARRAY);

        if (ClusterInfo.GetValuePointer("location", &jsonClusterLocation)
            && jsonClusterLocation->GetType() == NJson::JSON_STRING) {
            TString clusterLocation = jsonClusterLocation->GetString();
            NJson::TJsonValue* jsonDatabases;

            if (CpDatabasesInfo.GetValuePointer("databases", &jsonDatabases)) {
                if (jsonDatabases->GetType() == NJson::JSON_ARRAY) {
                    for (const NJson::TJsonValue& jsonDatabaseInfo : jsonDatabases->GetArray()) {
                        const NJson::TJsonValue* jsonControlPlane;
                        bool success = false;
                        if (jsonDatabaseInfo.GetValuePointer("ControlPlane", &jsonControlPlane)) {
                            const NJson::TJsonValue* jsonCloudId;
                            if (jsonControlPlane->GetValuePointer("cloud_id", &jsonCloudId)) {
                                if (jsonCloudId->GetType() == NJson::JSON_STRING) {
                                    TJsonMergePeer& peer = peers.emplace_back();
                                    TString cloudId = jsonCloudId->GetString();

                                    peer.URL = InstanceMVP->GetAppropriateEndpoint(Request.Request) +
                                                "/meta/cloud" + "?location=" + clusterLocation + "&cloud_id=" + cloudId;
                                    if (TYdbLocation::GetUserToken()) {
                                        peer.Headers.Set("Authorization", TYdbLocation::GetUserToken());
                                    }
                                    peer.Timeout = TDuration::Seconds(10);
                                    peer.ErrorHandler = Error();
                                    peer.Rules.Mappers["."] = MapDatabase(jsonDatabaseInfo);
                                    success = true;
                                }
                            }
                        }
                        if (!success) {
                            fallback["databases"].AppendValue(jsonDatabaseInfo);
                        }
                    }
                }
            }
        }
        {
            TJsonMergePeer& peer = peers.emplace_back();
            peer.Rules.Mappers[".databases"] = MapAll();
            peer.ParsedDocument = std::move(fallback);
        }
        rules.Reducers[".databases"] = ReduceWithArray();
        CreateJsonMerger(HttpProxyId, Request.Sender, std::move(Request.Request), std::move(rules), std::move(peers), ctx);
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaCpDatabasesVerbose : THandlerActorYdbc, public NActors::TActor<THandlerActorMetaCpDatabasesVerbose> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCpDatabasesVerbose>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    THandlerActorMetaCpDatabasesVerbose(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&THandlerActorMetaCpDatabasesVerbose::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaCpDatabasesVerboseGET(HttpProxyId, Location, event->Sender, request));
            return;
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}
