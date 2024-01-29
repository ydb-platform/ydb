#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcListDatastreamConsumersRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcListDatastreamConsumersRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcListDatastreamConsumersRequest>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    TString DatabaseName;

    THandlerActorYdbcListDatastreamConsumersRequest(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request
            )
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString databaseId = Request.Parameters["databaseId"];
        if (IsValidDatabaseId(databaseId)) {
            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequestGet(
                        TMVP::GetAppropriateEndpoint(Request.Request)
                        + "/ydbc/" + Location.Name + "/database" + "?databaseId=" + databaseId);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcListDatastreamConsumersRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                DatabaseName = responseData["name"].GetStringRobust();
                TString endpoint = responseData["endpoint"].GetStringRobust();
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                TString database = urlParams["database"];
                TString token = Request.GetAuthToken();
                NYdb::NTopic::TTopicClientSettings settings;
                settings.Database(database).AuthToken(token);
                if (responseData.Has("serverlessDatabase")) {
                    endpoint = responseData["serverlessInternals"]["sharedEndpoint"].GetStringRobust();
                    NHttp::CrackURL(endpoint, scheme, host, uri);
                }
                if (!database.empty()) {
                    TopicClient = Location.GetTopicClientPtr(host, scheme, settings);

                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;

                    TString name = UnescapeStreamName(Request.Parameters["name"]);

                    TopicClient->DescribeTopic(name, NYdb::NTopic::TDescribeTopicSettings().ClientTimeout(GetClientTimeout()))
                                .Subscribe([actorSystem, actorId](const NYdb::NTopic::TAsyncDescribeTopicResult& result) mutable {
                            NYdb::NTopic::TAsyncDescribeTopicResult res(result);
                            actorSystem->Send(actorId, new TEvPrivate::TEvDescribeTopicResult(res.ExtractValue()));
                        });
                    return;
                } else {
                    message = "Invalid database endpoint";
                    status = "400";
                }
            } else {
                message = "Unable to parse database information";
                status = "500";
            }
        } else {
            message = event->Get()->Response->Message;
            contentType = event->Get()->Response->ContentType;
            body = event->Get()->Response->Body;
        }
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }


    void Handle(TEvPrivate::TEvDescribeTopicResult::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_MAP);
        const NYdb::NTopic::TDescribeTopicResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            root["streamName"] = Request.Parameters["name"];
            root["databaseId"] = Request.Parameters["databaseId"];
            root["databaseName"] = DatabaseName;

            auto& list = root["consumers"];
            list.SetType(NJson::JSON_ARRAY);
            const auto& descr = result.GetTopicDescription();
            for (const auto& cons : descr.GetConsumers()) {
                auto& item = list.AppendValue(NJson::TJsonValue());
                item.SetType(NJson::JSON_MAP);
                item["consumerName"] = cons.GetConsumerName();
                item["consumerStatus"] = "ACTIVE";
                item["consumerCreationTimestamp"] = cons.GetReadFrom().Seconds();
                auto iter = cons.GetAttributes().find("_service_type");
                if (iter != cons.GetAttributes().end()) {
                    item["serviceType"] = iter->second;
                }
            }
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }


    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvDescribeTopicResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};




class THandlerActorYdbcListDatastreamConsumers : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcListDatastreamConsumers> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcListDatastreamConsumers>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcListDatastreamConsumers(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcListDatastreamConsumers::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;

        if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcListDatastreamConsumersRequest(Location, HttpProxyId, event->Sender, request));
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


} // namespace NMVP

