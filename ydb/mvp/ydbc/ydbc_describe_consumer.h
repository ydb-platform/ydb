#pragma once
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/core/persqueue/metering_sink.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;


class THandlerActorYdbcDescribeConsumerRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDescribeConsumerRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDescribeConsumerRequest>;
    static constexpr TDuration MAX_RETRY_TIME = TDuration::Seconds(1);
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    TString DatabaseId;
    TString DatabaseName;
    TString DatabasePath;
    mutable TString Path;
    mutable TString Consumer;
    TString FolderId;
    TString CloudId;
    TInstant Deadline;

    THandlerActorYdbcDescribeConsumerRequest(
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
        const TString databaseId = Request.Parameters["databaseId"];
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
        Deadline = ctx.Now() + MAX_RETRY_TIME;
        Become(&THandlerActorYdbcDescribeConsumerRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        using namespace NYdb::NTopic;
        TString status{event->Get()->Response->Status};
        TString message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                TString endpoint = responseData["endpoint"].GetStringRobust();
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                TString database = urlParams["database"];
                TString token = Request.GetAuthToken();
                DatabaseName = responseData["name"].GetStringRobust();
                DatabaseId = Request.Parameters["databaseId"];
                DatabasePath = database;
                FolderId = responseData["folderId"].GetStringRobust();
                CloudId = responseData["cloudId"].GetStringRobust();

                if (!database.empty()) {
                    TopicClient = Location.GetTopicClientPtr(host, scheme, NYdb::NTopic::TTopicClientSettings().Database(database).AuthToken(token));
                    std::tie(message, status) = ReplyOnTopicRequest(ctx);
                    if (message.empty() && status.empty()) return;
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
        NHttp::THttpOutgoingResponsePtr response =
            Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvDescribeConsumerResult::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        const NYdb::NTopic::TDescribeConsumerResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            NYdb::NTopic::TConsumerDescription consumerDescription = result.GetConsumerDescription();
            NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(consumerDescription), root, Proto2JsonConfig);

            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        ReplyOnTopicRequest(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvDescribeConsumerResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    std::pair<TString, TString> ReplyOnTopicRequest(const NActors::TActorContext& ctx) const noexcept {
        using namespace NYdb::NTopic;

        TString message, status;

        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        Path = Request.Parameters["name"];
        Consumer = Request.Parameters["consumer"];

        TopicClient->DescribeConsumer(Path, Consumer, TDescribeConsumerSettings().IncludeStats(true).ClientTimeout(GetClientTimeout()))
                .Subscribe([actorSystem, actorId](const TAsyncDescribeConsumerResult& result) mutable {
            TAsyncDescribeConsumerResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvDescribeConsumerResult(res.ExtractValue()));
        });

        return {message, status};
    }

};



class THandlerActorYdbcDescribeConsumer : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDescribeConsumer> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDescribeConsumer>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDescribeConsumer(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDescribeConsumer::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            TParameters params(request);
            ctx.Register(new THandlerActorYdbcDescribeConsumerRequest(Location, HttpProxyId, event->Sender, request));
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

