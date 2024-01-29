#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

class THandlerActorYdbcDatabasesStatsCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatabasesStatsCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatabasesStatsCollector>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;
    TRequest Request;
    TInstant DatabaseRequestDeadline;
    static constexpr TDuration MAX_DATABSE_REQUEST_TIME = TDuration::Seconds(10);
    TDuration databaseRequestRetryDelta = TDuration::MilliSeconds(50);
    THolder<TEvPrivate::TEvListDatabaseResponse> ListDatabaseResponse;
    THolder<TEvPrivate::TEvErrorResponse> ErrorResponse;

    THandlerActorYdbcDatabasesStatsCollector(
            const TYdbcLocation& location,
            const NActors::TActorId&,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , Request(sender, request)
    {}

    void SendDatabaseRequest(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        yandex::cloud::priv::ydb::v1::ListDatabasesRequest cpRequest;
        if (Request.Parameters.PostData.IsDefined()) {
            cpRequest.set_page_size(1000);
            try {
                NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
            }
            catch (const yexception& e) {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
                TBase::Die(ctx);
            }
        } else {
            cpRequest.set_page_size(1000);
            Request.Parameters.ParamsToProto(cpRequest, JsonSettings.NameGenerator);
        }
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListDatabasesResponse> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListDatabasesResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvListDatabaseResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        meta.Timeout = TDuration::MilliSeconds(FromStringWithDefault(Request.Parameters["timeout"], GetClientTimeout().MilliSeconds()));
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::DatabaseService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncList, meta);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        DatabaseRequestDeadline = ctx.Now() + MAX_DATABSE_REQUEST_TIME;
        SendDatabaseRequest(ctx);
        Become(&THandlerActorYdbcDatabasesStatsCollector::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvListDatabaseResponse::TPtr event, const NActors::TActorContext& ctx) {
        ListDatabaseResponse = event->Release();
        ReplyAndDie(ctx);
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendDatabaseRequest(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (event->Get()->Status.StartsWith("4") || DatabaseRequestDeadline <= ctx.Now()) {
            ErrorResponse = event->Release();
            ReplyAndDie(ctx);
        } else {
            ctx.Schedule(databaseRequestRetryDelta, new TEvPrivate::TEvRetryRequest());
            databaseRequestRetryDelta *= 2;
        }
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        if (ErrorResponse == nullptr) {
            if (ListDatabaseResponse != nullptr) {
                NJson::TJsonValue jsonRoot;
                NProtobufJson::Proto2Json(ListDatabaseResponse->Databases, jsonRoot, Proto2JsonConfig);
                if (!jsonRoot.Has("databases")) {
                    jsonRoot["databases"].SetType(NJson::JSON_ARRAY);
                }
                if (jsonRoot["databases"].GetType() == NJson::JSON_ARRAY) {
                    for (NJson::TJsonValue& jsonDatabase : jsonRoot["databases"].GetArraySafe()) {
                        jsonDatabase["location"] = Location.Name;
                    }
                }
                TStringStream body;
                NJson::WriteJson(&body, &jsonRoot, JsonWriterConfig);
                response = Request.Request->CreateResponseOK(body.Str(), "application/json; charset=utf-8");
            } else {
                response = Request.Request->CreateResponseServiceUnavailable("Databases info is not available", "text/plain");
            }
        } else {
            response = CreateErrorResponse(Request.Request, ErrorResponse.Get());
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvListDatabaseResponse, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcDatabases : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDatabases> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDatabases>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDatabases(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDatabases::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "GET" || request->Method == "HEAD" || (request->Method == "POST" && contentType == "application/json")) {
            ctx.Register(new THandlerActorYdbcDatabasesStatsCollector(Location, HttpProxyId, event->Sender, event->Get()->Request));
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
