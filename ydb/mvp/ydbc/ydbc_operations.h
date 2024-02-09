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
#include <ydb/public/api/client/yc_private/ydb/console_service.grpc.pb.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

class THandlerActorYdbcOperationsStatsCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcOperationsStatsCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcOperationsStatsCollector>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;
    TRequest Request;
    THolder<TEvPrivate::TEvListOperationsResponse> ListOperationsResponse;
    THolder<TEvPrivate::TEvErrorResponse> ErrorResponse;

    THandlerActorYdbcOperationsStatsCollector(
            const TYdbcLocation& location,
            const NActors::TActorId&,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , Request(sender, request)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        yandex::cloud::priv::ydb::v1::ListOperationsRequest cpRequest;
        if (Request.Parameters.PostData.IsDefined()) {
            //cpRequest.set_page_size(1000);
            try {
                NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
            }
            catch (const yexception& e) {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
                TBase::Die(ctx);
            }
        } else {
            //cpRequest.set_page_size(1000);
            Request.Parameters.ParamsToProto(cpRequest, JsonSettings.NameGenerator);
        }
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListOperationsResponse> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListOperationsResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvListOperationsResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::ConsoleService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncListOperations, meta);
        Become(&THandlerActorYdbcOperationsStatsCollector::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvListOperationsResponse::TPtr event, const NActors::TActorContext& ctx) {
        ListOperationsResponse = event->Release();
        ReplyAndDie(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        ErrorResponse = event->Release();
        ReplyAndDie(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        if (ErrorResponse == nullptr) {
            if (ListOperationsResponse != nullptr) {
                TJsonSettings jsonSettings(JsonSettings);
                jsonSettings.EmptyRepeated = true;
                TStringStream stream;
                TProtoToJson::ProtoToJson(stream, ListOperationsResponse->Response, jsonSettings);
                response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
            } else {
                response = Request.Request->CreateResponseServiceUnavailable("Operations info is not available", "text/plain");
            }
        } else {
            response = CreateErrorResponse(Request.Request, ErrorResponse.Get());
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvListOperationsResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcOperations : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcOperations> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcOperations>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcOperations(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcOperations::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET" || request->Method == "POST") {
            ctx.Register(new THandlerActorYdbcOperationsStatsCollector(Location, HttpProxyId, event->Sender, event->Get()->Request));
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
