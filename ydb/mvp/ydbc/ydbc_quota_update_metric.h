#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/client/yc_private/ydb/backup_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/database_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/quota_service.grpc.pb.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcQuotaUpdateMetricRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcQuotaUpdateMetricRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcQuotaUpdateMetricRequest>;
    const TYdbcLocation& Location;
    TRequest Request;

    THandlerActorYdbcQuotaUpdateMetricRequest(
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
        yandex::cloud::priv::quota::UpdateQuotaMetricRequest cpRequest;
        if (Request.Parameters.PostData.IsDefined()) {
            try {
                NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
            }
            catch (const yexception& e) {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
                TBase::Die(ctx);
            }
        }
        Request.Parameters.ParamsToProto(cpRequest, JsonSettings.NameGenerator);

        NYdbGrpc::TResponseCallback<google::protobuf::Empty> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, google::protobuf::Empty&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvEmpty(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeadersOnlyForIAM(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::QuotaService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::QuotaService::Stub::AsyncUpdateMetric, meta);
        Become(&THandlerActorYdbcQuotaUpdateMetricRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvEmpty::TPtr, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse("200", "OK");
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response = CreateErrorResponse(Request.Request, event->Get());
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvEmpty, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcQuotaUpdateMetric : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcQuotaUpdateMetric> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcQuotaUpdateMetric>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcQuotaUpdateMetric(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcQuotaUpdateMetric::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "GET" || (request->Method == "POST" && contentType == "application/json")) {
            ctx.Register(new THandlerActorYdbcQuotaUpdateMetricRequest(Location, HttpProxyId, event->Sender, request));
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
