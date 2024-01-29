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
#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/mvp/ydbc/protos/ydbc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/storage_type_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/resource_preset_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/console_service.grpc.pb.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcConfigCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcConfigCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcConfigCollector>;
    const TYdbcLocation& Location;
    TRequest Request;
    ui32 Requests = 0;
    THolder<TEvPrivate::TEvListStorageTypesResponse> StorageTypesResponse;
    THolder<TEvPrivate::TEvListResourcePresetsResponse> ResourcePresetsResponse;
    THolder<TEvPrivate::TEvGetConfigResponse> GetConfigResponse;
    TString Error;

    THandlerActorYdbcConfigCollector(
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
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        meta.Timeout = GetClientTimeout();
        {
            yandex::cloud::priv::ydb::v1::ListStorageTypesRequest cpRequest;
            cpRequest.set_page_size(1000);
            NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListStorageTypesResponse> responseCb =
                [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListStorageTypesResponse&& response) -> void {
                if (status.Ok()) {
                    actorSystem->Send(actorId, new TEvPrivate::TEvListStorageTypesResponse(std::move(response)));
                } else {
                    actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse("Error calling ListStorageTypesRequest: " + status.Msg));
                }
            };
            auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::StorageTypeService>("cp-api");
            connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::StorageTypeService::Stub::AsyncList, meta);
            ++Requests;
        }
        {
            yandex::cloud::priv::ydb::v1::ListResourcePresetsRequest cpRequest;
            cpRequest.set_page_size(1000);
            NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse> responseCb =
                [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse&& response) -> void {
                if (status.Ok()) {
                    actorSystem->Send(actorId, new TEvPrivate::TEvListResourcePresetsResponse(std::move(response)));
                } else {
                    actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse("Error calling ListResourcePresetsRequest: " + status.Msg));
                }
            };
            auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::ResourcePresetService>("cp-api");
            connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::ResourcePresetService::Stub::AsyncList, meta);
            ++Requests;
        }
        {
            yandex::cloud::priv::ydb::v1::GetConfigRequest cpRequest;
            if (Request.Parameters.PostData.IsDefined()) {
                try {
                    NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
                }
                catch (const yexception& e) {
                    ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
                    TBase::Die(ctx);
                }
            } else {
                Request.Parameters.ParamsToProto(cpRequest, JsonSettings.NameGenerator);
            }
            NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::GetConfigResponse> responseCb =
                [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::GetConfigResponse&& response) -> void {
                if (status.Ok()) {
                    actorSystem->Send(actorId, new TEvPrivate::TEvGetConfigResponse(std::move(response)));
                } else {
                    actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse("Error calling GetConfigRequest: " + status.Msg));
                }
            };
            auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::ConsoleService>("cp-api");
            connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::ConsoleService::Stub::AsyncGetConfig, meta);
            ++Requests;
        }
        Become(&THandlerActorYdbcConfigCollector::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvListStorageTypesResponse::TPtr event, const NActors::TActorContext& ctx) {
        StorageTypesResponse = event->Release();
        --Requests;
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvListResourcePresetsResponse::TPtr event, const NActors::TActorContext& ctx) {
        ResourcePresetsResponse = event->Release();
        --Requests;
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvGetConfigResponse::TPtr event, const NActors::TActorContext& ctx) {
        GetConfigResponse = event->Release();
        --Requests;
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        Error = event->Get()->Message;
        ReplyAndDie(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        if (StorageTypesResponse != nullptr && ResourcePresetsResponse != nullptr && GetConfigResponse != nullptr) {
            NMvp::YdbcConfig config;
            config.mutable_resource_presets()->Swap(ResourcePresetsResponse->Response.mutable_resource_presets());
            config.mutable_storage_types()->Swap(StorageTypesResponse->Response.mutable_storage_types());
            config.mutable_config_response()->Swap(&GetConfigResponse->Response);
            TStringStream stream;
            TProtoToJson::ProtoToJson(stream, config, JsonSettings);
            response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
        } else {
            response = CreateErrorResponse(Request.Request, Error);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvListStorageTypesResponse, Handle);
            HFunc(TEvPrivate::TEvListResourcePresetsResponse, Handle);
            HFunc(TEvPrivate::TEvGetConfigResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcConfig : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcConfig> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcConfig>;
    NActors::TActorId HttpProxyId;
    const TYdbcLocation& Location;

    THandlerActorYdbcConfig(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcConfig::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcConfigCollector(Location, HttpProxyId, event->Sender, request));
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
