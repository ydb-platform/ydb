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
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcBackupGET : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcBackupGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcBackupGET>;
    const TYdbcLocation& Location;
    TRequest Request;

    THandlerActorYdbcBackupGET(
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
        yandex::cloud::priv::ydb::v1::GetBackupRequest cpRequest;
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

        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::Backup> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::Backup&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvBackupResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeadersOnlyForIAM(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::BackupService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncGet, meta);
        Become(&THandlerActorYdbcBackupGET::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvBackupResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringStream stream;
        TProtoToJson::ProtoToJson(stream, event->Get()->Backup, JsonSettings);
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
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
            HFunc(TEvPrivate::TEvBackupResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcBackupPOST : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcBackupPOST> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcBackupPOST>;
    const TYdbcLocation& Location;
    TRequest Request;

    THandlerActorYdbcBackupPOST(
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
        yandex::cloud::priv::ydb::v1::BackupDatabaseRequest cpRequest;
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

        NYdbGrpc::TResponseCallback<yandex::cloud::priv::operation::Operation> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::operation::Operation&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvOperationResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeadersOnlyForIAM(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::DatabaseService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncBackup, meta);
        Become(&THandlerActorYdbcBackupPOST::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvOperationResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringStream stream;
        TJsonSettings settings(JsonSettings);
        yandex::cloud::priv::ydb::v1::BackupDatabaseMetadata meta;
        if (event->Get()->Operation.metadata().UnpackTo(&meta)) {
            const auto* metaField = yandex::cloud::priv::operation::Operation::descriptor()->FindFieldByName("metadata");
            settings.FieldRemapper[metaField] = [&meta](IOutputStream& stream, const ::google::protobuf::Message&, const TJsonSettings& settings) {
                stream << "\"metadata\":";
                TProtoToJson::ProtoToJson(stream, meta, settings);
            };
        }
        TProtoToJson::ProtoToJson(stream, event->Get()->Operation, settings);
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
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
            HFunc(TEvPrivate::TEvOperationResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcBackupDELETE : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcBackupDELETE> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcBackupDELETE>;
    const TYdbcLocation& Location;
    TRequest Request;

    THandlerActorYdbcBackupDELETE(
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
        yandex::cloud::priv::ydb::v1::DeleteBackupRequest cpRequest;
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

        NYdbGrpc::TResponseCallback<yandex::cloud::priv::operation::Operation> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::operation::Operation&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvOperationResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeadersOnlyForIAM(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::BackupService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::BackupService::Stub::AsyncDelete, meta);
        Become(&THandlerActorYdbcBackupDELETE::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvOperationResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringStream stream;
        TJsonSettings settings(JsonSettings);
        yandex::cloud::priv::ydb::v1::DeleteBackupMetadata meta;
        if (event->Get()->Operation.metadata().UnpackTo(&meta)) {
            const auto* metaField = yandex::cloud::priv::operation::Operation::descriptor()->FindFieldByName("metadata");
            settings.FieldRemapper[metaField] = [&meta](IOutputStream& stream, const ::google::protobuf::Message&, const TJsonSettings& settings) {
                stream << "\"metadata\":";
                TProtoToJson::ProtoToJson(stream, meta, settings);
            };
        }
        TProtoToJson::ProtoToJson(stream, event->Get()->Operation, settings);
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
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
            HFunc(TEvPrivate::TEvOperationResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcBackup : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcBackup> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcBackup>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcBackup(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcBackup::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            ctx.Register(new THandlerActorYdbcBackupPOST(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "DELETE") {
            ctx.Register(new THandlerActorYdbcBackupDELETE(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcBackupGET(Location, HttpProxyId, event->Sender, request));
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
