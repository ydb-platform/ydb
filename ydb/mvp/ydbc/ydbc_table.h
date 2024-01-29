#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcTablePost : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcTablePost> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcTablePost>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    NKikimr::NGRpcProxy::TGRpcClientConfig GrpcConfig;
    TString Database;
    TString Path;
    std::unique_ptr<NYdb::NTable::TTableClient> TableClient;

    THandlerActorYdbcTablePost(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
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

        Become(&THandlerActorYdbcTablePost::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void HandleResolveDatabase(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                TString endpoint = responseData["endpoint"].GetStringRobust();
                TStringBuf scheme = "grpc";
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                TString database = urlParams["database"];
                TString path = Request.Parameters["path"];
                TString hostStr = TString(host);
                TString schemeStr = TString(scheme);

                NJson::TJsonValue* jsonDiscovery;
                if (responseData.GetValuePointer("discovery", &jsonDiscovery)) {
                    NJson::TJsonValue* jsonEndpoints;
                    if (jsonDiscovery->GetValuePointer("endpoints", &jsonEndpoints)) {
                        for (const NJson::TJsonValue& jsonEndpoint : jsonEndpoints->GetArraySafe()) {
                            hostStr = jsonEndpoint["address"].GetStringRobust() + ":" + jsonEndpoint["port"].GetStringRobust();
                            if (jsonEndpoint["ssl"].GetBooleanRobust()) {
                                schemeStr = "grpcs";
                            } else {
                                schemeStr = "grpc";
                            }
                        }
                    }
                }

                if (!database.empty()) {
                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;
                    GrpcConfig.Locator = hostStr;
                    if (schemeStr == "grpcs") {
                        GrpcConfig.SslCredentials.pem_root_certs = Location.CaCertificate;
                    }
                    Database = database;
                    Path = Database;
                    if (!Database.EndsWith('/') && !path.StartsWith('/')) {
                        Path += '/';
                    }
                    Path += path;
                    NYdbGrpc::TResponseCallback<Ydb::Table::CreateSessionResponse> responseCb =
                        [actorSystem, actorId](NYdbGrpc::TGrpcStatus&& status, Ydb::Table::CreateSessionResponse&& response) -> void {
                            if (status.Ok()) {
                                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResponse(std::move(*response.mutable_operation())));
                            } else {
                                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                            }
                    };

                    Ydb::Table::CreateSessionRequest sessionRequest;
                    sessionRequest.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams_OperationMode_SYNC);
                    NYdbGrpc::TCallMeta meta;
                    meta.Aux.push_back({NYdb::YDB_DATABASE_HEADER, database});
                    Request.ForwardHeadersOnlyForIAM(meta);
                    auto connection = Location.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(GrpcConfig);
                    connection->DoRequest(sessionRequest, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncCreateSession, meta);

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

    void Handle(TEvPrivate::TEvCreateSessionResponse::TPtr event, const NActors::TActorContext& ctx) {
        Ydb::Operations::Operation& operation(event->Get()->Operation);
        if (operation.status() == Ydb::StatusIds_StatusCode_SUCCESS) {
            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;

            Ydb::Table::CreateSessionResult result;
            operation.result().UnpackTo(&result);

            NYdbGrpc::TResponseCallback<Ydb::Table::AlterTableResponse> responseCb =
                [actorSystem, actorId](NYdbGrpc::TGrpcStatus&& status, Ydb::Table::AlterTableResponse&& response) -> void {
                    if (status.Ok()) {
                        actorSystem->Send(actorId, new TEvPrivate::TEvAlterTableResponse(std::move(*response.mutable_operation())));
                    } else {
                        actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                    }
            };

            Ydb::Table::AlterTableRequest alterTableRequest;
            NProtobufJson::Json2Proto(Request.Parameters.PostData, alterTableRequest, Json2ProtoConfig);
            alterTableRequest.set_session_id(result.session_id());
            alterTableRequest.set_path(Path);
            NYdbGrpc::TCallMeta meta;
            meta.Aux.push_back({NYdb::YDB_DATABASE_HEADER, Database});
            Request.ForwardHeadersOnlyForIAM(meta);
            auto connection = Location.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(GrpcConfig);
            connection->DoRequest(alterTableRequest, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncAlterTable, meta);

            return;
        }
        NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, operation);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvAlterTableResponse::TPtr event, const NActors::TActorContext& ctx) {
        Ydb::Operations::Operation operation = event->Get()->Operation;
        if (operation.status() == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED && !operation.id().empty()) {
            operation.set_status(Ydb::StatusIds::SUCCESS);
        }
        NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, operation);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateErrorResponse(Request.Request, event->Get())));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleResolveDatabase);
            HFunc(TEvPrivate::TEvCreateSessionResponse, Handle);
            HFunc(TEvPrivate::TEvAlterTableResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcTable : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcTable> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcTable>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcTable(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcTable::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            ctx.Register(new THandlerActorYdbcTablePost(Location, HttpProxyId, event->Sender, request));
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
