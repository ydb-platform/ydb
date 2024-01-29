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
#include <ydb/public/api/client/yc_private/ydb/database_service.grpc.pb.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcDatabaseStatsCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatabaseStatsCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatabaseStatsCollector>;
    const TYdbcLocation& Location;
    TRequest Request;
    TInstant DatabaseRequestDeadline;
    TInstant DiscoveryDeadline;
    static constexpr TDuration MAX_DATABSE_REQUEST_TIME = TDuration::Seconds(10);
    static constexpr TDuration MAX_DISCOVERY_TIME = TDuration::Seconds(10);
    TDuration databaseRequestRetryDelta = TDuration::MilliSeconds(50);
    TString Database;
    TString Token;
    Ydb::Operations::Operation ListEndpointsOperation;
    THolder<TEvPrivate::TEvDatabaseResponse> DatabaseResponse;
    THolder<TEvPrivate::TEvErrorResponse> ErrorResponse;

    THandlerActorYdbcDatabaseStatsCollector(
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
        yandex::cloud::priv::ydb::v1::GetDatabaseRequest cpRequest;
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

        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::Database> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::Database&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvDatabaseResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeadersOnlyForIAM(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::DatabaseService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncGet, meta);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        DatabaseRequestDeadline = ctx.Now() + MAX_DATABSE_REQUEST_TIME;
        SendDatabaseRequest(ctx);
        Become(&THandlerActorYdbcDatabaseStatsCollector::StateRequestDatabase, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void SendDiscoveryRequest(const NActors::TActorContext& ctx) {
        NActors::TActorId actorId = ctx.SelfID;
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;

        Ydb::Discovery::ListEndpointsRequest ydbDiscoveryRequest;
        ydbDiscoveryRequest.set_database(Database);
        NYdbGrpc::TResponseCallback<Ydb::Discovery::ListEndpointsResponse> responseCb =
                [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, Ydb::Discovery::ListEndpointsResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvListEndpointsResponse(std::move(*response.mutable_operation())));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        meta.Aux.push_back({NYdb::YDB_DATABASE_HEADER, Database});
        Request.ForwardHeadersOnlyForIAM(meta);
        auto connection = Location.CreateGRpcServiceConnectionFromEndpoint<Ydb::Discovery::V1::DiscoveryService>(DatabaseResponse->Database.endpoint());
        connection->DoRequest(ydbDiscoveryRequest, std::move(responseCb), &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints, meta);
    }

    static bool IsDatabaseGoodForDiscovery(const yandex::cloud::priv::ydb::v1::Database& database) {
        if (database.name().empty()) {
            return false;
        }
        switch (database.status()) {
        case yandex::cloud::priv::ydb::v1::Database::RUNNING:
        case yandex::cloud::priv::ydb::v1::Database::UPDATING:
            return true;
        default:
            return false;
        }
        return false;
    }

    void HandleStateDatabase(TEvPrivate::TEvDatabaseResponse::TPtr event, const NActors::TActorContext& ctx) {
        DatabaseResponse = event->Release();
        TStringBuf scheme = "grpc";
        TStringBuf host;
        TStringBuf uri;
        NHttp::CrackURL(DatabaseResponse->Database.endpoint(), scheme, host, uri);
        NHttp::TUrlParameters urlParams(uri);
        Database = urlParams["database"];
        Token = Request.GetAuthToken();
        if (IsDatabaseGoodForDiscovery(DatabaseResponse->Database)) {
            Become(&THandlerActorYdbcDatabaseStatsCollector::StateRequestDiscovery);
            DiscoveryDeadline = ctx.Now() + MAX_DISCOVERY_TIME;
            SendDiscoveryRequest(ctx);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void HandleStateDatabase(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendDatabaseRequest(ctx);
    }

    static bool IsFinalStatus(Ydb::StatusIds_StatusCode status) {
        switch (status) {
        case Ydb::StatusIds::SUCCESS:
        case Ydb::StatusIds::UNAUTHORIZED:
            return true;
        default:
            return false;
        }
        return false;
    }

    void HandleStateDiscovery(TEvPrivate::TEvListEndpointsResponse::TPtr event, const NActors::TActorContext& ctx) {
        Ydb::Operations::Operation& operation(event->Get()->Operation);
        ListEndpointsOperation = operation;
        if (operation.ready() && IsFinalStatus(operation.status())) {
            ReplyAndDie(ctx); // discovery finished
        } else if (DiscoveryDeadline > ctx.Now()){
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
        } else {
            ReplyAndDie(ctx); // discovery retry period is over
        }
    }

    void HandleStateDiscovery(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendDiscoveryRequest(ctx);
    }

    void HandleStateDatabase(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (event->Get()->Status.StartsWith("4") || DatabaseRequestDeadline <= ctx.Now()) {
            ErrorResponse = event->Release();
            ReplyAndDie(ctx);
        } else {
            ctx.Schedule(databaseRequestRetryDelta, new TEvPrivate::TEvRetryRequest());
            databaseRequestRetryDelta *= 2;
        }
    }

    void HandleStateDiscovery(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (event->Get()->Status.StartsWith("4") || DiscoveryDeadline <= ctx.Now()) {
            // we only need this for diagnostics:
            ListEndpointsOperation = Ydb::Operations::Operation();
            ListEndpointsOperation.set_ready(true);
            ListEndpointsOperation.set_status(Ydb::StatusIds::UNAVAILABLE);
            Ydb::Issue::IssueMessage* issue = ListEndpointsOperation.add_issues();
            issue->set_issue_code(Ydb::StatusIds::INTERNAL_ERROR);
            issue->set_message(event->Get()->Message);
            ReplyAndDie(ctx);
        } else {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
        }
    }

    void HandleTimeoutStateDatabase(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    void HandleTimeoutStateDiscovery(const NActors::TActorContext& ctx) {
        ListEndpointsOperation = Ydb::Operations::Operation();
        ListEndpointsOperation.set_ready(true);
        ListEndpointsOperation.set_status(Ydb::StatusIds::TIMEOUT);
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        if (DatabaseResponse != nullptr) {
            NJson::TJsonValue jsonRoot;
            jsonRoot.SetType(NJson::JSON_MAP);
            NProtobufJson::Proto2Json(DatabaseResponse->Database, jsonRoot, Proto2JsonConfig);
            if (ListEndpointsOperation.ready()) {
                if (ListEndpointsOperation.status() == Ydb::StatusIds::SUCCESS) {
                    Ydb::Discovery::ListEndpointsResult listEndpointsResult;
                    ListEndpointsOperation.result().UnpackTo(&listEndpointsResult);
                    NProtobufJson::Proto2Json(listEndpointsResult, jsonRoot["discovery"], Proto2JsonConfig);
                } else {
                    if (ListEndpointsOperation.has_result()) {
                        ListEndpointsOperation.mutable_result()->clear_value(); // avoid dumping to json binary messages
                    }
                    NProtobufJson::Proto2Json(ListEndpointsOperation, jsonRoot["discovery"]["operation"], Proto2JsonConfig);
                }
            }
            jsonRoot["location"] = Location.Name;
            TStringStream body;
            NJson::WriteJson(&body, &jsonRoot, JsonWriterConfig);
            response = Request.Request->CreateResponseOK(body.Str(), "application/json; charset=utf-8");
        } else if (ErrorResponse != nullptr) {
            response = CreateErrorResponse(Request.Request, ErrorResponse.Get());
        } else {
            response = Request.Request->CreateResponseServiceUnavailable("Database info is not available", "text/plain");
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    STFUNC(StateRequestDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvDatabaseResponse, HandleStateDatabase);
            HFunc(TEvPrivate::TEvRetryRequest, HandleStateDatabase);
            HFunc(TEvPrivate::TEvErrorResponse, HandleStateDatabase);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeoutStateDatabase);
        }
    }

    STFUNC(StateRequestDiscovery) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvListEndpointsResponse, HandleStateDiscovery);
            HFunc(TEvPrivate::TEvRetryRequest, HandleStateDiscovery);
            HFunc(TEvPrivate::TEvErrorResponse, HandleStateDiscovery);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeoutStateDiscovery);
        }
    }
};

class THandlerActorYdbcDatabaseCreator : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatabaseCreator> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatabaseCreator>;
    const TYdbcLocation& Location;
    TRequest Request;
    THolder<TEvPrivate::TEvOperationResponse> OperationResponse;
    THolder<TEvPrivate::TEvErrorResponse> ErrorResponse;

    enum class EOperationClass {
        CREATE,
        UPDATE
    } OperationClass;

    THandlerActorYdbcDatabaseCreator(
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
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::operation::Operation> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::operation::Operation&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvOperationResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::DatabaseService>("cp-api");

        try {
            if (Request.Parameters.PostData.IsDefined()) {
                if (Request.Parameters.PostData.Has("databaseId")) {
                    OperationClass = EOperationClass::UPDATE;
                    yandex::cloud::priv::ydb::v1::UpdateDatabaseRequest cpRequest;
                    NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
                    if (cpRequest.update_mask().paths_size() == 0) {
                        for (const auto& [key, value] : Request.Parameters.PostData.GetMap()) {
                            if (key != "databaseId") {
                                cpRequest.mutable_update_mask()->add_paths(CamelToSnakeCase(key));
                            }
                        }
                    }
                    connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncUpdate, meta);
                } else {
                    OperationClass = EOperationClass::CREATE;
                    yandex::cloud::priv::ydb::v1::CreateDatabaseRequest cpRequest;
                    NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
                    connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncCreate, meta);
                }
            } else {
                throw yexception() << "Post data not found";
            }
        } catch (const yexception& e) {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
            return TBase::Die(ctx);
        }
        Become(&THandlerActorYdbcDatabaseCreator::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvOperationResponse::TPtr event, const NActors::TActorContext& ctx) {
        OperationResponse = event->Release();
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
            if (OperationResponse != nullptr) {
                TStringStream stream;
                TJsonSettings settings(JsonSettings);
                const auto* metaField = yandex::cloud::priv::operation::Operation::descriptor()->FindFieldByName("metadata");
                yandex::cloud::priv::ydb::v1::CreateDatabaseMetadata metaCreate;
                yandex::cloud::priv::ydb::v1::UpdateDatabaseMetadata metaUpdate;
                if (OperationClass == EOperationClass::CREATE) {
                    if (OperationResponse->Operation.metadata().UnpackTo(&metaCreate)) {
                        settings.FieldRemapper[metaField] = [&metaCreate](IOutputStream& stream, const ::google::protobuf::Message&, const TJsonSettings& settings) {
                            stream << "\"metadata\":";
                            TProtoToJson::ProtoToJson(stream, metaCreate, settings);
                        };
                    }
                }
                if (OperationClass == EOperationClass::UPDATE) {
                    if (OperationResponse->Operation.metadata().UnpackTo(&metaUpdate)) {
                        settings.FieldRemapper[metaField] = [&metaUpdate](IOutputStream& stream, const ::google::protobuf::Message&, const TJsonSettings& settings) {
                            stream << "\"metadata\":";
                            TProtoToJson::ProtoToJson(stream, metaUpdate, settings);
                        };
                    }
                }
                TProtoToJson::ProtoToJson(stream, OperationResponse->Operation, settings);
                response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
            } else {
                response = Request.Request->CreateResponseServiceUnavailable("Service is not available", "text/plain");
            }
        } else {
            response = CreateErrorResponse(Request.Request, ErrorResponse.Get());
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
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

class THandlerActorYdbcDatabaseRemover : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatabaseRemover> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatabaseRemover>;
    const TYdbcLocation& Location;
    TRequest Request;
    THolder<TEvPrivate::TEvOperationResponse> OperationResponse;
    THolder<TEvPrivate::TEvErrorResponse> ErrorResponse;

    THandlerActorYdbcDatabaseRemover(
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
        yandex::cloud::priv::ydb::v1::DeleteDatabaseRequest cpRequest;
        if (Request.Parameters.PostData.IsDefined()) {
            try {
                NProtobufJson::Json2Proto(Request.Parameters.PostData, cpRequest, Json2ProtoConfig);
            }
            catch (const yexception& e) {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest(e.what(), "text/plain")));
                TBase::Die(ctx);
            }
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
        Request.ForwardHeaders(meta);
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnection<yandex::cloud::priv::ydb::v1::DatabaseService>("cp-api");
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncDelete, meta);
        Become(&THandlerActorYdbcDatabaseRemover::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvOperationResponse::TPtr event, const NActors::TActorContext& ctx) {
        OperationResponse = event->Release();
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
            if (OperationResponse != nullptr) {
                TStringStream stream;
                TJsonSettings settings(JsonSettings);
                yandex::cloud::priv::ydb::v1::DeleteDatabaseMetadata meta;
                if (OperationResponse->Operation.metadata().UnpackTo(&meta)) {
                    const auto* metaField = yandex::cloud::priv::operation::Operation::descriptor()->FindFieldByName("metadata");
                    settings.FieldRemapper[metaField] = [&meta](IOutputStream& stream, const ::google::protobuf::Message&, const TJsonSettings& settings) {
                        stream << "\"metadata\":";
                        TProtoToJson::ProtoToJson(stream, meta, settings);
                    };
                }
                TProtoToJson::ProtoToJson(stream, OperationResponse->Operation, settings);
                response = Request.Request->CreateResponseOK(stream.Str(), "application/json; charset=utf-8");
            } else {
                response = Request.Request->CreateResponseServiceUnavailable("Service is not available", "text/plain");
            }
        } else {
            response = CreateErrorResponse(Request.Request, ErrorResponse.Get());
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
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

class THandlerActorYdbcDatabase : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDatabase> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDatabase>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDatabase(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDatabase::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            ctx.Register(new THandlerActorYdbcDatabaseCreator(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "DELETE") {
            ctx.Register(new THandlerActorYdbcDatabaseRemover(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "GET" || request->Method == "HEAD") {
            ctx.Register(new THandlerActorYdbcDatabaseStatsCollector(Location, HttpProxyId, event->Sender, request));
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
