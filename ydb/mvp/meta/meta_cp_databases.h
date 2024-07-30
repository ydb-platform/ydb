#pragma once
#include <random>
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
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorMetaCpDatabasesGET : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaCpDatabasesGET>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    Ydb::Discovery::ListEndpointsResult ListEndpointsResult;
    Ydb::Scripting::ExecuteYqlResult ExecuteYqlResult;
    TRequest Request;
    TMaybe<NYdb::NTable::TSession> Session;
    yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse Databases;
    NJson::TJsonValue TenantInfo;
    ui32 Requests = 0;
    TInstant DatabaseRequestDeadline;
    static constexpr TDuration MAX_DATABASE_REQUEST_TIME = TDuration::Seconds(10);
    TDuration DatabaseRequestRetryDelta = TDuration::MilliSeconds(50);
    TString ControlPlaneName;
    TString MvpTokenName;

    THandlerActorMetaCpDatabasesGET(
            const NActors::TActorId& httpProxyId,
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : HttpProxyId(httpProxyId)
        , Location(location)
        , Request(sender, request)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        {
            Location.GetTableClient(Request, NYdb::NTable::TClientSettings().Database(Location.RootDomain)).CreateSession().Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                NYdb::NTable::TAsyncCreateSessionResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
            });
        }

        Become(&THandlerActorMetaCpDatabasesGET::StateWork, GetTimeout(Request, TDuration::Seconds(60)), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            Session = result.GetSession();
            TString query = TStringBuilder() << "DECLARE $name AS Utf8; SELECT * FROM `" + Location.RootDomain + "/ydb/MasterClusterExt.db` WHERE name=$name";
            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
            NHttp::TUrlParameters parameters(Request.Request->URL);
            TString name(parameters["cluster_name"]);
            NYdb::TParamsBuilder params;
            params.AddParam("$name", NYdb::TValueBuilder().Utf8(name).Build());
            Session->ExecuteDataQuery(query,
                                      NYdb::NTable::TTxControl::BeginTx(
                                          NYdb::NTable::TTxSettings::OnlineRO(
                                              NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true)
                                              )
                                          ).CommitTx(),
                                      params.Build()
                                      ).Subscribe(
                        [actorSystem, actorId, session = Session](const NYdb::NTable::TAsyncDataQueryResult& result) {
                NYdb::NTable::TAsyncDataQueryResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
            });
        } else {
            auto response = CreateStatusResponse(Request.Request, result);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            Die(ctx);
        }
    }

    void SendDatabaseRequest(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        yandex::cloud::priv::ydb::v1::ListAllDatabasesRequest cpRequest;
        //cpRequest.set_page_size(1000);
        cpRequest.set_database_view(yandex::cloud::priv::ydb::v1::SERVERLESS_INTERNALS);
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvListAllDatabaseResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        NYdbGrpc::TCallMeta meta;
        Request.ForwardHeaders(meta);
        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        if (tokenator && MvpTokenName) {
            TString token = tokenator->GetToken(MvpTokenName);
            if (token) {
                Request.SetHeader(meta, "authorization", token);
            }
        }
        meta.Timeout = GetClientTimeout();
        auto connection = Location.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::ydb::v1::DatabaseService>(ControlPlaneName);
        connection->DoRequest(cpRequest, std::move(responseCb), &yandex::cloud::priv::ydb::v1::DatabaseService::Stub::AsyncListAll, meta);
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser rsParser(resultSet);
            if (rsParser.TryNextRow()) {
                TString name = ColumnValueToString(rsParser.GetValue("name"));
                TString balancer = ColumnValueToString(rsParser.GetValue("balancer"));
                TString apiUserTokenName = ColumnValueToString(rsParser.GetValue("api_user_token"));
                ControlPlaneName = ColumnValueToString(rsParser.GetValue("control_plane"));
                MvpTokenName = ColumnValueToString(rsParser.GetValue("mvp_token"));
                if (name && ControlPlaneName) {
                    DatabaseRequestDeadline = ctx.Now() + MAX_DATABASE_REQUEST_TIME;
                    SendDatabaseRequest(ctx);
                    ++Requests;
                }
                if (balancer) {
                    TString balancerEndpoint;
                    TStringBuilder balancerEndpointBuilder;
                    balancerEndpointBuilder << "/tenantinfo";
                    if (Request.Parameters["light"] == "0") {
                        balancerEndpointBuilder << "?tablets=1";
                    } else {
                        balancerEndpointBuilder << "?tablets=0"; // default
                    }
                    if (Request.Parameters["offload"] == "1") {
                        balancerEndpointBuilder << "&offload_merge=1";
                    } else {
                        balancerEndpointBuilder << "&offload_merge=0"; // default
                    }
                    balancerEndpointBuilder << "&storage=1&nodes=0&users=0&timeout=55000";
                    balancerEndpoint = GetApiUrl(balancer, balancerEndpointBuilder);
                    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(balancerEndpoint);
                    TString authHeaderValue = GetAuthHeaderValue(apiUserTokenName);
                    if (balancerEndpoint.StartsWith("https") && !authHeaderValue.empty()) {
                        httpRequest->Set("Authorization", authHeaderValue);
                    }
                    THolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> request = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(httpRequest);
                    request->Timeout = TDuration::Seconds(60);
                    ctx.Send(HttpProxyId, request.Release());
                    ++Requests;
                }
            }
            if (Requests == 0) {
                ReplyAndDie(ctx);
            }
            return;
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendDatabaseRequest(ctx);
    }

    void Handle(TEvPrivate::TEvListAllDatabaseResponse::TPtr event, const NActors::TActorContext& ctx) {
        Databases = std::move(event->Get()->Databases);
        if (--Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (event->Get()->Status.StartsWith("4") || DatabaseRequestDeadline <= ctx.Now()) {
            NHttp::THttpOutgoingResponsePtr response = CreateErrorResponse(Request.Request, event->Get());
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            Die(ctx);
        } else {
            ctx.Schedule(DatabaseRequestRetryDelta, new TEvPrivate::TEvRetryRequest());
            DatabaseRequestRetryDelta *= 2;
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &TenantInfo);
        }
        if (--Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NProtobufJson::TProto2JsonConfig proto2JsonConfig = NProtobufJson::TProto2JsonConfig()
                .SetMapAsObject(true)
                .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumValueMode::EnumName);


        std::unordered_map<TString, const yandex::cloud::priv::ydb::v1::Database*> indexDatabaseById;
        std::unordered_map<TString, const yandex::cloud::priv::ydb::v1::Database*> indexDatabaseByName;
        std::unordered_map<TString, NJson::TJsonValue*> indexJsonDatabaseById;

        for (const yandex::cloud::priv::ydb::v1::Database& protoDatabase : Databases.databases()) {
            indexDatabaseById[protoDatabase.id()] = &protoDatabase;
            indexDatabaseByName[protoDatabase.name()] = &protoDatabase;
        }

        NJson::TJsonValue root;
        NJson::TJsonValue& databases = root["databases"];
        databases.SetType(NJson::JSON_ARRAY);

        NJson::TJsonValue::TArray tenantArray(TenantInfo["TenantInfo"].GetArray());
        std::sort(tenantArray.begin(), tenantArray.end(), [](const NJson::TJsonValue& a, const NJson::TJsonValue& b) -> bool {
            return a["Name"].GetStringRobust() < b["Name"].GetStringRobust();
        });
        for (const NJson::TJsonValue& tenant : tenantArray) {
            NJson::TJsonValue& jsonDatabase = databases.AppendValue(NJson::TJsonValue());
            jsonDatabase = std::move(tenant);
            TString id = jsonDatabase["Id"].GetStringRobust();
            if (!id.empty()) {
                indexJsonDatabaseById[id] = &jsonDatabase;
            }
            NJson::TJsonValue* jsonUserAttributes;
            if (jsonDatabase.GetValuePointer("UserAttributes", &jsonUserAttributes)) {
                NJson::TJsonValue* jsonDatabaseId;
                if (jsonUserAttributes->GetValuePointer("database_id", &jsonDatabaseId)) {
                    if (jsonDatabaseId->GetType() == NJson::JSON_STRING) {
                        auto itDatabase = indexDatabaseById.find(jsonDatabaseId->GetStringRobust());
                        if (itDatabase != indexDatabaseById.end()) {
                            NProtobufJson::Proto2Json(*itDatabase->second, jsonDatabase["ControlPlane"], proto2JsonConfig);
                        }
                    }
                }
            }
            NJson::TJsonValue* jsonName;
            if (jsonDatabase.GetValuePointer("Name", &jsonName)) {
                if (jsonName->GetType() == NJson::JSON_STRING) {
                    auto itDatabase = indexDatabaseByName.find(jsonName->GetStringRobust());
                    if (itDatabase != indexDatabaseByName.end()) {
                        NProtobufJson::Proto2Json(*itDatabase->second, jsonDatabase["ControlPlane"], proto2JsonConfig);
                    }
                }
            }
        }

        for (const auto& [id, jsonDatabase] : indexJsonDatabaseById) {
            NJson::TJsonValue* jsonData = jsonDatabase;
            NJson::TJsonValue* jsonNodes;
            TString monitoringEndpoint;

            if (!jsonData->Has("Nodes")) {
                const NJson::TJsonValue* resourceId;
                if (jsonData->GetValuePointer("ResourceId", &resourceId)) {
                    auto itResourceDatabase = indexJsonDatabaseById.find(resourceId->GetStringRobust());
                    if (itResourceDatabase != indexJsonDatabaseById.end()) {
                        jsonData = itResourceDatabase->second;
                    }
                }
            }
            if (jsonData->GetValuePointer("Nodes", &jsonNodes)) {
                if (jsonNodes->GetType() == NJson::JSON_ARRAY) {
                    size_t size = jsonNodes->GetArray().size();
                    if (size > 0) {
                        std::random_device rd;
                        std::mt19937 gen(rd());
                        std::uniform_int_distribution<size_t> dist(0, size - 1);
                        size_t pos = dist(gen);
                        if (pos < size) {
                            const NJson::TJsonValue& jsonNode = jsonNodes->GetArray()[pos];
                            const NJson::TJsonValue* jsonEndpoints;
                            if (jsonNode.GetValuePointer("Endpoints", &jsonEndpoints)) {
                                if (jsonEndpoints->GetType() == NJson::JSON_ARRAY) {
                                    for (const auto& jsonEndpoint : jsonEndpoints->GetArray()) {
                                        if (jsonEndpoint["Name"] == "http-mon") {
                                            monitoringEndpoint = jsonNode["Host"].GetStringRobust() + jsonEndpoint["Address"].GetStringRobust();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!monitoringEndpoint.empty()) {
                (*jsonDatabase)["MonitoringEndpoint"] = monitoringEndpoint;
            }
            jsonDatabase->EraseValue("Nodes"); // to reduce response size
        }

        TString body(NJson::WriteJson(root, false));
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            HFunc(TEvPrivate::TEvListAllDatabaseResponse, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaCpDatabases : THandlerActorYdbc, public NActors::TActor<THandlerActorMetaCpDatabases> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCpDatabases>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    THandlerActorMetaCpDatabases(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&THandlerActorMetaCpDatabases::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaCpDatabasesGET(HttpProxyId, Location, event->Sender, request));
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
