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
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/client/yc_private/resourcemanager/cloud_service.grpc.pb.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorMetaCloudGET : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorMetaCloudGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaCloudGET>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    Ydb::Discovery::ListEndpointsResult ListEndpointsResult;
    Ydb::Scripting::ExecuteYqlResult ExecuteYqlResult;
    TRequest Request;
    TMaybe<NYdb::NTable::TSession> Session;

    THandlerActorMetaCloudGET(
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
            Location.GetTableClient(TMVP::GetMetaDatabaseClientSettings(Request, Location))
                .CreateSession().Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                NYdb::NTable::TAsyncCreateSessionResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
            });
        }

        Become(&THandlerActorMetaCloudGET::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            Session = result.GetSession();
            TString location(Request.Parameters["location"]);
            TString query = TStringBuilder() << "DECLARE $location AS Utf8;"
                << "SELECT resource_manager, mvp_token FROM `" + Location.RootDomain + "/ydb/Endpoints.db` WHERE "
                << "location=$location";

            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
            NYdb::TParamsBuilder params;
            params.AddParam("$location", NYdb::TValueBuilder().Utf8(location).Build());
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

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser rsParser(resultSet);
            if (rsParser.TryNextRow()) {
                TString resource_manager = ColumnValueToString(rsParser.GetValue("resource_manager"));
                TString token = ColumnValueToString(rsParser.GetValue("mvp_token"));
                if (resource_manager) {
                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;
                    yandex::cloud::priv::resourcemanager::v1::GetCloudRequest request;

                    request.set_cloud_id(Request.Parameters["cloud_id"]);

                    NYdbGrpc::TResponseCallback<yandex::cloud::priv::resourcemanager::v1::Cloud> responseCb =
                        [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::resourcemanager::v1::Cloud&& response) -> void {
                        if (status.Ok()) {
                            actorSystem->Send(actorId, new TEvPrivate::TEvGetCloudResponse(std::move(response)));
                        } else {
                            actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                        }
                    };
                    NYdbGrpc::TCallMeta meta;
                    Request.ForwardHeaders(meta);
                    NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
                    if (tokenator) {
                        token = tokenator->GetToken(token);
                        if (token) {
                            Request.SetHeader(meta, "Authorization", token);
                        }
                    }
                    meta.Timeout = GetClientTimeout();
                    auto connection = Location.CreateGRpcServiceConnectionFromEndpoint<yandex::cloud::priv::resourcemanager::v1::CloudService>(resource_manager);
                    connection->DoRequest(request, std::move(responseCb), &yandex::cloud::priv::resourcemanager::v1::CloudService::Stub::AsyncGet, meta);
                    return;
                } else {
                    ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseNotFound("resource_manager not specified")));
                    Die(ctx);
                    return;
                }
            } else {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseNotFound("Location not found")));
                Die(ctx);
                return;
            }
        }
        NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, result);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void Handle(TEvPrivate::TEvGetCloudResponse::TPtr event, const NActors::TActorContext& ctx) {
        NProtobufJson::TProto2JsonConfig proto2JsonConfig = NProtobufJson::TProto2JsonConfig()
                .SetMapAsObject(true)
                .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumValueMode::EnumName);
        NJson::TJsonValue root;
        NJson::TJsonValue& cloud = root["cloud"];
        NProtobufJson::Proto2Json(event->Get()->Response, cloud, proto2JsonConfig);
        TString body(NJson::WriteJson(root, false));
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response = CreateErrorResponse(Request.Request, event->Get());
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            HFunc(TEvPrivate::TEvGetCloudResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaCloud : THandlerActorYdbc, public NActors::TActor<THandlerActorMetaCloud> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCloud>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    THandlerActorMetaCloud(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&THandlerActorMetaCloud::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaCloudGET(HttpProxyId, Location, event->Sender, request));
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
