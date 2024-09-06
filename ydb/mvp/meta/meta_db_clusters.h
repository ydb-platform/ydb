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
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorMetaDbClustersQuery : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaDbClustersQuery> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaDbClustersQuery>;
    const TYdbLocation& Location;
    Ydb::Discovery::ListEndpointsResult ListEndpointsResult;
    Ydb::Scripting::ExecuteYqlResult ExecuteYqlResult;
    TRequest Request;
    TMaybe<NYdb::NTable::TSession> Session;

    THandlerActorMetaDbClustersQuery(
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
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

        Become(&THandlerActorMetaDbClustersQuery::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            clusters.SetType(NJson::JSON_ARRAY);
            const auto& columnsMeta = resultSet.GetColumnsMeta();
            NYdb::TResultSetParser rsParser(resultSet);
            while (rsParser.TryNextRow()) {
                NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                    cluster[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                }
            }

            TString body(NJson::WriteJson(root, false));
            response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            Session = result.GetSession();
            TStringBuilder query;
            query << "SELECT * FROM `" + Location.RootDomain + "/ydb/MasterClusterExt.db`";
            NYdb::TParamsBuilder params;
            auto name = Request.Parameters["name"];
            if (name) {
                query.insert(0, "DECLARE $name AS Utf8; ");
                query << " WHERE name=$name";
                params.AddParam("$name", NYdb::TValueBuilder().Utf8(name).Build());
            }
            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
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

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaDbClusters : THandlerActorYdb, public NActors::TActor<THandlerActorMetaDbClusters> {
public:
    using TBase = NActors::TActor<THandlerActorMetaDbClusters>;
    const TYdbLocation& Location;

    THandlerActorMetaDbClusters(const TYdbLocation& location)
        : TBase(&THandlerActorMetaDbClusters::StateWork)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaDbClustersQuery(Location, event->Sender, request));
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
