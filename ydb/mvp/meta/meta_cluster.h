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

class THandlerActorMetaClusterGET : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaClusterGET> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaClusterGET>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    Ydb::Discovery::ListEndpointsResult ListEndpointsResult;
    Ydb::Scripting::ExecuteYqlResult ExecuteYqlResult;
    TRequest Request;
    TMaybe<NYdb::NTable::TSession> Session;

    THandlerActorMetaClusterGET(
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

        Become(&THandlerActorMetaClusterGET::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            Session = result.GetSession();
            TString query = TStringBuilder() << "DECLARE $name AS Utf8; SELECT * FROM `" + Location.RootDomain + "/ydb/MasterClusterExt.db` WHERE name=$name";
            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
            TString name(Request.Parameters["name"]);
            if (name.empty()) {
                auto response = Request.Request->CreateResponseBadRequest();
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                return Die(ctx);
            }
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

    static TJsonMapper MapCluster(const TString& name, const TString& endpoint) {
        return [name, endpoint](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& cluster = root["cluster"];
            cluster["name"] = name;
            cluster["endpoint"] = endpoint;
            cluster["cluster"] = std::move(input);
            context.Stop = true;
            return root;
        };
    }

    static TJsonMapper MapSysInfo() {
        return [](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& cluster = root["cluster"];
            NJson::TJsonValue& sysinfo = cluster["sysinfo"];
            sysinfo = std::move(input);
            context.Stop = true;
            return root;
        };
    }

    static TErrorHandler Error() {
        return [](const TString& error, TStringBuf body, TStringBuf contentType) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            root["error"] = error;
            Y_UNUSED(body);
            Y_UNUSED(contentType);
            return root;
        };
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            auto resultSet = result.GetResultSet(0);
            NJson::TJsonValue root;
            NJson::TJsonValue& cluster = root["cluster"];
            cluster.SetType(NJson::JSON_MAP);
            const auto& columnsMeta = resultSet.GetColumnsMeta();
            NYdb::TResultSetParser rsParser(resultSet);
            TJsonMergeRules rules;
            TVector<TJsonMergePeer> peers;
            if (rsParser.TryNextRow()) {
                TString name;
                TString balancer;
                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                    cluster[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                    if (columnMeta.Name == "name") {
                        name = cluster[columnMeta.Name].GetStringRobust();
                    }
                    if (columnMeta.Name == "balancer") {
                        balancer = cluster[columnMeta.Name].GetStringRobust();
                    }
                }
                if (name && balancer) {
                    auto tablets = Request.Parameters["tablets"];
                    auto sysinfo = Request.Parameters["sysinfo"];
                    TString authHeaderValue = GetAuthHeaderValue(ColumnValueToString(rsParser.GetValue("api_user_token")));
                    {
                        TJsonMergePeer& peer = peers.emplace_back();
                        if (tablets.empty() || tablets == "1") {
                            peer.URL = GetApiUrl(balancer, "/cluster?tablets=1");
                        } else {
                            peer.URL = GetApiUrl(balancer, "/cluster");
                        }
                        if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                            peer.Headers.Set("Authorization", authHeaderValue);
                        }
                        peer.Timeout = TDuration::Seconds(30);
                        peer.ErrorHandler = Error();
                        peer.Rules.Mappers["."] = MapCluster(name, peer.URL);
                    }
                    if (sysinfo == "1") {
                        TJsonMergePeer& peer = peers.emplace_back();
                        peer.URL = GetApiUrl(balancer, "/sysinfo");
                        if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                            peer.Headers.Set("Authorization", authHeaderValue);
                        }
                        peer.Timeout = TDuration::Seconds(30);
                        peer.ErrorHandler = Error();
                        peer.Rules.Mappers[".SystemStateInfo"] = MapSysInfo();
                    }
                }
            }

            {
                TJsonMergePeer& peer = peers.emplace_back();
                peer.Rules.Mappers[".cluster"] = MapAll();
                peer.ParsedDocument = std::move(root);
            }
            rules.Reducers[".cluster.name"] = ReduceWithUniqueValue();
            CreateJsonMerger(HttpProxyId, Request.Sender, std::move(Request.Request), std::move(rules), std::move(peers), ctx);
            Die(ctx);
            return;
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
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
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaCluster : THandlerActorYdb, public NActors::TActor<THandlerActorMetaCluster> {
public:
    using TBase = NActors::TActor<THandlerActorMetaCluster>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    THandlerActorMetaCluster(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&THandlerActorMetaCluster::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaClusterGET(HttpProxyId, Location, event->Sender, request));
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
