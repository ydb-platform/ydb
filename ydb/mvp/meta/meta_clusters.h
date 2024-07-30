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
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include "meta_versions.h"
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorMetaClustersQuery : THandlerActorYdb, public NActors::TActorBootstrapped<THandlerActorMetaClustersQuery> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorMetaClustersQuery>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    TRequest Request;
    std::shared_ptr<NYdb::NTable::TTableClient> Client;
    TVersionInfoCachePtr VersionInfoCache;
    TMaybe<NYdb::TResultSet> ClusterListResultSet;
    int QueryCount;

    THandlerActorMetaClustersQuery(
            const NActors::TActorId& httpProxyId,
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : HttpProxyId(httpProxyId)
        , Location(location)
        , Request(sender, request)
        , QueryCount(2)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Client = std::make_shared<NYdb::NTable::TTableClient>(std::move(Location.GetTableClient(Request, NYdb::NTable::TClientSettings().Database(Location.RootDomain), TMVP::MetaDatabaseTokenName)));

        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        CreateLoadVersionsActor(actorId, Client, Location.RootDomain, ctx);

        Client->CreateSession().Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
            NYdb::NTable::TAsyncCreateSessionResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
        });

        Become(&THandlerActorMetaClustersQuery::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            LOG_DEBUG_S(ctx, EService::MVP, "MetaClusters: got session, making query");

            auto session = result.GetSession();
            TString query = TStringBuilder() << "SELECT * FROM `" + Location.RootDomain + "/ydb/MasterClusterExt.db`";
            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
            session.ExecuteDataQuery(query,
                                     NYdb::NTable::TTxControl::BeginTx(
                                         NYdb::NTable::TTxSettings::OnlineRO(
                                             NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true)
                                             )
                                         ).CommitTx()
                                     ).Subscribe(
                        [actorSystem, actorId, session](const NYdb::NTable::TAsyncDataQueryResult& result) {
                NYdb::NTable::TAsyncDataQueryResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
            });
        } else {
            LOG_ERROR_S(ctx, EService::MVP, "MetaClusters: failed to get session: " << static_cast<NYdb::TStatus>(result));

            auto response = CreateStatusResponse(Request.Request, result);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            Die(ctx);
        }
    }

    static TJsonMapper MapCluster(const TString& name, const TString& endpoint) {
        return [name, endpoint](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
            cluster["name"] = name;
            cluster["endpoint"] = endpoint;
            cluster["cluster"] = std::move(input);
            context.Stop = true;
            return root;
        };
    }

    static TJsonMapper MapSysInfo(const TString& clusterName) {
        return [clusterName](NJson::TJsonValue& input, TJsonMergeContext& context) -> NJson::TJsonValue {
            NJson::TJsonValue root;
            NJson::TJsonValue& clusters = root["clusters"];
            NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
            cluster["name"] = clusterName;
            NJson::TJsonValue& host = input["Host"];
            if (host.GetType() == NJson::JSON_STRING) {
                cluster["hosts"][host.GetString()] = 1;
            }
            NJson::TJsonValue& version = cluster["versions"].AppendValue(NJson::TJsonValue());
            TString roleName = "compute";
            if (input.Has("Roles")) {
                NJson::TJsonValue& jsonRoles = input["Roles"];
                if (jsonRoles.GetType() == NJson::JSON_ARRAY) {
                    const auto& array = jsonRoles.GetArray();
                    if (!array.empty() && Find(array, "Storage") != array.end()) {
                        roleName = "storage";
                    }
                }
            }
            version["role"] = roleName;
            version["version"] = std::move(input["Version"]);
            version["count"] = 1;
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

    static bool ReduceMapWithSum(NJson::TJsonValue& output, NJson::TJsonValue& input, TJsonMergeContext&) {
        if (!output.IsDefined()) {
            output.SetType(NJson::JSON_MAP);
        }
        NJson::TJsonValue::TMapType& target(output.GetMapSafe());
        NJson::TJsonValue::TMapType& source(input.GetMapSafe());
        for (auto& pair : source) {
            target[pair.first] = target[pair.first].GetUIntegerRobust() + pair.second.GetUIntegerRobust();
        }
        return true;
    }

    static TJsonFilter AppendVersionBaseColorIndex(TVersionInfoCachePtr versionInfoCacheShared) {
        return [versionInfoCacheShared](NJson::TJsonValue& output, TJsonMergeContext&) {
            auto getAssignedColorIndex = [](const TVersionInfoCache& list, const TString& version_str) -> int {
                auto found = std::find_if(list.begin(), list.end(), [&version_str](const TVersionInfo& v) {
                    return version_str.StartsWith(v.Version);
                });
                return (found != list.end()) ? found->ColorClass : -1;
            };

            auto version_str = output["version"].GetStringRobust();
            int color_index = getAssignedColorIndex(*versionInfoCacheShared, version_str);
            if (color_index >= 0) {
                output["version_base_color_index"] = color_index;
            }
        };
    }

    void CollectClustersData(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "MetaClusters: collecting clusters data");

        NJson::TJsonValue root;
        NJson::TJsonValue& clusters = root["clusters"];
        clusters.SetType(NJson::JSON_ARRAY);

        const auto& columnsMeta = ClusterListResultSet->GetColumnsMeta();
        NYdb::TResultSetParser rsParser(*ClusterListResultSet);

        TJsonMergeRules rules;
        TVector<TJsonMergePeer> peers;
        //NHttp::TUrlParameters parameters(event->Get()->Request->URL);

        while (rsParser.TryNextRow()) {
            NJson::TJsonValue& cluster = clusters.AppendValue(NJson::TJsonValue());
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
                TString authHeaderValue = GetAuthHeaderValue(ColumnValueToString(rsParser.GetValue("api_user_token")));
                {
                    TJsonMergePeer& peer = peers.emplace_back();
                    peer.URL = GetApiUrl(balancer, "/cluster?tablets=true");
                    if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                        peer.Headers.Set("Authorization", authHeaderValue);
                    }
                    peer.Timeout = TDuration::Seconds(30);
                    peer.ErrorHandler = Error();
                    peer.Rules.Mappers["."] = MapCluster(name, peer.URL);
                }
                {
                    TJsonMergePeer& peer = peers.emplace_back();
                    peer.URL = GetApiUrl(balancer, "/sysinfo");
                    if (peer.URL.StartsWith("https") && !authHeaderValue.empty()) {
                        peer.Headers.Set("Authorization", authHeaderValue);
                    }
                    peer.Timeout = TDuration::Seconds(30);
                    peer.ErrorHandler = Error();
                    peer.Rules.Mappers[".SystemStateInfo[]"] = MapSysInfo(name);
                    peer.Rules.Reducers[".clusters"] = ReduceGroupBy("name");
                    peer.Rules.Reducers[".clusters[].name"] = ReduceWithUniqueValue();
                    peer.Rules.Reducers[".clusters[].versions"] = ReduceGroupBy("role", "version");
                    peer.Rules.Reducers[".clusters[].versions[].role"] = ReduceWithUniqueValue();
                    peer.Rules.Reducers[".clusters[].versions[].version"] = ReduceWithUniqueValue();
                    peer.Rules.Reducers[".clusters[].versions[].count"] = ReduceWithSum();
                    peer.Rules.Reducers[".clusters[].hosts"] = &ReduceMapWithSum;
                }
            }
        }

        {
            TJsonMergePeer& peer = peers.emplace_back();
            peer.Rules.Mappers[".clusters"] = MapAll();
            peer.ParsedDocument = std::move(root);
        }
        rules.Reducers[".clusters"] = ReduceGroupBy("name");
        rules.Reducers[".clusters[].name"] = ReduceWithUniqueValue();

        if (VersionInfoCache && !VersionInfoCache->empty()) {
            // augment full versions with base color indexes assigned to major versions
            rules.Filters[".clusters[].versions[]"] = AppendVersionBaseColorIndex(VersionInfoCache);
        }

        CreateJsonMerger(HttpProxyId, Request.Sender, std::move(Request.Request), std::move(rules), std::move(peers), ctx);
        Die(ctx);
    }

    void Handle(THandlerActorYdbMeta::TEvPrivateMeta::TEvVersionListLoadResult::TPtr event, const NActors::TActorContext& ctx) {
        --QueryCount;
        VersionInfoCache = std::move(event->Get()->Result);
        // we conseal any errors with version info cache because version info isn't that critical

        LOG_DEBUG_S(ctx, EService::MVP, "MetaClusters: got version info data" << (VersionInfoCache ? "" : " (empty)"));

        if (QueryCount == 0) {
            CollectClustersData(ctx);
        }
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            --QueryCount;
            ClusterListResultSet = std::move(result.GetResultSet(0));

            LOG_DEBUG_S(ctx, EService::MVP, "MetaClusters: got cluster list");

            if (QueryCount == 0) {
                CollectClustersData(ctx);
            }

        } else {
            LOG_ERROR_S(ctx, EService::MVP, "MetaClusters: failed to get cluster list: " << static_cast<NYdb::TStatus>(result));

            NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, result);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            Die(ctx);
        }
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        LOG_ERROR_S(ctx, EService::MVP, "MetaClusters: timeout, sending error");

        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            HFunc(THandlerActorYdbMeta::TEvPrivateMeta::TEvVersionListLoadResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaClusters : THandlerActorYdb, public NActors::TActor<THandlerActorMetaClusters> {
public:
    using TBase = NActors::TActor<THandlerActorMetaClusters>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    THandlerActorMetaClusters(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&THandlerActorMetaClusters::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorMetaClustersQuery(HttpProxyId, Location, event->Sender, request));
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
