#include "mvp.h"
#include "meta_db_clusters.h"
#include "meta_clusters.h"
#include "meta_cluster.h"
#include "meta_cp_databases.h"
#include "meta_cp_databases_verbose.h"
#include "meta_cloud.h"
#include "meta_cache.h"
#include <util/system/hostname.h>
#include <ydb/mvp/core/http_check.h>
#include <ydb/mvp/core/http_sensors.h>
#include <ydb/mvp/core/mvp_swagger.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/cache_policy.h>
#include <ydb/library/actors/http/http_static.h>
#include <ydb/library/actors/http/http_cache.h>

#define MLOG_D(stream) LOG_DEBUG_S((NMVP::InstanceMVP->ActorSystem), EService::MVP, stream)

using namespace NMVP;

NHttp::TCachePolicy GetIncomingMetaCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    if (url.starts_with("/meta/cp_databases")) {
        policy.TimeToExpire = TDuration::Days(3);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    if (url.starts_with("/meta/clusters")) {
        policy.TimeToExpire = TDuration::Days(7);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    return NHttp::GetDefaultCachePolicy(request, policy);
}

TYdbLocation MetaLocation =
    {
        "meta",
        "meta",
        {},
        {}
    };

// TODO(xenoxeno)
TString LocalEndpoint;

namespace {

std::mutex SeenLock;
std::unordered_set<TString> SeenIds;

bool HasSeenId(const TString& id) {
    std::lock_guard<std::mutex> lock(SeenLock);
    return SeenIds.count(id) != 0;
}

void MarkIdAsSeen(const TString& id) {
    std::lock_guard<std::mutex> lock(SeenLock);
    SeenIds.insert(id);
}

}

bool GetCacheOwnership(const TString& id, NMeta::TGetCacheOwnershipCallback cb) {
    MetaLocation.GetTableClient(NYdb::NTable::TClientSettings().Database(MetaLocation.RootDomain).AuthToken(MVPAppData()->Tokenator->GetToken("meta-token")))
                .CreateSession().Subscribe([id, cb = move(cb)](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                    auto resultCopy = result;
                    auto res = resultCopy.ExtractValue();
                    if (res.IsSuccess()) {
                        // got session
                        auto session = res.GetSession();
                        TStringBuilder query;
                        query << "DECLARE $ID AS Text;\n"
                                 "DECLARE $FORWARD AS Text;\n";
                        if (!HasSeenId(id)) {
                            query << "UPSERT INTO `ydb/Forwards.db`(Id) VALUES($ID);\n";
                        }
                        query << "UPDATE `ydb/Forwards.db` SET Forward=$FORWARD, Deadline=CurrentUtcTimestamp() + Interval('PT60S') WHERE Id=$ID AND (Deadline IS NULL OR (Deadline < CurrentUtcTimestamp()) OR (Forward = $FORWARD AND Deadline < (CurrentUtcTimestamp() + Interval('PT30S'))));\n"
                                 "SELECT Forward, Deadline FROM `ydb/Forwards.db` WHERE Id=$ID;\n";
                        NYdb::TParamsBuilder params;
                        params.AddParam("$ID", NYdb::TValueBuilder().Utf8(id).Build());
                        params.AddParam("$FORWARD", NYdb::TValueBuilder().Utf8(LocalEndpoint).Build());
                        session.ExecuteDataQuery(
                            query,
                            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
                            params.Build()).Subscribe([id, cb = move(cb), session](const NYdb::NTable::TAsyncDataQueryResult& result) mutable {
                                NYdb::NTable::TAsyncDataQueryResult resultCopy = result;
                                auto res = resultCopy.ExtractValue();
                                if (res.IsSuccess()) {
                                    MarkIdAsSeen(id);
                                    try {
                                        // got result
                                        auto resultSet = res.GetResultSet(0);
                                        NYdb::TResultSetParser rsParser(resultSet);
                                        if (rsParser.TryNextRow()) {
                                            TString forward = (rsParser.ColumnParser(0).GetOptionalUtf8()).GetRef();
                                            TInstant deadline = (rsParser.ColumnParser(1).GetOptionalTimestamp()).GetRef();
                                            if (forward == LocalEndpoint) {
                                                MLOG_D("GetCacheOwnership(" << id << ") - got data (forward to myself until " << deadline << ")");
                                                cb({.Deadline = deadline});
                                            } else {
                                                MLOG_D("GetCacheOwnership(" << id << ") - got data (forward to " << forward << " until " << deadline << ")");
                                                cb({.ForwardUrl = forward, .Deadline = deadline});
                                            }
                                        } else {
                                            // no data
                                            MLOG_D("GetCacheOwnership(" << id << ") - failed to get data");
                                            cb({});
                                        }
                                    } catch (const std::exception& e) {
                                        // exception
                                        MLOG_D("GetCacheOwnership(" << id << ") - exception: " << e.what());
                                        cb({});
                                    }
                                } else {
                                    // no result
                                    MLOG_D("GetCacheOwnership(" << id << ") - failed to get result:\n" << (NYdb::TStatus&)res);
                                    cb({});
                                }
                                session.Close();
                            });
                    } else {
                        // no session
                        MLOG_D("GetCacheOwnership(" << id << ") - failed to get session:\n" << (NYdb::TStatus&)res);
                        cb({});
                    }
                });

    return true;
}

NActors::IActor* CreateMemProfiler();

TString TMVP::GetMetaDatabaseAuthToken(const TRequest& request) {
    TString authToken;
    if (TMVP::MetaDatabaseTokenName.empty()) {
        authToken = request.GetAuthToken();
    } else {
        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        if (tokenator) {
            authToken = tokenator->GetToken(TMVP::MetaDatabaseTokenName);
        }
    }
    return authToken;
}

NYdb::NTable::TClientSettings TMVP::GetMetaDatabaseClientSettings(const TRequest& request, const TYdbLocation& location) {
    NYdb::NTable::TClientSettings clientSettings;
    clientSettings.AuthToken(GetMetaDatabaseAuthToken(request));
    clientSettings.Database(location.RootDomain);
    if (TString database = location.GetDatabaseName(request)) {
        clientSettings.Database(database);
    }
    return clientSettings;
}

void TMVP::InitMeta() {
    MetaLocation.Endpoints.emplace_back("api", MetaApiEndpoint);
    MetaLocation.Endpoints.emplace_back("cluster-api", MetaApiEndpoint);
    MetaLocation.RootDomain = MetaDatabase;

    LocalEndpoint = TStringBuilder() << "http://" << FQDNHostName() << ":" << HttpPort;

    TActorId httpIncomingProxyId = ActorSystem.Register(NHttp::CreateIncomingHttpCache(HttpProxyId, GetIncomingMetaCachePolicy));

    if (MetaCache) {
        httpIncomingProxyId = ActorSystem.Register(NMeta::CreateHttpMetaCache(httpIncomingProxyId, GetIncomingMetaCachePolicy, GetCacheOwnership));
    }

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/db_clusters",
                         ActorSystem.Register(new NMVP::THandlerActorMetaDbClusters(MetaLocation))
                         )
                     );

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/clusters",
                         ActorSystem.Register(new NMVP::THandlerActorMetaClusters(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cluster",
                         ActorSystem.Register(new NMVP::THandlerActorMetaCluster(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cp_databases",
                         ActorSystem.Register(new NMVP::THandlerActorMetaCpDatabases(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cp_databases_verbose",
                         ActorSystem.Register(new NMVP::THandlerActorMetaCpDatabasesVerbose(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cloud",
                         ActorSystem.Register(new NMVP::THandlerActorMetaCloud(HttpProxyId, MetaLocation))
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/ping",
                         ActorSystem.Register(new NMVP::THandlerActorHttpCheck())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/mem_profiler",
                         ActorSystem.Register(CreateMemProfiler())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/mvp/sensors.json",
                         ActorSystem.Register(new NMVP::THandlerActorHttpSensors())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/mvp.json",
                         ActorSystem.Register(new NMVP::THandlerActorMvpSwagger())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/",
                         ActorSystem.Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/api/", // url
                                                  "./content/api/", // file path
                                                  "/mvp/content/api/", // resource path
                                                  "index.html" // index name
                                                  )
                                              )
                         )
                     );

}
