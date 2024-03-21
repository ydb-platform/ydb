#include "meta.h"
#include "meta_db_clusters.h"
#include "meta_clusters.h"
#include "meta_cluster.h"
#include "meta_cp_databases.h"
#include "meta_cp_databases_verbose.h"
#include "meta_cloud.h"

#include <ydb/library/actors/http/http_cache.h>

NHttp::TCachePolicy GetIncomingMetaCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    static std::vector<TString> headersToCache = {"Cookie", "Authorization", "x-yacloud-subjecttoken"};
    if (url.starts_with("/meta/cp_databases")) {
        policy.HeadersToCacheKey = headersToCache;
        policy.TimeToExpire = TDuration::Hours(12);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    if (url.starts_with("/meta/clusters")) {
        //policy.HeadersToCacheKey = headersToCache;
        policy.TimeToExpire = TDuration::Days(7);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    return NHttp::GetDefaultCachePolicy(request, policy);
}

NHttp::TCachePolicy GetOutgoingMetaCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    if (url.EndsWith("/viewer/json/cluster") || url.EndsWith("/viewer/json/sysinfo") || url.find("/viewer/json/tenantinfo") != TStringBuf::npos) {
        policy.TimeToExpire = TDuration::Hours(24);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }

    return NHttp::GetDefaultCachePolicy(request, policy);
}

void InitMeta(NActors::TActorSystem& actorSystem,
              const NActors::TActorId& httpProxyId,
              const TString& metaApiEndpoint,
              const TString& metaDatabase) {
    static TMap<std::pair<TStringBuf, TStringBuf>, TYdbUnitResources> ydbUnitResources = {
        {
            {
                "compute",
                "slot"
            },
            {
                10.0,
                (ui64)50*1024*1024*1024,
                0
            }
        },
        {
            {
                "storage",
                "hdd"
            },
            {
                0,
                0,
                (ui64)500*1024*1024*1024
            }
        },
        {
            {
                "storage",
                "ssd"
            },
            {
                0,
                0,
                (ui64)100*1024*1024*1024
            }
        }
    };

    static TYdbLocation location = {
        "meta",
        "meta",
        {
            { "api", metaApiEndpoint },
            { "cluster-api", metaApiEndpoint }
        },
        metaDatabase,
        {},
        ydbUnitResources,
        0
    };

    TActorId httpIncomingProxyId = actorSystem.Register(NHttp::CreateIncomingHttpCache(httpProxyId, GetIncomingMetaCachePolicy));
    TActorId httpOutgoingProxyId = actorSystem.Register(NHttp::CreateOutgoingHttpCache(httpProxyId, GetOutgoingMetaCachePolicy));

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/db_clusters",
                         actorSystem.Register(new NMVP::THandlerActorMetaDbClusters(location))
                         )
                     );

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/clusters",
                         actorSystem.Register(new NMVP::THandlerActorMetaClusters(httpOutgoingProxyId, location))
                         )
                     );

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cluster",
                         actorSystem.Register(new NMVP::THandlerActorMetaCluster(httpOutgoingProxyId, location))
                         )
                     );

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cp_databases",
                         actorSystem.Register(new NMVP::THandlerActorMetaCpDatabases(httpOutgoingProxyId, location))
                         )
                     );

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cp_databases_verbose",
                         actorSystem.Register(new NMVP::THandlerActorMetaCpDatabasesVerbose(httpOutgoingProxyId, location))
                         )
                     );

    actorSystem.Send(httpIncomingProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/meta/cloud",
                         actorSystem.Register(new NMVP::THandlerActorMetaCloud(httpOutgoingProxyId, location))
                         )
                     );
}
