#include <ydb/mvp/core/core_ydbc.h>

#include <ydb/mvp/core/core_ydbc_impl.h>
#include "ydbc_locations.h"
#include "ydbc_databases.h"
#include "ydbc_all_databases.h"
#include "ydbc_database.h"
#include "ydbc_config.h"
#include "ydbc_operation.h"
#include "ydbc_operations.h"
#include "ydbc_query.h"
#include "ydbc_query1.h"
#include "ydbc_browse.h"
#include "ydbc_meta.h"
#include "ydbc_directory.h"
#include "ydbc_simulate_database.h"
#include "ydbc_dynamo_describe_table.h"
#include "ydbc_dynamo_table.h"
#include "ydbc_dynamo_item.h"
#include "ydbc_table.h"
#include "ydbc_backup.h"
#include "ydbc_restore.h"
#include "ydbc_stop.h"
#include "ydbc_start.h"
#include "ydbc_backup_list.h"
#include "ydbc_acl.h"
#include "ydbc_quota_get.h"
#include "ydbc_quota_get_default.h"
#include "ydbc_quota_update_metric.h"
#include "ydbc_quota_batch_update_metric.h"
#include "ydbc_datastreams.h"
#include "ydbc_datastream.h"
#include "ydbc_list_datastream_consumers.h"
#include "ydbc_describe_consumer.h"

#include <ydb/core/util/wildcard.h>
#include <ydb/library/actors/http/http_cache.h>

#include <util/string/split.h>
#include <util/string/join.h>

void InitYdbcCommon(NActors::TActorSystem& actorSystem,
                    const NActors::TActorId& httpProxyId,
                    const TString& endpointName,
                    const TMap<TString, TYdbcLocation>& locations) {

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/locations",
                         actorSystem.Register(new NMVP::THandlerActorYdbcLocations(locations))
                         )
                     );
}

void InitYdbcLocation(NActors::TActorSystem& actorSystem,
                      const NActors::TActorId& httpProxyId,
                      const TString& endpointName,
                      const TYdbcLocation& location) {

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/databases",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatabases(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/all_databases",
                         actorSystem.Register(new NMVP::THandlerActorYdbcAllDatabases(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatabase(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/simulateDatabase",
                         actorSystem.Register(new NMVP::THandlerActorYdbcSimulateDatabase(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/operations",
                         actorSystem.Register(new NMVP::THandlerActorYdbcOperations(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/operation",
                         actorSystem.Register(new NMVP::THandlerActorYdbcOperation(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/config",
                         actorSystem.Register(new NMVP::THandlerActorYdbcConfig(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/query",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuery(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/query1",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuery1(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/browse",
                         actorSystem.Register(new NMVP::THandlerActorYdbcBrowse(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/meta",
                         actorSystem.Register(new NMVP::THandlerActorYdbcMeta(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/directory",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDirectory(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/dynamo_describe_table",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDynamoDescribeTable(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/dynamo_table",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDynamoTable(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/dynamo_item",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDynamoItem(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/table",
                         actorSystem.Register(new NMVP::THandlerActorYdbcTable(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/backup",
                         actorSystem.Register(new NMVP::THandlerActorYdbcBackup(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/start_database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcStart(location, httpProxyId))
                         )
                    );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/stop_database",
                         actorSystem.Register(new NMVP::THandlerActorYdbcStop(location, httpProxyId))
                         )
                    );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/restore",
                         actorSystem.Register(new NMVP::THandlerActorYdbcRestore(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/backup_list",
                         actorSystem.Register(new NMVP::THandlerActorYdbcBackupList(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/acl",
                         actorSystem.Register(new NMVP::THandlerActorYdbcAcl(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_get",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaGet(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_get_default",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaGetDefault(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_update_metric",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaUpdateMetric(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/quota_batch_update_metric",
                         actorSystem.Register(new NMVP::THandlerActorYdbcQuotaBatchUpdateMetric(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/datastreams",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastreams(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/describe_consumer",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDescribeConsumer(location, httpProxyId))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/datastream",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "delete"))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/datastream_consumer",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "delete_consumer"))
                         )
                     );


    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/simulate_datastream",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "simulate"))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/v1/simulate_datastream",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "simulate_v1"))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/create_datastream",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "create"))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/create_datastream_consumer",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "create_consumer"))
                         )
                     );

    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/update_datastream",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "update"))
                         )
                     );
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/put_datastream_records",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "put_records"))
                         )
                     );
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/get_datastream_records",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "get_records"))
                         )
                     );
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/list_datastream_shards",
                         actorSystem.Register(new NMVP::THandlerActorYdbcDatastream(location, httpProxyId, "list_shards"))
                         )
                     );
    actorSystem.Send(httpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         endpointName + "/list_datastream_consumers",
                         actorSystem.Register(new NMVP::THandlerActorYdbcListDatastreamConsumers(location, httpProxyId))
                         )
                     );
}

NHttp::TCachePolicy GetYdbcCachePolicy(const NHttp::THttpRequest* request) {
    NHttp::TCachePolicy policy;
    if (request->Method != "GET") {
        return policy;
    }
    TStringBuf url(request->URL);
    // YDBC
    if (url.starts_with("/ydbc/ydb-yandex/database") || url.starts_with("/ydbc/ydb-yandex/all_databases")) { // database + databases
        static std::vector<TString> headersToCache = {"Cookie", "Authorization", "x-yacloud-subjecttoken"};
        static std::vector<TString> statusesToRetry = {"500", "503", "504"};
        policy.HeadersToCacheKey = headersToCache;
        policy.StatusesToRetry = statusesToRetry;
        policy.RetriesCount = 2;
        policy.TimeToExpire = TDuration::Minutes(10);
        policy.TimeToRefresh = TDuration::Seconds(60);
        policy.KeepOnError = true;
    }
    if (NKikimr::IsMatchesWildcard(url, "/ydbc/*/browse?*") || NKikimr::IsMatchesWildcard(url, "/ydbc/*/meta?*")) { // browse + meta
        static std::vector<TString> headersToCache = {"Cookie", "Authorization", "x-yacloud-subjecttoken"};
        static std::vector<TString> statusesToRetry = {"503", "504"};
        policy.HeadersToCacheKey = headersToCache;
        policy.StatusesToRetry = statusesToRetry;
        policy.RetriesCount = 2;
    }
    return NHttp::GetDefaultCachePolicy(request, policy); // no caching for ydbc
}

void InitYdbc(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId, const TMap<TString, TYdbcLocation>& ydbcLocations) {
    TActorId ydbcHttpProxy = actorSystem.Register(NHttp::CreateIncomingHttpCache(httpProxyId, GetYdbcCachePolicy));

    InitYdbcCommon(actorSystem, ydbcHttpProxy, "/ydbc", ydbcLocations);

    for (const auto& pr : ydbcLocations) {
        InitYdbcLocation(actorSystem, ydbcHttpProxy, "/ydbc/" + pr.first, pr.second);
    }
}
