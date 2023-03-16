#pragma once

#include <ydb/core/mon/mon.h>

#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/config/protos/quotas_manager.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

/*

    1. TQuotaGetRequest is sent to get actual quota limits and usage from QM. QM guarantees actual limits and only eventually
       consistent usage. Also QM provides no upper time for reply. Client must control timeout and may resent the message with
       AllowStaleUsage = true to avoid waiting for usage requests (in any case it will wait for limits to be loaded into cache).
       If message is resent N times client must be ready to receive from 1 up to (1 + N) replies and my use Cookies to dedup them

    2. TQuotaGetResponse contains actual limits and may provide last known usages. Client may check UsageUpdatedAt to decide if
       value provided is fresh enough. If usage is missed or stale, client must reply upstream with retriable error "quota is 
       not accessible yet"

    3. TQuotaUsageRequest is sent by QM to handlers configured to calculate quota usage. For sake of simplicity QM requires no
       time or event reply quarantees. Handler must do the best to reply as soon as possible but may delay or event discard the
       request. But it should reply with the same Cookie (QM uses it to match reply with request). Also handler may receive another
       request before previous is replied. To mitigate overload handler may implement throttling on its side

    4. TQuotaUsageResponse is sent by usage calculation handler back to QM. Cookie must be kept. Also handler my sent unpaired
       message any time to refresh usage in pull mode with Cookie == 0. QM will refresh usage or discard the message if Subject
       limits are not in the cache

    5. TQuotaChangeNotification may be sent from any point to toggle usage refresh if there is reason for usage to change

*/

namespace NFq {

NActors::TActorId MakeQuotaServiceActorId(ui32 nodeId);

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    const NConfig::TYdbStorageConfig& storageConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    NKikimr::TYdbCredentialsProviderFactory credProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc,
    NActors::TMon* monitoring = nullptr);

} /* NFq */
