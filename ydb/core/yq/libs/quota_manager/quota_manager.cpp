#include "quota_manager.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/yq/libs/control_plane_storage/util.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/core/protos/services.pb.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)

namespace NYq {

NActors::TActorId MakeQuotaServiceActorId() {
    constexpr TStringBuf name = "FQ_QUOTA";
    return NActors::TActorId(0, name);
}

constexpr TDuration USAGE_REFRESH_PERIOD = TDuration::Seconds(10);

struct TQuotaCachedUsage {
    TQuotaUsage Usage;
    TInstant RequestedAt = TInstant::Zero();
    TQuotaCachedUsage(ui64 limit) 
        : Usage(limit) {}
    TQuotaCachedUsage(ui64 limit, ui64 usage, const TInstant& updatedAt) 
        : Usage(limit, usage, updatedAt) {}
};

struct TQuotaCache {
    THashMap<NActors::TActorId /* Sender */, ui64 /* Cookie */> PendingRequests; 
    THashMap<TString /* MetricName */, TQuotaCachedUsage> UsageMap;
    THashSet<TString> PendingUsage;
};

class TQuotaManagementService : public NActors::TActorBootstrapped<TQuotaManagementService> {
public:
    TQuotaManagementService(
        const NConfig::TQuotasManagerConfig& config,
        /* const NYq::TYqSharedResources::TPtr& yqSharedResources, */
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::vector<TQuotaDescription> quotaDescriptions)
        : Config(config)
        , ServiceCounters(counters->GetSubgroup("subsystem", "QuotaService"))
    {
        /* Y_UNUSED(yqSharedResources); */
        for (auto& description : quotaDescriptions) {
            QuotaInfoMap[description.SubjectType].emplace(description.MetricName, description.Info);
        }
        UsageRefreshPeriod = GetDuration(Config.GetUsageRefreshPeriod(), USAGE_REFRESH_PERIOD);
    }

    static constexpr char ActorName[] = "FQ_QUOTA_SERVICE";

    void Bootstrap() {
        Become(&TQuotaManagementService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetRequest, Handle)
        hFunc(TEvQuotaService::TQuotaChangeNotification, Handle)
        hFunc(TEvQuotaService::TQuotaUsageResponse, Handle)
        hFunc(TEvQuotaService::TQuotaSetRequest, Handle)
    );

    void Handle(TEvQuotaService::TQuotaGetRequest::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto& infoMap = QuotaInfoMap[subjectType];

        if (subjectId.empty()) { // Just get defaults
            auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
            response->SubjectType = subjectType;
            for (auto& it : infoMap) {
                response->Quotas.emplace(it.first, TQuotaUsage(it.second.DefaultLimit));
            }
            Send(ev->Sender, response.Release());
            return;
        }

        auto it = subjectMap.find(subjectId);
        bool pended = false;

        // Load into cache, if needed
        if (it == subjectMap.end()) {
            TQuotaCache cache;
            // 1. Load from Config 
            for (const auto& quota : Config.GetQuotas()) {
                if (quota.GetSubjectType() == subjectType && quota.GetSubjectId() == subjectId) {
                    for (const auto& limit : quota.GetLimit()) {
                        cache.UsageMap.emplace(limit.GetName(), TQuotaCachedUsage(limit.GetLimit()));
                    }
                }
            }
            // 2. Append defaults
            for (auto& it : infoMap) {
                if (cache.UsageMap.find(it.first) == cache.UsageMap.end()) {
                    cache.UsageMap.emplace(it.first, TQuotaCachedUsage(it.second.DefaultLimit));
                }
            }
            // 3. Load from DB
            subjectMap.emplace(subjectId, cache);
        }

        auto& cache = subjectMap[subjectId];
        if (!ev->Get()->AllowStaleUsage) {
            // Refresh usage
            for (auto& itUsage : cache.UsageMap) {
                auto metricName = itUsage.first;
                auto& cachedUsage = itUsage.second;
                if (cachedUsage.Usage.UpdatedAt + UsageRefreshPeriod < Now()) {
                    auto it = infoMap.find(metricName);
                    if (it != infoMap.end()) {
                        if (it->second.UsageUpdater != NActors::TActorId{}) {
                            if (!cache.PendingUsage.contains(metricName)) {
                                Send(it->second.UsageUpdater, new TEvQuotaService::TQuotaUsageRequest(subjectType, subjectId, metricName));
                                cache.PendingUsage.insert(metricName);
                                cachedUsage.RequestedAt = Now();
                            }
                            if (!pended) {
                                cache.PendingRequests.emplace(ev->Sender, ev->Cookie);
                                pended = true;
                            }
                        }
                    }
                }
            }
        }

        if (!pended) {
            auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
            response->SubjectType = subjectType;
            response->SubjectId = subjectId;
            for (auto it : cache.UsageMap) {
                response->Quotas.emplace(it.first, it.second.Usage);
            }
            Send(ev->Sender, response.Release());
            cache.PendingRequests.erase(ev->Sender);
        }
    }

    void Handle(TEvQuotaService::TQuotaChangeNotification::TPtr& ev) {
        auto& request = *ev->Get();
        auto itt = QuotaInfoMap.find(request.SubjectType);
        if (itt != QuotaInfoMap.end()) {
            auto& metricMap = itt->second;
            auto itm = metricMap.find(request.MetricName);
            if (itm != metricMap.end()) {
                auto& info = itm->second;
                if (info.UsageUpdater != NActors::TActorId{}) {
                    Send(info.UsageUpdater, new TEvQuotaService::TQuotaUsageRequest(request.SubjectType, request.SubjectId, request.MetricName));
                }
            }
        }
    }

    void Handle(TEvQuotaService::TQuotaUsageResponse::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto metricName = ev->Get()->MetricName;
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto it = subjectMap.find(subjectId);

        if (it == subjectMap.end()) {
            return;
        }

        auto& cache = it->second;    
        cache.PendingUsage.erase(metricName);

        auto itQ = cache.UsageMap.find(metricName);
        if (itQ != cache.UsageMap.end()) {
            itQ->second.Usage.Usage = ev->Get()->Usage;
        }

        if (cache.PendingUsage.size() == 0) {
            for (auto& itR : cache.PendingRequests) {
                auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
                response->SubjectType = subjectType;
                response->SubjectId = subjectId;
                for (auto it : cache.UsageMap) {
                    response->Quotas.emplace(it.first, it.second.Usage);
                }
                Send(itR.first, response.Release());
            }
            cache.PendingRequests.clear();
        }
    }

    void Handle(TEvQuotaService::TQuotaSetRequest::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto& infoMap = QuotaInfoMap[subjectType];

        auto it = subjectMap.find(subjectId);

        // Load into cache, if needed
        if (it == subjectMap.end()) {
            TQuotaCache cache;
            // 1. Load from Config 
            for (const auto& quota : Config.GetQuotas()) {
                if (quota.GetSubjectType() == subjectType && quota.GetSubjectId() == subjectId) {
                    for (const auto& limit : quota.GetLimit()) {
                        cache.UsageMap.emplace(limit.GetName(), TQuotaCachedUsage(limit.GetLimit()));
                    }
                }
            }
            // 2. Load from DB (TBD)
            // 3. Append defaults
            for (auto& it : infoMap) {
                if (cache.UsageMap.find(it.first) == cache.UsageMap.end()) {
                    cache.UsageMap.emplace(it.first, TQuotaCachedUsage(it.second.DefaultLimit));
                }
            }
            bool _;
            std::tie(it, _) = subjectMap.emplace(subjectId, cache);
        }

        auto& cache = it->second;

        for (auto metricLimit : ev->Get()->Limits) {
            auto& name = metricLimit.first;
            auto it = cache.UsageMap.find(name);
            if (it != cache.UsageMap.end()) {
                auto& cached = it->second;
                auto limit = metricLimit.second;
                if (cached.Usage.Limit == 0 || limit == 0 || limit > cached.Usage.Limit) {
                    // check hard limit only if quota is increased
                    auto itI = infoMap.find(name);
                    if (itI != infoMap.end()) {
                        auto& info = itI->second;
                        if (info.HardLimit != 0 && (limit == 0 || limit > info.HardLimit)) {
                            limit = info.HardLimit;
                        }
                    }
                }
                cached.Usage.Limit = limit;
            }
        }

        auto response = MakeHolder<TEvQuotaService::TQuotaSetResponse>(subjectType, subjectId);
        for (auto it : cache.UsageMap) {
            response->Limits.emplace(it.first, it.second.Usage.Limit);
        }
        Send(ev->Sender, response.Release());
    }

    NConfig::TQuotasManagerConfig Config;
    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
    THashMap<TString /* SubjectType */, THashMap<TString /* MetricName */, TQuotaInfo>> QuotaInfoMap;
    THashMap<TString /* SubjectType */, THashMap<TString /* SubjectId */, TQuotaCache>> QuotaCacheMap;
    TDuration UsageRefreshPeriod;
};

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    /* const NYq::TYqSharedResources::TPtr& yqSharedResources, */
    const ::NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc) {
        return new TQuotaManagementService(config, /* yqSharedResources, */ counters, quotaDesc);
}

} /* NYq */
