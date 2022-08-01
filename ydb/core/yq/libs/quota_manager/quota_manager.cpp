#include "quota_manager.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/interconnect/interconnect_impl.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/yq/libs/control_plane_storage/util.h>
#include <ydb/core/yq/libs/shared_resources/db_exec.h>
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
#define LOG_T(stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)

namespace NYq {

NActors::TActorId MakeQuotaServiceActorId(ui32 nodeId) {
    constexpr TStringBuf name = "FQ_QUOTA";
    return NActors::TActorId(nodeId, name);
}

constexpr TDuration LIMIT_REFRESH_PERIOD = TDuration::Minutes(1);
constexpr TDuration USAGE_REFRESH_PERIOD = TDuration::Seconds(10);

struct TQuotaCachedUsage {
    TQuotaUsage Usage;
    TInstant RequestedAt = TInstant::Zero();
    bool SyncInProgress = false;
    bool ChangedAfterSync = false;
    TQuotaCachedUsage() = default;
    TQuotaCachedUsage(ui64 limit) 
        : Usage(limit) {}
    TQuotaCachedUsage(ui64 limit, const TInstant& limitUpdatedAt, ui64 usage, const TInstant& usageUpdatedAt) 
        : Usage(limit, limitUpdatedAt, usage, usageUpdatedAt) {}
};

struct TQuotaCache {
    THashMap<NActors::TActorId /* Sender */, ui64 /* Cookie */> PendingRequests; 
    THashMap<TString /* MetricName */, TQuotaCachedUsage> UsageMap;
    THashSet<TString> PendingUsage;
    TInstant LoadedAt = TInstant::Zero();
};

struct TReadQuotaState {
    TString SubjectType;
    TString SubjectId;
    THashMap<TString /* MetricName */, TQuotaUsage> UsageMap;
};

using TReadQuotaExecuter = TDbExecuter<TReadQuotaState>;

struct TSyncQuotaState {
    TString SubjectType;
    TString SubjectId;
    TString MetricName;
    TQuotaUsage Usage;
    bool Refreshed = false;
};

using TSyncQuotaExecuter = TDbExecuter<TSyncQuotaState>;

TString ToString(const std::vector<ui32>& v) {
    if (v.empty()) {
        return "[]";
    }
    TStringBuilder builder;
    for (auto i : v) {
        if (builder.empty()) {
            builder << "[" << i;
        } else {
            builder << ", " << i;
        }
        if (builder.size() > 1024) {
            builder << "...";
            break;
        }
    }
    builder << "]";
    return builder;
}

TString ToString(const TQuotaMap& quota){
    if (quota.empty()) {
        return "{}";
    }
    TStringBuilder builder;
    for (auto p : quota) {
        builder << (builder.empty() ? "{" : ", ") << p.second.ToString(p.first);
        if (builder.size() > 1024) {
            builder << "...";
            break;
        }
    }
    builder << "}";
    return builder;
}

TString ToString(const THashMap<TString, TQuotaCachedUsage>& usageMap){
    if (usageMap.empty()) {
        return "{}";
    }
    TStringBuilder builder;
    for (auto p : usageMap) {
        builder << (builder.empty() ? "{" : ", ") << p.second.Usage.ToString(p.first);
        if (builder.size() > 1024) {
            builder << "...";
            break;
        }
    }
    builder << "}";
    return builder;
}

class TQuotaManagementService : public NActors::TActorBootstrapped<TQuotaManagementService> {
public:
    TQuotaManagementService(
        const NConfig::TQuotasManagerConfig& config,
        const NConfig::TYdbStorageConfig& storageConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        NKikimr::TYdbCredentialsProviderFactory credProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        std::vector<TQuotaDescription> quotaDescriptions)
        : Config(config)
        , StorageConfig(storageConfig)
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credProviderFactory)
        , ServiceCounters(counters->GetSubgroup("subsystem", "quota_manager"))
    {
        for (auto& description : quotaDescriptions) {
            QuotaInfoMap[description.SubjectType].emplace(description.MetricName, description.Info);
        }
        LimitRefreshPeriod = GetDuration(Config.GetLimitRefreshPeriod(), LIMIT_REFRESH_PERIOD);
        UsageRefreshPeriod = GetDuration(Config.GetUsageRefreshPeriod(), USAGE_REFRESH_PERIOD);
    }

    static constexpr char ActorName[] = "FQ_QUOTA_SERVICE";

    void Bootstrap() {
        YdbConnection = NewYdbConnection(StorageConfig, CredProviderFactory, YqSharedResources->CoreYdbDriver);
        DbPool = YqSharedResources->DbPoolHolder->GetOrCreate(EDbPoolId::MAIN, 10, YdbConnection->TablePathPrefix);
        Send(GetNameserviceActorId(), new NActors::TEvInterconnect::TEvListNodes());
        Become(&TQuotaManagementService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetRequest, Handle)
        hFunc(TEvQuotaService::TQuotaChangeNotification, Handle)
        hFunc(TEvQuotaService::TQuotaUsageResponse, Handle)
        hFunc(TEvQuotaService::TQuotaSetRequest, Handle)
        hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
        hFunc(NActors::TEvInterconnect::TEvNodesInfo, Handle)
        hFunc(TEvQuotaService::TEvQuotaUpdateNotification, Handle)
        hFunc(NActors::TEvents::TEvUndelivered, Handle)
    );

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_I("UNDELIVERED to Peer " << ev->Sender.NodeId() << ", " << ev->Get()->Reason);
    }

    void Handle(NActors::TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        auto oldPeerCount = NodeIds.size();
        NodeIds.clear();
        auto selfNodeId = SelfId().NodeId();
        for (auto node : ev->Get()->Nodes) {
            if (node.NodeId != selfNodeId) {
                NodeIds.push_back(node.NodeId);
            }
        }
        *ServiceCounters->GetCounter("PeerCount") = NodeIds.size();
        if (oldPeerCount != NodeIds.size()) {
            LOG_D("IC Peers[" << NodeIds.size() << "]: " << ToString(NodeIds));
        }
        TActivationContext::Schedule(TDuration::Seconds(NodeIds.empty() ? 1 : 5), new IEventHandle(GetNameserviceActorId(), SelfId(), new NActors::TEvInterconnect::TEvListNodes()));
    }

    void Handle(TEvQuotaService::TQuotaGetRequest::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto& infoMap = QuotaInfoMap[subjectType];

        if (subjectId.empty()) { // Just reply with defaults
            auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
            response->SubjectType = subjectType;
            for (auto& it : infoMap) {
                response->Quotas.emplace(it.first, TQuotaUsage(it.second.DefaultLimit));
            }
            LOG_T(subjectType << ".<defaults>: " << ToString(response->Quotas));
            Send(ev->Sender, response.Release());
            return;
        }

        auto it = subjectMap.find(subjectId);

        if (it == subjectMap.end()) {
            LOG_D(subjectType << "." << subjectId << " NOT CASHED, Loading ...");
        } else {
            if (it->second.LoadedAt + LimitRefreshPeriod < Now()) {
                LOG_D(subjectType << "." << subjectId << " FORCE CASHE RELOAD, Loading ...");
                it = subjectMap.end();
            }
        }

        if (it == subjectMap.end()) {
            ReadQuota(subjectType, subjectId, 
                [this, ev=ev](TReadQuotaExecuter& executer) {
                    // This block is executed in correct self-context, no locks/syncs required
                    auto& subjectMap = this->QuotaCacheMap[executer.State.SubjectType];
                    auto& cache = subjectMap[executer.State.SubjectId];
                    LOG_D(executer.State.SubjectType << "." << executer.State.SubjectId << ToString(cache.UsageMap) << " LOADED");
                    CheckUsageMaybeReply(executer.State.SubjectType, executer.State.SubjectId, cache, ev);
                }
            );
        } else {
            CheckUsageMaybeReply(subjectType, subjectId, it->second, ev);
        }
    }

    void CheckUsageMaybeReply(const TString& subjectType, const TString& subjectId, TQuotaCache& cache, const TEvQuotaService::TQuotaGetRequest::TPtr& ev) {

        bool pended = false;
        auto& infoMap = QuotaInfoMap[subjectType];
        if (!ev->Get()->AllowStaleUsage) {
            // Refresh usage
            for (auto& itUsage : cache.UsageMap) {
                auto metricName = itUsage.first;
                auto& cachedUsage = itUsage.second;
                if (!cachedUsage.Usage.Usage || cachedUsage.Usage.Usage->UpdatedAt + UsageRefreshPeriod < Now()) {
                    auto it = infoMap.find(metricName);
                    if (it != infoMap.end()) {
                        if (it->second.UsageUpdater != NActors::TActorId{}) {
                            if (!cache.PendingUsage.contains(metricName)) {
                                LOG_T(subjectType << "." << subjectId << "." << metricName << " IS STALE, Refreshing ...");
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
            SendQuota(ev->Sender, ev->Cookie, subjectType, subjectId, cache);
            cache.PendingRequests.erase(ev->Sender);
        }
    }

    void ChangeLimitsAndReply(const TString& subjectType, const TString& subjectId, TQuotaCache& cache, const TEvQuotaService::TQuotaSetRequest::TPtr& ev) {

        auto& infoMap = QuotaInfoMap[subjectType];
        for (auto metricLimit : ev->Get()->Limits) {
            auto& metricName = metricLimit.first;

            auto it = cache.UsageMap.find(metricName);
            if (it != cache.UsageMap.end()) {
                auto& cached = it->second;
                auto limit = metricLimit.second;
                if (cached.Usage.Limit.Value == 0 || limit == 0 || limit > cached.Usage.Limit.Value) {
                    // check hard limit only if quota is increased
                    auto itI = infoMap.find(metricName);
                    if (itI != infoMap.end()) {
                        auto& info = itI->second;
                        if (info.HardLimit != 0 && (limit == 0 || limit > info.HardLimit)) {
                            limit = info.HardLimit;
                        }
                    }
                }
                if (cached.Usage.Limit.Value != limit) {
                    cached.Usage.Limit.Value = limit;
                    cached.Usage.Limit.UpdatedAt = Now();
                    LOG_T(cached.Usage.ToString(subjectType, subjectId, metricName) << " LIMIT Changed");
                    SyncQuota(subjectType, subjectId, metricName, cached);
                }
            }
        }

        auto response = MakeHolder<TEvQuotaService::TQuotaSetResponse>(subjectType, subjectId);
        for (auto it : cache.UsageMap) {
            response->Limits.emplace(it.first, it.second.Usage.Limit.Value);
        }
        Send(ev->Sender, response.Release());
    }


    void ReadQuota(const TString& subjectType, const TString& subjectId, TReadQuotaExecuter::TCallback callback) {
    
        TDbExecutable::TPtr executable;
        auto& executer = TReadQuotaExecuter::Create(executable, false, nullptr);

        executer.State.SubjectType = subjectType;
        executer.State.SubjectId = subjectId;

        executer.Read(
            [](TReadQuotaExecuter& executer, TSqlQueryBuilder& builder) {
                builder.AddText(
                    "SELECT `" METRIC_NAME_COLUMN_NAME "`, `" METRIC_LIMIT_COLUMN_NAME "`, `" LIMIT_UPDATED_AT_COLUMN_NAME "`, `" METRIC_USAGE_COLUMN_NAME "`, `" USAGE_UPDATED_AT_COLUMN_NAME "`\n"
                    "FROM `" QUOTAS_TABLE_NAME "`\n"
                    "WHERE `" SUBJECT_TYPE_COLUMN_NAME "` = $subject\n"
                    "  AND `" SUBJECT_ID_COLUMN_NAME "` = $id\n"
                );
                builder.AddString("subject", executer.State.SubjectType);
                builder.AddString("id", executer.State.SubjectId);
            },
            [](TReadQuotaExecuter& executer, const TVector<NYdb::TResultSet>& resultSets) {
                TResultSetParser parser(resultSets.front());
                while (parser.TryNextRow()) {
                    auto name = *parser.ColumnParser(METRIC_NAME_COLUMN_NAME).GetOptionalString();
                    auto& quotaUsage = executer.State.UsageMap[name];
                    quotaUsage.Limit.Value = *parser.ColumnParser(METRIC_LIMIT_COLUMN_NAME).GetOptionalUint64();
                    quotaUsage.Limit.UpdatedAt = *parser.ColumnParser(LIMIT_UPDATED_AT_COLUMN_NAME).GetOptionalTimestamp();
                    auto usage = parser.ColumnParser(METRIC_USAGE_COLUMN_NAME).GetOptionalUint64();
                    if (usage) {
                        quotaUsage.Usage.ConstructInPlace();
                        quotaUsage.Usage->Value = *usage;
                        quotaUsage.Usage->UpdatedAt = *parser.ColumnParser(USAGE_UPDATED_AT_COLUMN_NAME).GetOptionalTimestamp();
                    }
                }
            },
            "ReadQuotas"
        ).Process(SelfId(),
            [this, callback=callback](TReadQuotaExecuter& executer) {
                auto& subjectMap = this->QuotaCacheMap[executer.State.SubjectType];
                auto& cache = subjectMap[executer.State.SubjectId];

                LOG_T(executer.State.SubjectType << "." << executer.State.SubjectId << " " << ToString(executer.State.UsageMap) << " FROM DB");

                // 1. Fill from DB
                for (auto& itUsage : executer.State.UsageMap) {
                    cache.UsageMap[itUsage.first].Usage = itUsage.second;
                }

                // 2. Append from Config 
                for (const auto& quota : this->Config.GetQuotas()) {
                    if (quota.GetSubjectType() == executer.State.SubjectType && quota.GetSubjectId() == executer.State.SubjectId) {
                        for (const auto& limit : quota.GetLimit()) {
                            if (cache.UsageMap.find(limit.GetName()) == cache.UsageMap.end()) {
                                cache.UsageMap.emplace(limit.GetName(), TQuotaCachedUsage(limit.GetLimit()));
                            }
                        }
                    }
                }

                // 3. Append defaults
                auto& infoMap = this->QuotaInfoMap[executer.State.SubjectType];
                for (auto& it : infoMap) {
                    if (cache.UsageMap.find(it.first) == cache.UsageMap.end()) {
                        cache.UsageMap.emplace(it.first, TQuotaCachedUsage(it.second.DefaultLimit));
                    }
                }

                cache.LoadedAt = Now();

                if (callback) {
                    callback(executer);
                }
            }
        );

        Exec(DbPool, executable);
    }

    void SendQuota(NActors::TActorId receivedId, ui64 cookie, const TString& subjectType, const TString& subjectId, TQuotaCache& cache) {
        auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
        response->SubjectType = subjectType;
        response->SubjectId = subjectId;
        for (auto it : cache.UsageMap) {
            response->Quotas.emplace(it.first, it.second.Usage);
        }
        LOG_T(subjectType << "." << subjectId << ToString(response->Quotas) << " SEND QUOTAS");
        Send(receivedId, response.Release(), 0, cookie);
    }

    void Handle(TEvQuotaService::TEvQuotaUpdateNotification::TPtr& ev) {
        auto& record = ev->Get()->Record;
        TQuotaUsage usage(record.metric_limit(), NProtoInterop::CastFromProto(record.limit_updated_at()));
        if (record.has_usage_updated_at()) {
            usage.Usage.ConstructInPlace();
            usage.Usage->Value = record.metric_usage();
            usage.Usage->UpdatedAt = NProtoInterop::CastFromProto(record.usage_updated_at());
        }
        LOG_T(usage.ToString(record.subject_type(), record.subject_id(), record.metric_name()) << " UPDATE from Peer " << ev->Sender.NodeId());
        UpdateQuota(record.subject_type(), record.subject_id(), record.metric_name(), usage);
    }

    void NotifyClusterNodes(const TString& subjectType, const TString& subjectId, const TString& metricName, TQuotaUsage& usage) {
        LOG_T(usage.ToString(subjectType, subjectId, metricName) << " NOTIFY CHANGE");
        for (auto nodeId : NodeIds) {
            Fq::Quota::EvQuotaUpdateNotification notification;
            notification.set_subject_type(subjectType);
            notification.set_subject_id(subjectId);
            notification.set_metric_name(metricName);
            notification.set_metric_limit(usage.Limit.Value);
            *notification.mutable_limit_updated_at() = NProtoInterop::CastToProto(usage.Limit.UpdatedAt);
            if (usage.Usage) {
                notification.set_metric_usage(usage.Usage->Value);
                *notification.mutable_usage_updated_at() = NProtoInterop::CastToProto(usage.Usage->UpdatedAt);
            }
            Send(MakeQuotaServiceActorId(nodeId), new TEvQuotaService::TEvQuotaUpdateNotification(notification), IEventHandle::FlagTrackDelivery);
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
                    LOG_T(request.SubjectType << "." << request.SubjectId << "." << request.MetricName << " FORCE UPDATE, Updating ...");
                    Send(info.UsageUpdater, new TEvQuotaService::TQuotaUsageRequest(request.SubjectType, request.SubjectId, request.MetricName));
                }
            }
        }
    }

    void SyncQuota(const TString& subjectType, const TString& subjectId, const TString& metricName, TQuotaCachedUsage& cached) {

        if (cached.SyncInProgress) {
            cached.ChangedAfterSync = true;
            return;
        }

        TDbExecutable::TPtr executable;
        auto& executer = TSyncQuotaExecuter::Create(executable, false, nullptr);

        executer.State.SubjectType = subjectType;
        executer.State.SubjectId = subjectId;
        executer.State.MetricName = metricName;
        executer.State.Usage = cached.Usage;

        cached.ChangedAfterSync = false;
        cached.SyncInProgress = true;

        executer.Read(
            [](TSyncQuotaExecuter& executer, TSqlQueryBuilder& builder) {
                builder.AddText(
                    "SELECT `" METRIC_LIMIT_COLUMN_NAME "`, `" LIMIT_UPDATED_AT_COLUMN_NAME"`, `" METRIC_USAGE_COLUMN_NAME "`, `" USAGE_UPDATED_AT_COLUMN_NAME "`\n"
                    "FROM `" QUOTAS_TABLE_NAME "`\n"
                    "WHERE `" SUBJECT_TYPE_COLUMN_NAME "` = $subject\n"
                    "  AND `" SUBJECT_ID_COLUMN_NAME "` = $id\n"
                    "  AND `" METRIC_NAME_COLUMN_NAME "` = $metric\n"
                );
                builder.AddString("subject", executer.State.SubjectType);
                builder.AddString("id", executer.State.SubjectId);
                builder.AddString("metric", executer.State.MetricName);
            },
            [](TSyncQuotaExecuter& executer, const TVector<NYdb::TResultSet>& resultSets) {
                TResultSetParser parser(resultSets.front());
                if (parser.TryNextRow()) {
                    auto limitUpdatedAt = parser.ColumnParser(LIMIT_UPDATED_AT_COLUMN_NAME).GetOptionalTimestamp();
                    if (limitUpdatedAt && *limitUpdatedAt > executer.State.Usage.Limit.UpdatedAt) {
                        // DB changed since last read, use it and ignore local changes
                        executer.State.Usage.Limit.Value = *parser.ColumnParser(METRIC_LIMIT_COLUMN_NAME).GetOptionalUint64();
                        executer.State.Usage.Limit.UpdatedAt = *limitUpdatedAt;
                        executer.State.Refreshed = true;
                    }
                    auto usageUpdatedAt = parser.ColumnParser(USAGE_UPDATED_AT_COLUMN_NAME).GetOptionalTimestamp();
                    if (usageUpdatedAt && (!executer.State.Usage.Usage || *usageUpdatedAt > executer.State.Usage.Usage->UpdatedAt)) {
                        if (!executer.State.Usage.Usage) {
                            executer.State.Usage.Usage.ConstructInPlace();
                        }
                        executer.State.Usage.Usage->Value = *parser.ColumnParser(METRIC_USAGE_COLUMN_NAME).GetOptionalUint64();
                        executer.State.Usage.Usage->UpdatedAt = *usageUpdatedAt;
                    }
                }
                if (!executer.State.Refreshed) {
                    executer.Write(
                        [](TSyncQuotaExecuter& executer, TSqlQueryBuilder& builder) {
                            builder.AddText(
                                "UPSERT INTO `" QUOTAS_TABLE_NAME "` (`" SUBJECT_TYPE_COLUMN_NAME "`, `" SUBJECT_ID_COLUMN_NAME "`, `" METRIC_NAME_COLUMN_NAME "`,  `" METRIC_LIMIT_COLUMN_NAME "`, `" LIMIT_UPDATED_AT_COLUMN_NAME "`, `" METRIC_USAGE_COLUMN_NAME "`, `" USAGE_UPDATED_AT_COLUMN_NAME "`)\n"
                            );
                            builder.AddText(
                                executer.State.Usage.Usage ?
                                    "VALUES ($subject, $id, $metric, $limit, $limit_updated_at, $usage, $usage_updated_at);\n"
                                :   "VALUES ($subject, $id, $metric, $limit, $limit_updated_at, NULL, NULL);\n"
                            );
                            builder.AddString("subject", executer.State.SubjectType);
                            builder.AddString("id", executer.State.SubjectId);
                            builder.AddString("metric", executer.State.MetricName);
                            builder.AddUint64("limit", executer.State.Usage.Limit.Value);
                            builder.AddTimestamp("limit_updated_at", executer.State.Usage.Limit.UpdatedAt);
                            if (executer.State.Usage.Usage) {
                                builder.AddUint64("usage", executer.State.Usage.Usage->Value);
                                builder.AddTimestamp("usage_updated_at", executer.State.Usage.Usage->UpdatedAt);
                            }
                        },
                        "WriteQuota"
                    );
                }
            },
            "CheckQuota"
        ).Process(SelfId(),
            [this](TSyncQuotaExecuter& executer) {
                if (executer.State.Refreshed) {
                    UpdateQuota(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, executer.State.Usage);
                } else {
                    this->NotifyClusterNodes(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, executer.State.Usage);
                }

                auto& subjectMap = this->QuotaCacheMap[executer.State.SubjectType];
                auto it = subjectMap.find(executer.State.SubjectId);
                if (it != subjectMap.end()) {
                    auto& cache = it->second;
                    auto itQ = cache.UsageMap.find(executer.State.MetricName);
                    if (itQ != cache.UsageMap.end()) {
                        itQ->second.SyncInProgress = false;
                        if (itQ->second.ChangedAfterSync) {
                            LOG_T(itQ->second.Usage.ToString(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName) << " RESYNC");
                            SyncQuota(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, itQ->second);
                        }
                    }                        
                }
            }
        );

        Exec(DbPool, executable);
    }

    void UpdateQuota(const TString& subjectType, const TString& subjectId, const TString& metricName, TQuotaUsage& usage) {
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto it = subjectMap.find(subjectId);
        if (it != subjectMap.end()) {
            auto& cache = it->second;
            auto itQ = cache.UsageMap.find(metricName);
            if (itQ != cache.UsageMap.end()) {
                itQ->second.Usage.Merge(usage);
                LOG_T(itQ->second.Usage.ToString(subjectType, subjectId, metricName) << " MERGED");
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
            // if quotas are not cached - ignore usage update
            return;
        }

        auto& cache = it->second;    
        cache.PendingUsage.erase(metricName);

        auto itQ = cache.UsageMap.find(metricName);
        if (itQ == cache.UsageMap.end()) {
            // if metric is not defined - ignore usage update
            return;
        }
        itQ->second.Usage.Usage = ev->Get()->Usage;
        LOG_T(itQ->second.Usage.ToString(subjectType, subjectId, metricName) << " REFRESHED");

        if (cache.PendingUsage.size() == 0) {
            for (auto& itR : cache.PendingRequests) {
                SendQuota(itR.first, itR.second, subjectType, subjectId, cache);
            }
            cache.PendingRequests.clear();
        }

        SyncQuota(subjectType, subjectId, metricName, itQ->second);
    }

    void Handle(TEvQuotaService::TQuotaSetRequest::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto& subjectMap = QuotaCacheMap[subjectType];

        auto it = subjectMap.find(subjectId);
        if (it == subjectMap.end()) {
            ReadQuota(subjectType, subjectId, 
                [this, ev=ev](TReadQuotaExecuter& executer) {
                    // This block is executed in correct self-context, no locks/syncs required
                    auto& subjectMap = this->QuotaCacheMap[executer.State.SubjectType];
                    auto& cache = subjectMap[executer.State.SubjectId];
                    LOG_D(executer.State.SubjectType << "." << executer.State.SubjectId << ToString(cache.UsageMap) << " LOADED");
                    ChangeLimitsAndReply(executer.State.SubjectType, executer.State.SubjectId, cache, ev);
                }
            );
        } else {
            ChangeLimitsAndReply(subjectType, subjectId, it->second, ev);
        }
    }

    NConfig::TQuotasManagerConfig Config;
    NConfig::TYdbStorageConfig StorageConfig;
    ::NYq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;
    TDbPool::TPtr DbPool;
    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
    THashMap<TString /* SubjectType */, THashMap<TString /* MetricName */, TQuotaInfo>> QuotaInfoMap;
    THashMap<TString /* SubjectType */, THashMap<TString /* SubjectId */, TQuotaCache>> QuotaCacheMap;
    TDuration LimitRefreshPeriod;
    TDuration UsageRefreshPeriod;
    std::vector<ui32> NodeIds;
};

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    const NConfig::TYdbStorageConfig& storageConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    NKikimr::TYdbCredentialsProviderFactory credProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc) {
        return new TQuotaManagementService(config, storageConfig, yqSharedResources, credProviderFactory, counters, quotaDesc);
}

} /* NYq */
