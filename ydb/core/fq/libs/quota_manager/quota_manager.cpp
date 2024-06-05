#include "quota_manager.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/shared_resources/db_exec.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/ydb/ydb.h>


#include <ydb/library/services/services.pb.h>

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

#define LOG_AS_E(actorSystem, stream) \
    LOG_ERROR_S(actorSystem, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_AS_W(actorSystem, stream) \
    LOG_WARN_S(actorSystem, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_AS_I(actorSystem, stream) \
    LOG_INFO_S(actorSystem, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_AS_D(actorSystem, stream) \
    LOG_DEBUG_S(actorSystem, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_AS_T(actorSystem, stream) \
    LOG_TRACE_S(actorSystem, NKikimrServices::FQ_QUOTA_SERVICE, stream)

namespace NFq {

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
    THashMap<NActors::TActorId /* Sender */, ui64 /* Cookie */> PendingUsageRequests;
    THashMap<TString /* MetricName */, TQuotaCachedUsage> UsageMap;
    THashSet<TString> PendingUsage;
    THashSet<TString> PendingLimit;
    NActors::TActorId PendingLimitRequest;
    ui64 PendingLimitCookie;
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

TString ToString(const TQuotaMap& quota) {
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

TString ToString(const THashMap<TString, TQuotaCachedUsage>& usageMap) {
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
        std::vector<TQuotaDescription> quotaDescriptions,
        NActors::TMon* monitoring)
        : Config(config)
        , StorageConfig(storageConfig)
        , YqSharedResources(yqSharedResources)
        , CredProviderFactory(credProviderFactory)
        , ServiceCounters(counters->GetSubgroup("subsystem", "quota_manager"))
        , Monitoring(monitoring)
    {
        for (const auto& description : quotaDescriptions) {
            QuotaInfoMap[description.SubjectType].emplace(description.MetricName, description.Info);
        }
        // Override static settings with config
        for (const auto& config : Config.GetQuotaDescriptions()) {
            Y_ABORT_UNLESS(config.GetSubjectType());
            Y_ABORT_UNLESS(config.GetMetricName());
            auto& metricsMap = QuotaInfoMap[config.GetSubjectType()];
            auto infoIt = metricsMap.find(config.GetMetricName());
            Y_ABORT_UNLESS(infoIt != metricsMap.end());
            auto& info = infoIt->second;
            if (config.GetDefaultLimit()) {
                info.DefaultLimit = config.GetDefaultLimit();
            }
            if (config.GetHardLimit()) {
                info.HardLimit = config.GetHardLimit();
            }
        }
        LimitRefreshPeriod = GetDuration(Config.GetLimitRefreshPeriod(), LIMIT_REFRESH_PERIOD);
        UsageRefreshPeriod = GetDuration(Config.GetUsageRefreshPeriod(), USAGE_REFRESH_PERIOD);
    }

    static constexpr char ActorName[] = "FQ_QUOTA_SERVICE";

    void Bootstrap() {
        if (Monitoring) {
            Monitoring->RegisterActorPage(Monitoring->RegisterIndexPage("fq_diag", "Federated Query diagnostics"),
                "quotas", "Quota Manager", false, NActors::TActivationContext::ActorSystem(), SelfId());
        }

        YdbConnection = NewYdbConnection(StorageConfig, CredProviderFactory, YqSharedResources->CoreYdbDriver);
        TablePathPrefix = YdbConnection->TablePathPrefix;
        DbPool = YqSharedResources->DbPoolHolder->GetOrCreate(static_cast<ui32>(EDbPoolId::MAIN));
        Send(NActors::GetNameserviceActorId(), new NActors::TEvInterconnect::TEvListNodes());
        Become(&TQuotaManagementService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetRequest, Handle)
        hFunc(TEvQuotaService::TQuotaGetResponse, Handle) // http monitoring
        hFunc(TEvQuotaService::TQuotaChangeNotification, Handle)
        hFunc(TEvQuotaService::TQuotaUsageResponse, Handle)
        hFunc(TEvQuotaService::TQuotaLimitChangeResponse, Handle)
        hFunc(TEvQuotaService::TQuotaSetRequest, Handle)
        hFunc(TEvQuotaService::TQuotaSetResponse, Handle) // http monitoring
        hFunc(TEvents::TEvCallback, [](TEvents::TEvCallback::TPtr& ev) { ev->Get()->Callback(); } );
        hFunc(NActors::TEvInterconnect::TEvNodesInfo, Handle)
        hFunc(TEvQuotaService::TEvQuotaUpdateNotification, Handle)
        hFunc(NActors::TEvents::TEvUndelivered, Handle)
        hFunc(NActors::NMon::TEvHttpInfo, Handle)
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
        NActors::TActivationContext::Schedule(TDuration::Seconds(NodeIds.empty() ? 1 : 5), new IEventHandle(NActors::GetNameserviceActorId(), SelfId(), new NActors::TEvInterconnect::TEvListNodes()));
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
                        if (it->second.QuotaController != NActors::TActorId{}) {
                            if (!cache.PendingUsage.contains(metricName)) {
                                LOG_T(subjectType << "." << subjectId << "." << metricName << " IS STALE, Refreshing ...");
                                Send(it->second.QuotaController, new TEvQuotaService::TQuotaUsageRequest(subjectType, subjectId, metricName));
                                cache.PendingUsage.insert(metricName);
                                cachedUsage.RequestedAt = Now();
                            }
                            if (!pended) {
                                cache.PendingUsageRequests.emplace(ev->Sender, ev->Cookie);
                                pended = true;
                            }
                        }
                    }
                }
            }
        }

        if (!pended) {
            SendQuota(ev->Sender, ev->Cookie, subjectType, subjectId, cache);
            cache.PendingUsageRequests.erase(ev->Sender);
        }
    }

    void ChangeLimitsAndReply(const TString& subjectType, const TString& subjectId, TQuotaCache& cache, const TEvQuotaService::TQuotaSetRequest::TPtr& ev) {

        auto pended = false;
        auto& infoMap = QuotaInfoMap[subjectType];
        for (auto metricLimit : ev->Get()->Limits) {
            auto& metricName = metricLimit.first;

            auto it = cache.UsageMap.find(metricName);
            if (it != cache.UsageMap.end()) {
                auto& cached = it->second;
                auto limit = metricLimit.second;

                auto itI = infoMap.find(metricName);
                if (itI != infoMap.end()) {
                    auto& info = itI->second;
                    if (cached.Usage.Limit.Value == 0 || limit == 0 || limit > cached.Usage.Limit.Value) {
                        // check hard limit only if quota is increased
                        if (info.HardLimit != 0 && (limit == 0 || limit > info.HardLimit)) {
                            limit = info.HardLimit;
                        }
                    }

                    if (info.QuotaController != NActors::TActorId{}) {
                        pended = true;
                        cache.PendingLimitRequest = ev->Sender;
                        cache.PendingLimitCookie = ev->Cookie;
                        cache.PendingLimit.insert(metricName);
                        Send(info.QuotaController, new TEvQuotaService::TQuotaLimitChangeRequest(subjectType, subjectId, metricName, cached.Usage.Limit.Value, limit));
                        continue;
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

        if (!pended) {
            auto response = MakeHolder<TEvQuotaService::TQuotaSetResponse>(subjectType, subjectId);
            for (auto it : cache.UsageMap) {
                response->Limits.emplace(it.first, it.second.Usage.Limit.Value);
            }
            Send(ev->Sender, response.Release());
        }
    }

    void Handle(TEvQuotaService::TQuotaLimitChangeResponse::TPtr& ev) {
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
        cache.PendingLimit.erase(metricName);

        auto itQ = cache.UsageMap.find(metricName);
        if (itQ != cache.UsageMap.end()) {
            // if metric is not defined - ignore usage update
            auto& cached = itQ->second;
            if (cached.Usage.Limit.Value != ev->Get()->Limit) {
                cached.Usage.Limit.Value = ev->Get()->Limit;
                cached.Usage.Limit.UpdatedAt = Now();
                LOG_T(cached.Usage.ToString(subjectType, subjectId, metricName) << " LIMIT Change Accepted");
                SyncQuota(subjectType, subjectId, metricName, cached);
            }
        }

        if (cache.PendingLimit.size() == 0) {
            if (cache.PendingLimitRequest != NActors::TActorId{}) {
                auto response = MakeHolder<TEvQuotaService::TQuotaSetResponse>(subjectType, subjectId);
                for (auto it : cache.UsageMap) {
                    response->Limits.emplace(it.first, it.second.Usage.Limit.Value);
                }
                Send(cache.PendingLimitRequest, response.Release());
                cache.PendingLimitRequest = NActors::TActorId{};
            }
        }
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
            "ReadQuotas", true
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

        Exec(DbPool, executable, TablePathPrefix).Apply([this, executable, actorSystem=NActors::TActivationContext::ActorSystem(), subjectType, subjectId, callback, selfId=SelfId()](const auto& future) {
            actorSystem->Send(selfId, new TEvents::TEvCallback([this, executable, subjectType, subjectId, callback, future]() {
                auto issues = GetIssuesFromYdbStatus(executable, future);
                if (issues) {
                    LOG_E("ReadQuota finished with error: " << issues->ToOneLineString());
                    this->ReadQuota(subjectType, subjectId, callback); // TODO: endless retry possible
                }
            }));
        });
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
                if (info.QuotaController != NActors::TActorId{}) {
                    LOG_T(request.SubjectType << "." << request.SubjectId << "." << request.MetricName << " FORCE UPDATE, Updating ...");
                    Send(info.QuotaController, new TEvQuotaService::TQuotaUsageRequest(request.SubjectType, request.SubjectId, request.MetricName));
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
                        "WriteQuota", true
                    );
                }
            },
            "CheckQuota"
        ).Process(SelfId(),
            [this](TSyncQuotaExecuter& executer) {
                if (executer.State.Refreshed) {
                    this->UpdateQuota(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, executer.State.Usage);
                } else {
                    this->NotifyClusterNodes(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, executer.State.Usage);
                }

                auto& subjectMap = this->QuotaCacheMap[executer.State.SubjectType];
                auto it = subjectMap.find(executer.State.SubjectId);
                if (it != subjectMap.end()) {
                    auto& cache = it->second;
                    auto itQ = cache.UsageMap.find(executer.State.MetricName);
                    if (itQ != cache.UsageMap.end()) {
                        auto& cached = itQ->second;
                        cached.SyncInProgress = false;
                        if (cached.ChangedAfterSync) { // this check will be processed in a separate event
                            LOG_T(cached.Usage.ToString(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName) << " RESYNC");
                            this->SyncQuota(executer.State.SubjectType, executer.State.SubjectId, executer.State.MetricName, cached);
                        }
                    }
                }
            }
        );

        Exec(DbPool, executable, TablePathPrefix).Apply([this, executable, subjectId, subjectType, metricName, actorSystem=NActors::TActivationContext::ActorSystem(), selfId=SelfId()](const auto& future) {
            actorSystem->Send(selfId, new TEvents::TEvCallback([this, executable, subjectId, subjectType, metricName, future]() {
                auto issues = GetIssuesFromYdbStatus(executable, future);
                if (issues) {
                    LOG_E("SyncQuota finished with error: " << issues->ToOneLineString());
                    auto& subjectMap = this->QuotaCacheMap[subjectType];
                    auto it = subjectMap.find(subjectId);
                    if (it != subjectMap.end()) {
                        auto& cache = it->second;
                        auto itQ = cache.UsageMap.find(metricName);
                        if (itQ != cache.UsageMap.end()) {
                            auto& cached = itQ->second;
                            cached.SyncInProgress = false;
                            LOG_T(cached.Usage.ToString(metricName, subjectId, metricName) << " RESYNC after error");
                            this->SyncQuota(subjectType, subjectId, metricName, cached); // TODO: endless retry possible
                        }
                    }
                }
            }));


        });
    }

    void UpdateQuota(const TString& subjectType, const TString& subjectId, const TString& metricName, TQuotaUsage& usage) {
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto it = subjectMap.find(subjectId);
        if (it != subjectMap.end()) {
            auto& cache = it->second;
            auto itQ = cache.UsageMap.find(metricName);
            if (itQ != cache.UsageMap.end()) {
                auto& cached = itQ->second;
                cached.Usage.Merge(usage);
                LOG_T(cached.Usage.ToString(subjectType, subjectId, metricName) << " MERGED " << reinterpret_cast<ui64>(&cached));
            }
        }
    }

    void Handle(TEvQuotaService::TQuotaUsageResponse::TPtr& ev) {
        auto subjectType = ev->Get()->SubjectType;
        auto subjectId = ev->Get()->SubjectId;
        auto metricName = ev->Get()->MetricName;
        auto& subjectMap = QuotaCacheMap[subjectType];
        auto it = subjectMap.find(subjectId);

        if (!ev->Get()->Success) {
            LOG_E("TQuotaUsageResponse error for subject type: " << subjectType << ", subject id: " << subjectId << ", metrics name: " << metricName << ", issues: " << ev->Get()->Issues.ToOneLineString());
            Send(ev->Sender, new TEvQuotaService::TQuotaUsageRequest(subjectType, subjectId, metricName)); // retry. TODO: it may be useful to report the error to the user
            return;
        }

        if (it == subjectMap.end()) {
            // if quotas are not cached - ignore usage update
            return;
        }

        auto& cache = it->second;
        cache.PendingUsage.erase(metricName);

        auto itQ = cache.UsageMap.find(metricName);
        if (itQ != cache.UsageMap.end()) {
            // if metric is not defined - ignore usage update
            itQ->second.Usage.Usage = ev->Get()->Usage;
            LOG_T(itQ->second.Usage.ToString(subjectType, subjectId, metricName) << " REFRESHED");
            SyncQuota(subjectType, subjectId, metricName, itQ->second);
        }

        if (cache.PendingUsage.size() == 0) {
            for (auto& itR : cache.PendingUsageRequests) {
                SendQuota(itR.first, itR.second, subjectType, subjectId, cache);
            }
            cache.PendingUsageRequests.clear();
        }
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

    void Handle(TEvQuotaService::TQuotaSetResponse::TPtr& ev) {

        TStringStream html;
        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Quota Set Response</th>";
        html << "<th></th>";
        html << "</tr></thead><tbody>";
        html << "<tr><td>Subject Type:</td><td>" << ev->Get()->SubjectType << "</td></tr>";
        html << "<tr><td>Subject ID:</td><td>" << ev->Get()->SubjectId << "</td></tr>";
        for (const auto& limit : ev->Get()->Limits) {
            html << "<tr><td>" << limit.first << "</td><td>" << limit.second << "</td></tr>";
        }
        html << "</tbody></table>";

        Send(HttpMonId, new NActors::NMon::TEvHttpInfoRes(html.Str()));
        HttpMonId = NActors::TActorId();
    }

    void Handle(TEvQuotaService::TQuotaGetResponse::TPtr& ev) {

        TStringStream html;
        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Quota Get Response</th>";
        html << "<th></th>";
        html << "</tr></thead><tbody>";
        html << "<tr><td>Subject Type:</td><td>" << ev->Get()->SubjectType << "</td></tr>\n";
        html << "<tr><td>Subject ID:</td><td>" << ev->Get()->SubjectId << "</td></tr>\n";
        html << "</tbody></table>\n";
        html << "<br>";

        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Metric Name</th>";
        html << "<th>Limit Value</th>";
        html << "<th>Updated At</th>";
        html << "<th>Usage</th>";
        html << "</tr></thead><tbody>";
        for (const auto& [metricName, metricUsageInfo] : ev->Get()->Quotas) {
            html << "<tr>";
            html << "<td>" << metricName << "</td>";
            html << "<td>" << metricUsageInfo.Limit.Value << "</td>";
            if (metricUsageInfo.Limit.UpdatedAt) {
                html << "<td>" << metricUsageInfo.Limit.UpdatedAt << "</td>";
            } else {
                html << "<td></td>";
            }
            if (metricUsageInfo.Usage) {
                html << "<td>" << metricUsageInfo.Usage->Value << "</td>";
            } else {
                html << "<td></td>";
            }
            html << "</tr>\n";
        }
        html << "</tbody></table>";

        Send(HttpMonId, new NActors::NMon::TEvHttpInfoRes(html.Str()));
        HttpMonId = NActors::TActorId();
    }

    void Handle(NActors::NMon::TEvHttpInfo::TPtr& ev) {

        const auto& params = ev->Get()->Request.GetParams();

        if (params.Has("submit")) {
            TString subjectId = params.Get("subject_id");
            TString subjectType = params.Get("subject_type");
            if (subjectType.empty()) {
                subjectType = "cloud";
            }
            TString metricName = params.Get("metric_name");
            TString metricValue = params.Get("metric_value");

            auto request = MakeHolder<TEvQuotaService::TQuotaSetRequest>(subjectType, subjectId);
            request->Limits.emplace(metricName, FromStringWithDefault(metricValue, 0));

            Send(SelfId(), request.Release());

            HttpMonId = ev->Sender;
            return;
        } else if (params.Has("get")) {
            TString subjectId = params.Get("subject_id");
            TString subjectType = params.Get("subject_type");
            if (subjectType.empty()) {
                subjectType = "cloud";
            }

            auto request = MakeHolder<TEvQuotaService::TQuotaGetRequest>(subjectType, subjectId);

            Send(SelfId(), request.Release());

            HttpMonId = ev->Sender;
            return;
        }

        TStringStream html;
        html << "<form method='get'>";
        html << "<p>Subject ID (i.e. Cloud ID):<input name='subject_id' type='text'/></p>";
        html << "<p>Subject Type (defaulted to 'cloud'):<input name='subject_type' type='text'/></p>";
        html << "<p>Quota Name:<input name='metric_name' type='text'/></p>";
        html << "<p>New value:<input name='metric_value' type='number'/></p>";
        html << "<button name='get' type='submit'><b>Get</b></button>";
        html << "<button name='submit' type='submit'><b>Change</b></button>";
        html << "</form>";

        // Table with known metrics info
        html << "<br>";
        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Metric Name</th>";
        html << "<th>Subject Type</th>";
        html << "<th>Default Limit</th>";
        html << "<th>Hard Limit</th>";
        html << "</tr></thead><tbody>";
        for (const auto& [subjectType, subjectMetricsInfo] : QuotaInfoMap) {
            for (const auto& [metricName, quotaInfo] : subjectMetricsInfo) {
            html << "<tr>";
            html << "<td>" << metricName << "</td>";
            html << "<td>" << subjectType << "</td>";
            html << "<td>" << quotaInfo.DefaultLimit << "</td>";
            html << "<td>" << quotaInfo.HardLimit << "</td>";
            html << "</tr>\n";
            }
        }
        html << "</tbody></table>";

        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(html.Str()));
    }

    NConfig::TQuotasManagerConfig Config;
    NConfig::TYdbStorageConfig StorageConfig;
    ::NFq::TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredProviderFactory;
    TYdbConnectionPtr YdbConnection;
    NDbPool::TDbPool::TPtr DbPool;
    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
    THashMap<TString /* SubjectType */, THashMap<TString /* MetricName */, TQuotaInfo>> QuotaInfoMap;
    THashMap<TString /* SubjectType */, THashMap<TString /* SubjectId */, TQuotaCache>> QuotaCacheMap;
    TDuration LimitRefreshPeriod;
    TDuration UsageRefreshPeriod;
    std::vector<ui32> NodeIds;
    NActors::TMon* Monitoring;
    NActors::TActorId HttpMonId;
    TString TablePathPrefix;
};

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    const NConfig::TYdbStorageConfig& storageConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    NKikimr::TYdbCredentialsProviderFactory credProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc,
    NActors::TMon* monitoring) {
        return new TQuotaManagementService(config, storageConfig, yqSharedResources, credProviderFactory, counters, quotaDesc, monitoring);
}

} /* NFq */
