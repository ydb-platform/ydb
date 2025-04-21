#pragma once

#include "builder_merge_issues.h"
#include "builder_group_checker.h"
#include "log.h"
#include "health_check.h"
#include "health_check_structs.h"
#include "health_check_utils.h"
#include "self_check_request.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/core/util/tuples.h>

#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

namespace NKikimr::NHealthCheck {

using namespace NSchemeShard;

struct TOverallStateContext {
    Ydb::Monitoring::SelfCheckResult* Result;
    Ydb::Monitoring::StatusFlag::Status Status = Ydb::Monitoring::StatusFlag::GREY;
    bool HasDegraded = false;
    std::unordered_set<std::pair<TString, TString>> IssueIds;

    TOverallStateContext(Ydb::Monitoring::SelfCheckResult* result) {
        Result = result;
    }

    void FillSelfCheckResult() {
        switch (Status) {
        case Ydb::Monitoring::StatusFlag::GREEN:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            break;
        case Ydb::Monitoring::StatusFlag::YELLOW:
            if (HasDegraded) {
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            } else {
                Result->set_self_check_result(Ydb::Monitoring::SelfCheck::GOOD);
            }
            break;
        case Ydb::Monitoring::StatusFlag::BLUE:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::DEGRADED);
            break;
        case Ydb::Monitoring::StatusFlag::ORANGE:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::MAINTENANCE_REQUIRED);
            break;
        case Ydb::Monitoring::StatusFlag::RED:
            Result->set_self_check_result(Ydb::Monitoring::SelfCheck::EMERGENCY);
            break;
        default:
            break;
        }
    }

    void UpdateMaxStatus(Ydb::Monitoring::StatusFlag::Status status) {
        Status = MaxStatus(Status, status);
    }

    void AddIssues(TList<TSelfCheckResult::TIssueRecord>& issueRecords) {
        for (auto& issueRecord : issueRecords) {
            std::pair<TString, TString> key{issueRecord.IssueLog.location().database().name(), issueRecord.IssueLog.id()};
            if (IssueIds.emplace(key).second) {
                Result->mutable_issue_log()->Add()->CopyFrom(issueRecord.IssueLog);
            }
        }
    }
};

template <typename Derived>
class TBuilderBase {
public:
    template <typename... TArgs>
    decltype(auto) Fill(TArgs&&... args) {
        return static_cast<Derived*>(this)->FillImpl(std::forward<TArgs>(args)...);
    }
};

template<typename TContext>
class TBuilderResult : public TBuilderBase<TBuilderResult<TContext>>  {
    TIntrusivePtr<TContext> Builders;

    TString DomainPath;
    TString FilterDatabase;
    bool HaveAllBSControllerInfo;
    THashMap<TString, TDatabaseState>& DatabaseState;
    THashMap<ui64, TStoragePoolState>& StoragePoolState;
    THashSet<ui64>& StoragePoolSeen;
    THashMap<TString, TStoragePoolState>& StoragePoolStateByName;
    THashSet<TString>& StoragePoolSeenByName;

public:
    TBuilderResult(TIntrusivePtr<TContext> builders,
                   const TString& domainPath,
                   const TString& filterDatabase,
                   const bool haveAllBSControllerInfo,
                   THashMap<TString, TDatabaseState>& databaseState,
                   THashMap<ui64, TStoragePoolState>& storagePoolState,
                   THashSet<ui64>& storagePoolSeen,
                   THashMap<TString, TStoragePoolState>& storagePoolStateByName,
                   THashSet<TString>& storagePoolSeenByName)
        : Builders(builders)
        , DomainPath(domainPath)
        , FilterDatabase(filterDatabase)
        , HaveAllBSControllerInfo(haveAllBSControllerInfo)
        , DatabaseState(databaseState)
        , StoragePoolState(storagePoolState)
        , StoragePoolSeen(storagePoolSeen)
        , StoragePoolStateByName(storagePoolStateByName)
        , StoragePoolSeenByName(storagePoolSeenByName)
    {}

    bool IsSpecificDatabaseFilter() const {
        return FilterDatabase && FilterDatabase != DomainPath;
    }

    void FillImpl(TOverallStateContext context) {
        if (IsSpecificDatabaseFilter()) {
            Builders->Database->Fill(context, FilterDatabase, DatabaseState[FilterDatabase]);
        } else {
            for (auto& [path, state] : DatabaseState) {
                Builders->Database->Fill(context, path, state);
            }
        }
        if (DatabaseState.empty()) {
            Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
            TSelfCheckResult tabletContext;
            tabletContext.Location.mutable_database()->set_name(DomainPath);
            databaseStatus.set_name(DomainPath);
            {
                TDatabaseState databaseState;
                Builders->SystemTablets->Fill(databaseState, TSelfCheckResult(&tabletContext, "SYSTEM_TABLET"));
                context.UpdateMaxStatus(tabletContext.GetOverallStatus());
            }
        }
        if (!FilterDatabase) {
            TDatabaseState unknownDatabase;
            bool fillUnknownDatabase = false;
            if (HaveAllBSControllerInfo) {
                for (auto& [id, pool] : StoragePoolState) {
                    if (StoragePoolSeen.count(id) == 0) {
                        unknownDatabase.StoragePools.insert(id);
                    }
                }
                fillUnknownDatabase = !unknownDatabase.StoragePools.empty();
            } else {
                for (auto& [name, pool] : StoragePoolStateByName) {
                    if (StoragePoolSeenByName.count(name) == 0) {
                        unknownDatabase.StoragePoolNames.insert(name);
                    }
                }
                fillUnknownDatabase = !unknownDatabase.StoragePoolNames.empty();
            }
            if (fillUnknownDatabase) {
                Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
                TSelfCheckResult storageContext;
                Builders->Storage->Fill(unknownDatabase, *databaseStatus.mutable_storage(), TSelfCheckResult(&storageContext, "STORAGE"));
                databaseStatus.set_overall(storageContext.GetOverallStatus());
                context.UpdateMaxStatus(storageContext.GetOverallStatus());
                context.AddIssues(storageContext.IssueRecords);
            }
        }
        context.FillSelfCheckResult();
    }
};

template<typename TContext>
class TBuilderDatabase : public TBuilderBase<TBuilderDatabase<TContext>> {
    TIntrusivePtr<TContext> Builders;

    bool IsSpecificDatabaseFilter;

public:
    TBuilderDatabase(TIntrusivePtr<TContext> builders, const bool isSpecificDatabaseFilter)
        : Builders(builders)
        , IsSpecificDatabaseFilter(isSpecificDatabaseFilter)
    {}

    void FillImpl(TOverallStateContext& context, const TString& path, TDatabaseState& state) {
        Ydb::Monitoring::DatabaseStatus& databaseStatus(*context.Result->add_database_status());
        TSelfCheckResult dbContext("DATABASE");
        if (!IsSpecificDatabaseFilter) {
            dbContext.Location.mutable_database()->set_name(path);
        }
        databaseStatus.set_name(path);
        Builders->Compute->Fill(state, *databaseStatus.mutable_compute(), TSelfCheckResult(&dbContext, "COMPUTE"));
        Builders->Storage->Fill(state, *databaseStatus.mutable_storage(), TSelfCheckResult(&dbContext, "STORAGE"));
        if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN
                && databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(MaxStatus(databaseStatus.compute().overall(), databaseStatus.storage().overall()),
                "Database has multiple issues", ETags::DBState, { ETags::ComputeState, ETags::StorageState});
        } else if (databaseStatus.compute().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(databaseStatus.compute().overall(), "Database has compute issues", ETags::DBState, {ETags::ComputeState});
        } else if (databaseStatus.storage().overall() != Ydb::Monitoring::StatusFlag::GREEN) {
            dbContext.ReportStatus(databaseStatus.storage().overall(), "Database has storage issues", ETags::DBState, {ETags::StorageState});
        }
        databaseStatus.set_overall(dbContext.GetOverallStatus());
        context.UpdateMaxStatus(dbContext.GetOverallStatus());
        context.AddIssues(dbContext.IssueRecords);
        if (!context.HasDegraded && context.Status != Ydb::Monitoring::StatusFlag::GREEN && dbContext.HasTags({ETags::StorageState})) {
            context.HasDegraded = true;
        }
    }
};

template<typename TContext>
class TBuilderCompute : public TBuilderBase<TBuilderCompute<TContext>> {
    TIntrusivePtr<TContext> Builders;

    const THashMap<TSubDomainKey, TString>& FilterDomainKey;
    const THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*>& MergedNodeSystemState;
    THashMap<TString, TDatabaseState>& DatabaseState;
    const bool ReturnHints;
    THashMap<TString, THintOverloadedShard> OverloadedShardHints;

public:
    TBuilderCompute(TIntrusivePtr<TContext> builders,
                    const THashMap<TSubDomainKey, TString>& filterDomainKey,
                    const THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*>& mergedNodeSystemState,
                    THashMap<TString, TDatabaseState>& databaseState,
                    const bool returnHints,
                    THashMap<TString, THintOverloadedShard>& overloadedShardHints)
        : Builders(builders)
        , FilterDomainKey(filterDomainKey)
        , MergedNodeSystemState(mergedNodeSystemState)
        , DatabaseState(databaseState)
        , ReturnHints(returnHints)
        , OverloadedShardHints(overloadedShardHints)
    {}

    void FillImpl(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckResult context) {
        TVector<TNodeId>* computeNodeIds = &databaseState.ComputeNodeIds;
        if (databaseState.ResourcePathId
            && databaseState.ServerlessComputeResourcesMode != NKikimrSubDomains::EServerlessComputeResourcesModeExclusive)
        {
            auto itDatabase = FilterDomainKey.find(TSubDomainKey(databaseState.ResourcePathId.OwnerId, databaseState.ResourcePathId.LocalPathId));
            if (itDatabase != FilterDomainKey.end()) {
                const TString& sharedDatabaseName = itDatabase->second;
                TDatabaseState& sharedDatabase = DatabaseState[sharedDatabaseName];
                computeNodeIds = &sharedDatabase.ComputeNodeIds;
            }
        }
        std::sort(computeNodeIds->begin(), computeNodeIds->end());
        computeNodeIds->erase(std::unique(computeNodeIds->begin(), computeNodeIds->end()), computeNodeIds->end());
        if (computeNodeIds->empty()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no compute nodes", ETags::ComputeState);
        } else {
            long maxTimeDifferenceUs = 0;
            for (TNodeId nodeId : *computeNodeIds) {
                auto itNodeSystemState = MergedNodeSystemState.find(nodeId);
                if (itNodeSystemState != MergedNodeSystemState.end()) {
                    if (std::count(computeNodeIds->begin(), computeNodeIds->end(), itNodeSystemState->second->GetMaxClockSkewPeerId()) > 0
                            && abs(itNodeSystemState->second->GetMaxClockSkewWithPeerUs()) > maxTimeDifferenceUs) {
                        maxTimeDifferenceUs = abs(itNodeSystemState->second->GetMaxClockSkewWithPeerUs());
                        databaseState.MaxTimeDifferenceNodeId = nodeId;
                    }
                }
            }
            for (TNodeId nodeId : *computeNodeIds) {
                auto& computeNode = *computeStatus.add_nodes();
                Builders->ComputeNode->Fill(databaseState, nodeId, computeNode, TSelfCheckResult(&context, "COMPUTE_NODE"));
            }
        }
        Ydb::Monitoring::StatusFlag::Status systemStatus = Builders->SystemTablets->Fill(databaseState, TSelfCheckResult(&context, "SYSTEM_TABLET"));
        if (systemStatus != Ydb::Monitoring::StatusFlag::GREEN && systemStatus != Ydb::Monitoring::StatusFlag::GREY) {
            context.ReportStatus(systemStatus, "Compute has issues with system tablets", ETags::ComputeState, {ETags::SystemTabletState});
        }
        Builders->ComputeDatabase->Fill(databaseState, computeStatus, TSelfCheckResult(&context, "COMPUTE_QUOTA"));
        context.ReportWithMaxChildStatus("Some nodes are restarting too often", ETags::ComputeState, {ETags::Uptime});
        context.ReportWithMaxChildStatus("Compute is overloaded", ETags::ComputeState, {ETags::OverloadState});
        context.ReportWithMaxChildStatus("Compute quota usage", ETags::ComputeState, {ETags::QuotaUsage});
        context.ReportWithMaxChildStatus("Database has time difference between nodes", ETags::ComputeState, {ETags::SyncState});
        Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
        computeNodeIds->push_back(0); // for tablets without node
        for (TNodeId nodeId : *computeNodeIds) {
            tabletsStatus = MaxStatus(tabletsStatus, Builders->Tablets->Fill(databaseState, nodeId, *computeStatus.mutable_tablets(), context));
        }
        if (tabletsStatus != Ydb::Monitoring::StatusFlag::GREEN) {
            context.ReportStatus(tabletsStatus, "Compute has issues with tablets", ETags::ComputeState, {ETags::TabletState});
        }
        if (ReturnHints) {
            auto schemeShardId = databaseState.SchemeShardId;
            if (schemeShardId) {
                for (const auto& [path, hint] : OverloadedShardHints) {
                    if (hint.SchemeShardId == schemeShardId) {
                        TSelfCheckResult hintContext(&context, "HINT-OVERLOADED-SHARD");
                        //hintContext.Location.mutable_compute()->mutable_tablet()->set_type(NKikimrTabletBase::TTabletTypes::EType_Name(NKikimrTabletBase::TTabletTypes::DataShard));
                        TStringBuilder tabletId;
                        tabletId << hint.TabletId;
                        if (hint.FollowerId) {
                            tabletId << '-' << hint.FollowerId;
                        }
                        hintContext.Location.mutable_compute()->mutable_tablet()->add_id(tabletId);
                        hintContext.Location.mutable_compute()->mutable_schema()->set_type("table");
                        hintContext.Location.mutable_compute()->mutable_schema()->set_path(path);
                        hintContext.ReportStatus(Ydb::Monitoring::StatusFlag::UNSPECIFIED, hint.Message);
                    }
                }
            }
        }
        computeStatus.set_overall(context.GetOverallStatus());
    }

};

class TBuilderComputeDatabase : public TBuilderBase<TBuilderComputeDatabase> {
    const THashMap<TString, TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult>>& DescribeByPath;

public:
    TBuilderComputeDatabase(const THashMap<TString, TRequestResponse<TEvSchemeShard::TEvDescribeSchemeResult>>& describeByPath)
        : DescribeByPath(describeByPath)
    {}

    void FillImpl(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckResult context) {
        auto itDescribe = DescribeByPath.find(databaseState.Path);
        if (itDescribe != DescribeByPath.end() && itDescribe->second.IsOk()) {
            const auto& domain(itDescribe->second->GetRecord().GetPathDescription().GetDomainDescription());
            if (domain.GetPathsLimit() > 0) {
                float usage = (float)domain.GetPathsInside() / domain.GetPathsLimit();
                computeStatus.set_paths_quota_usage(usage);
                if (static_cast<i64>(domain.GetPathsLimit()) - static_cast<i64>(domain.GetPathsInside()) <= 1) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Paths quota exhausted", ETags::QuotaUsage);
                } else if (usage >= 0.99) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Paths quota usage is over 99%", ETags::QuotaUsage);
                } else if (usage >= 0.90) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Paths quota usage is over 90%", ETags::QuotaUsage);
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
            }
            if (domain.GetShardsLimit() > 0) {
                float usage = (float)domain.GetShardsInside() / domain.GetShardsLimit();
                computeStatus.set_shards_quota_usage(usage);
                if (static_cast<i64>(domain.GetShardsLimit()) - static_cast<i64>(domain.GetShardsInside()) <= 1) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Shards quota exhausted", ETags::QuotaUsage);
                } else if (usage >= 0.99) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Shards quota usage is over 99%", ETags::QuotaUsage);
                } else if (usage >= 0.90) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Shards quota usage is over 90%", ETags::QuotaUsage);
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
            }
        }
    }
};

template<typename TContext>
class TBuilderComputeNode : public TBuilderBase<TBuilderComputeNode<TContext>> {
   TIntrusivePtr<TContext> Builders;

    const NKikimrConfig::THealthCheckConfig& HealthCheckConfig;

    const THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*>& MergedNodeSystemState;

public:
    TBuilderComputeNode(TIntrusivePtr<TContext> builders,
                        const NKikimrConfig::THealthCheckConfig& healthCheckConfig,
                        const THashMap<TNodeId, const NKikimrWhiteboard::TSystemStateInfo*>& mergedNodeSystemState)
        : Builders(builders)
        , HealthCheckConfig(healthCheckConfig)
        , MergedNodeSystemState(mergedNodeSystemState)
    {}

    static void Check(TSelfCheckResult& context, const NKikimrWhiteboard::TSystemStateInfo::TPoolStats& poolStats) {
        if (poolStats.name() == "System" || poolStats.name() == "IC" || poolStats.name() == "IO") {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Pool usage is over 99%", ETags::OverloadState);
            } else if (poolStats.usage() >= 0.95) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over 95%", ETags::OverloadState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        } else {
            if (poolStats.usage() >= 0.99) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Pool usage is over 99%", ETags::OverloadState);
            } else {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
        }
    }

    void FillImpl(TDatabaseState& databaseState, TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckResult context) {
        Builders->Node->Fill(nodeId, context.Location.mutable_compute()->mutable_node());

        TSelfCheckResult rrContext(&context, "NODE_UPTIME");
        if (databaseState.NodeRestartsPerPeriod[nodeId] >= HealthCheckConfig.GetThresholds().GetNodeRestartsOrange()) {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Node is restarting too often", ETags::Uptime);
        } else if (databaseState.NodeRestartsPerPeriod[nodeId] >= HealthCheckConfig.GetThresholds().GetNodeRestartsYellow()) {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "The number of node restarts has increased", ETags::Uptime);
        } else {
            rrContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
        }

        auto itNodeSystemState = MergedNodeSystemState.find(nodeId);
        if (itNodeSystemState != MergedNodeSystemState.end()) {
            const NKikimrWhiteboard::TSystemStateInfo& nodeSystemState(*itNodeSystemState->second);

            for (const auto& poolStat : nodeSystemState.poolstats()) {
                TSelfCheckResult poolContext(&context, "COMPUTE_POOL");
                poolContext.Location.mutable_compute()->mutable_pool()->set_name(poolStat.name());
                Check(poolContext, poolStat);
                Ydb::Monitoring::ThreadPoolStatus& threadPoolStatus = *computeNodeStatus.add_pools();
                threadPoolStatus.set_name(poolStat.name());
                threadPoolStatus.set_usage(poolStat.usage());
                threadPoolStatus.set_overall(poolContext.GetOverallStatus());
            }

            if (nodeSystemState.loadaverage_size() > 0 && nodeSystemState.numberofcpus() > 0) {
                TSelfCheckResult laContext(&context, "LOAD_AVERAGE");
                Ydb::Monitoring::LoadAverageStatus& loadAverageStatus = *computeNodeStatus.mutable_load();
                loadAverageStatus.set_load(nodeSystemState.loadaverage(0));
                loadAverageStatus.set_cores(nodeSystemState.numberofcpus());
                if (loadAverageStatus.load() > loadAverageStatus.cores()) {
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "LoadAverage above 100%", ETags::OverloadState);
                } else {
                    laContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                }
                loadAverageStatus.set_overall(laContext.GetOverallStatus());
            }

            if (nodeSystemState.HasMaxClockSkewPeerId()) {
                TNodeId peerId = nodeSystemState.GetMaxClockSkewPeerId();
                long timeDifferenceUs = nodeSystemState.GetMaxClockSkewWithPeerUs();
                TDuration timeDifferenceDuration = TDuration::MicroSeconds(abs(timeDifferenceUs));
                Ydb::Monitoring::StatusFlag::Status status;
                if (timeDifferenceDuration > TDuration::MicroSeconds(HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceOrange())) {
                    status = Ydb::Monitoring::StatusFlag::ORANGE;
                } else if (timeDifferenceDuration > TDuration::MicroSeconds(HealthCheckConfig.GetThresholds().GetNodesTimeDifferenceYellow())) {
                    status = Ydb::Monitoring::StatusFlag::YELLOW;
                } else {
                    status = Ydb::Monitoring::StatusFlag::GREEN;
                }

                if (databaseState.MaxTimeDifferenceNodeId == nodeId) {
                    TSelfCheckResult tdContext(&context, "NODES_TIME_DIFFERENCE");
                    if (status == Ydb::Monitoring::StatusFlag::GREEN) {
                        tdContext.ReportStatus(status);
                    } else {
                        tdContext.ReportStatus(status, TStringBuilder() << "Node is  "
                                                                        << timeDifferenceDuration.MilliSeconds() << " ms "
                                                                        << (timeDifferenceUs > 0 ? "behind " : "ahead of ")
                                                                        << "peer [" << peerId << "]", ETags::SyncState);
                    }
                }
            }
        } else {
            // context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
            //                      TStringBuilder() << "Compute node is not available",
                //                      ETags::NodeState);
        }
        computeNodeStatus.set_id(ToString(nodeId));
        computeNodeStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderTablets : public TBuilderBase<TBuilderTablets<TContext>> {
   TIntrusivePtr<TContext> Builders;

public:
    TBuilderTablets(TIntrusivePtr<TContext> builders)
        : Builders(builders)
    {}

    Ydb::Monitoring::StatusFlag::Status FillImpl(TDatabaseState& databaseState,
                                                    TNodeId nodeId,
                                                    google::protobuf::RepeatedPtrField<Ydb::Monitoring::ComputeTabletStatus>& parent,
                                                    TSelfCheckResult& context) {
        Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
        auto itNodeTabletState = databaseState.MergedNodeTabletState.find(nodeId);
        if (itNodeTabletState != databaseState.MergedNodeTabletState.end()) {
            TSelfCheckResult tabletsContext(&context);
            for (const auto& count : itNodeTabletState->second.Count) {
                if (count.Count > 0) {
                    TSelfCheckResult tabletContext(&tabletsContext, "TABLET");
                    auto& protoTablet = *tabletContext.Location.mutable_compute()->mutable_tablet();
                    Builders->Node->Fill(nodeId, tabletContext.Location.mutable_node());
                    protoTablet.set_type(TTabletTypes::EType_Name(count.Type));
                    protoTablet.set_count(count.Count);
                    if (!count.Identifiers.empty()) {
                        for (const TString& id : count.Identifiers) {
                            protoTablet.add_id(id);
                        }
                    }
                    Ydb::Monitoring::ComputeTabletStatus& computeTabletStatus = *parent.Add();
                    computeTabletStatus.set_type(NKikimrTabletBase::TTabletTypes::EType_Name(count.Type));
                    computeTabletStatus.set_count(count.Count);
                    for (const TString& id : count.Identifiers) {
                        computeTabletStatus.add_id(id);
                    }
                    switch (count.State) {
                        case TNodeTabletState::ETabletState::Good:
                            computeTabletStatus.set_state("GOOD");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                            break;
                        case TNodeTabletState::ETabletState::Stopped:
                            computeTabletStatus.set_state("STOPPED");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                            break;
                        case TNodeTabletState::ETabletState::RestartsTooOften:
                            computeTabletStatus.set_state("RESTARTS_TOO_OFTEN");
                            tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Tablets are restarting too often", ETags::TabletState);
                            break;
                        case TNodeTabletState::ETabletState::Dead:
                            computeTabletStatus.set_state("DEAD");
                            if (count.Leader) {
                                tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Tablets are dead", ETags::TabletState);
                            } else {
                                tabletContext.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Followers are dead", ETags::TabletState);
                            }
                            break;
                    }
                    computeTabletStatus.set_overall(tabletContext.GetOverallStatus());
                    tabletsStatus = MaxStatus(tabletsStatus, tabletContext.GetOverallStatus());
                }
            }
        }
        return tabletsStatus;
    }

};

template<typename TContext>
class TBuilderSystemTablets : public TBuilderBase<TBuilderSystemTablets<TContext>> {
   TIntrusivePtr<TContext> Builders;
    TTabletRequestsState& TabletRequests;
    ui32 timeoutMs;

public:
    TBuilderSystemTablets(TIntrusivePtr<TContext> builders, TTabletRequestsState& tabletRequests, ui32 timeoutMs)
        : Builders(builders)
        , TabletRequests(tabletRequests)
        , timeoutMs(timeoutMs)
    {}

    Ydb::Monitoring::StatusFlag::Status FillImpl(TDatabaseState& databaseState, TSelfCheckResult context) {
        TString databaseId = context.Location.database().name();
        for (auto& [tabletId, tablet] : TabletRequests.TabletStates) {
            if (tablet.Database == databaseId) {
                auto tabletIt = databaseState.MergedTabletState.find(std::make_pair(tabletId, 0));
                if (tabletIt != databaseState.MergedTabletState.end()) {
                    auto nodeId = tabletIt->second->GetNodeID();
                    if (nodeId) {
                        Builders->Node->Fill(nodeId, context.Location.mutable_node());
                    }
                }

                context.Location.mutable_compute()->clear_tablet();
                auto& protoTablet = *context.Location.mutable_compute()->mutable_tablet();
                auto orangeTimeout = timeoutMs / 2;
                auto yellowTimeout = timeoutMs / 10;
                if (tablet.IsUnresponsive || tablet.MaxResponseTime >= TDuration::MilliSeconds(yellowTimeout)) {
                    if (tablet.Type != TTabletTypes::Unknown) {
                        protoTablet.set_type(TTabletTypes::EType_Name(tablet.Type));
                    }
                    protoTablet.add_id(ToString(tabletId));
                    if (tablet.IsUnresponsive) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet is unresponsive", ETags::SystemTabletState);
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(orangeTimeout)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, TStringBuilder() << "System tablet response time is over " << orangeTimeout << "ms", ETags::SystemTabletState);
                    } else if (tablet.MaxResponseTime >= TDuration::MilliSeconds(yellowTimeout)) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, TStringBuilder() << "System tablet response time is over " << yellowTimeout << "ms", ETags::SystemTabletState);
                    }
                }
            }
        }
        return context.GetOverallStatus();
    }
};

template<typename TContext>
class TBuilderStorage : public TBuilderBase<TBuilderStorage<TContext>> {
public:
   TIntrusivePtr<TContext> Builders;

    bool HaveAllBSControllerInfo;
    THashMap<ui64, TStoragePoolState>& StoragePoolState;
    THashSet<ui64>& StoragePoolSeen;
    THashMap<TString, TStoragePoolState>& StoragePoolStateByName;
    THashSet<TString>& StoragePoolSeenByName;

    TBuilderStorage(TIntrusivePtr<TContext> builders,
                    bool haveAllBSControllerInfo,
                    THashMap<ui64, TStoragePoolState>& storagePoolState,
                    THashSet<ui64>& storagePoolSeen,
                    THashMap<TString, TStoragePoolState>& storagePoolStateByName,
                    THashSet<TString>& storagePoolSeenByName)
        : Builders(builders)
        , HaveAllBSControllerInfo(haveAllBSControllerInfo)
        , StoragePoolState(storagePoolState)
        , StoragePoolSeen(storagePoolSeen)
        , StoragePoolStateByName(storagePoolStateByName)
        , StoragePoolSeenByName(storagePoolSeenByName)
    {}

    void FillImpl(TDatabaseState& databaseState, Ydb::Monitoring::StorageStatus& storageStatus, TSelfCheckResult context) {
        if (HaveAllBSControllerInfo && databaseState.StoragePools.empty()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "There are no storage pools", ETags::StorageState);
        } else {
            if (HaveAllBSControllerInfo) {
                for (const ui64 poolId : databaseState.StoragePools) {
                    auto itStoragePoolState = StoragePoolState.find(poolId);
                    if (itStoragePoolState != StoragePoolState.end() && itStoragePoolState->second.Groups) {
                        Builders->StoragePool->Fill(itStoragePoolState->second, *storageStatus.add_pools(), TSelfCheckResult(&context, "STORAGE_POOL"));
                        StoragePoolSeen.emplace(poolId);
                    }
                }
            } else {
                for (const TString& poolName : databaseState.StoragePoolNames) {
                    auto itStoragePoolState = StoragePoolStateByName.find(poolName);
                    if (itStoragePoolState != StoragePoolStateByName.end()) {
                        Builders->StoragePool->Fill(itStoragePoolState->second, *storageStatus.add_pools(), TSelfCheckResult(&context, "STORAGE_POOL"));
                        StoragePoolSeenByName.emplace(poolName);
                    }
                }
            }
            switch (context.GetOverallStatus()) {
                case Ydb::Monitoring::StatusFlag::BLUE:
                case Ydb::Monitoring::StatusFlag::YELLOW:
                    context.ReportStatus(context.GetOverallStatus(), "Storage degraded", ETags::StorageState, {ETags::PoolState});
                    break;
                case Ydb::Monitoring::StatusFlag::ORANGE:
                    context.ReportStatus(context.GetOverallStatus(), "Storage has no redundancy", ETags::StorageState, {ETags::PoolState});
                    break;
                case Ydb::Monitoring::StatusFlag::RED:
                    context.ReportStatus(context.GetOverallStatus(), "Storage failed", ETags::StorageState, {ETags::PoolState});
                    break;
                default:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
            }
            if (!HaveAllBSControllerInfo) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide information", ETags::StorageState);
            }
        }
        if (databaseState.StorageQuota > 0) {
            auto usage = (float)databaseState.StorageUsage / databaseState.StorageQuota;
            if (usage > 0.9) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Storage usage over 90%", ETags::StorageState);
            } else if (usage > 0.85) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Storage usage over 85%", ETags::StorageState);
            } else if (usage > 0.75) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Storage usage over 75%", ETags::StorageState);
            }
        }
        storageStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderStoragePool : public TBuilderBase<TBuilderStoragePool<TContext>> {
   TIntrusivePtr<TContext> Builders;

    bool HaveAllBSControllerInfo;
    THashSet<TNodeId>& UnknownStaticGroups;
    std::unordered_map<TGroupId, const NKikimrWhiteboard::TBSGroupStateInfo*>& MergedBSGroupState;
    bool NeedMergeRecords; // Request->Request.merge_records()

public:
    TBuilderStoragePool(TIntrusivePtr<TContext> builders, bool haveAllBSControllerInfo,
                        THashSet<TNodeId>& unknownStaticGroups,
                        std::unordered_map<TGroupId, const NKikimrWhiteboard::TBSGroupStateInfo*>& mergedBSGroupState,
                        bool needMergeRecords)
        : Builders(builders)
        , HaveAllBSControllerInfo(haveAllBSControllerInfo)
        , UnknownStaticGroups(unknownStaticGroups)
        , MergedBSGroupState(mergedBSGroupState)
        , NeedMergeRecords(needMergeRecords)
    {}

    void MergeRecords(TList<TSelfCheckResult::TIssueRecord>& records) {
        TMergeIssuesContext mergeContext(records);
        if (NeedMergeRecords) {
            mergeContext.MergeLevelRecords(ETags::GroupState);
            mergeContext.MergeLevelRecords(ETags::VDiskState, ETags::GroupState);
            mergeContext.MergeLevelRecords(ETags::PDiskState, ETags::VDiskState);
        }
        mergeContext.FillRecords(records);
    }

    void FillImpl(const TStoragePoolState& pool, Ydb::Monitoring::StoragePoolStatus& storagePoolStatus, TSelfCheckResult context) {
        context.Location.mutable_storage()->mutable_pool()->set_name(pool.Name);
        storagePoolStatus.set_id(pool.Name);
        for (auto groupId : pool.Groups) {
            if (HaveAllBSControllerInfo && !UnknownStaticGroups.contains(groupId)) {
                Builders->StorageGroup->Fill(groupId, *storagePoolStatus.add_groups(), TSelfCheckResult(&context, "STORAGE_GROUP"));
            } else if (IsStaticGroup(groupId)) {
                auto itGroup = MergedBSGroupState.find(groupId);
                if (itGroup != MergedBSGroupState.end()) {
                    Builders->StorageGroupWithWhiteboard->Fill(groupId, *itGroup->second, *storagePoolStatus.add_groups(), TSelfCheckResult(&context, "STORAGE_GROUP"));
                }
            }
        }

        MergeRecords(context.IssueRecords);

        switch (context.GetOverallStatus()) {
            case Ydb::Monitoring::StatusFlag::BLUE:
            case Ydb::Monitoring::StatusFlag::YELLOW:
                context.ReportStatus(context.GetOverallStatus(), "Pool degraded", ETags::PoolState, {ETags::GroupState});
                break;
            case Ydb::Monitoring::StatusFlag::ORANGE:
                context.ReportStatus(context.GetOverallStatus(), "Pool has no redundancy", ETags::PoolState, {ETags::GroupState});
                break;
            case Ydb::Monitoring::StatusFlag::RED:
                context.ReportStatus(context.GetOverallStatus(), "Pool failed", ETags::PoolState, {ETags::GroupState});
                break;
            default:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
        }
        storagePoolStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderStorageGroup : public TBuilderBase<TBuilderStorageGroup<TContext>> {
    TIntrusivePtr<TContext> Builders;

    THashMap<ui32, TGroupState>& GroupState;
    std::unordered_map<TString, Ydb::Monitoring::StatusFlag::Status>& VDiskStatuses;

public:
    TBuilderStorageGroup(TIntrusivePtr<TContext> builders, THashMap<ui32, TGroupState>& groupState,
                         std::unordered_map<TString, Ydb::Monitoring::StatusFlag::Status>& vDiskStatuses)
        : Builders(builders)
        , GroupState(groupState)
        , VDiskStatuses(vDiskStatuses)
    {}

    void FillImpl(TGroupId groupId, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckResult context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id(ToString(groupId));
        storageGroupStatus.set_id(ToString(groupId));

        auto itGroup = GroupState.find(groupId);
        if (itGroup == GroupState.end()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "Group has no vslots", ETags::GroupState);
            storageGroupStatus.set_overall(context.GetOverallStatus());
            return;
        }

        TGroupChecker checker(itGroup->second.ErasureSpecies, itGroup->second.LayoutCorrect);
        const auto& slots = itGroup->second.VSlots;
        for (const auto* slot : slots) {
            const auto& slotInfo = slot->GetInfo();
            auto slotId = GetVSlotId(slot->GetKey());
            auto [statusIt, inserted] = VDiskStatuses.emplace(slotId, Ydb::Monitoring::StatusFlag::UNSPECIFIED);
            if (inserted) {
                Ydb::Monitoring::StorageVDiskStatus& vDiskStatus = *storageGroupStatus.add_vdisks();
                Builders->VDisk->Fill(slot, vDiskStatus, TSelfCheckResult(&context, "VDISK"));
                statusIt->second = vDiskStatus.overall();
            }
            checker.AddVDiskStatus(statusIt->second, slotInfo.GetFailRealm());
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);
        checker.ReportStatus(context);

        storageGroupStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderStorageGroupWithWhiteboard : public TBuilderBase<TBuilderStorageGroupWithWhiteboard<TContext>> {
    TIntrusivePtr<TContext> Builders;
    std::unordered_map<TString, const NKikimrWhiteboard::TVDiskStateInfo*>& MergedVDiskState;
    const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& MergedNodeInfo;

public:
    TBuilderStorageGroupWithWhiteboard(TIntrusivePtr<TContext> builders,
                                       std::unordered_map<TString, const NKikimrWhiteboard::TVDiskStateInfo*>& mergedVDiskState,
                                       const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& mergedNodeInfo)
        : Builders(builders)
        , MergedVDiskState(mergedVDiskState)
        , MergedNodeInfo(mergedNodeInfo)
    {}

    void FillImpl(TGroupId groupId, const NKikimrWhiteboard::TBSGroupStateInfo& groupInfo, Ydb::Monitoring::StorageGroupStatus& storageGroupStatus, TSelfCheckResult context) {
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_id()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->add_id();
        }
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->set_id(0, ToString(groupId));
        storageGroupStatus.set_id(ToString(groupId));
        TGroupChecker checker(groupInfo.erasurespecies());
        for (const auto& protoVDiskId : groupInfo.vdiskids()) {
            TString vDiskId = GetVDiskId(protoVDiskId);
            auto itVDisk = MergedVDiskState.find(vDiskId);
            const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
            if (itVDisk != MergedVDiskState.end()) {
                TNodeId nodeId = itVDisk->second->nodeid();
                auto itNodeInfo = MergedNodeInfo.find(nodeId);
                if (itNodeInfo != MergedNodeInfo.end()) {
                    nodeInfo = itNodeInfo->second;
                }
                context.Location.mutable_storage()->mutable_node()->set_id(nodeId);
            } else {
                context.Location.mutable_storage()->mutable_node()->clear_id();
            }
            if (nodeInfo) {
                context.Location.mutable_storage()->mutable_node()->set_host(nodeInfo->Host);
                context.Location.mutable_storage()->mutable_node()->set_port(nodeInfo->Port);
            } else {
                context.Location.mutable_storage()->mutable_node()->clear_host();
                context.Location.mutable_storage()->mutable_node()->clear_port();
            }
            Ydb::Monitoring::StorageVDiskStatus& vDiskStatus = *storageGroupStatus.add_vdisks();
            Builders->VDiskWithWhiteboard->Fill(vDiskId, itVDisk != MergedVDiskState.end() ? *itVDisk->second : NKikimrWhiteboard::TVDiskStateInfo(), vDiskStatus, TSelfCheckResult(&context, "VDISK"));
            checker.AddVDiskStatus(vDiskStatus.overall(), protoVDiskId.ring());
        }

        context.Location.mutable_storage()->clear_node(); // group doesn't have node
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::YELLOW);
        checker.ReportStatus(context);

        BLOG_D("Group " << groupId << " has status " << context.GetOverallStatus());
        storageGroupStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderVDisk : public TBuilderBase<TBuilderVDisk<TContext>> {
    TIntrusivePtr<TContext> Builders;

    const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& MergedNodeInfo;

public:
    TBuilderVDisk(TIntrusivePtr<TContext> builders,
                         const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& mergedNodeInfo)
        : Builders(builders)
        , MergedNodeInfo(mergedNodeInfo)
    {}

    void FillImpl(const NKikimrSysView::TVSlotEntry* vSlot, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckResult context) {
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // you can see VDisks Group Id in vSlotId field

        TNodeId nodeId = vSlot->GetKey().GetNodeId();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_id(GetVDiskId(vSlot->GetInfo()));
        context.Location.mutable_storage()->mutable_node()->set_id(nodeId);

        auto itNodeInfo = MergedNodeInfo.find(nodeId);
        if (itNodeInfo != MergedNodeInfo.end()) {
            context.Location.mutable_storage()->mutable_node()->set_host(itNodeInfo->second->Host);
            context.Location.mutable_storage()->mutable_node()->set_port(itNodeInfo->second->Port);
        } else {
            context.Location.mutable_storage()->mutable_node()->clear_host();
            context.Location.mutable_storage()->mutable_node()->clear_port();
        }

        storageVDiskStatus.set_id(GetVSlotId(vSlot->GetKey()));

        const auto& vSlotInfo = vSlot->GetInfo();

        if (!vSlotInfo.HasStatusV2()) {
            // this should mean that BSC recently restarted and does not have accurate data yet - we should not report to avoid false positives
            context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        const auto *descriptor = NKikimrBlobStorage::EVDiskStatus_descriptor();
        auto status = descriptor->FindValueByName(vSlot->GetInfo().GetStatusV2());
        if (!status) { // this case is not expected because becouse bsc assignes status according EVDiskStatus enum
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide known status", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vSlot->GetKey().HasPDiskId()) {
            TString pDiskId = GetPDiskId(vSlot->GetKey());
            Builders->PDisk->Fill(pDiskId, *storageVDiskStatus.mutable_pdisk(), TSelfCheckResult(&context, "PDISK"));
        }

        if (status->number() == NKikimrBlobStorage::ERROR) {
            // the disk is not operational at all
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "VDisk is not available", ETags::VDiskState,{ETags::PDiskState});
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vSlotInfo.HasIsThrottling() && vSlotInfo.GetIsThrottling()) {
            // throttling is active
            auto message = TStringBuilder() << "VDisk is being throttled, rate "
                << vSlotInfo.GetThrottlingRate() << " per mille";
            context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, message, ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        switch (status->number()) {
            case NKikimrBlobStorage::REPLICATING: { // the disk accepts queries, but not all the data was replicated
                context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, TStringBuilder() << "Replication in progress", ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            }
            case NKikimrBlobStorage::INIT_PENDING:
            case NKikimrBlobStorage::READY: { // the disk is fully operational and does not affect group fault tolerance
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
            }
            default:
                break;
        }

        context.ReportWithMaxChildStatus("VDisk have space issue",
                            ETags::VDiskState,
                            {ETags::PDiskSpace});

        storageVDiskStatus.set_overall(context.GetOverallStatus());
    }
};

template<typename TContext>
class TBuilderVDiskWithWhiteboard : public TBuilderBase<TBuilderVDiskWithWhiteboard<TContext>> {
    TIntrusivePtr<TContext> Builders;

    std::unordered_map<TString, const NKikimrWhiteboard::TPDiskStateInfo*>& MergedPDiskState;

public:
    TBuilderVDiskWithWhiteboard(TIntrusivePtr<TContext> builders,
                                       std::unordered_map<TString, const NKikimrWhiteboard::TPDiskStateInfo*>& mergedPDiskState)
        : Builders(builders)
        , MergedPDiskState(mergedPDiskState)
    {}

    void FillImpl(const TString& vDiskId, const NKikimrWhiteboard::TVDiskStateInfo& vDiskInfo, Ydb::Monitoring::StorageVDiskStatus& storageVDiskStatus, TSelfCheckResult context) {
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_id();
        }
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->set_id(0, vDiskId);
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // you can see VDisks Group Id in vDiskId field
        storageVDiskStatus.set_id(vDiskId);
        TString pDiskId = GetPDiskId(vDiskInfo);
        auto itPDisk = MergedPDiskState.find(pDiskId);
        if (itPDisk != MergedPDiskState.end()) {
            Builders->PDiskWithWhiteboard->Fill(pDiskId, *itPDisk->second, *storageVDiskStatus.mutable_pdisk(), TSelfCheckResult(&context, "PDISK"));
        }

        if (!vDiskInfo.HasVDiskState()) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                    TStringBuilder() << "VDisk is not available",
                                    ETags::VDiskState,
                                    {ETags::PDiskState});
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        switch (vDiskInfo.GetVDiskState()) {
            case NKikimrWhiteboard::EVDiskState::OK:
            case NKikimrWhiteboard::EVDiskState::Initial:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecovery:
                context.IssueRecords.clear();
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                        TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                        ETags::VDiskState);
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
            case NKikimrWhiteboard::EVDiskState::LocalRecoveryError:
            case NKikimrWhiteboard::EVDiskState::SyncGuidRecoveryError:
            case NKikimrWhiteboard::EVDiskState::PDiskError:
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                        TStringBuilder() << "VDisk state is " << NKikimrWhiteboard::EVDiskState_Name(vDiskInfo.GetVDiskState()),
                                        ETags::VDiskState,
                                        {ETags::PDiskState});
                storageVDiskStatus.set_overall(context.GetOverallStatus());
                return;
        }

        if (!vDiskInfo.GetReplicated()) {
            context.IssueRecords.clear();
            context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Replication in progress", ETags::VDiskState);
            storageVDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        if (vDiskInfo.HasDiskSpace()) {
            switch(vDiskInfo.GetDiskSpace()) {
                case NKikimrWhiteboard::EFlag::Green:
                    if (context.IssueRecords.size() == 0) {
                        context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    } else {
                        context.ReportStatus(context.IssueRecords.begin()->IssueLog.status(),
                                            TStringBuilder() << "VDisk is degraded",
                                            ETags::VDiskState,
                                            {ETags::PDiskSpace});
                    }
                    break;
                case NKikimrWhiteboard::EFlag::Red:
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                            TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                            ETags::VDiskState,
                                            {ETags::PDiskSpace});
                    break;
                default:
                    context.ReportStatus(GetFlagFromWhiteboardFlag(vDiskInfo.GetDiskSpace()),
                                            TStringBuilder() << "DiskSpace is " << NKikimrWhiteboard::EFlag_Name(vDiskInfo.GetDiskSpace()),
                                            ETags::VDiskSpace,
                                            {ETags::PDiskSpace});
                    break;
            }
        }

        storageVDiskStatus.set_overall(context.GetOverallStatus());
    }
};

class TBuilderPDisk : public TBuilderBase<TBuilderPDisk> {
    std::unordered_map<TString, const NKikimrSysView::TPDiskEntry*> PDisksMap;

public:
    TBuilderPDisk(std::unordered_map<TString, const NKikimrSysView::TPDiskEntry*>& pdisksMap)
        : PDisksMap(pdisksMap)
    {}

    void FillImpl(const TString& pDiskId, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckResult context) {
        context.Location.clear_database(); // PDisks are shared between databases
        context.Location.mutable_storage()->mutable_pool()->clear_name(); // PDisks are shared between pools
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // PDisks are shared between groups
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->clear_id(); // PDisks are shared between vdisks
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->Clear();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_pdisk();
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_id(pDiskId);
        storagePDiskStatus.set_id(pDiskId);

        auto itPDisk = PDisksMap.find(pDiskId);
        if (itPDisk == PDisksMap.end()) { // this report, in theory, can't happen because there was pdisk mention in bsc vslot info. this pdisk info have to exists in bsc too
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, TStringBuilder() << "System tablet BSC didn't provide expected pdisk information", ETags::PDiskState);
            storagePDiskStatus.set_overall(context.GetOverallStatus());
            return;
        }

        const auto& pDisk = itPDisk->second->GetInfo();

        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_path(pDisk.GetPath());
        const auto& statusString = pDisk.GetStatusV2();
        const auto *descriptor = NKikimrBlobStorage::EDriveStatus_descriptor();
        auto status = descriptor->FindValueByName(statusString);
        if (!status) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                    TStringBuilder() << "Unknown PDisk state: " << statusString,
                                    ETags::PDiskState);
        }
        switch (status->number()) {
            case NKikimrBlobStorage::ACTIVE:
            case NKikimrBlobStorage::INACTIVE: {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                break;
            }
            case NKikimrBlobStorage::FAULTY:
            case NKikimrBlobStorage::BROKEN:
            case NKikimrBlobStorage::TO_BE_REMOVED: {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                        TStringBuilder() << "PDisk state is " << statusString,
                                        ETags::PDiskState);
                break;
            }
        }

        if (pDisk.GetAvailableSize() != 0 && pDisk.GetTotalSize() != 0) { // do not replace it with Has()
            double avail = (double)pDisk.GetAvailableSize() / pDisk.GetTotalSize();
            if (avail < 0.06) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", ETags::PDiskSpace);
            } else if (avail < 0.09) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", ETags::PDiskSpace);
            } else if (avail < 0.12) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", ETags::PDiskSpace);
            }
        }

        // do not propagate RED status to vdisk - so that vdisk is not considered down when computing group status
        context.OverallStatus = MinStatus(context.OverallStatus, Ydb::Monitoring::StatusFlag::ORANGE);
        storagePDiskStatus.set_overall(context.GetOverallStatus());
    }
};

class TBuilderPDiskWithWhiteboard : public TBuilderBase<TBuilderPDiskWithWhiteboard> {
    THashMap<TNodeId, const TEvInterconnect::TNodeInfo*> MergedNodeInfo;
    THashSet<TNodeId> UnavailableStorageNodes;

public:
    TBuilderPDiskWithWhiteboard(THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& mergedNodeInfo,
                                        THashSet<TNodeId>& unavailableStorageNodes)
        : MergedNodeInfo(mergedNodeInfo)
        , UnavailableStorageNodes(unavailableStorageNodes)
    {}

    void FillImpl(const TString& pDiskId, const NKikimrWhiteboard::TPDiskStateInfo& pDiskInfo, Ydb::Monitoring::StoragePDiskStatus& storagePDiskStatus, TSelfCheckResult context) {
        context.Location.clear_database(); // PDisks are shared between databases
        if (context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->empty()) {
            context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->add_pdisk();
        }
        context.Location.mutable_storage()->mutable_pool()->clear_name(); // PDisks are shared between pools
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->clear_id(); // PDisks are shared between groups
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->clear_id(); // PDisks are shared between vdisks
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_id(pDiskId);
        context.Location.mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk()->begin()->set_path(pDiskInfo.path());
        storagePDiskStatus.set_id(pDiskId);

        if (pDiskInfo.HasState()) {
            switch (pDiskInfo.GetState()) {
                case NKikimrBlobStorage::TPDiskState::Normal:
                case NKikimrBlobStorage::TPDiskState::Stopped:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::GREEN);
                    break;
                case NKikimrBlobStorage::TPDiskState::Initial:
                case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW,
                                            TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskInfo.GetState()),
                                            ETags::PDiskState);
                    break;
                case NKikimrBlobStorage::TPDiskState::InitialFormatReadError:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogReadError:
                case NKikimrBlobStorage::TPDiskState::InitialSysLogParseError:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError:
                case NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError:
                case NKikimrBlobStorage::TPDiskState::CommonLoggerInitError:
                case NKikimrBlobStorage::TPDiskState::OpenFileError:
                case NKikimrBlobStorage::TPDiskState::ChunkQuotaError:
                case NKikimrBlobStorage::TPDiskState::DeviceIoError:
                case NKikimrBlobStorage::TPDiskState::Missing:
                case NKikimrBlobStorage::TPDiskState::Timeout:
                case NKikimrBlobStorage::TPDiskState::NodeDisconnected:
                case NKikimrBlobStorage::TPDiskState::Unknown:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                            TStringBuilder() << "PDisk state is " << NKikimrBlobStorage::TPDiskState::E_Name(pDiskInfo.GetState()),
                                            ETags::PDiskState);
                    break;
                case NKikimrBlobStorage::TPDiskState::Reserved15:
                case NKikimrBlobStorage::TPDiskState::Reserved16:
                case NKikimrBlobStorage::TPDiskState::Reserved17:
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Unknown PDisk state");
                    break;
            }

            //if (pDiskInfo.HasAvailableSize() && pDiskInfo.GetTotalSize() != 0) {
            if (pDiskInfo.GetAvailableSize() != 0 && pDiskInfo.GetTotalSize() != 0) { // hotfix until KIKIMR-12659
                double avail = (double)pDiskInfo.GetAvailableSize() / pDiskInfo.GetTotalSize();
                if (avail < 0.06) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Available size is less than 6%", ETags::PDiskSpace);
                } else if (avail < 0.09) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Available size is less than 9%", ETags::PDiskSpace);
                } else if (avail < 0.12) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Available size is less than 12%", ETags::PDiskSpace);
                }
            }
        } else {
            if (UnavailableStorageNodes.count(pDiskInfo.nodeid()) != 0) {
                TSelfCheckResult nodeContext(&context, "STORAGE_NODE");
                nodeContext.Location.mutable_storage()->clear_pool();
                nodeContext.Location.mutable_storage()->mutable_node()->set_id(pDiskInfo.nodeid());
                const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
                auto itNodeInfo = MergedNodeInfo.find(pDiskInfo.nodeid());
                if (itNodeInfo != MergedNodeInfo.end()) {
                    nodeInfo = itNodeInfo->second;
                }
                if (nodeInfo) {
                    nodeContext.Location.mutable_storage()->mutable_node()->set_host(nodeInfo->Host);
                    nodeContext.Location.mutable_storage()->mutable_node()->set_port(nodeInfo->Port);
                }
                nodeContext.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                            TStringBuilder() << "Storage node is not available",
                                            ETags::NodeState);
            }
            context.ReportStatus(Ydb::Monitoring::StatusFlag::RED,
                                    TStringBuilder() << "PDisk is not available",
                                    ETags::PDiskState,
                                    {ETags::NodeState});
        }

        storagePDiskStatus.set_overall(context.GetOverallStatus());
    }
};

class TBuilderNode : public TBuilderBase<TBuilderNode> {
    const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& MergedNodeInfo;

public:
    TBuilderNode(const THashMap<TNodeId, const TEvInterconnect::TNodeInfo*>& mergedNodeInfo)
        : MergedNodeInfo(mergedNodeInfo)
    {}

    void FillImpl(TNodeId nodeId, Ydb::Monitoring::LocationNode* node) {
        const TEvInterconnect::TNodeInfo* nodeInfo = nullptr;
        auto itNodeInfo = MergedNodeInfo.find(nodeId);
        if (itNodeInfo != MergedNodeInfo.end()) {
            nodeInfo = itNodeInfo->second;
        }
        TString id(ToString(nodeId));

        node->set_id(nodeId);
        if (nodeInfo) {
            node->set_host(nodeInfo->Host);
            node->set_port(nodeInfo->Port);
        }
    }
};

struct TBuilderContext: public TThrRefBase {
public:
    THolder<TBuilderResult<TBuilderContext>> Result;
    THolder<TBuilderDatabase<TBuilderContext>> Database;
    THolder<TBuilderCompute<TBuilderContext>> Compute;
    THolder<TBuilderComputeDatabase> ComputeDatabase;
    THolder<TBuilderComputeNode<TBuilderContext>> ComputeNode;
    THolder<TBuilderSystemTablets<TBuilderContext>> SystemTablets;
    THolder<TBuilderTablets<TBuilderContext>> Tablets;
    THolder<TBuilderStorage<TBuilderContext>> Storage;
    THolder<TBuilderStoragePool<TBuilderContext>> StoragePool;
    THolder<TBuilderStorageGroup<TBuilderContext>> StorageGroup;
    THolder<TBuilderVDisk<TBuilderContext>> VDisk;
    THolder<TBuilderPDisk> PDisk;
    THolder<TBuilderStorageGroupWithWhiteboard<TBuilderContext>> StorageGroupWithWhiteboard;
    THolder<TBuilderVDiskWithWhiteboard<TBuilderContext>> VDiskWithWhiteboard;
    THolder<TBuilderPDiskWithWhiteboard> PDiskWithWhiteboard;
    THolder<TBuilderNode> Node;
};

} // NKikimr::NHealthCheck
