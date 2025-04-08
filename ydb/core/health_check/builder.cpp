# include "builder.h"

namespace NKikimr::NHealthCheck {






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

static TString GetNodeLocation(const TEvInterconnect::TNodeInfo& nodeInfo) {
    return TStringBuilder() << nodeInfo.NodeId << '/' << nodeInfo.Host << ':' << nodeInfo.Port;
}

Ydb::Monitoring::StatusFlag::Status TBuilderSystemTablets::FillImpl(TDatabaseState& databaseState, TSelfCheckResult context) {
    TString databaseId = context.Location.database().name();
    for (auto& [tabletId, tablet] : TabletRequests.TabletStates) {
        if (tablet.Database == databaseId) {
            auto tabletIt = databaseState.MergedTabletState.find(std::make_pair(tabletId, 0));
            if (tabletIt != databaseState.MergedTabletState.end()) {
                auto nodeId = tabletIt->second->GetNodeID();
                if (nodeId) {
                    FillNodeInfo(nodeId, context.Location.mutable_node());
                }
            }

            context.Location.mutable_compute()->clear_tablet();
            auto& protoTablet = *context.Location.mutable_compute()->mutable_tablet();
            auto timeoutMs = Timeout.MilliSeconds();
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

Ydb::Monitoring::StatusFlag::Status TBuilderTablets::FillImpl(TDatabaseState& databaseState,
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
                FillNodeInfo(nodeId, tabletContext.Location.mutable_node());
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

void FillNodeInfo(TNodeId nodeId, Ydb::Monitoring::LocationNode* node) {
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

void TBuilderComputeNode::FillImpl(TDatabaseState& databaseState, TNodeId nodeId, Ydb::Monitoring::ComputeNodeStatus& computeNodeStatus, TSelfCheckResult context) {
    FillNodeInfo(nodeId, context.Location.mutable_compute()->mutable_node());

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

void TBuilderCompute::FillImpl(TDatabaseState& databaseState, Ydb::Monitoring::ComputeStatus& computeStatus, TSelfCheckResult context) {
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
            ComputeNode.Build(databaseState, nodeId, computeNode, {&context, "COMPUTE_NODE"});
        }
    }
    Ydb::Monitoring::StatusFlag::Status systemStatus = SystemTablets.Build(databaseState, {&context, "SYSTEM_TABLET"});
    if (systemStatus != Ydb::Monitoring::StatusFlag::GREEN && systemStatus != Ydb::Monitoring::StatusFlag::GREY) {
        context.ReportStatus(systemStatus, "Compute has issues with system tablets", ETags::ComputeState, {ETags::SystemTabletState});
    }
    ComputeDatabase.Build(databaseState, computeStatus, {&context, "COMPUTE_QUOTA"});
    context.ReportWithMaxChildStatus("Some nodes are restarting too often", ETags::ComputeState, {ETags::Uptime});
    context.ReportWithMaxChildStatus("Compute is overloaded", ETags::ComputeState, {ETags::OverloadState});
    context.ReportWithMaxChildStatus("Compute quota usage", ETags::ComputeState, {ETags::QuotaUsage});
    context.ReportWithMaxChildStatus("Database has time difference between nodes", ETags::ComputeState, {ETags::SyncState});
    Ydb::Monitoring::StatusFlag::Status tabletsStatus = Ydb::Monitoring::StatusFlag::GREEN;
    computeNodeIds->push_back(0); // for tablets without node
    for (TNodeId nodeId : *computeNodeIds) {
        tabletsStatus = MaxStatus(tabletsStatus, Tablets.Build(databaseState, nodeId, *computeStatus.mutable_tablets(), context));
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

static Ydb::Monitoring::StatusFlag::Status GetFlagFromBSPDiskSpaceColor(NKikimrBlobStorage::TPDiskSpaceColor::E flag) {
    switch (flag) {
        case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:
        case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:
            return Ydb::Monitoring::StatusFlag::GREEN;
        case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:
        case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:
            return Ydb::Monitoring::StatusFlag::YELLOW;
        case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:
        case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:
        case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:
            return Ydb::Monitoring::StatusFlag::ORANGE;
        case NKikimrBlobStorage::TPDiskSpaceColor::RED:
            return Ydb::Monitoring::StatusFlag::RED;
        default:
            return Ydb::Monitoring::StatusFlag::UNSPECIFIED;
    }
}

static Ydb::Monitoring::StatusFlag::Status GetFlagFromWhiteboardFlag(NKikimrWhiteboard::EFlag flag) {
    switch (flag) {
        case NKikimrWhiteboard::EFlag::Green:
            return Ydb::Monitoring::StatusFlag::GREEN;
        case NKikimrWhiteboard::EFlag::Yellow:
            return Ydb::Monitoring::StatusFlag::YELLOW;
        case NKikimrWhiteboard::EFlag::Orange:
            return Ydb::Monitoring::StatusFlag::ORANGE;
        case NKikimrWhiteboard::EFlag::Red:
            return Ydb::Monitoring::StatusFlag::RED;
        default:
            return Ydb::Monitoring::StatusFlag::UNSPECIFIED;
    }
}

} // NKikimr::NHealthCheck
