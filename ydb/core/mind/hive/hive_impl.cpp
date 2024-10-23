#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/util/tuples.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/generic/array_ref.h>

Y_DECLARE_OUT_SPEC(inline, TArrayRef<const NKikimrHive::TDataCentersGroup*>, out, vec) {
    out << '[';
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        if (it != vec.begin())
            out << ';';
        out << (*it)->ShortDebugString();
    }
    out << ']';
}

Y_DECLARE_OUT_SPEC(inline, TArrayRef<const NKikimr::TSubDomainKey>, out, vec) {
    out << '[' << JoinSeq(',', vec) << ']';
}

namespace NKikimr {
namespace NHive {

void THive::Handle(TEvHive::TEvCreateTablet::TPtr& ev) {
    NKikimrHive::TEvCreateTablet& rec = ev->Get()->Record;
    if (rec.HasOwner() && rec.HasOwnerIdx() && rec.HasTabletType() && rec.BindedChannelsSize() != 0) {
        BLOG_D("Handle TEvHive::TEvCreateTablet(" << rec.GetTabletType() << '(' << rec.GetOwner() << ',' << rec.GetOwnerIdx() << "))");
        Execute(CreateCreateTablet(std::move(rec), ev->Sender, ev->Cookie));
    } else {
        BLOG_ERROR("Invalid arguments specified to TEvCreateTablet: " << rec.DebugString());
        THolder<TEvHive::TEvCreateTabletReply> reply = MakeHolder<TEvHive::TEvCreateTabletReply>();
        reply->Record.SetStatus(NKikimrProto::EReplyStatus::ERROR);
        reply->Record.SetErrorReason(NKikimrHive::EErrorReason::ERROR_REASON_INVALID_ARGUMENTS);
        if (rec.HasOwner()) {
            reply->Record.SetOwner(rec.GetOwner());
        }
        if (rec.HasOwnerIdx()) {
            reply->Record.SetOwnerIdx(rec.GetOwnerIdx());
        }
        Send(ev->Sender, reply.Release(), 0, ev->Cookie);
    }
}

void THive::Handle(TEvHive::TEvAdoptTablet::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvAdoptTablet");
    NKikimrHive::TEvAdoptTablet& rec = ev->Get()->Record;
    Y_ABORT_UNLESS(rec.HasOwner() && rec.HasOwnerIdx() && rec.HasTabletType());
    Execute(CreateAdoptTablet(rec, ev->Sender, ev->Cookie));
}

void THive::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->ClientId == BSControllerPipeClient && msg->Status != NKikimrProto::OK) {
        RestartBSControllerPipe();
        return;
    }
    if (msg->ClientId == RootHivePipeClient && msg->Status != NKikimrProto::OK) {
        RestartRootHivePipe();
        return;
    }
    if (!PipeClientCache->OnConnect(ev)) {
        BLOG_ERROR("Failed to connect to tablet " << ev->Get()->TabletId << " from tablet " << TabletID());
        RestartPipeTx(ev->Get()->TabletId);
    } else {
        BLOG_D("Connected to tablet " << ev->Get()->TabletId << " from tablet " << TabletID());
    }
}

void THive::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
    if (msg->ClientId == BSControllerPipeClient) {
        RestartBSControllerPipe();
        return;
    }
    if (msg->ClientId == RootHivePipeClient) {
        RestartRootHivePipe();
        return;
    }
    BLOG_D("Client pipe to tablet " << ev->Get()->TabletId << " from " << TabletID() << " is reset");
    PipeClientCache->OnDisconnect(ev);
    RestartPipeTx(ev->Get()->TabletId);
}

void THive::RestartPipeTx(ui64 tabletId) {
    for (auto txid : PipeTracker.FindTx(tabletId)) {
        BLOG_D("Pipe reset to tablet " << tabletId << " caused restart of txid# " << txid << " at tablet " << TabletID());
        // TODO: restart all the dependent transactions
    }
}

bool THive::TryToDeleteNode(TNodeInfo* node) {
    if (node->CanBeDeleted()) {
        BLOG_I("TryToDeleteNode(" << node->Id << "): deleting");
        DeleteNode(node->Id);
        return true;
    }
    if (!node->DeletionScheduled) {
        BLOG_D("TryToDeleteNode(" << node->Id << "): waiting " << GetNodeDeletePeriod());
        Schedule(GetNodeDeletePeriod(), new TEvPrivate::TEvDeleteNode(node->Id));
        node->DeletionScheduled = true;
    }
    return false;
}

void THive::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev) {
    if (ev->Get()->TabletId == TabletID()) {
        BLOG_TRACE("Handle TEvTabletPipe::TEvServerConnected(" << ev->Get()->ClientId << ") " << ev->Get()->ServerId);
        TNodeInfo& node = GetNode(ev->Get()->ClientId.NodeId());
        node.PipeServers.emplace_back(ev->Get()->ServerId);
    }
}

void THive::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
    if (ev->Get()->TabletId == TabletID()) {
        BLOG_TRACE("Handle TEvTabletPipe::TEvServerDisconnected(" << ev->Get()->ClientId << ") " << ev->Get()->ServerId);
        TNodeInfo* node = FindNode(ev->Get()->ClientId.NodeId());
        if (node != nullptr) {
            Erase(node->PipeServers, ev->Get()->ServerId);
            if (node->PipeServers.empty() && node->IsUnknown()) {
                ObjectDistributions.RemoveNode(*node);
                TryToDeleteNode(node);
            }
        }
    }
}

void THive::Handle(TEvLocal::TEvRegisterNode::TPtr& ev) {
    NKikimrLocal::TEvRegisterNode& record = ev->Get()->Record;
    if (record.GetHiveId() == TabletID()) {
        const TActorId &local = ev->Sender;
        BLOG_D("Handle TEvLocal::TEvRegisterNode from " << ev->Sender << " " << record.ShortDebugString());
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(ev->Sender.NodeId()));
        Execute(CreateRegisterNode(local, std::move(record)));
    } else {
        BLOG_W("Handle incorrect TEvLocal::TEvRegisterNode from " << ev->Sender << " " << record.ShortDebugString());
    }
}

bool THive::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive)
        return false;

    if (!ev)
        return true;

    CreateEvMonitoring(ev, ctx);
    return true;
}

void THive::Handle(TEvHive::TEvStopTablet::TPtr& ev) {
    BLOG_D("Handle StopTablet");
    NKikimrHive::TEvStopTablet& rec = ev->Get()->Record;
    const TActorId actorToNotify = rec.HasActorToNotify() ? ActorIdFromProto(rec.GetActorToNotify()) : ev->Sender;
    if (rec.HasTabletID()) {

    } else {
        Y_ENSURE_LOG(rec.HasTabletID(), rec.ShortDebugString());
        Send(actorToNotify, new TEvHive::TEvStopTabletResult(NKikimrProto::ERROR, 0), 0, ev->Cookie);
    }
}

void THive::Handle(TEvHive::TEvDeleteTablet::TPtr& ev) {
    Execute(CreateDeleteTablet(ev));
}

void THive::Handle(TEvHive::TEvDeleteOwnerTablets::TPtr& ev) {
    Execute(CreateDeleteOwnerTablets(ev));
}

void THive::DeleteTabletWithoutStorage(TLeaderTabletInfo* tablet) {
    Y_ENSURE_LOG(tablet->IsDeleting(), "tablet " << tablet->Id);
    Y_ENSURE_LOG(tablet->TabletStorageInfo->Channels.empty() || tablet->TabletStorageInfo->Channels[0].History.empty(), "tablet " << tablet->Id);

    // Tablet has no storage, so there's nothing to block or delete
    // Simulate a response from CreateTabletReqDelete as if all steps have been completed
    Send(SelfId(), new TEvTabletBase::TEvDeleteTabletResult(NKikimrProto::OK, tablet->Id));
}

void THive::DeleteTabletWithoutStorage(TLeaderTabletInfo* tablet, TSideEffects& sideEffects) {
    Y_ENSURE_LOG(tablet->IsDeleting(), "tablet " << tablet->Id);
    Y_ENSURE_LOG(tablet->TabletStorageInfo->Channels.empty() || tablet->TabletStorageInfo->Channels[0].History.empty(), "tablet " << tablet->Id);

    // Tablet has no storage, so there's nothing to block or delete
    // Simulate a response from CreateTabletReqDelete as if all steps have been completed
    sideEffects.Send(SelfId(), new TEvTabletBase::TEvDeleteTabletResult(NKikimrProto::OK, tablet->Id));
}

TInstant THive::GetAllowedBootingTime() {
    auto connectedNodes = TabletCounters->Simple()[NHive::COUNTER_NODES_CONNECTED].Get();
    BLOG_D(connectedNodes << " nodes connected out of " << ExpectedNodes);
    if (connectedNodes == 0) {
        return {};
    }
    TInstant result = LastConnect + MaxTimeBetweenConnects * std::max<i64>(static_cast<i64>(ExpectedNodes) - static_cast<i64>(connectedNodes), 1);
    if (connectedNodes < ExpectedNodes) {
        result = std::max(result, StartTime() + GetWarmUpBootWaitingPeriod());
    }
    result = std::min(result, StartTime() + GetMaxWarmUpPeriod());
    return result;
}

void THive::ExecuteProcessBootQueue(NIceDb::TNiceDb&, TSideEffects& sideEffects) {
    TInstant now = TActivationContext::Now();
    if (WarmUp) {
        TInstant allowed = GetAllowedBootingTime();
        if (now < allowed) {
            BLOG_D("ProcessBootQueue - waiting until " << allowed << " because of warmup, now: " << now);
            ProcessBootQueueScheduled = false;
            PostponeProcessBootQueue(allowed - now);
            return;
        }
    }
    BLOG_D("Handle ProcessBootQueue (size: " << BootQueue.BootQueue.size() << ")");
    THPTimer bootQueueProcessingTimer;
    if (ProcessWaitQueueScheduled) {
        BLOG_D("Handle ProcessWaitQueue (size: " << BootQueue.WaitQueue.size() << ")");
        BootQueue.MoveFromWaitQueueToBootQueue();
        ProcessWaitQueueScheduled = false;
    }
    ProcessBootQueueScheduled = false;
    ui64 processedItems = 0;
    ui64 tabletsStarted = 0;
    TInstant postponedStart;
    TStackVec<TBootQueue::TBootQueueRecord> delayedTablets;
    while (!BootQueue.BootQueue.empty() && processedItems < GetMaxBootBatchSize()) {
        TBootQueue::TBootQueueRecord record = BootQueue.PopFromBootQueue();
        BLOG_TRACE("Tablet " << record.TabletId << "." << record.FollowerId << " has priority " << record.Priority);
        ++processedItems;
        TTabletInfo* tablet = FindTablet(record.TabletId, record.FollowerId);
        if (tablet == nullptr) {
            continue;
        }
        tablet->InWaitQueue = false;
        if (tablet->IsAlive()) {
            BLOG_D("tablet " << record.TabletId << " already alive, skipping");
            continue;
        }
        if (tablet->IsReadyToStart(now)) {
            TBestNodeResult bestNodeResult = FindBestNode(*tablet, record.SuggestedNodeId);
            if (std::holds_alternative<TNodeInfo*>(bestNodeResult)) {
                if (tablet->InitiateStart(std::get<TNodeInfo*>(bestNodeResult))) {
                    ++tabletsStarted;
                    continue;
                }
            } else {
                if (std::holds_alternative<TTooManyTabletsStarting>(bestNodeResult)) {
                    delayedTablets.push_back(record);
                    break;
                } else if (std::holds_alternative<TNoNodeFound>(bestNodeResult)) {
                    for (const TActorId actorToNotify : tablet->ActorsToNotifyOnRestart) {
                        sideEffects.Send(actorToNotify, new TEvPrivate::TEvRestartComplete(tablet->GetFullTabletId(), "boot delay"));
                    }
                    tablet->ActorsToNotifyOnRestart.clear();
                    BootQueue.AddToWaitQueue(record); // waiting for new node
                    tablet->InWaitQueue = true;
                    continue;
                }
            }
        } else {
            TInstant tabletPostponedStart = tablet->PostponedStart;
            BLOG_D("tablet " << record.TabletId << " has postponed start at " << tabletPostponedStart);
            if (tabletPostponedStart > now) {
                if (postponedStart) {
                    postponedStart = std::min(postponedStart, tabletPostponedStart);
                } else {
                    postponedStart = tabletPostponedStart;
                }
            }
        }
        if (tablet->IsBooting()) {
            delayedTablets.push_back(record);
        }
    }
    for (TBootQueue::TBootQueueRecord record : delayedTablets) {
        record.Priority -= 1;
        BootQueue.AddToBootQueue(record);
    }
    if (TabletCounters != nullptr) {
        UpdateCounterBootQueueSize(BootQueue.BootQueue.size());
        TabletCounters->Simple()[NHive::COUNTER_WAITQUEUE_SIZE].Set(BootQueue.WaitQueue.size());
        TabletCounters->Cumulative()[NHive::COUNTER_BOOTQUEUE_PROCESSED].Increment(1);
        TabletCounters->Cumulative()[NHive::COUNTER_BOOTQUEUE_TIME].Increment(ui64(1000000. * bootQueueProcessingTimer.PassedReset()));
    }
    if (BootQueue.BootQueue.empty()) {
        BLOG_D("ProcessBootQueue - BootQueue empty (WaitQueue: " << BootQueue.WaitQueue.size() << ")");
    }
    if (processedItems > 0) {
        if (tabletsStarted > 0) {
            WarmUp = false;
        }
        if (processedItems == delayedTablets.size() && postponedStart < now) {
            BLOG_D("ProcessBootQueue - BootQueue throttling (size: " << BootQueue.BootQueue.size() << ")");
            return;
        }
        if (processedItems == GetMaxBootBatchSize()) {
            BLOG_D("ProcessBootQueue - rescheduling");
            ProcessBootQueue();
        } else if (postponedStart > now) {
            BLOG_D("ProcessBootQueue - postponing");
            PostponeProcessBootQueue(postponedStart - now);
        }
    }
}

void THive::HandleInit(TEvPrivate::TEvProcessBootQueue::TPtr&) {
    BLOG_W("Received TEvProcessBootQueue while in StateInit");
    Schedule(TDuration::Seconds(1), new TEvPrivate::TEvProcessBootQueue());
}

void THive::Handle(TEvPrivate::TEvProcessBootQueue::TPtr&) {
    BLOG_TRACE("ProcessBootQueue - executing");
    Execute(CreateProcessBootQueue());
}

void THive::Handle(TEvPrivate::TEvPostponeProcessBootQueue::TPtr&) {
    BLOG_D("Handle PostponeProcessBootQueue");
    ProcessBootQueuePostponed = false;
    ProcessBootQueue();
}

void THive::ProcessBootQueue() {
    BLOG_D("ProcessBootQueue (" << BootQueue.BootQueue.size() << ")");
    if (!ProcessBootQueueScheduled) {
        BLOG_TRACE("ProcessBootQueue - sending");
        ProcessBootQueueScheduled = true;
        Send(SelfId(), new TEvPrivate::TEvProcessBootQueue());
    }
}

void THive::PostponeProcessBootQueue(TDuration after) {
    TInstant postponeUntil = TActivationContext::Now() + after;
    if (!ProcessBootQueuePostponed || postponeUntil < ProcessBootQueuePostponedUntil) {
        BLOG_D("PostponeProcessBootQueue (" << after << ")");
        ProcessBootQueuePostponed = true;
        ProcessBootQueuePostponedUntil = postponeUntil;
        Schedule(after, new TEvPrivate::TEvPostponeProcessBootQueue());
    }
}

void THive::ProcessWaitQueue() {
    BLOG_D("ProcessWaitQueue (" << BootQueue.WaitQueue.size() << ")");
    ProcessWaitQueueScheduled = true;
    ProcessBootQueue();
}

void THive::AddToBootQueue(TTabletInfo* tablet, TNodeId node) {
    tablet->UpdateWeight();
    tablet->BootState = BootStateBooting;
    BootQueue.EmplaceToBootQueue(*tablet, node);
    UpdateCounterBootQueueSize(BootQueue.BootQueue.size());
}

void THive::Handle(TEvPrivate::TEvProcessPendingOperations::TPtr&) {
    BLOG_D("Handle ProcessPendingOperations");
}

void THive::Handle(TEvPrivate::TEvBalancerOut::TPtr&) {
    BLOG_D("Handle BalancerOut");
}


void THive::Handle(TEvPrivate::TEvStartStorageBalancer::TPtr& ev) {
    BLOG_D("Handle StartStorageBalancer");
    StartHiveStorageBalancer(std::move(ev->Get()->Settings));
}

void THive::Handle(TEvHive::TEvBootTablet::TPtr& ev) {
    TTabletId tabletId = ev->Get()->Record.GetTabletID();
    TTabletInfo* tablet = FindTablet(tabletId);
    Y_ABORT_UNLESS(tablet != nullptr);
    if (tablet->IsReadyToBoot()) {
        tablet->InitiateBoot();
    }
}

TVector<TTabletId> THive::UpdateStoragePools(const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters>& groups) {
    TVector<TTabletId> tabletsToUpdate;
    std::unordered_map<TString, std::vector<const NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters*>> poolToGroup;
    for (const auto& gp : groups) {
        poolToGroup[gp.GetStoragePoolName()].emplace_back(&gp);
    }
    for (const auto& [poolName, groupParams] : poolToGroup) {
        std::unordered_set<TStorageGroupId> groups;
        TStoragePoolInfo& storagePool = GetStoragePool(poolName);
        std::transform(storagePool.Groups.begin(), storagePool.Groups.end(), std::inserter(groups, groups.end()), [](const auto& pr) { return pr.first; });
        for (const NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters* group : groupParams) {
            TStorageGroupId groupId = group->GetGroupID();
            groups.erase(groupId);
            storagePool.UpdateStorageGroup(groupId, *group);
        }
        for (TStorageGroupId groupId : groups) {
            storagePool.DeleteStorageGroup(groupId);
        }
        if (storagePool.RefreshRequestInFlight > 0) {
            --storagePool.RefreshRequestInFlight;
        } else {
            BLOG_W("THive::Handle TEvControllerSelectGroupsResult: Out of inflight counter response received");
        }
        storagePool.SetAsFresh();
        storagePool.ConfigurationGeneration = ConfigurationGeneration;
        TVector<TTabletId> tabletsWaiting = storagePool.PullWaitingTablets();
        tabletsToUpdate.reserve(tabletsToUpdate.size() + tabletsWaiting.size());
        for (TTabletId tabletId : tabletsWaiting) {
            tabletsToUpdate.emplace_back(tabletId);
        }
    }
    return tabletsToUpdate;
}

void THive::Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr& ev) {
    NKikimrBlobStorage::TEvControllerSelectGroupsResult& rec = ev->Get()->Record;
    if (rec.GetStatus() == NKikimrProto::OK) {
        BLOG_D("THive::Handle TEvControllerSelectGroupsResult: success " << rec.ShortDebugString());
        if (rec.MatchingGroupsSize()) {
            TVector<TTabletId> tablets;
            for (const auto& matchingGroups : rec.GetMatchingGroups()) {
                if (matchingGroups.GroupsSize() == 0) {
                    BLOG_ERROR("THive::Handle TEvControllerSelectGroupsResult: BSC didn't return matching groups set");
                    continue;
                }
                TVector<TTabletId> tabletsWaiting = UpdateStoragePools(matchingGroups.GetGroups());
                tablets.reserve(tablets.size() + tabletsWaiting.size());
                for (TTabletId tablet : tabletsWaiting) {
                    tablets.emplace_back(tablet);
                }
            }
            std::sort(tablets.begin(), tablets.end());
            tablets.erase(std::unique(tablets.begin(), tablets.end()), tablets.end());
            for (TTabletId tabletId : tablets) {
                TLeaderTabletInfo* tablet = FindTablet(tabletId);
                if (!tablet) {
                    BLOG_ERROR("THive::Handle TEvControllerSelectGroupsResult: tablet# " << tabletId << " not found");
                } else {
                    Execute(CreateUpdateTabletGroups(tabletId));
                }
            }
            if (tablets.empty()) {
                ProcessStorageBalancer();
            }
        } else {
            BLOG_ERROR("THive::Handle TEvControllerSelectGroupsResult: obsolete BSC response");
        }
    } else {
        BLOG_ERROR("THive::Handle TEvControllerSelectGroupsResult: " << rec.GetStatus());
    }
}

void THive::Handle(TEvLocal::TEvTabletStatus::TPtr& ev) {
    TNodeId nodeId = ev->Sender.NodeId();
    TNodeInfo* node = FindNode(nodeId);
    if (node != nullptr) {
        if (node->IsDisconnected()) {
            BLOG_W("Handle TEvLocal::TEvTabletStatus, NodeId " << nodeId << " disconnected, reconnecting");
            node->SendReconnect(ev->Sender);
            return;
        }
    }
    TEvLocal::TEvTabletStatus* msg = ev->Get();
    NKikimrLocal::TEvTabletStatus& record = msg->Record;
    BLOG_D("Handle TEvLocal::TEvTabletStatus, TabletId: " << record.GetTabletID());
    if (FindTablet(record.GetTabletID(), record.GetFollowerId()) != nullptr) {
        Execute(CreateUpdateTabletStatus(
                    record.GetTabletID(),
                    ev->Sender,
                    record.GetGeneration(),
                    record.GetFollowerId(),
                    static_cast<TEvLocal::TEvTabletStatus::EStatus>(record.GetStatus()),
                    static_cast<TEvTablet::TEvTabletDead::EReason>(record.GetReason())
                ));
    } else {
        BLOG_W("Handle TEvLocal::TEvTabletStatus from node " << nodeId << ", TabletId: " << record.GetTabletID() << " not found");
    }
}

void THive::Handle(TEvPrivate::TEvBootTablets::TPtr&) {
    BLOG_D("Handle BootTablets");
    SignalTabletActive(DEPRECATED_CTX);
    ReadyForConnections = true;
    RequestPoolsInformation();
    std::vector<TNodeInfo*> unimportantNodes; // ping nodes with tablets first
    unimportantNodes.reserve(Nodes.size());
    for (auto& [id, node] : Nodes) {
        if (node.IsUnknown() && node.Local) {
            if (node.GetTabletsTotal() > 0) {
                node.Ping();
            } else {
                unimportantNodes.push_back(&node);
            }
        }
    }
    for (auto* node : unimportantNodes) {
        node->Ping();
    }
    ProcessNodePingQueue();
    TVector<TTabletId> tabletsToReleaseFromParent;
    TSideEffects sideEffects;
    sideEffects.Reset(SelfId());
    for (auto& tab : Tablets) {
        TLeaderTabletInfo& tablet = tab.second;
        if (tablet.NeedToReleaseFromParent) {
            BLOG_D("Need to release from parent tablet " << tablet.ToString());
            tabletsToReleaseFromParent.push_back(tablet.Id);
        } else if (tablet.IsReadyToBoot()) {
            tablet.InitiateBoot();
        } else if (tablet.IsReadyToAssignGroups()) {
            tablet.InitiateAssignTabletGroups();
        } else if (tablet.IsReadyToBlockStorage()) {
            tablet.InitiateBlockStorage(sideEffects);
        } else if (tablet.IsDeleting()) {
            if (!tablet.InitiateBlockStorage(sideEffects, std::numeric_limits<ui32>::max())) {
                DeleteTabletWithoutStorage(&tablet);
            }
        } else if (tablet.IsLockedToActor()) {
            // we are wating for a lock
        } else if (tablet.IsExternalBoot()) {
            // we are wating for external boot request
        } else if (tablet.IsStopped() && tablet.State == ETabletState::Stopped) {
            ReportStoppedToWhiteboard(tablet);
            BLOG_D("Report tablet " << tablet.ToString() << " as stopped to Whiteboard");
        } else {
            BLOG_W("The tablet "
                   << tablet.ToString()
                   << " is not ready for anything State:"
                   << ETabletStateName(tablet.State)
                   << " VolatileState:"
                   << TTabletInfo::EVolatileStateName(tablet.GetVolatileState()));
        }
        for (const auto& domain : tablet.NodeFilter.AllowedDomains) {
            SeenDomain(domain);
        }
        if (tablet.ObjectDomain) {
            SeenDomain(tablet.ObjectDomain);
        }
    }
    sideEffects.Complete(DEPRECATED_CTX);
    if (AreWeRootHive()) {
        BLOG_D("Root Hive is ready");
    } else {
        BLOG_D("SubDomain Hive is ready");

        if (!PrimaryDomainKey && Info()->TenantPathId) {
            //NOTE: Primary(Sub)DomainKey isn't set after loading everything from the local db --
            // -- this is first time boot or incomplete configuration.
            BLOG_I("Primary(Sub)DomainKey is not set, setting it from TTabletStorageInfo::TenantPathId to " << Info()->TenantPathId);

            auto msg = MakeHolder<TEvHive::TEvConfigureHive>(TSubDomainKey(Info()->TenantPathId.OwnerId, Info()->TenantPathId.LocalPathId));
            TEvHive::TEvConfigureHive::TPtr event((TEventHandle<TEvHive::TEvConfigureHive>*) new IEventHandle(
                TActorId(), TActorId(), msg.Release()
            ));
            Execute(CreateConfigureSubdomain(event));
        }

        if (!TabletOwnersSynced) {
            // this code should be removed later
            THolder<TEvHive::TEvRequestTabletOwners> request(new TEvHive::TEvRequestTabletOwners());
            request->Record.SetOwnerID(TabletID());
            BLOG_D("Requesting TabletOwners from the Root");
            SendToRootHivePipe(request.Release());
            // this code should be removed later
        }
    }
    if (!tabletsToReleaseFromParent.empty()) {
        THolder<TEvHive::TEvReleaseTablets> request(new TEvHive::TEvReleaseTablets());
        request->Record.SetNewOwnerID(TabletID());
        for (TTabletId tabletId : tabletsToReleaseFromParent) {
            request->Record.AddTabletIDs(tabletId);
        }
        SendToRootHivePipe(request.Release());
    }
    MakeScaleRecommendation();
    ProcessPendingOperations();
}

void THive::Handle(TEvHive::TEvInitMigration::TPtr& ev) {
    BLOG_D("Handle InitMigration " << ev->Get()->Record);
    if (MigrationState == NKikimrHive::EMigrationState::MIGRATION_READY || MigrationState == NKikimrHive::EMigrationState::MIGRATION_COMPLETE) {
        if (ev->Get()->Record.GetMigrationFilter().GetFilterDomain().GetSchemeShard() == 0 && GetMySubDomainKey().GetSchemeShard() == 0) {
            BLOG_ERROR("Migration ignored - unknown domain");
            Send(ev->Sender, new TEvHive::TEvInitMigrationReply(NKikimrProto::ERROR));
            return;
        }
        MigrationFilter = ev->Get()->Record.GetMigrationFilter();
        if (!MigrationFilter.HasFilterDomain()) {
            MigrationFilter.MutableFilterDomain()->CopyFrom(GetMySubDomainKey());
        }
        MigrationState = NKikimrHive::EMigrationState::MIGRATION_ACTIVE;
        // MigrationProgress = 0;
        // ^ we want cumulative statistics
        if (MigrationFilter.GetMaxTabletsToSeize() == 0) {
            MigrationFilter.SetMaxTabletsToSeize(1);
        }
        MigrationFilter.SetNewOwnerID(TabletID());
        BLOG_D("Requesting migration " << MigrationFilter.ShortDebugString());
        SendToRootHivePipe(new TEvHive::TEvSeizeTablets(MigrationFilter));
        Send(ev->Sender, new TEvHive::TEvInitMigrationReply(NKikimrProto::OK));
    } else {
        BLOG_D("Migration already in progress " << MigrationProgress);
        Send(ev->Sender, new TEvHive::TEvInitMigrationReply(NKikimrProto::ALREADY));
    }
}

void THive::Handle(TEvHive::TEvQueryMigration::TPtr& ev) {
    BLOG_D("Handle QueryMigration");
    Send(ev->Sender, new TEvHive::TEvQueryMigrationReply(MigrationState, MigrationProgress));
}

void THive::OnDetach(const TActorContext&) {
    BLOG_D("THive::OnDetach");
    Cleanup();
    PassAway();
}

void THive::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) {
    BLOG_I("OnTabletDead: " << TabletID());
    Cleanup();
    return PassAway();
}

void THive::BuildLocalConfig() {
    LocalConfig.Clear();
    if (ResourceProfiles)
        ResourceProfiles->StoreProfiles(*LocalConfig.MutableResourceProfiles());
}

void THive::BuildCurrentConfig() {
    CurrentConfig = ClusterConfig;
    CurrentConfig.MergeFrom(DatabaseConfig);
    TabletLimit.clear();
    for (const auto& tabletLimit : CurrentConfig.GetDefaultTabletLimit()) {
        TabletLimit.insert_or_assign(tabletLimit.GetType(), tabletLimit);
    }
    DefaultDataCentersPreference.clear();
    for (const NKikimrConfig::THiveTabletPreference& tabletPreference : CurrentConfig.GetDefaultTabletPreference()) {
        DefaultDataCentersPreference[tabletPreference.GetType()] = tabletPreference.GetDataCentersPreference();
    }
    BalancerIgnoreTabletTypes.clear();
    for (auto i : CurrentConfig.GetBalancerIgnoreTabletTypes()) {
        const auto type = TTabletTypes::EType(i);
        if (IsValidTabletType(type)) {
            BalancerIgnoreTabletTypes.emplace_back(type);
        }
    }
    MakeTabletTypeSet(BalancerIgnoreTabletTypes);
    CutHistoryDenyList.clear();
    for (auto name : SplitString(CurrentConfig.GetCutHistoryDenyList(), ",")) {
        TTabletTypes::EType type = TTabletTypes::StrToType(Strip(name));
        if (IsValidTabletType(type)) {
            CutHistoryDenyList.emplace_back(type);
        }
    }
    MakeTabletTypeSet(CutHistoryDenyList);
    CutHistoryAllowList.clear();
    for (auto name : SplitString(CurrentConfig.GetCutHistoryAllowList(), ",")) {
        TTabletTypes::EType type = TTabletTypes::StrToType(Strip(name));
        if (IsValidTabletType(type)) {
            CutHistoryAllowList.emplace_back(type);
        }
    }
    MakeTabletTypeSet(CutHistoryAllowList);
    if (!CurrentConfig.GetSpreadNeighbours()) {
        // SpreadNeighbours can be turned off anytime, but
        // cannot be safely turned on without Hive restart
        // as the in-memory data on neighbours would not be accurate
        SpreadNeighbours = false;
        ObjectDistributions.Disable();
    }
}

void THive::Cleanup() {
    BLOG_D("THive::Cleanup");

    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest());

    while (!SubActors.empty()) {
        SubActors.front()->Cleanup();
    }

    PipeClientCache->Detach(DEPRECATED_CTX);

    if (BSControllerPipeClient) {
        NTabletPipe::CloseClient(SelfId(), BSControllerPipeClient);
        BSControllerPipeClient = TActorId();
    }

    if (RootHivePipeClient) {
        NTabletPipe::CloseClient(SelfId(), RootHivePipeClient);
        RootHivePipeClient = TActorId();
    }

    if (ResponsivenessPinger) {
        ResponsivenessPinger->Detach(TlsActivationContext->ActorContextFor(ResponsivenessActorID));
        ResponsivenessPinger = nullptr;
    }
}

void THive::Handle(TEvLocal::TEvStatus::TPtr& ev) {
    BLOG_D("Handle TEvLocal::TEvStatus for Node " << ev->Sender.NodeId() << ": " << ev->Get()->Record.ShortDebugString());
    RemoveFromPingInProgress(ev->Sender.NodeId());
    Execute(CreateStatus(ev->Sender, ev->Get()->Record));
}

void THive::Handle(TEvLocal::TEvSyncTablets::TPtr& ev) {
    BLOG_D("THive::Handle::TEvSyncTablets");
    RemoveFromPingInProgress(ev->Sender.NodeId());
    Execute(CreateSyncTablets(ev->Sender, ev->Get()->Record));
}

void THive::Handle(TEvPrivate::TEvProcessDisconnectNode::TPtr& ev) {
    TAutoPtr<TEvPrivate::TEvProcessDisconnectNode> event = ev->Release();
    TNodeInfo* node = FindNode(event->NodeId);
    if (!node || node->IsDisconnecting()) {
        auto itCategory = event->Tablets.begin();
        if (itCategory != event->Tablets.end()) {
            BLOG_D("THive::Handle::TEvProcessDisconnectNode: Node " << event->NodeId << " Category " << itCategory->first);
            for (std::pair<TTabletId, TFollowerId> tabletId : itCategory->second) {
                TTabletInfo* tablet = FindTablet(tabletId);
                if (tablet != nullptr) {
                    if (tablet->IsAlive()) {
                        Execute(CreateRestartTablet(tabletId));
                    }
                }
            }
            event->Tablets.erase(itCategory);
        }
        ScheduleDisconnectNode(event);
    }
}

void THive::Handle(TEvHive::TEvTabletMetrics::TPtr& ev) {
    TNodeId nodeId = ev->Sender.NodeId();
    BLOG_TRACE("THive::Handle::TEvTabletMetrics, NodeId " << nodeId << " " << ev->Get()->Record.ShortDebugString());
    if (UpdateTabletMetricsInProgress < MAX_UPDATE_TABLET_METRICS_IN_PROGRESS) {
        UpdateTabletMetricsInProgress++;
        if (UpdateTabletMetricsInProgress > (MAX_UPDATE_TABLET_METRICS_IN_PROGRESS / 2)) {
            BLOG_W("THive::Handle::TEvTabletMetrics, NodeId " << nodeId << " transactions in progress is over 50% of MAX_UPDATE_TABLET_METRICS_IN_PROGRESS");
        }
        Execute(CreateUpdateTabletMetrics(ev));
    } else {
        BLOG_ERROR("THive::Handle::TEvTabletMetrics, NodeId " << nodeId << " was skipped due to reaching of MAX_UPDATE_TABLET_METRICS_IN_PROGRESS");
        Send(ev->Sender, new TEvLocal::TEvTabletMetricsAck);
    }
}

void THive::Handle(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    TNodeId nodeId = ev->Get()->NodeId;
    if (ConnectedNodes.insert(nodeId).second) {
        BLOG_W("Handle TEvInterconnect::TEvNodeConnected, NodeId " << nodeId << " Cookie " << ev->Cookie);
        UpdateCounterNodesConnected(+1);
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(nodeId));
    } else {
        BLOG_TRACE("Handle TEvInterconnect::TEvNodeConnected (duplicate), NodeId " << nodeId << " Cookie " << ev->Cookie);
    }
}

void THive::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    TNodeId nodeId = ev->Get()->NodeId;
    BLOG_W("Handle TEvInterconnect::TEvNodeDisconnected, NodeId " << nodeId);
    RemoveFromPingInProgress(nodeId);
    if (ConnectedNodes.erase(nodeId)) {
       UpdateCounterNodesConnected(-1);
    }
    Execute(CreateDisconnectNode(THolder<TEvInterconnect::TEvNodeDisconnected>(ev->Release().Release())));
}

void THive::Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
    THolder<TEvInterconnect::TNodeInfo>& node = ev->Get()->Node;
    if (node) {
        TEvInterconnect::TNodeInfo& nodeInfo = *node;
        NodesInfo[node->NodeId] = nodeInfo;
        TNodeInfo* hiveNodeInfo = FindNode(nodeInfo.NodeId);
        if (hiveNodeInfo != nullptr) {
            hiveNodeInfo->Location = nodeInfo.Location;
            hiveNodeInfo->LocationAcquired = true;
            BLOG_D("TEvInterconnect::TEvNodeInfo NodeId " << nodeInfo.NodeId << " Location " << GetLocationString(hiveNodeInfo->Location));
        }
    }
}

void THive::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
    for (const TEvInterconnect::TNodeInfo& node : ev->Get()->Nodes) {
        NodesInfo[node.NodeId] = node;
        auto dataCenterId = node.Location.GetDataCenterId();
        if (dataCenterId) {
            DataCenters[dataCenterId]; // just create entry in hash map
        }
    }
    Execute(CreateLoadEverything());
}

void THive::ScheduleDisconnectNode(THolder<TEvPrivate::TEvProcessDisconnectNode> event) {
    auto itCategory = event->Tablets.begin();
    if (itCategory != event->Tablets.end()) {
        TTabletCategoryInfo& category = GetTabletCategory(itCategory->first);
        TDuration spentTime = TActivationContext::Now() - event->StartTime;
        TDuration disconnectTimeout = TDuration::MilliSeconds(category.MaxDisconnectTimeout);
        if (disconnectTimeout > spentTime) {
            Schedule(disconnectTimeout - spentTime, event.Release());
        } else {
            Send(SelfId(), event.Release());
        }
    } else {
        KillNode(event->NodeId, event->Local);
    }
}

void THive::Handle(TEvPrivate::TEvKickTablet::TPtr &ev) {
    TFullTabletId tabletId(ev->Get()->TabletId);
    TTabletInfo* tablet = FindTablet(tabletId);
    if (tablet == nullptr) {
        BLOG_W("THive::Handle::TEvKickTablet" <<
                   " TabletId=" << tabletId <<
                   " tablet not found");
        return;
    }

    if (!tablet->IsAlive()) {
        BLOG_D("THive::Handle::TEvKickTablet" <<
                    " TabletId=" << tabletId <<
                    " tablet isn't alive");
        return;
    }

    BLOG_D("THive::Handle::TEvKickTablet TabletId=" << tabletId);
    TBestNodeResult result = FindBestNode(*tablet);
    if (std::holds_alternative<TTooManyTabletsStarting>(result)) {
        if (tablet->Node == nullptr || !tablet->Node->IsAllowedToRunTablet(*tablet)) {
            Execute(CreateRestartTablet(tabletId));
        }
    } else if (std::holds_alternative<TNodeInfo*>(result)) {
        TNodeInfo* node = std::get<TNodeInfo*>(result);
        if (node != tablet->Node && IsTabletMoveExpedient(*tablet, *node)) {
            Execute(CreateRestartTablet(tabletId));
        }
    } else {
        Execute(CreateRestartTablet(tabletId));
    }
}

void THive::Handle(TEvHive::TEvInitiateBlockStorage::TPtr& ev) {
    TTabletId tabletId = ev->Get()->TabletId;
    BLOG_D("THive::Handle::TEvInitiateBlockStorage TabletId=" << tabletId);
    TSideEffects sideEffects;
    sideEffects.Reset(SelfId());
    TLeaderTabletInfo* tablet = FindTabletEvenInDeleting(tabletId);
    if (tablet != nullptr) {
        if (tablet->IsDeleting()) {
            if (!tablet->InitiateBlockStorage(sideEffects, std::numeric_limits<ui32>::max())) {
                DeleteTabletWithoutStorage(tablet);
            }
        } else
        if (tablet->IsReadyToBlockStorage()) {
            tablet->InitiateBlockStorage(sideEffects);
        }
    }
    sideEffects.Complete(DEPRECATED_CTX);
}

void THive::Handle(TEvHive::TEvInitiateDeleteStorage::TPtr &ev) {
    TTabletId tabletId = ev->Get()->TabletId;
    BLOG_D("THive::Handle::TEvInitiateDeleteStorage TabletId=" << tabletId);
    TSideEffects sideEffects;
    sideEffects.Reset(SelfId());
    TLeaderTabletInfo* tablet = FindTabletEvenInDeleting(tabletId);
    if (tablet != nullptr) {
        tablet->InitiateDeleteStorage(sideEffects);
    }
    sideEffects.Complete(DEPRECATED_CTX);
}

void THive::Handle(TEvHive::TEvGetTabletStorageInfo::TPtr& ev) {
    TTabletId tabletId = ev->Get()->Record.GetTabletID();
    BLOG_D("THive::Handle::TEvGetTabletStorageInfo TabletId=" << tabletId);

    TLeaderTabletInfo* tablet = FindTabletEvenInDeleting(tabletId);
    if (tablet == nullptr) {
        // Tablet doesn't exist
        Send(
            ev->Sender,
            new TEvHive::TEvGetTabletStorageInfoResult(tabletId, NKikimrProto::ERROR, "Tablet doesn't exist"),
            0, ev->Cookie);
        return;
    }

    switch (tablet->State) {
    case ETabletState::Unknown:
    case ETabletState::StoppingInGroupAssignment:
        // Subscribing in these states doesn't make sense, as it will never complete
        BLOG_ERROR("Requesting TabletStorageInfo with tablet State="
                << ETabletStateName(tablet->State));
        Send(
            ev->Sender,
            new TEvHive::TEvGetTabletStorageInfoResult(tabletId, NKikimrProto::ERROR, "Tablet is in an unexpected state"),
            0, ev->Cookie);
        break;
    case ETabletState::Deleting:
    case ETabletState::GroupAssignment:
        // We need to subscribe until group assignment or deletion is finished
        tablet->StorageInfoSubscribers.emplace_back(ev->Sender);
        Send(ev->Sender, new TEvHive::TEvGetTabletStorageInfoRegistered(tabletId), 0, ev->Cookie);
        break;
    default:
        // Return what we have right now
        Send(
            ev->Sender,
            new TEvHive::TEvGetTabletStorageInfoResult(tabletId, *tablet->TabletStorageInfo),
            0, ev->Cookie);
        break;
    }
}

void THive::Handle(TEvents::TEvUndelivered::TPtr &ev) {
    BLOG_W("THive::Handle::TEvUndelivered Sender=" << ev->Sender << ", Type=" << ev->Get()->SourceType );
    switch (ev->Get()->SourceType) {
    case TEvLocal::EvBootTablet: {
        // restart boot of the tablet (on different node)
        TTabletId tabletId = ev->Cookie;
        TLeaderTabletInfo* tablet = FindTablet(tabletId);
        if (tablet != nullptr && tablet->IsStarting()) {
            Execute(CreateRestartTablet(tablet->GetFullTabletId()));
        }
        break;
    }
    case TEvLocal::EvPing: {
        TNodeId nodeId = ev->Cookie;
        TNodeInfo* node = FindNode(nodeId);
        NodePingsInProgress.erase(nodeId);
        if (node != nullptr && ev->Sender == node->Local) {
            if (node->IsDisconnecting()) {
                // ping continiousily until we fully disconnected from the node
                node->Ping();
            } else {
                KillNode(node->Id, node->Local);
            }
        }
        ProcessNodePingQueue();
        break;
    }
    };
}

void THive::Handle(TEvHive::TEvReassignTablet::TPtr &ev) {
    BLOG_D("THive::TEvReassignTablet " << ev->Get()->Record.ShortUtf8DebugString());
    TLeaderTabletInfo* tablet = FindTablet(ev->Get()->Record.GetTabletID());
    if (tablet != nullptr) {
        tablet->ChannelProfileReassignReason = ev->Get()->Record.GetReassignReason();
        std::bitset<MAX_TABLET_CHANNELS> channelProfileNewGroup;
        const auto& record(ev->Get()->Record);
        auto channels = tablet->GetChannelCount();
        for (ui32 channel : record.GetChannels()) {
            if (channel >= channels) {
                break;
            }
            channelProfileNewGroup.set(channel);
        }
        auto forcedGroupsSize = record.ForcedGroupIDsSize();
        if (forcedGroupsSize > 0) {
            TVector<NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters> groups;
            tablet->ChannelProfileNewGroup = channelProfileNewGroup;
            groups.resize(forcedGroupsSize);
            for (ui32 i = 0; i < forcedGroupsSize; ++i) {
                ui32 channel = record.GetChannels(i);
                Y_ABORT_UNLESS(channel < channels);
                auto parameters = BuildGroupParametersForChannel(*tablet, channel);
                const auto& groupParameters = parameters->GroupParameters;
                if (groupParameters.HasStoragePoolSpecifier()) {
                    groups[i].SetStoragePoolName(groupParameters.GetStoragePoolSpecifier().GetName());
                }
                if (groupParameters.HasErasureSpecies()) {
                    groups[i].SetErasureSpecies(groupParameters.GetErasureSpecies());
                }
                groups[i].SetGroupID(record.GetForcedGroupIDs(i));
            }
            Execute(CreateUpdateTabletGroups(tablet->Id, std::move(groups)));
        } else {
            Execute(CreateReassignGroups(tablet->Id, ev.Get()->Sender, channelProfileNewGroup));
        }
    }
}

void THive::OnActivateExecutor(const TActorContext&) {
    BLOG_D("THive::OnActivateExecutor");
    TDomainsInfo* domainsInfo = AppData()->DomainsInfo.Get();
    const TDomainsInfo::TDomain& domain = *domainsInfo->GetDomain();
    RootHiveId = domainsInfo->GetHive();
    HiveId = TabletID();
    HiveGeneration = Executor()->Generation();
    RootDomainKey = TSubDomainKey(domain.SchemeRoot, 1);
    RootDomainName = "/" + domain.Name;
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    ResourceProfiles = AppData()->ResourceProfiles ? AppData()->ResourceProfiles : new TResourceProfiles;
    BuildLocalConfig();
    ClusterConfig = AppData()->HiveConfig;
    SpreadNeighbours = ClusterConfig.GetSpreadNeighbours();
    NodeBrokerEpoch = TDuration::MicroSeconds(NKikimrNodeBroker::TConfig().GetEpochDuration());
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({NKikimrConsole::TConfigItem::HiveConfigItem, NKikimrConsole::TConfigItem::NodeBrokerConfigItem}));
    Execute(CreateInitScheme());
    if (!ResponsivenessPinger) {
        ResponsivenessPinger = new TTabletResponsivenessPinger(TabletCounters->Simple()[NHive::COUNTER_RESPONSE_TIME_USEC], TDuration::Seconds(1));
        ResponsivenessActorID = RegisterWithSameMailbox(ResponsivenessPinger);
    }
}

void THive::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}


void THive::AssignTabletGroups(TLeaderTabletInfo& tablet) {
    ui32 channels = tablet.GetChannelCount();
    THashSet<TString> storagePoolsToRefresh;
    // was this method called for the first time for this tablet?
    bool firstInvocation = tablet.ChannelProfileNewGroup.none();

    for (ui32 channelId = 0; channelId < channels; ++channelId) {
        if (firstInvocation || tablet.ChannelProfileNewGroup.test(channelId)) {
            tablet.ChannelProfileNewGroup.set(channelId);
            TStoragePoolInfo& storagePool = tablet.GetStoragePool(channelId);
            if (!storagePool.IsFresh() || storagePool.ConfigurationGeneration != ConfigurationGeneration) {
                storagePoolsToRefresh.insert(storagePool.Name);
            }
        }
    }

    if (!storagePoolsToRefresh.empty()) {
        // we need to refresh storage pool state from BSC
        TVector<THolder<NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters>> requests;
        for (TString storagePoolName : storagePoolsToRefresh) {
            TStoragePoolInfo& storagePool = GetStoragePool(storagePoolName);
            if (storagePool.AddTabletToWait(tablet.Id)) {
                THolder<NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters> item = storagePool.BuildRefreshRequest();
                ++storagePool.RefreshRequestInFlight;
                requests.emplace_back(std::move(item));
            }
        }
        if (!requests.empty()) {
            THolder<TEvBlobStorage::TEvControllerSelectGroups> ev = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
            NKikimrBlobStorage::TEvControllerSelectGroups& record = ev->Record;
            record.SetReturnAllMatchingGroups(true);
            for (auto& request : requests) {
                record.MutableGroupParameters()->AddAllocated(std::move(request).Release());
            }
            BLOG_D("THive::AssignTabletGroups TEvControllerSelectGroups tablet " << tablet.Id << " " << ev->Record.ShortDebugString());
            SendToBSControllerPipe(ev.Release());
        } else {
            BLOG_D("THive::AssignTabletGroups TEvControllerSelectGroups tablet " << tablet.Id << " waiting for response");
        }
    } else {
        // we ready to update tablet groups immediately
        BLOG_D("THive::AssignTabletGroups CreateUpdateTabletGroups tablet " << tablet.Id);
        Execute(CreateUpdateTabletGroups(tablet.Id));
    }
}

void THive::SendToBSControllerPipe(IEventBase* payload) {
    if (!BSControllerPipeClient) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        BSControllerPipeClient = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(), pipeConfig));
    }
    NTabletPipe::SendData(SelfId(), BSControllerPipeClient, payload);
}

void THive::SendToRootHivePipe(IEventBase* payload) {
    if (!RootHivePipeClient) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        RootHivePipeClient = Register(NTabletPipe::CreateClient(SelfId(), RootHiveId, pipeConfig));
    }
    NTabletPipe::SendData(SelfId(), RootHivePipeClient, payload);
}

void THive::RestartBSControllerPipe() {
    BLOG_D("THive::RestartBSControllerPipe");
    if (BSControllerPipeClient) {
        NTabletPipe::CloseClient(SelfId(), BSControllerPipeClient);
        BSControllerPipeClient = TActorId();
    }
    RequestPoolsInformation();
    for (auto it = Tablets.begin(); it != Tablets.end(); ++it) {
        TLeaderTabletInfo& tablet(it->second);
        if (tablet.IsReadyToAssignGroups()) {
            tablet.ResetTabletGroupsRequests();
            tablet.InitiateAssignTabletGroups();
        }
    }
}

void THive::RestartRootHivePipe() {
    BLOG_D("THive::RestartRootHivePipe");
    if (RootHivePipeClient) {
        NTabletPipe::CloseClient(SelfId(), RootHivePipeClient);
        RootHivePipeClient = TActorId();
    }
    // trying to retry for free sequence request
    if (RequestingSequenceNow) {
        RequestFreeSequence();
    }
    // trying to restart migration
    if (MigrationState == NKikimrHive::EMigrationState::MIGRATION_ACTIVE) {
        SendToRootHivePipe(new TEvHive::TEvSeizeTablets(MigrationFilter));
    }
}

void THive::Handle(TEvTabletBase::TEvBlockBlobStorageResult::TPtr &ev) {
    Execute(CreateBlockStorageResult(ev));
}

void THive::Handle(TEvTabletBase::TEvDeleteTabletResult::TPtr &ev) {
    Execute(CreateDeleteTabletResult(ev));
}

template <>
TNodeInfo* THive::SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM>(const std::vector<THive::TSelectedNode>& selectedNodes) {
    if (selectedNodes.empty()) {
        return nullptr;
    }
    return selectedNodes[TAppData::RandomProvider->GenRand() % selectedNodes.size()].Node;
}

template <>
TNodeInfo* THive::SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM>(const std::vector<THive::TSelectedNode>& selectedNodes) {
    if (selectedNodes.empty()) {
        return nullptr;
    }
    double sumUsage = 0;
    double maxUsage = 0;
    for (const TSelectedNode& selectedNode : selectedNodes) {
        double usage = selectedNode.Usage;
        sumUsage += usage;
        maxUsage = std::max(maxUsage, usage);
    }
    double sumAvail = maxUsage * selectedNodes.size() - sumUsage;
    if (sumAvail > 0) {
        double pos = TAppData::RandomProvider->GenRandReal2() * sumAvail;
        for (const TSelectedNode& selectedNode : selectedNodes) {
            double avail = maxUsage - selectedNode.Usage;
            if (pos < avail) {
                return selectedNode.Node;
            } else {
                pos -= avail;
            }
        }
    }
    return SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM>(selectedNodes);
}

template <>
TNodeInfo* THive::SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_EXACT_MIN>(const std::vector<THive::TSelectedNode>& selectedNodes) {
    if (selectedNodes.empty()) {
        return nullptr;
    }
    auto itMin = std::min_element(selectedNodes.begin(), selectedNodes.end());
    return itMin->Node;
}

template <>
TNodeInfo* THive::SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P>(const std::vector<THive::TSelectedNode>& selectedNodes) {
    if (selectedNodes.empty()) {
        return nullptr;
    }
    std::vector<TSelectedNode> nodes(selectedNodes);
    auto itNode = nodes.begin();
    auto itPartition = itNode;
    size_t percent7 = std::max<size_t>(nodes.size() * 7 / 100, 1);
    std::advance(itPartition, percent7);
    std::nth_element(nodes.begin(), itPartition, nodes.end());
    std::advance(itNode, TAppData::RandomProvider->GenRand64() % percent7);
    return itNode->Node;
}

TVector<THive::TSelectedNode> THive::SelectMaxPriorityNodes(TVector<TSelectedNode> selectedNodes, const TTabletInfo& tablet) const
{
    i32 priority = std::numeric_limits<i32>::min();
    for (const TSelectedNode& selectedNode : selectedNodes) {
        priority = std::max(priority, selectedNode.Node->GetPriorityForTablet(tablet));
    }

    auto it = std::partition(selectedNodes.begin(), selectedNodes.end(), [&] (const TSelectedNode& selectedNode) {
        return selectedNode.Node->GetPriorityForTablet(tablet) == priority;
    });

    selectedNodes.erase(it, selectedNodes.end());

    return selectedNodes;
}

THive::TBestNodeResult THive::FindBestNode(const TTabletInfo& tablet, TNodeId suggestedNodeId) {
    BLOG_D("[FBN] Finding best node for tablet " << tablet.ToString());
    BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " family " << tablet.FamilyString());

    if (tablet.PreferredNodeId != 0) {
        TNodeInfo* node = FindNode(tablet.PreferredNodeId);
        if (node != nullptr) {
            if (node->IsAlive() && node->IsAllowedToRunTablet(tablet) && node->IsAbleToScheduleTablet() && node->IsAbleToRunTablet(tablet)) {
                BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " choose node " << node->Id << " because of preferred node");
                return node;
            } else {
                BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " preferred unavailable node " << node->Id);
                tablet.BootState = TStringBuilder() << "Preferred unavailable node " << node->Id;
                return TNoNodeFound();
            }
        }
    }

    if (suggestedNodeId != 0) {
        TNodeInfo* node = FindNode(suggestedNodeId);
        if (node && node->IsAlive() && node->IsAllowedToRunTablet(tablet) && node->IsAbleToScheduleTablet() && node->IsAbleToRunTablet(tablet)) {
            BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " choose node " << node->Id << " because of suggested node");
            return node;
        }
    }

    /*
    TNodeInfo* bestNodeInfo = nullptr;
    double bestUsage = 0;
    if (tablet.IsAlive() && tablet.Node->IsAllowedToRunTablet(tablet) && !tablet.Node->IsOverloaded()) {
        bestNodeInfo = &(Nodes.find(tablet.Node->Id)->second);
        bestUsage = tablet.Node->GetNodeUsageForTablet(tablet);
        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " starting with usage " << Sprintf("%.9f", bestUsage) << " of node " << bestNodeInfo->Id);
    }
    */

    TTabletDebugState debugState;
    std::vector<NKikimrHive::TDataCentersGroup> dataCentersGroupsHolder;
    std::vector<const NKikimrHive::TDataCentersGroup*> dataCentersGroupsPointers;
    TArrayRef<const NKikimrHive::TDataCentersGroup*> dataCentersGroups; // std::span

    if (tablet.IsLeader()) {
        const TLeaderTabletInfo& leader(tablet.GetLeader());
        dataCentersGroups = TArrayRef<const NKikimrHive::TDataCentersGroup*>(
                    const_cast<const NKikimrHive::TDataCentersGroup**>(leader.DataCentersPreference.GetDataCentersGroups().data()),
                    leader.DataCentersPreference.GetDataCentersGroups().size());
        if (dataCentersGroups.empty()) {
            dataCentersGroups = GetDefaultDataCentersPreference(leader.Type);
        }
        if (dataCentersGroups.empty()) {
            if (leader.Category && leader.Category->StickTogetherInDC) {
                std::unordered_map<TDataCenterId, ui32> dcTablets;
                for (TLeaderTabletInfo* tab : leader.Category->Tablets) {
                    if (tab->IsAlive()) {
                        TDataCenterId dc = tab->Node->GetDataCenter();
                        dcTablets[dc]++;
                    }
                }
                if (!dcTablets.empty()) {
                    dataCentersGroupsHolder.resize(dcTablets.size());
                    dataCentersGroupsPointers.resize(dcTablets.size());
                    std::vector<TDataCenterId> dcs;
                    for (const auto& [dc, count] : dcTablets) {
                        dcs.push_back(dc);
                    }
                    std::sort(dcs.begin(), dcs.end(), [&](TDataCenterId a, TDataCenterId b) -> bool {
                        return dcTablets[a] > dcTablets[b];
                    });
                    for (size_t i = 0; i < dcs.size(); ++i) {
                        dataCentersGroupsHolder[i].AddDataCenter(dcs[i]);
                        dataCentersGroupsHolder[i].AddDataCenterNum(DataCenterFromString(dcs[i]));
                        dataCentersGroupsPointers[i] = dataCentersGroupsHolder.data() + i;
                    }
                    dataCentersGroups = TArrayRef<const NKikimrHive::TDataCentersGroup*>(dataCentersGroupsPointers.data(), dcTablets.size());
                }
            }
        }
        if (!dataCentersGroups.empty()) {
            BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " using DC preference: " << dataCentersGroups);
        }
    }

    std::vector<std::vector<TNodeInfo*>> candidateGroups;
    candidateGroups.resize(dataCentersGroups.size() + 1);
    std::unordered_map<TDataCenterId, std::vector<TNodeInfo*>*> indexDC2Group;
    for (size_t numGroup = 0; numGroup < dataCentersGroups.size(); ++numGroup) {
        const NKikimrHive::TDataCentersGroup* dcGroup = dataCentersGroups[numGroup];
        if (dcGroup->DataCenterSize()) {
            for (TDataCenterId dc : dcGroup->GetDataCenter()) {
                indexDC2Group[dc] = candidateGroups.data() + numGroup;
            }
        } else {
            for (const ui64 dcId : dcGroup->GetDataCenterNum()) {
                indexDC2Group[DataCenterToString(dcId)] = candidateGroups.data() + numGroup;
            }
        }
    }
    for (auto it = Nodes.begin(); it != Nodes.end(); ++it) {
        TNodeInfo* nodeInfo = &it->second;
        if (nodeInfo->IsAlive()) {
            TDataCenterId dataCenterId = nodeInfo->GetDataCenter();
            auto itDataCenter = indexDC2Group.find(dataCenterId);
            if (itDataCenter != indexDC2Group.end()) {
                itDataCenter->second->push_back(nodeInfo);
            } else {
                candidateGroups.back().push_back(nodeInfo);
            }
        } else {
            BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " node " << nodeInfo->Id << " is not alive");
            debugState.NodesDead++;
        }
    }

    TVector<TSelectedNode> selectedNodes;
    bool thereAreNodesWithManyStarts = false;

    for (auto itCandidateNodes = candidateGroups.begin(); itCandidateNodes != candidateGroups.end(); ++itCandidateNodes) {
        const std::vector<TNodeInfo*>& candidateNodes(*itCandidateNodes);
        if (candidateGroups.size() > 1) {
            BLOG_TRACE("[FBN] Tablet " << tablet.ToString()
                       << " checking candidates group " << (itCandidateNodes - candidateGroups.begin() + 1)
                       << " of " << candidateGroups.size());
        }

        selectedNodes.clear();
        selectedNodes.reserve(candidateNodes.size());

        for (auto it = candidateNodes.begin(); it != candidateNodes.end(); ++it) {
            TNodeInfo& nodeInfo = *(*it);
            if (nodeInfo.IsAllowedToRunTablet(tablet, &debugState)) {
                if (nodeInfo.IsAbleToScheduleTablet()) {
                    if (nodeInfo.IsAbleToRunTablet(tablet, &debugState)) {
                        double usage = nodeInfo.GetNodeUsageForTablet(tablet);
                        selectedNodes.emplace_back(usage, &nodeInfo);
                        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " selected usage " << Sprintf("%.9f", usage) << " of node " << nodeInfo.Id);
                    } else {
                        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " node " << nodeInfo.Id << " is not able to run the tablet");
                    }
                } else {
                    BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " node " << nodeInfo.Id << " is not able to schedule the tablet");
                    thereAreNodesWithManyStarts = true;
                    if (GetBootStrategy() == NKikimrConfig::THiveConfig::HIVE_BOOT_STRATEGY_BALANCED) {
                        tablet.BootState = BootStateTooManyStarting;
                        return TTooManyTabletsStarting();
                    }
                }
            } else {
                BLOG_TRACE("[FBN] Node " << nodeInfo.Id << " is not allowed"
                            << " to run the tablet " << tablet.ToString()
                            << " node domains " << nodeInfo.ServicedDomains
                            << " tablet object domain " << tablet.GetLeader().ObjectDomain
                            << " tablet allowed domains " << tablet.GetNodeFilter().AllowedDomains
                            << " tablet effective allowed domains " << tablet.GetNodeFilter().GetEffectiveAllowedDomains());
            }
        }
        if (!selectedNodes.empty()) {
            break;
        }
    }
    BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " selected nodes count " << selectedNodes.size());
    if (selectedNodes.empty() && thereAreNodesWithManyStarts) {
        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " all available nodes are booting too many tablets");
        return TTooManyTabletsStarting();
    }

    TNodeInfo* selectedNode = nullptr;
    if (!selectedNodes.empty()) {
        selectedNodes = SelectMaxPriorityNodes(std::move(selectedNodes), tablet);
        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " selected max priority nodes count " << selectedNodes.size());

        switch (GetNodeSelectStrategy()) {
            case NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM:
                selectedNode = SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_WEIGHTED_RANDOM>(selectedNodes);
                break;
            case NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_EXACT_MIN:
                selectedNode = SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_EXACT_MIN>(selectedNodes);
                break;
            case NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P:
                selectedNode = SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM_MIN_7P>(selectedNodes);
                break;
            case NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM:
            default:
                selectedNode = SelectNode<NKikimrConfig::THiveConfig::HIVE_NODE_SELECT_STRATEGY_RANDOM>(selectedNodes);
                break;
        }
    }
    if (selectedNode != nullptr) {
        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " selected node " << selectedNode->Id);
        tablet.BootState = BootStateStarting;
        return selectedNode;
    } else {
        BLOG_TRACE("[FBN] Tablet " << tablet.ToString() << " no node was selected");

        ui32 nodesLeft = Nodes.size();

        if (tablet.IsFollower() && debugState.LeaderNotRunning) {
            tablet.BootState = BootStateLeaderNotRunning;
            return TNoNodeFound();
        }
        if (debugState.NodesDead == nodesLeft) {
            tablet.BootState = BootStateAllNodesAreDead;
            return TNoNodeFound();
        }
        nodesLeft -= debugState.NodesDead;
        if (debugState.NodesDown == nodesLeft) {
            tablet.BootState = BootStateAllNodesAreDeadOrDown;
            return TNoNodeFound();
        }
        nodesLeft -= debugState.NodesDown;
        if (debugState.NodesNotAllowed + debugState.NodesInDatacentersNotAllowed == nodesLeft) {
            tablet.BootState = BootStateNoNodesAllowedToRun;
            return TNoNodeFound();
        }
        nodesLeft -= debugState.NodesNotAllowed;
        nodesLeft -= debugState.NodesInDatacentersNotAllowed;
        if (debugState.NodesWithSomeoneFromOurFamily == nodesLeft) {
            tablet.BootState = BootStateWeFilledAllAvailableNodes;
            return TNoNodeFound();
        }
        nodesLeft -= debugState.NodesWithSomeoneFromOurFamily;
        if (debugState.NodesWithoutDomain == nodesLeft) {
            tablet.BootState = TStringBuilder() << "Can't find domain " << tablet.GetNodeFilter().GetEffectiveAllowedDomains();
            return TNoNodeFound();
        }
        nodesLeft -= debugState.NodesWithoutDomain;
        if (tablet.IsFollower() && debugState.NodesFilledWithDatacenterFollowers == nodesLeft) {
            tablet.BootState = BootStateNotEnoughDatacenters;
            return TNoNodeFound();
        }
        if (debugState.NodesWithoutResources == nodesLeft) {
            tablet.BootState = BootStateNotEnoughResources;
            return TNoNodeFound();
        }
        if (debugState.NodesWithoutLocation == nodesLeft) {
            tablet.BootState = BootStateNodesLocationUnknown;
            return TNoNodeFound();
        }

        TStringBuilder state;

        if (debugState.LeaderNotRunning) {
            state << "LeaderNotRunning;";
        }
        if (debugState.NodesDead) {
            state << "NodesDead:" << debugState.NodesDead << ";";
        }
        if (debugState.NodesDown) {
            state << "NodesDown:" << debugState.NodesDown << ";";
        }
        if (debugState.NodesNotAllowed) {
            state << "NodesNotAllowed:" << debugState.NodesNotAllowed << ";";
        }
        if (debugState.NodesInDatacentersNotAllowed) {
            state << "NodesInDatacentersNotAllowed:" << debugState.NodesInDatacentersNotAllowed << ";";
        }
        if (debugState.NodesWithLeaderNotLocal) {
            state << "NodesWithLeaderNotLocal:" << debugState.NodesWithLeaderNotLocal << ";";
        }
        if (debugState.NodesWithoutDomain) {
            state << "NodesWithoutDomain:" << debugState.NodesWithoutDomain << ";";
        }
        if (debugState.NodesFilledWithDatacenterFollowers) {
            state << "NodesFilledWithDatacenterFollowers:" << debugState.NodesFilledWithDatacenterFollowers << ";";
        }
        if (debugState.NodesWithoutResources) {
            state << "NodesWithoutResources:" << debugState.NodesWithoutResources << ";";
        }
        if (debugState.NodesWithSomeoneFromOurFamily) {
            state << "NodesWithSomeoneFromOurFamily:" << debugState.NodesWithSomeoneFromOurFamily << ";";
        }
        if (debugState.NodesWithoutLocation) {
            state << "NodesWithoutLocation:" << debugState.NodesWithoutLocation << ";";
        }
        tablet.BootState = state;

        return TNoNodeFound();
    }
}

const TNodeLocation& THive::GetNodeLocation(TNodeId nodeId) const {
    auto it = NodesInfo.find(nodeId);
    if (it != NodesInfo.end())
        return it->second.Location;
    static TNodeLocation defaultLocation;
    return defaultLocation;
}

TNodeInfo& THive::GetNode(TNodeId nodeId) {
    auto it = Nodes.find(nodeId);
    if (it == Nodes.end()) {
        it = Nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, *this)).first;
        TabletCounters->Simple()[NHive::COUNTER_NODES_TOTAL].Add(1);
    }
    return it->second;
}

TNodeInfo* THive::FindNode(TNodeId nodeId) {
    auto it = Nodes.find(nodeId);
    if (it == Nodes.end())
        return nullptr;
    return &it->second;
}

TLeaderTabletInfo& THive::GetTablet(TTabletId tabletId) {
    auto it = Tablets.find(tabletId);
    if (it == Tablets.end()) {
        it = Tablets.emplace(std::piecewise_construct, std::tuple<TTabletId>(tabletId), std::tuple<TTabletId, THive&>(tabletId, *this)).first;
        UpdateCounterTabletsTotal(+1);
    }
    return it->second;
}

TLeaderTabletInfo* THive::FindTablet(TTabletId tabletId) {
    auto it = Tablets.find(tabletId);
    if (it == Tablets.end() || it->second.IsDeleting())
        return nullptr;
    return &it->second;
}

TLeaderTabletInfo* THive::FindTabletEvenInDeleting(TTabletId tabletId) {
    auto it = Tablets.find(tabletId);
    if (it == Tablets.end())
        return nullptr;
    return &it->second;
}

TTabletInfo& THive::GetTablet(TTabletId tabletId, TFollowerId followerId) {
    TLeaderTabletInfo& leader = GetTablet(tabletId);
    return leader.GetTablet(followerId);
}

TTabletInfo* THive::FindTablet(TTabletId tabletId, TFollowerId followerId) {
    TLeaderTabletInfo* leader = FindTablet(tabletId);
    if (leader == nullptr) {
        return nullptr;
    }
    return leader->FindTablet(followerId);
}

TTabletInfo* THive::FindTabletEvenInDeleting(TTabletId tabletId, TFollowerId followerId) {
    TLeaderTabletInfo* leader = FindTabletEvenInDeleting(tabletId);
    if (leader == nullptr) {
        return nullptr;
    }
    return leader->FindTablet(followerId);
}

TStoragePoolInfo& THive::GetStoragePool(const TString& name) {
    auto it = StoragePools.find(name);
    if (it == StoragePools.end()) {
        it = StoragePools.emplace(std::piecewise_construct, std::tuple<TString>(name), std::tuple<TString, THive*>(name, this)).first;
    }
    return it->second;
}

TStoragePoolInfo* THive::FindStoragePool(const TString& name) {
    auto it = StoragePools.find(name);
    if (it == StoragePools.end()) {
        return nullptr;
    }
    return &it->second;
}

TDomainInfo* THive::FindDomain(TSubDomainKey key) {
    auto it = Domains.find(key);
    if (it == Domains.end()) {
        return nullptr;
    }
    return &it->second;
}

const TDomainInfo* THive::FindDomain(TSubDomainKey key) const {
    auto it = Domains.find(key);
    if (it == Domains.end()) {
        return nullptr;
    }
    return &it->second;
}

void THive::DeleteTablet(TTabletId tabletId) {
    auto it = Tablets.find(tabletId);
    if (it != Tablets.end()) {
        TLeaderTabletInfo& tablet(it->second);
        tablet.BecomeStopped();
        for (TFollowerTabletInfo& follower : tablet.Followers) {
            follower.BecomeStopped();
        }
        ReportDeletedToWhiteboard(tablet);
        tablet.ReleaseAllocationUnits();
        OwnerToTablet.erase(tablet.Owner);
        if (tablet.Category) {
            tablet.Category->Tablets.erase(&tablet);
        }
        auto itObj = ObjectToTabletMetrics.find(tablet.ObjectId);
        if (itObj != ObjectToTabletMetrics.end()) {
            itObj->second.DecreaseCount();
            if (itObj->second.Counter == 0) {
                ObjectToTabletMetrics.erase(itObj);
            }
        }
        auto itType = TabletTypeToTabletMetrics.find(tablet.Type);
        if (itType != TabletTypeToTabletMetrics.end()) {
            itType->second.DecreaseCount();
            if (itType->second.Counter == 0) {
                TabletTypeToTabletMetrics.erase(itType);
            }
        }
        for (auto nt = Nodes.begin(); nt != Nodes.end(); ++nt) {
            for (auto st = nt->second.Tablets.begin(); st != nt->second.Tablets.end(); ++st) {
                Y_ENSURE_LOG(st->second.count(&tablet) == 0, " Deleting tablet found on node " << nt->first << " in state " << TTabletInfo::EVolatileStateName(st->first));
            }
            Y_ENSURE_LOG(nt->second.LockedTablets.count(&tablet) == 0, " Deleting tablet found on node " << nt->first << " in locked set");
        }
        const i64 tabletsTotalDiff = -1 - (tablet.Followers.size());
        UpdateCounterTabletsTotal(tabletsTotalDiff);
        UpdateDomainTabletsTotal(tablet.ObjectDomain, tabletsTotalDiff);
        Tablets.erase(it);
    }
}

void THive::DeleteNode(TNodeId nodeId) {
    TabletCounters->Simple()[NHive::COUNTER_NODES_TOTAL].Sub(1);
    Nodes.erase(nodeId);
}

TTabletCategoryInfo& THive::GetTabletCategory(TTabletCategoryId tabletCategoryId) {
    auto it = TabletCategories.find(tabletCategoryId);
    if (it == TabletCategories.end())
        it = TabletCategories.emplace(tabletCategoryId, tabletCategoryId).first; // emplace()
    return it->second;
}

void THive::KillNode(TNodeId nodeId, const TActorId& local) {
    TNodeInfo* node = FindNode(nodeId);
    if (node != nullptr) {
        TVector<TTabletInfo*> tabletsToKill;
        for (const auto& t : node->Tablets) {
            for (TTabletInfo* tablet : t.second) {
                tabletsToKill.push_back(tablet);
            }
        }
        for (TTabletInfo* tablet : tabletsToKill) {
            Execute(CreateRestartTablet(tablet->GetFullTabletId()));
        }
    }
    Execute(CreateKillNode(nodeId, local));
}

void THive::UpdateDomainTabletsTotal(const TSubDomainKey& objectDomain, i64 tabletsTotalDiff) {
    if (objectDomain) {
        Domains[objectDomain].TabletsTotal += tabletsTotalDiff;
    }
}

void THive::UpdateDomainTabletsAlive(const TSubDomainKey& objectDomain, i64 tabletsAliveDiff, const TSubDomainKey& tabletNodeDomain) {
    if (objectDomain) {
        Domains[objectDomain].TabletsAlive += tabletsAliveDiff;
        if (tabletNodeDomain == objectDomain) {
            Domains[objectDomain].TabletsAliveInObjectDomain += tabletsAliveDiff;
        }
    }
}

void THive::SetCounterTabletsTotal(ui64 tabletsTotal) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_TABLETS_TOTAL];
        TabletsTotal = tabletsTotal;
        counter.Set(TabletsTotal);
        TabletCounters->Simple()[NHive::COUNTER_STATE_DONE].Set(TabletsTotal == TabletsAlive ? 1 : 0);
    }
}

void THive::UpdateCounterTabletsTotal(i64 tabletsTotalDiff) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_TABLETS_TOTAL];
        TabletsTotal = counter.Get() + tabletsTotalDiff;
        counter.Set(TabletsTotal);
        TabletCounters->Simple()[NHive::COUNTER_STATE_DONE].Set(TabletsTotal == TabletsAlive ? 1 : 0);
    }
}

void THive::UpdateCounterTabletsAlive(i64 tabletsAliveDiff) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_TABLETS_ALIVE];
        TabletsAlive = counter.Get() + tabletsAliveDiff;
        counter.Set(TabletsAlive);
        TabletCounters->Simple()[NHive::COUNTER_STATE_DONE].Set(TabletsTotal == TabletsAlive ? 1 : 0);
    }
}

void THive::UpdateCounterBootQueueSize(ui64 bootQueueSize) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_BOOTQUEUE_SIZE];
        counter.Set(bootQueueSize);
    }
}
void THive::UpdateCounterEventQueueSize(i64 eventQueueSizeDiff) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_EVENTQUEUE_SIZE];
        auto newValue = counter.Get() + eventQueueSizeDiff;
        counter.Set(newValue);
    }
}

void THive::UpdateCounterNodesConnected(i64 nodesConnectedDiff) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_NODES_CONNECTED];
        auto newValue = counter.Get() + nodesConnectedDiff;
        counter.Set(newValue);
    }
}

void THive::UpdateCounterTabletsStarting(i64 tabletsStartingDiff) {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_TABLETS_STARTING];
        auto newValue = counter.Get() + tabletsStartingDiff;
        counter.Set(newValue);
    }
}

void THive::UpdateCounterPingQueueSize() {
    if (TabletCounters != nullptr) {
        auto& counter = TabletCounters->Simple()[NHive::COUNTER_PINGQUEUE_SIZE];
        counter.Set(NodePingQueue.size());
    }
}

void THive::UpdateCounterTabletChannelHistorySize() {
    auto& histogram = TabletCounters->Percentile()[NHive::COUNTER_TABLET_CHANNEL_HISTORY_SIZE];
    histogram.Clear();
    for (const auto& [_, tablet] : Tablets) {
        for (const auto& channel : tablet.TabletStorageInfo->Channels) {
            histogram.IncrementFor(channel.History.size());
        }
    }
}

void THive::RecordTabletMove(const TTabletMoveInfo& moveInfo) {
    TabletMoveHistory.PushBack(moveInfo);
    TabletCounters->Cumulative()[NHive::COUNTER_TABLETS_MOVED].Increment(1);
    if (TabletMoveSamplesForLog.size() < MOVE_SAMPLES_PER_LOG_ENTRY) {
        TabletMoveSamplesForLog.push_back(moveInfo);
        std::push_heap(TabletMoveSamplesForLog.begin(), TabletMoveSamplesForLog.end(), std::greater<TTabletMoveInfo>{});
    } else if (moveInfo.Priority > TabletMoveSamplesForLog.front().Priority) {
        TabletMoveSamplesForLog.push_back(moveInfo);
        std::pop_heap(TabletMoveSamplesForLog.begin(), TabletMoveSamplesForLog.end(), std::greater<TTabletMoveInfo>{});
        TabletMoveSamplesForLog.pop_back();
    }
    TabletMovesByTypeForLog[moveInfo.TabletType]++;
    if (!LogTabletMovesScheduled) {
        LogTabletMovesScheduled = true;
        LogTabletMovesSchedulingTime = moveInfo.Timestamp;
        Schedule(TDuration::Minutes(5), new TEvPrivate::TEvLogTabletMoves());
    }
}

bool THive::DomainHasNodes(const TSubDomainKey &domainKey) const {
    return !DomainsView.IsEmpty(domainKey);
}

TResourceNormalizedValues THive::GetStDevResourceValues() const {
    TVector<TResourceNormalizedValues> values;
    values.reserve(Nodes.size());
    for (const auto& ni : Nodes) {
        if (ni.second.IsAlive() && !ni.second.Down) {
            values.push_back(NormalizeRawValues(ni.second.GetResourceCurrentValues(), ni.second.GetResourceMaximumValues()));
        }
    }
    return GetStDev(values);
}

bool THive::IsTabletMoveExpedient(const TTabletInfo& tablet, const TNodeInfo& node) const {
    if (!tablet.IsAlive()) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " to " << node.Id
                   << " is expedient because the tablet is not alive");
        return true;
    }
    if (tablet.Node->Freeze) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is not expedient because the source node is freezed");
        return false;
    }
    if (node.Freeze) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is not expedient because the target node is freezed");
        return false;
    }
    if (tablet.Node->Down) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is expedient because the node is down");
        return true;
    }
    if (!tablet.Node->IsAllowedToRunTablet(tablet)) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is expedient because the current node is unappropriate target for the tablet");
        return true;
    }
    if (tablet.Node->Id == node.Id) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is not expedient because node is the same");
        return false;
    }
    if (tablet.Node->IsOverloaded() && !node.IsOverloaded()) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is forcefully expedient because source node is overloaded");
        return true;
    }
    if (GetSpreadNeighbours() && tablet.Node->GetTabletNeighboursCount(tablet) > node.GetTabletNeighboursCount(tablet)) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is expedient because it spreads neighbours");
        return true;
    }

    if (!GetCheckMoveExpediency()) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is forcefully expedient because of the setting");
        return true;
    }

    TVector<TResourceNormalizedValues> values;
    std::size_t oldNode = std::numeric_limits<std::size_t>::max();
    std::size_t newNode = std::numeric_limits<std::size_t>::max();
    values.reserve(Nodes.size());
    for (const auto& ni : Nodes) {
        if (ni.second.IsAlive() && !ni.second.Down) {
            if (ni.first == node.Id)
                newNode = values.size();
            if (ni.first == tablet.Node->Id)
                oldNode = values.size();
            values.push_back(NormalizeRawValues(ni.second.GetResourceCurrentValues(), ni.second.GetResourceMaximumValues()));
        }
    }

    if (oldNode == std::numeric_limits<std::size_t>::max()
            || newNode == std::numeric_limits<std::size_t>::max()) {
        return false;
    }

    auto tabletResources = tablet.GetResourceCurrentValues();
//    NMetrics::TResourceMetrics::TResourceNormalizedValues oldValues = values[oldNode];
//    NMetrics::TResourceMetrics::TResourceNormalizedValues newValues = values[newNode] + NMetrics::TResourceMetrics::Normalize(tabletResources, node.GetResourceMaximumValues());
//    return sum(newValues) < sum(oldValues);

    TResourceNormalizedValues beforeStDev = GetStDev(values);
    values[oldNode] -= NormalizeRawValues(tabletResources, tablet.Node->GetResourceMaximumValues());
    values[newNode] += NormalizeRawValues(tabletResources, node.GetResourceMaximumValues());
    TResourceNormalizedValues afterStDev = GetStDev(values);
    tablet.FilterRawValues(beforeStDev);
    tablet.FilterRawValues(afterStDev);
    double before = max(beforeStDev);
    double after = max(afterStDev);
    bool result = after < before;
    if (result) {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is expedient, beforeStDev " << beforeStDev << " afterStDev " << afterStDev);
    } else {
        BLOG_TRACE("[TME] Move of tablet " << tablet.ToString() << " from " << tablet.NodeId << " to " << node.Id
                   << " is not expedient, beforeStDev " << beforeStDev << " afterStDev " << afterStDev);
    }
    return result;
}

void THive::FillTabletInfo(NKikimrHive::TEvResponseHiveInfo& response, ui64 tabletId, const TLeaderTabletInfo *info, const NKikimrHive::TEvRequestHiveInfo &req) {
    if (info) {
        TInstant now = TActivationContext::Now();
        TInstant restartsBarrierTime = now - GetTabletRestartsPeriod();
        auto& tabletInfo = *response.AddTablets();
        tabletInfo.SetTabletID(tabletId);
        tabletInfo.SetTabletType(info->Type);
        tabletInfo.SetObjectId(info->ObjectId.second);
        tabletInfo.SetState(static_cast<ui32>(info->State));
        tabletInfo.SetTabletBootMode(info->BootMode);
        tabletInfo.SetVolatileState(info->GetVolatileState());
        tabletInfo.SetNodeID(info->NodeId);
        tabletInfo.MutableTabletOwner()->SetOwner(info->Owner.first);
        tabletInfo.MutableTabletOwner()->SetOwnerIdx(info->Owner.second);
        tabletInfo.SetGeneration(info->KnownGeneration);
        tabletInfo.MutableObjectDomain()->CopyFrom(info->ObjectDomain);
        if (info->BalancerPolicy != NKikimrHive::EBalancerPolicy::POLICY_BALANCE) {
            tabletInfo.SetBalancerPolicy(info->BalancerPolicy);
        }
        if (!info->IsRunning() && info->Statistics.HasLastAliveTimestamp()) {
            tabletInfo.SetLastAliveTimestamp(info->Statistics.GetLastAliveTimestamp());
        }
        tabletInfo.SetRestartsPerPeriod(info->GetRestartsPerPeriod(restartsBarrierTime));
        if (req.GetReturnMetrics()) {
            tabletInfo.MutableMetrics()->CopyFrom(info->GetResourceValues());
        }
        if (info->InWaitQueue) {
            tabletInfo.SetInWaitQueue(true);
        }
        if (req.GetReturnChannelHistory()) {
            for (const auto& channel : info->TabletStorageInfo->Channels) {
                auto& tabletChannel = *tabletInfo.AddTabletChannels();
                for (const auto& history : channel.History) {
                    auto& tabletHistory = *tabletChannel.AddHistory();
                    tabletHistory.SetGroup(history.GroupID);
                    tabletHistory.SetGeneration(history.FromGeneration);
                    tabletHistory.SetTimestamp(history.Timestamp.MilliSeconds());
                }
            }
        }
        if (req.GetReturnFollowers()) {
            for (const auto& follower : info->Followers) {
                if (req.HasFollowerID() && req.GetFollowerID() != follower.Id)
                    continue;
                NKikimrHive::TTabletInfo& tabletInfo = *response.AddTablets();
                tabletInfo.SetTabletID(tabletId);
                tabletInfo.SetTabletType(info->Type);
                tabletInfo.SetFollowerID(follower.Id);
                tabletInfo.SetVolatileState(follower.GetVolatileState());
                tabletInfo.SetNodeID(follower.NodeId);
                tabletInfo.MutableTabletOwner()->SetOwner(info->Owner.first);
                tabletInfo.MutableTabletOwner()->SetOwnerIdx(info->Owner.second);
                tabletInfo.MutableObjectDomain()->CopyFrom(info->ObjectDomain);
                if (!follower.IsRunning()) {
                    tabletInfo.SetLastAliveTimestamp(follower.Statistics.GetLastAliveTimestamp());
                }
                tabletInfo.SetRestartsPerPeriod(follower.GetRestartsPerPeriod(restartsBarrierTime));
                if (req.GetReturnMetrics()) {
                    tabletInfo.MutableMetrics()->CopyFrom(follower.GetResourceValues());
                }
            }
        }
    }
}

void THive::Handle(TEvHive::TEvRequestHiveInfo::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();
    if (record.HasTabletID()) {
        TTabletId tabletId = record.GetTabletID();
        NKikimrHive::TForwardRequest forwardRequest;
        if (CheckForForwardTabletRequest(tabletId, forwardRequest)) {
            response->Record.MutableForwardRequest()->CopyFrom(forwardRequest);
        }
        const TLeaderTabletInfo* tablet = FindTablet(tabletId);
        if (tablet) {
            FillTabletInfo(response->Record, record.GetTabletID(), tablet, record);
        } else {
            BLOG_W("Can't find the tablet from RequestHiveInfo(TabletID=" << tabletId << ")");
        }
    } else {
        response->Record.MutableTablets()->Reserve(Tablets.size());
        for (auto it = Tablets.begin(); it != Tablets.end(); ++it) {
            if (record.HasTabletType() && record.GetTabletType() != it->second.Type) {
                continue;
            }
            if (it->second.IsDeleting()) {
                continue;
            }
            FillTabletInfo(response->Record, it->first, &it->second, record);
        }
        response->Record.set_starttimetimestamp(StartTime().MilliSeconds());
        response->Record.set_responsetimestamp(TAppData::TimeProvider->Now().MilliSeconds());
    }

    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

NKikimrTabletBase::TMetrics& operator +=(NKikimrTabletBase::TMetrics& metrics, const NKikimrTabletBase::TMetrics& toAdd) {
    if (toAdd.HasCPU()) {
        metrics.SetCPU(metrics.GetCPU() + toAdd.GetCPU());
    }
    if (toAdd.HasMemory()) {
        metrics.SetMemory(metrics.GetMemory() + toAdd.GetMemory());
    }
    if (toAdd.HasNetwork()) {
        metrics.SetNetwork(metrics.GetNetwork() + toAdd.GetNetwork());
    }
    if (toAdd.HasStorage()) {
        metrics.SetStorage(metrics.GetStorage() + toAdd.GetStorage());
    }
    if (toAdd.HasReadThroughput()) {
        metrics.SetReadThroughput(metrics.GetReadThroughput() + toAdd.GetReadThroughput());
    }
    if (toAdd.HasWriteThroughput()) {
        metrics.SetWriteThroughput(metrics.GetWriteThroughput() + toAdd.GetWriteThroughput());
    }
    return metrics;
}

void THive::Handle(TEvHive::TEvRequestHiveDomainStats::TPtr& ev) {
    struct TSubDomainStats {
        THashMap<TTabletInfo::EVolatileState, ui32> StateCounter;
        THashSet<TNodeId> NodeIds;
        ui32 AliveNodes = 0;
        NKikimrTabletBase::TMetrics Metrics;
    };

    THashMap<TSubDomainKey, TSubDomainStats> subDomainStats;

    for (auto it = Tablets.begin(); it != Tablets.end(); ++it) {
        const TLeaderTabletInfo& tablet = it->second;
        TSubDomainKey domain = tablet.ObjectDomain;
        TSubDomainStats& stats = subDomainStats[domain];
        stats.StateCounter[tablet.GetVolatileState()]++;
        if (ev->Get()->Record.GetReturnMetrics()) {
            stats.Metrics += tablet.GetResourceValues();
        }
    }

    for (auto it = Nodes.begin(); it != Nodes.end(); ++it) {
        const TNodeInfo& node = it->second;
        for (const TSubDomainKey& domain : node.ServicedDomains) {
            TSubDomainStats& stats = subDomainStats[domain];
            if (node.IsAlive()) {
                stats.AliveNodes++;
                stats.NodeIds.emplace(node.Id);
            }
        }
    }

    THolder<TEvHive::TEvResponseHiveDomainStats> response = MakeHolder<TEvHive::TEvResponseHiveDomainStats>();
    auto& record = response->Record;

    for (const auto& pr1 : subDomainStats) {
        auto& domainStats = *record.AddDomainStats();
        domainStats.SetShardId(pr1.first.first);
        domainStats.SetPathId(pr1.first.second);
        if (ev->Get()->Record.GetReturnMetrics()) {
            domainStats.MutableMetrics()->CopyFrom(pr1.second.Metrics);
        }
        for (const auto& pr2 : pr1.second.StateCounter) {
            auto& stateStats = *domainStats.AddStateStats();
            stateStats.SetVolatileState(pr2.first);
            stateStats.SetCount(pr2.second);
        }
        for (const TNodeId nodeId : pr1.second.NodeIds) {
            domainStats.AddNodeIds(nodeId);
        }
        domainStats.SetAliveNodes(pr1.second.AliveNodes);
    }

    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void THive::Handle(TEvHive::TEvRequestHiveNodeStats::TPtr& ev) {
    const auto& request(ev->Get()->Record);
    TInstant now = TActivationContext::Now();
    TInstant restartsBarrierTime = now - GetNodeRestartWatchPeriod();
    THolder<TEvHive::TEvResponseHiveNodeStats> response = MakeHolder<TEvHive::TEvResponseHiveNodeStats>();
    auto& record = response->Record;
    if (request.GetReturnExtendedTabletInfo()) {
        record.SetExtendedTabletInfo(true);
    }
    for (auto it = Nodes.begin(); it != Nodes.end(); ++it) {
        TNodeInfo& node = it->second;
        if (node.IsUnknown()) {
            continue;
        }
        auto& nodeStats = *record.AddNodeStats();
        nodeStats.SetNodeId(node.Id);
        if (!node.ServicedDomains.empty()) {
            nodeStats.MutableNodeDomain()->CopyFrom(node.ServicedDomains.front());
        }
        if (!node.Name.empty()) {
            nodeStats.SetNodeName(node.Name);
        }
        if (request.GetReturnExtendedTabletInfo()) {
            if (request.HasFilterTabletsByPathId()) {
                auto itTabletsOfObject = node.TabletsOfObject.find({request.GetFilterTabletsBySchemeShardId(), request.GetFilterTabletsByPathId()});
                if (itTabletsOfObject != node.TabletsOfObject.end()) {
                    std::vector<std::vector<ui32>> tabletStateToTypeToCount;
                    tabletStateToTypeToCount.resize(NKikimrHive::ETabletVolatileState_ARRAYSIZE);
                    for (const TTabletInfo* tablet : itTabletsOfObject->second) {
                        NKikimrHive::ETabletVolatileState state = tablet->GetVolatileState();
                        if (state > NKikimrHive::ETabletVolatileState_MAX) {
                            continue;
                        }
                        std::vector<ui32>& tabletTypeToCount(tabletStateToTypeToCount[state]);
                        TTabletTypes::EType type = tablet->GetTabletType();
                        if (static_cast<size_t>(type) >= tabletTypeToCount.size()) {
                            tabletTypeToCount.resize(type + 1);
                        }
                        tabletTypeToCount[type]++;
                    }
                    for (unsigned int state = 0; state < tabletStateToTypeToCount.size(); ++state) {
                        const std::vector<ui32>& tabletTypeToCount(tabletStateToTypeToCount[state]);
                        for (unsigned int type = 0; type < tabletTypeToCount.size(); ++type) {
                            if (tabletTypeToCount[type] > 0) {
                                auto* stateStats = nodeStats.AddStateStats();
                                stateStats->SetVolatileState(static_cast<NKikimrHive::ETabletVolatileState>(state));
                                stateStats->SetTabletType(static_cast<TTabletTypes::EType>(type));
                                stateStats->SetCount(tabletTypeToCount[type]);
                            }
                        }
                    }
                }
            } else {
                for (const auto& [state, set] : node.Tablets) {
                    std::vector<ui32> tabletTypeToCount;
                    for (const TTabletInfo* tablet : set) {
                        TTabletTypes::EType type = tablet->GetTabletType();
                        if (static_cast<size_t>(type) >= tabletTypeToCount.size()) {
                            tabletTypeToCount.resize(type + 1);
                        }
                        tabletTypeToCount[type]++;
                    }
                    for (unsigned int type = 0; type < tabletTypeToCount.size(); ++type) {
                        if (tabletTypeToCount[type] > 0) {
                            auto* stateStats = nodeStats.AddStateStats();
                            stateStats->SetVolatileState(state);
                            stateStats->SetTabletType(static_cast<TTabletTypes::EType>(type));
                            stateStats->SetCount(tabletTypeToCount[type]);
                        }
                    }
                }
            }
        } else {
            for (const auto& [state, set] : node.Tablets) {
                if (!set.empty()) {
                    auto* stateStats = nodeStats.AddStateStats();
                    stateStats->SetVolatileState(state);
                    stateStats->SetCount(set.size());
                }
            }
        }
        if (request.GetReturnMetrics()) {
            nodeStats.MutableMetrics()->CopyFrom(MetricsFromResourceRawValues(node.GetResourceCurrentValues()));
        }
        if (!node.IsAlive()) {
            nodeStats.SetLastAliveTimestamp(node.Statistics.GetLastAliveTimestamp());
        }
        nodeStats.SetRestartsPerPeriod(node.GetRestartsPerPeriod(restartsBarrierTime));
    }
    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void THive::Handle(TEvHive::TEvRequestHiveStorageStats::TPtr& ev) {
    THolder<TEvHive::TEvResponseHiveStorageStats> response = MakeHolder<TEvHive::TEvResponseHiveStorageStats>();
    auto& record = response->Record;
    for (const auto& [name, pool] : StoragePools) {
        auto& pbPool = *record.AddPools();
        pbPool.SetName(name);
        for (const auto& [id, group] : pool.Groups) {
            auto& pbGroup = *pbPool.AddGroups();
            pbGroup.SetGroupID(id);
            pbGroup.SetAcquiredUnits(group.Units.size());
            pbGroup.SetAcquiredIOPS(group.AcquiredResources.IOPS);
            pbGroup.SetAcquiredThroughput(group.AcquiredResources.Throughput);
            pbGroup.SetAcquiredSize(group.AcquiredResources.Size);
            pbGroup.SetMaximumIOPS(group.MaximumResources.IOPS);
            pbGroup.SetMaximumThroughput(group.MaximumResources.Throughput);
            pbGroup.SetMaximumSize(group.MaximumResources.Size);
            pbGroup.SetAllocatedSize(group.GroupParameters.GetAllocatedSize());
            pbGroup.SetAvailableSize(group.GroupParameters.GetAvailableSize());
        }
    }
    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void THive::Handle(TEvHive::TEvLookupTablet::TPtr& ev) {
    const auto& request(ev->Get()->Record);
    TOwnerIdxType::TValueType ownerIdx(request.GetOwner(), request.GetOwnerIdx());
    auto itOwner = OwnerToTablet.find(ownerIdx);
    if (itOwner == OwnerToTablet.end()) {
        Send(ev->Sender, new TEvHive::TEvCreateTabletReply(NKikimrProto::NODATA, ownerIdx.first, ownerIdx.second), 0, ev->Cookie);
    } else {
        Send(ev->Sender, new TEvHive::TEvCreateTabletReply(NKikimrProto::OK, ownerIdx.first, ownerIdx.second, itOwner->second), 0, ev->Cookie);
    }
}

void THive::Handle(TEvHive::TEvLookupChannelInfo::TPtr& ev) {
    const auto& request(ev->Get()->Record);
    const TLeaderTabletInfo* tablet = FindTablet(request.GetTabletID());
    if (tablet == nullptr) {
        Send(ev->Sender, new TEvHive::TEvChannelInfo(NKikimrProto::ERROR, request.GetTabletID()));
        return;
    }
    TAutoPtr<TEvHive::TEvChannelInfo> response = new TEvHive::TEvChannelInfo(NKikimrProto::OK, tablet->Id);
    for (const TTabletChannelInfo& channelInfo : tablet->TabletStorageInfo->Channels) {
        if (request.ChannelsSize() > 0 ) {
            const auto& channels(request.GetChannels());
            if (std::find(channels.begin(), channels.end(), channelInfo.Channel) == channels.end())
                continue;
        }
        NKikimrHive::TChannelInfo* channel = response->Record.AddChannelInfo();
        channel->SetType(channelInfo.Type.GetErasure());
        for (const TTabletChannelInfo::THistoryEntry& historyInfo : channelInfo.History) {
            // TODO
            //if (request.HasForGeneration() && request.GetForGeneration() ? historyInfo.FromGeneration)
            //    continue;
            NKikimrHive::TChannelInfo_THistorySlot* history = channel->AddHistory();
            history->SetGroupID(historyInfo.GroupID);
            history->SetFromGeneration(historyInfo.FromGeneration);
            history->SetTimestamp(historyInfo.Timestamp.MilliSeconds());
        }
    }
    Send(ev->Sender, response.Release());
}

void THive::Handle(TEvHive::TEvCutTabletHistory::TPtr& ev) {
    Execute(CreateCutTabletHistory(ev));
}

void THive::Handle(TEvHive::TEvDrainNode::TPtr& ev) {
    NKikimrHive::EDrainDownPolicy policy;
    if (!ev->Get()->Record.HasDownPolicy() && ev->Get()->Record.HasKeepDown()) {
        if (ev->Get()->Record.GetKeepDown()) {
            policy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_KEEP_DOWN;
        } else {
            policy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_NO_DOWN;
        }
    } else {
        policy = ev->Get()->Record.GetDownPolicy();
    }
    Execute(CreateSwitchDrainOn(ev->Get()->Record.GetNodeID(),
    {
        .Persist = ev->Get()->Record.GetPersist(),
        .DownPolicy = policy,
        .DrainInFlight = ev->Get()->Record.GetDrainInFlight(),
    }, ev->Sender));
}

void THive::Handle(TEvHive::TEvFillNode::TPtr& ev) {
    StartHiveFill(ev->Get()->Record.GetNodeID(), ev->Sender);
}

void THive::Handle(TEvHive::TEvInitiateTabletExternalBoot::TPtr& ev) {
    TTabletId tabletId = ev->Get()->Record.GetTabletID();
    TLeaderTabletInfo* tablet = FindTablet(tabletId);

    if (!tablet) {
        Send(ev->Sender, new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR), 0, ev->Cookie);
        BLOG_ERROR("Tablet not found " << tabletId);
        return;
    }

    if (tablet->State == ETabletState::GroupAssignment ||
        tablet->State == ETabletState::BlockStorage)
    {
        Send(ev->Sender, new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::TRYLATER), 0, ev->Cookie);
        BLOG_W("Tablet waiting for group assignment " << tabletId);
        return;
    }

    if (!tablet->IsBootingSuppressed()) {
        Send(ev->Sender, new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR), 0, ev->Cookie);
        BLOG_ERROR("Tablet " << tabletId << " is not expected to boot externally");
        return;
    }

    Execute(CreateStartTablet(TFullTabletId(tabletId, 0), ev->Sender, ev->Cookie, /* external */ true));
}

void THive::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    const NKikimrConsole::TConfigNotificationRequest& record = ev->Get()->Record;
    ClusterConfig = record.GetConfig().GetHiveConfig();
    NodeBrokerEpoch = TDuration::MicroSeconds(record.GetConfig().GetNodeBrokerConfig().GetEpochDuration());
    BuildCurrentConfig();
    BLOG_D("Merged config: " << CurrentConfig);
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

void THive::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
    // dummy
}

TResourceRawValues THive::GetDefaultResourceInitialMaximumValues() {
    TResourceRawValues values = {};
    std::get<NMetrics::EResource::Counter>(values) = 100000000; // MaximumTablets?
    std::get<NMetrics::EResource::CPU>(values) = 1000000 * 10; // 1 sec (per second) * 10 threads
    std::get<NMetrics::EResource::Memory>(values) = (ui64)512 << 30; // 512 GB
    std::get<NMetrics::EResource::Network>(values) = 1 << 30; // 10gbit network ~ 1gb/sec
    return values;
}

void THive::ProcessTabletBalancer() {
    if (!ProcessTabletBalancerScheduled && !ProcessTabletBalancerPostponed && BootQueue.BootQueue.empty()) {
        Schedule(GetBalancerCooldown(LastBalancerTrigger), new TEvPrivate::TEvProcessTabletBalancer());
        ProcessTabletBalancerScheduled = true;
    }
}

void THive::ProcessStorageBalancer() {
    if (!ProcessStorageBalancerScheduled && BootQueue.BootQueue.empty()) {
        Schedule(GetBalancerCooldown(EBalancerType::Storage), new TEvPrivate::TEvProcessStorageBalancer());
        ProcessStorageBalancerScheduled = true;
    }
}

THive::THiveStats THive::GetStats() const {
    THiveStats stats = {};
    stats.Values.reserve(Nodes.size());
    for (const auto& ni : Nodes) {
        if (ni.second.IsAlive() && !ni.second.Down) {
            auto nodeValues = NormalizeRawValues(ni.second.ResourceValues, ni.second.GetResourceMaximumValues());
            stats.Values.emplace_back(ni.first, ni.second.GetNodeUsage(nodeValues), nodeValues);
        }
    }
    if (stats.Values.empty()) {
        return stats;
    }
    auto it = std::minmax_element(stats.Values.begin(), stats.Values.end(), [](const THiveStats::TNodeStat& a, const THiveStats::TNodeStat& b) -> bool {
        return a.Usage < b.Usage;
    });
    stats.MaxUsage = it.second->Usage;
    stats.MaxUsageNodeId = it.second->NodeId;
    stats.MinUsage = it.first->Usage;
    stats.MinUsageNodeId = it.first->NodeId;

    TResourceNormalizedValues minValues = stats.Values.front().ResourceNormValues;
    TResourceNormalizedValues maxValues = stats.Values.front().ResourceNormValues;
    for (size_t i = 1; i < stats.Values.size(); ++i) {
        minValues = piecewise_min(minValues, stats.Values[i].ResourceNormValues);
        maxValues = piecewise_max(maxValues, stats.Values[i].ResourceNormValues);
    }

    auto minValuesToBalance = GetMinNodeUsageToBalance();
    maxValues = piecewise_max(maxValues, minValuesToBalance);
    minValues = piecewise_max(minValues, minValuesToBalance);
    auto discrepancy = maxValues - minValues;
    auto& counterDiscrepancy = std::get<NMetrics::EResource::Counter>(discrepancy);
    if (counterDiscrepancy * CurrentConfig.GetMaxResourceCounter() <= 1.5) {
        // We should ignore counter discrepancy of one - it cannot be fixed by balancer
        // Value 1.5 is used to avoid rounding errors
        counterDiscrepancy = 0;
    }
    stats.ScatterByResource = safe_div(discrepancy, maxValues);
    stats.Scatter = max(stats.ScatterByResource);

    return stats;
}

double THive::GetScatter() const {
    THiveStats stats = GetStats();
    return stats.Scatter;
}

double THive::GetUsage() const {
    THiveStats stats = GetStats();
    return stats.MaxUsage;
}

std::optional<EResourceToBalance> THive::CheckScatter(const TResourceNormalizedValues& scatterByResource) const {
    auto minScatterToBalance = GetMinScatterToBalance();
    auto cmp = piecewise_compare(scatterByResource, minScatterToBalance);
    if (std::get<NMetrics::EResource::Counter>(cmp) == std::partial_ordering::greater) {
        return EResourceToBalance::Counter;
    }
    if (std::get<NMetrics::EResource::CPU>(cmp) == std::partial_ordering::greater) {
        return EResourceToBalance::CPU;
    }
    if (std::get<NMetrics::EResource::Memory>(cmp) == std::partial_ordering::greater) {
        return EResourceToBalance::Memory;
    }
    if (std::get<NMetrics::EResource::Network>(cmp) == std::partial_ordering::greater) {
        return EResourceToBalance::Network;
    }
    return std::nullopt;
}

void THive::HandleInit(TEvPrivate::TEvProcessTabletBalancer::TPtr&) {
    BLOG_W("Received TEvProcessTabletBalancer while in StateInit");
    Schedule(TDuration::Seconds(1), new TEvPrivate::TEvProcessTabletBalancer());
}

void THive::Handle(TEvPrivate::TEvProcessTabletBalancer::TPtr&) {
    ProcessTabletBalancerScheduled = false;
    if (!SubActors.empty()) {
        BLOG_D("Balancer has been postponed because of sub activity");
        ProcessTabletBalancerPostponed = true;
        return;
    }

    THiveStats stats = GetStats();
    BLOG_D("ProcessTabletBalancer"
           << " MaxUsage=" << Sprintf("%.9f", stats.MaxUsage) << " on #" << stats.MaxUsageNodeId
           << " MinUsage=" << Sprintf("%.9f", stats.MinUsage) << " on #" << stats.MinUsageNodeId
           << " Scatter=" << Sprintf("%.9f", stats.Scatter));

    TabletCounters->Simple()[NHive::COUNTER_BALANCE_SCATTER].Set(stats.Scatter * 100);
    TabletCounters->Simple()[NHive::COUNTER_BALANCE_USAGE_MIN].Set(stats.MinUsage * 100);
    TabletCounters->Simple()[NHive::COUNTER_BALANCE_USAGE_MAX].Set(stats.MaxUsage * 100);

    auto& nodeUsageHistogram = TabletCounters->Percentile()[NHive::COUNTER_NODE_USAGE];
    nodeUsageHistogram.Clear();
    for (const auto& record : stats.Values) {
        nodeUsageHistogram.IncrementFor(record.Usage * 100);
    }

    double minUsageToKick = GetMaxNodeUsageToKick() - GetNodeUsageRangeToKick();
    if (stats.MaxUsage >= GetMaxNodeUsageToKick() && stats.MinUsage < minUsageToKick) {
        std::vector<TNodeId> overloadedNodes;
        for (const auto& [nodeId, nodeInfo] : Nodes) {
            if (nodeInfo.IsAlive() && !nodeInfo.Down && nodeInfo.IsOverloaded()) {
                overloadedNodes.emplace_back(nodeId);
            }
        }

        if (!overloadedNodes.empty()) {
            BLOG_D("Nodes " << overloadedNodes << " with usage over limit " << GetMaxNodeUsageToKick() << " - starting balancer");
            StartHiveBalancer({
                .Type = EBalancerType::Emergency,
                .MaxMovements = (int)CurrentConfig.GetMaxMovementsOnEmergencyBalancer(),
                .RecheckOnFinish = CurrentConfig.GetContinueEmergencyBalancer(),
                .MaxInFlight = GetEmergencyBalancerInflight(),
                .FilterNodeIds = std::move(overloadedNodes),
            });
            return;
        }
    }

    if (stats.MaxUsage < CurrentConfig.GetMinNodeUsageToBalance()) {
        TabletCounters->Cumulative()[NHive::COUNTER_SUGGESTED_SCALE_DOWN].Increment(1);
    }

    if (ObjectDistributions.GetMaxImbalance() > GetObjectImbalanceToBalance()) {
        TInstant now = TActivationContext::Now();
        if (LastBalancerTrigger != EBalancerType::SpreadNeighbours
            || BalancerStats[static_cast<std::size_t>(EBalancerType::SpreadNeighbours)].LastRunMovements != 0
            || BalancerStats[static_cast<std::size_t>(EBalancerType::SpreadNeighbours)].LastRunTimestamp + TDuration::Seconds(1) < now) {
            auto objectToBalance = ObjectDistributions.GetObjectToBalance();
            BLOG_D("Max imbalance " << ObjectDistributions.GetMaxImbalance() << " - starting balancer for object " << objectToBalance.ObjectId);
            StartHiveBalancer({
                .Type = EBalancerType::SpreadNeighbours,
                .MaxMovements = (int)CurrentConfig.GetMaxMovementsOnAutoBalancer(),
                .RecheckOnFinish = CurrentConfig.GetContinueAutoBalancer(),
                .MaxInFlight = GetBalancerInflight(),
                .FilterNodeIds = std::move(objectToBalance.Nodes),
                .ResourceToBalance = EResourceToBalance::Counter,
                .FilterObjectId = objectToBalance.ObjectId,
            });
            return;
        } else {
            BLOG_D("Skipping SpreadNeigbours Balancer, now: " << now << ", allowed: " << BalancerStats[static_cast<std::size_t>(EBalancerType::SpreadNeighbours)].LastRunTimestamp + TDuration::Seconds(1));
        }
    }

    auto scatteredResource = CheckScatter(stats.ScatterByResource);
    if (scatteredResource) {
        EBalancerType balancerType = EBalancerType::Scatter;
        switch (*scatteredResource) {
            case EResourceToBalance::Counter:
                balancerType = EBalancerType::ScatterCounter;
                break;
            case EResourceToBalance::CPU:
                balancerType = EBalancerType::ScatterCPU;
                break;
            case EResourceToBalance::Memory:
                balancerType = EBalancerType::ScatterMemory;
                break;
            case EResourceToBalance::Network:
                balancerType = EBalancerType::ScatterNetwork;
                break;
            case EResourceToBalance::ComputeResources:
                balancerType = EBalancerType::Scatter;
                break;
        }
        BLOG_TRACE("Scatter " << stats.ScatterByResource << " over limit "
                   << GetMinScatterToBalance() << " - starting balancer " << EBalancerTypeName(balancerType));
        StartHiveBalancer({
            .Type = balancerType,
            .MaxMovements = (int)CurrentConfig.GetMaxMovementsOnAutoBalancer(),
            .RecheckOnFinish = CurrentConfig.GetContinueAutoBalancer(),
            .MaxInFlight = GetBalancerInflight(),
            .ResourceToBalance = *scatteredResource,
        });
        return;
    }

    Send(SelfId(), new TEvPrivate::TEvBalancerOut());
}

void THive::Handle(TEvPrivate::TEvProcessStorageBalancer::TPtr&) {
    ProcessStorageBalancerScheduled = false;
    if (StoragePools.empty()) {
        return;
    }
    using TPoolStat = std::pair<TStoragePoolInfo::TStats, const TStoragePoolInfo&>;
    std::vector<TPoolStat> poolStats;
    poolStats.reserve(StoragePools.size());
    for (const auto& [name, pool] : StoragePools) {
        poolStats.emplace_back(pool.GetStats(), pool);
    }
    auto& [stats, pool] = *std::max_element(poolStats.begin(), poolStats.end(), [](const TPoolStat& lhs, const TPoolStat& rhs) {
        return lhs.first.Scatter < rhs.first.Scatter;
    });
    StorageScatter = stats.Scatter;
    TabletCounters->Simple()[NHive::COUNTER_STORAGE_SCATTER].Set(StorageScatter * 100);
    BLOG_D("StorageScatter = " << StorageScatter << ": " << stats.MaxUsage << " at " << stats.MaxUsageGroupId << " vs " << stats.MinUsage << " at " << stats.MinUsageGroupId);
    if (StorageScatter > GetMinStorageScatterToBalance()) {
        BLOG_D("Starting StorageBalancer");
        ui64 numReassigns = 1;
        auto it = pool.Groups.find(stats.MaxUsageGroupId);
        if (it != pool.Groups.end()) {
            // We want a ballpark estimate of how many reassigns it would take to balance the pool
            // Using the number of units in the most loaded group ensures we won't reassign the whole pool on a whim,
            // while also giving the balancer some room to work.
            // Note that the balancer is not actually required to do that many reassigns, but will never do more
            numReassigns = it->second.Units.size();
        }
        StartHiveStorageBalancer({
            .NumReassigns = numReassigns,
            .MaxInFlight = GetStorageBalancerInflight(),
            .StoragePool = pool.Name
        });
    }
}

void THive::UpdateTotalResourceValues(
        const TNodeInfo* node,
        const TTabletInfo* tablet,
        const NKikimrTabletBase::TMetrics& before,
        const NKikimrTabletBase::TMetrics& after,
        TResourceRawValues deltaRaw,
        TResourceNormalizedValues deltaNormalized) {
    TotalRawResourceValues = TotalRawResourceValues + deltaRaw;
    TotalNormalizedResourceValues = TotalNormalizedResourceValues + deltaNormalized;
    TInstant now = TInstant::Now();

    if (LastResourceChangeReaction + GetResourceChangeReactionPeriod() < now) {
        // in case we had overloaded nodes
        if (!BootQueue.WaitQueue.empty()) {
            ProcessWaitQueue();
        } else if (!BootQueue.BootQueue.empty()) {
            ProcessBootQueue();
        }
        ProcessTabletBalancer();
        LastResourceChangeReaction = now;
    }

    Y_UNUSED(node);

    if (tablet != nullptr) {
        auto& objectMetrics = ObjectToTabletMetrics[tablet->GetObjectId()];
        auto beforeMetrics = objectMetrics.Metrics;
        objectMetrics.AggregateDiff(before, after, tablet);
        BLOG_TRACE("UpdateTotalResources: ObjectId " << tablet->GetObjectId() <<
                   ": {" << beforeMetrics.ShortDebugString() <<
                   "} -> {" << objectMetrics.Metrics.ShortDebugString() << "}");
        auto& typeMetrics = TabletTypeToTabletMetrics[tablet->GetTabletType()];
        beforeMetrics = typeMetrics.Metrics;
        typeMetrics.AggregateDiff(before, after, tablet);
        BLOG_TRACE("UpdateTotalResources: Type " << tablet->GetTabletType() <<
                   ": {" << beforeMetrics.ShortDebugString() <<
                   "} -> {" << typeMetrics.Metrics.ShortDebugString() << "}");
    }
    TabletCounters->Simple()[NHive::COUNTER_METRICS_COUNTER].Set(std::get<NMetrics::EResource::Counter>(TotalRawResourceValues));
    TabletCounters->Simple()[NHive::COUNTER_METRICS_CPU].Set(std::get<NMetrics::EResource::CPU>(TotalRawResourceValues));
    TabletCounters->Simple()[NHive::COUNTER_METRICS_MEMORY].Set(std::get<NMetrics::EResource::Memory>(TotalRawResourceValues));
    TabletCounters->Simple()[NHive::COUNTER_METRICS_NETWORK].Set(std::get<NMetrics::EResource::Network>(TotalRawResourceValues));
}

void THive::RemoveSubActor(ISubActor* subActor) {
    auto it = std::find(SubActors.begin(), SubActors.end(), subActor);
    if (it != SubActors.end()) {
        SubActors.erase(it);
    }
    if (SubActors.empty() && ProcessTabletBalancerPostponed) {
        ProcessTabletBalancerPostponed = false;
        ProcessTabletBalancer();
    }
}

bool THive::StopSubActor(TSubActorId subActorId) {
    for (auto* subActor : SubActors) {
        if (subActor->GetId() == subActorId) {
            subActor->Cleanup();
            return true;
        }
    }
    return false;
}

void THive::WaitToMoveTablets(TActorId actor) {
    if (std::find(ActorsWaitingToMoveTablets.begin(), ActorsWaitingToMoveTablets.end(), actor) == ActorsWaitingToMoveTablets.end()) {
        ActorsWaitingToMoveTablets.push_back(actor);
    }
}

bool THive::IsValidMetrics(const NKikimrTabletBase::TMetrics& metrics) {
    return IsValidMetricsCPU(metrics) || IsValidMetricsMemory(metrics) || IsValidMetricsNetwork(metrics);
}

bool THive::IsValidMetricsCPU(const NKikimrTabletBase::TMetrics& metrics) {
    return metrics.GetCPU() > 1'000/*1ms*/;
}

bool THive::IsValidMetricsMemory(const NKikimrTabletBase::TMetrics& metrics) {
    return metrics.GetMemory() > 128'000/*128KB*/;
}

bool THive::IsValidMetricsNetwork(const NKikimrTabletBase::TMetrics& metrics) {
    return metrics.GetNetwork() > 1024/*1KBps*/;
}

TString THive::DebugDomainsActiveNodes() const {
    return DomainsView.AsString();
}

void THive::AggregateMetricsMax(NKikimrTabletBase::TMetrics& aggregate, const NKikimrTabletBase::TMetrics& value) {
    aggregate.SetCPU(std::max(aggregate.GetCPU(), value.GetCPU()));
    aggregate.SetMemory(std::max(aggregate.GetMemory(), value.GetMemory()));
    aggregate.SetNetwork(std::max(aggregate.GetNetwork(), value.GetNetwork()));
    aggregate.SetCounter(std::max(aggregate.GetCounter(), value.GetCounter()));
    aggregate.SetStorage(std::max(aggregate.GetStorage(), value.GetStorage()));
    aggregate.SetReadThroughput(std::max(aggregate.GetReadThroughput(), value.GetReadThroughput()));
    aggregate.SetWriteThroughput(std::max(aggregate.GetWriteThroughput(), value.GetWriteThroughput()));
}

template <void (NKikimrTabletBase::TMetrics::* setter)(ui64), ui64 (NKikimrTabletBase::TMetrics::* getter)() const, void (NKikimrTabletBase::TMetrics::* clear)()>
static void AggregateDiff(NKikimrTabletBase::TMetrics& aggregate, const NKikimrTabletBase::TMetrics& before, const NKikimrTabletBase::TMetrics& after, TTabletId tabletId, const TString& name) {
    i64 oldValue = (aggregate.*getter)();
    i64 delta = (after.*getter)() - (before.*getter)();
    i64 newValue = oldValue + delta;
    Y_ENSURE_LOG(newValue >= 0, "tablet " << tabletId << " name=" << name << " oldValue=" << oldValue << " delta=" << delta << " newValue=" << newValue);
    newValue = Max(newValue, (i64)0);
    if (newValue != 0) {
        (aggregate.*setter)(newValue);
    } else {
        (aggregate.*clear)();
    }
}

void THive::AggregateMetricsDiff(NKikimrTabletBase::TMetrics& aggregate, const NKikimrTabletBase::TMetrics& before, const NKikimrTabletBase::TMetrics& after, const TTabletInfo* tablet) {
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetCPU, &NKikimrTabletBase::TMetrics::GetCPU, &NKikimrTabletBase::TMetrics::ClearCPU>(aggregate, before, after, tablet->GetLeader().Id, "cpu");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetMemory, &NKikimrTabletBase::TMetrics::GetMemory, &NKikimrTabletBase::TMetrics::ClearMemory>(aggregate, before, after, tablet->GetLeader().Id, "memory");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetNetwork, &NKikimrTabletBase::TMetrics::GetNetwork, &NKikimrTabletBase::TMetrics::ClearNetwork>(aggregate, before, after, tablet->GetLeader().Id, "network");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetCounter, &NKikimrTabletBase::TMetrics::GetCounter, &NKikimrTabletBase::TMetrics::ClearCounter>(aggregate, before, after, tablet->GetLeader().Id, "counter");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetStorage, &NKikimrTabletBase::TMetrics::GetStorage, &NKikimrTabletBase::TMetrics::ClearStorage>(aggregate, before, after, tablet->GetLeader().Id, "storage");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetReadThroughput, &NKikimrTabletBase::TMetrics::GetReadThroughput, &NKikimrTabletBase::TMetrics::ClearReadThroughput>(aggregate, before, after, tablet->GetLeader().Id, "read");
    AggregateDiff<&NKikimrTabletBase::TMetrics::SetWriteThroughput, &NKikimrTabletBase::TMetrics::GetWriteThroughput, &NKikimrTabletBase::TMetrics::ClearWriteThroughput>(aggregate, before, after, tablet->GetLeader().Id, "write");
}

void THive::DivideMetrics(NKikimrTabletBase::TMetrics& metrics, ui64 divider) {
    metrics.SetCPU(metrics.GetCPU() / divider);
    metrics.SetMemory(metrics.GetMemory() / divider);
    metrics.SetNetwork(metrics.GetNetwork() / divider);
    metrics.SetCounter(metrics.GetCounter() / divider);
    metrics.SetStorage(metrics.GetStorage() / divider);
    metrics.SetReadThroughput(metrics.GetReadThroughput() / divider);
    metrics.SetWriteThroughput(metrics.GetWriteThroughput() / divider);
}

NKikimrTabletBase::TMetrics THive::GetDefaultResourceValuesForObject(TFullObjectId objectId) {
    NKikimrTabletBase::TMetrics metrics;
    auto itTablets = ObjectToTabletMetrics.find(objectId);
    if (itTablets != ObjectToTabletMetrics.end()) {
        metrics = itTablets->second.GetAverage();
        metrics.ClearCounter();
    }
    return metrics;
}

NKikimrTabletBase::TMetrics THive::GetDefaultResourceValuesForTabletType(TTabletTypes::EType type) {
    NKikimrTabletBase::TMetrics metrics;
    auto it = TabletTypeToTabletMetrics.find(type);
    if (it != TabletTypeToTabletMetrics.end()) {
        metrics = it->second.GetAverage();
        metrics.ClearCounter();
    }
    return metrics;
}

NKikimrTabletBase::TMetrics THive::GetDefaultResourceValuesForProfile(TTabletTypes::EType type, const TString& resourceProfile) {
    NKikimrTabletBase::TMetrics resourceValues;
    // copy default resource usage from resource profile
    if (ResourceProfiles) {
        // TODO: provide Hive with resource profile used by the tablet instead of default one.
        auto profile = ResourceProfiles->GetProfile(type, resourceProfile);
        resourceValues.SetMemory(profile->GetDefaultTabletMemoryUsage());
    }
    return resourceValues;
}

const TVector<i64>& THive::GetDefaultAllowedMetricIdsForType(TTabletTypes::EType type) {
    static const TVector<i64> defaultAllowedMetricIds = {
        NKikimrTabletBase::TMetrics::kCounterFieldNumber,
        NKikimrTabletBase::TMetrics::kCPUFieldNumber,
        NKikimrTabletBase::TMetrics::kMemoryFieldNumber,
        NKikimrTabletBase::TMetrics::kNetworkFieldNumber,
        NKikimrTabletBase::TMetrics::kStorageFieldNumber,
        NKikimrTabletBase::TMetrics::kGroupReadThroughputFieldNumber,
        NKikimrTabletBase::TMetrics::kGroupWriteThroughputFieldNumber
    };
    static const TVector<i64> onlyCounterAndStorage = {
        NKikimrTabletBase::TMetrics::kCounterFieldNumber,
        NKikimrTabletBase::TMetrics::kStorageFieldNumber,
        NKikimrTabletBase::TMetrics::kGroupReadThroughputFieldNumber,
        NKikimrTabletBase::TMetrics::kGroupWriteThroughputFieldNumber,
    };
    switch (type) {
        case TTabletTypes::ColumnShard:
            return onlyCounterAndStorage;
        default:
            return defaultAllowedMetricIds;
    }
}

const TVector<i64>& THive::GetTabletTypeAllowedMetricIds(TTabletTypes::EType type) const {
    const TVector<i64>& defaultAllowedMetricIds = GetDefaultAllowedMetricIdsForType(type);
    auto it = TabletTypeAllowedMetrics.find(type);
    if (it != TabletTypeAllowedMetrics.end()) {
        return it->second;
    }
    return defaultAllowedMetricIds;
}

THolder<TGroupFilter> THive::BuildGroupParametersForChannel(const TLeaderTabletInfo& tablet, ui32 channelId) {
    auto filter = MakeHolder<TGroupFilter>();
    Y_ABORT_UNLESS(channelId < tablet.BoundChannels.size());
    const auto& binding = tablet.BoundChannels[channelId];
    filter->GroupParameters.MutableStoragePoolSpecifier()->SetName(binding.GetStoragePoolName());
    if (binding.HasPhysicalGroupsOnly()) {
        filter->PhysicalGroupsOnly = binding.GetPhysicalGroupsOnly();
    } else {
        filter->PhysicalGroupsOnly = channelId < 2; // true for channels 0, 1 by default and false for the others
    }
    return filter;
}

void THive::ExecuteStartTablet(TFullTabletId tabletId, const TActorId& local, ui64 cookie, bool external) {
    Execute(CreateStartTablet(tabletId, local, cookie, external));
}

void THive::QueuePing(const TActorId& local) {
    NodePingQueue.push(local);
}

void THive::ProcessNodePingQueue() {
    while (!NodePingQueue.empty() && NodePingsInProgress.size() < GetMaxPingsInFlight()) {
        TActorId local = NodePingQueue.front();
        TNodeId node = local.NodeId();
        NodePingQueue.pop();
        NodePingsInProgress.insert(node);
        SendPing(local, node);
    }
}

void THive::RemoveFromPingInProgress(TNodeId node) {
    NodePingsInProgress.erase(node);
    ProcessNodePingQueue();
}

void THive::SendPing(const TActorId& local, TNodeId id) {
    Send(local,
         new TEvLocal::TEvPing(HiveId,
                               HiveGeneration,
                               false,
                               GetLocalConfig()),
         IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
         id);
}

void THive::SendReconnect(const TActorId& local) {
    Send(local, new TEvLocal::TEvReconnect(HiveId, HiveGeneration));
}

ui32 THive::GetDataCenters() {
    return DataCenters.size() ? DataCenters.size() : 1;
}

void THive::AddRegisteredDataCentersNode(TDataCenterId dataCenterId, TNodeId nodeId) {
    BLOG_D("AddRegisteredDataCentersNode(" << dataCenterId << ", " << nodeId << ")");
    if (dataCenterId) { // ignore default data center id if exists
        auto& dataCenter = DataCenters[dataCenterId];
        bool wasRegistered = dataCenter.IsRegistered();
        dataCenter.RegisteredNodes.insert(nodeId);
        if (!wasRegistered && !dataCenter.UpdateScheduled) {
            dataCenter.UpdateScheduled = true;
            Schedule(TDuration::Seconds(1), new TEvPrivate::TEvUpdateDataCenterFollowers(dataCenterId));
        }
    }
}

void THive::RemoveRegisteredDataCentersNode(TDataCenterId dataCenterId, TNodeId nodeId) {
    BLOG_D("RemoveRegisteredDataCentersNode(" << dataCenterId << ", " << nodeId << ")");
    if (dataCenterId) { // ignore default data center id if exists
        auto& dataCenter = DataCenters[dataCenterId];
        bool wasRegistered = dataCenter.IsRegistered();
        dataCenter.RegisteredNodes.erase(nodeId);
        if (wasRegistered && !dataCenter.IsRegistered() && !dataCenter.UpdateScheduled) {
            dataCenter.UpdateScheduled = true;
            Schedule(TDuration::Seconds(1), new TEvPrivate::TEvUpdateDataCenterFollowers(dataCenterId));
        }
    }
}

void THive::CreateTabletFollowers(TLeaderTabletInfo& tablet, NIceDb::TNiceDb& db, TSideEffects& sideEffects) {
    BLOG_D("CreateTabletFollowers Tablet " << tablet.ToString());

    // In case tablet already has followers (happens if tablet is modified through CreateTablet), delete them
    // But create new ones before deleting old ones, to avoid issues with reusing ids
    decltype(tablet.Followers)::iterator oldFollowersIt;
    if (tablet.Followers.empty()) {
        oldFollowersIt = tablet.Followers.end();
    } else {
        oldFollowersIt = std::prev(tablet.Followers.end());
    }

    for (TFollowerGroup& group : tablet.FollowerGroups) {
        if (group.FollowerCountPerDataCenter) {
            for (auto& [dataCenterId, dataCenter] : DataCenters) {
                if (!dataCenter.IsRegistered()) {
                    continue;
                }
                for (ui32 i = 0; i < group.GetFollowerCountForDataCenter(dataCenterId); ++i) {
                    TFollowerTabletInfo& follower = tablet.AddFollower(group);
                    follower.NodeFilter.AllowedDataCenters = {dataCenterId};
                    follower.Statistics.SetLastAliveTimestamp(TlsActivationContext->Now().MilliSeconds());
                    db.Table<Schema::TabletFollowerTablet>().Key(tablet.Id, follower.Id).Update(
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(follower.FollowerGroup.Id),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(follower.Statistics),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::DataCenter>(dataCenterId));
                    follower.InitTabletMetrics();
                    follower.BecomeStopped();
                    dataCenter.Followers[{tablet.Id, group.Id}].push_back(std::prev(tablet.Followers.end()));
                    BLOG_D("Created follower " << follower.GetFullTabletId() << " for dc " << dataCenterId);
                }
            }
        } else {
            for (ui32 i = 0; i < group.GetRawFollowerCount(); ++i) {
                TFollowerTabletInfo& follower = tablet.AddFollower(group);
                follower.Statistics.SetLastAliveTimestamp(TlsActivationContext->Now().MilliSeconds());
                db.Table<Schema::TabletFollowerTablet>().Key(tablet.Id, follower.Id).Update(
                            NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(follower.FollowerGroup.Id),
                            NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0),
                            NIceDb::TUpdate<Schema::TabletFollowerTablet::Statistics>(follower.Statistics));
                follower.InitTabletMetrics();
                follower.BecomeStopped();
                BLOG_D("Created follower " << follower.GetFullTabletId());
            }

        }
    }

    if (oldFollowersIt == tablet.Followers.end()) {
        return;
    }
    auto endIt = std::next(oldFollowersIt);
    for (auto followerIt = tablet.Followers.begin(); followerIt != endIt; ++followerIt) {
        TFollowerTabletInfo& follower = *followerIt;
        BLOG_D("Deleting follower " << follower.GetFullTabletId());
        db.Table<Schema::TabletFollowerTablet>().Key(tablet.Id, follower.Id).Delete();
        db.Table<Schema::Metrics>().Key(tablet.Id, follower.Id).Delete();
        follower.InitiateStop(sideEffects);
        UpdateCounterTabletsTotal(-1);
    }
    tablet.Followers.erase(tablet.Followers.begin(), endIt);
}

TDuration THive::GetBalancerCooldown(EBalancerType balancerType) const {
    switch(balancerType) {
        case EBalancerType::Scatter:
        case EBalancerType::ScatterCounter:
        case EBalancerType::ScatterCPU:
        case EBalancerType::ScatterMemory:
        case EBalancerType::ScatterNetwork:
        case EBalancerType::SpreadNeighbours:
        case EBalancerType::Storage:
            return GetMinPeriodBetweenBalance();
        case EBalancerType::Emergency:
            return GetMinPeriodBetweenEmergencyBalance();
        case EBalancerType::Manual:
            return TDuration::Seconds(1);
    }
}

void THive::UpdateObjectCount(const TLeaderTabletInfo& tablet, const TNodeInfo& node, i64 diff) {
    if (!GetSpreadNeighbours()) {
        return;
    }
    ObjectDistributions.UpdateCountForTablet(tablet, node, diff);
    TabletCounters->Simple()[NHive::COUNTER_IMBALANCED_OBJECTS].Set(ObjectDistributions.GetImbalancedObjectsCount());
    TabletCounters->Simple()[NHive::COUNTER_WORST_OBJECT_VARIANCE].Set(ObjectDistributions.GetWorstObjectVariance());
    BLOG_TRACE("UpdateObjectCount " << "for " << tablet.ObjectId << " on " << node.Id << " (" << diff << ") ~> Imbalance: " << ObjectDistributions.GetMaxImbalance());
}

ui64 THive::GetObjectImbalance(TFullObjectId object) {
    auto it = ObjectDistributions.Distributions.find(object);
    if (it == ObjectDistributions.Distributions.end()) {
        return 0;
    }
    return it->second->GetImbalance();
}

THive::THive(TTabletStorageInfo *info, const TActorId &tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , RootHiveId()
    , HiveId(Max<ui64>())
    , HiveGeneration(0)
    , PipeClientCacheConfig(new NTabletPipe::TBoundedClientCacheConfig())
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(PipeClientCacheConfig))
    , PipeTracker(*PipeClientCache)
    , PipeRetryPolicy()
    , ObjectDistributions(Nodes)
    , ResponsivenessPinger(nullptr)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();
}

void THive::Handle(TEvHive::TEvInvalidateStoragePools::TPtr& ev) {
    auto& record = ev->Get()->Record;
    if (record.StoragePoolNameSize()) {
        for (const auto& name : record.GetStoragePoolName()) {
            if (const auto it = StoragePools.find(name); it != StoragePools.end()) {
                it->second.Invalidate();
            }
        }
    } else {
        for (auto& [_, pool] : StoragePools) {
            pool.Invalidate();
        }
    }

    auto reply = std::make_unique<IEventHandle>(TEvHive::EvInvalidateStoragePoolsReply, 0, ev->Sender, SelfId(), nullptr, ev->Cookie);
    if (ev->InterconnectSession) {
        reply->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
    }
    TActivationContext::Send(reply.release());
}

void THive::Handle(TEvHive::TEvReassignOnDecommitGroup::TPtr& ev) {
    const ui32 groupId = ev->Get()->Record.GetGroupId();
    BLOG_D("THive::Handle(TEvReassignOnDecommitGroup) GroupId# " << groupId);
    auto reply = std::make_unique<IEventHandle>(TEvHive::EvReassignOnDecommitGroupReply, 0, ev->Sender, SelfId(), nullptr, ev->Cookie);
    if (ev->InterconnectSession) {
        reply->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
    }
    Execute(CreateReassignGroupsOnDecommit(groupId, std::move(reply)));
}

void THive::Handle(TEvPrivate::TEvProcessIncomingEvent::TPtr&) {
    UpdateCounterEventQueueSize(-1);
    EventQueue.ProcessIncomingEvent();
}

void THive::InitDefaultChannelBind(TChannelBind& bind) {
    if (!bind.HasIOPS()) {
        bind.SetIOPS(GetDefaultUnitIOPS());
    }
    if (!bind.HasSize()) {
        bind.SetSize(GetDefaultUnitSize());
    }
    if (!bind.HasThroughput()) {
        bind.SetThroughput(GetDefaultUnitThroughput());
    }
}

void THive::RequestPoolsInformation() {
    BLOG_D("THive::RequestPoolsInformation()");
    TVector<THolder<NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters>> requests;

    for (auto& [poolName, storagePool] : StoragePools) {
        THolder<NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters> item = storagePool.BuildRefreshRequest();
        ++storagePool.RefreshRequestInFlight;
        requests.emplace_back(std::move(item));
    }

    if (!requests.empty()) {
        THolder<TEvBlobStorage::TEvControllerSelectGroups> ev = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
        NKikimrBlobStorage::TEvControllerSelectGroups& record = ev->Record;
        record.SetReturnAllMatchingGroups(true);
        record.SetBlockUntilAllResourcesAreComplete(true);
        for (auto& request : requests) {
            record.MutableGroupParameters()->AddAllocated(std::move(request).Release());
        }
        SendToBSControllerPipe(ev.Release());
    }
    Schedule(GetStorageInfoRefreshFrequency(), new TEvPrivate::TEvRefreshStorageInfo());
}

ui32 THive::GetEventPriority(IEventHandle* ev) {
    switch (ev->GetTypeRewrite()) {
        case TEvHive::EvRequestHiveInfo:
        case TEvHive::EvRequestHiveDomainStats:
        case TEvHive::EvRequestHiveNodeStats:
        case TEvHive::EvRequestHiveStorageStats:
            return 10;
        default:
            return 50;
    }
}

void THive::PushProcessIncomingEvent() {
    Send(SelfId(), new TEvPrivate::TEvProcessIncomingEvent());
}

void THive::ProcessEvent(std::unique_ptr<IEventHandle> event) {
    TAutoPtr ev = event.release();
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvHive::TEvCreateTablet, Handle);
        hFunc(TEvHive::TEvAdoptTablet, Handle);
        hFunc(TEvHive::TEvStopTablet, Handle);
        hFunc(TEvHive::TEvBootTablet, Handle);
        hFunc(TEvLocal::TEvStatus, Handle);
        hFunc(TEvLocal::TEvTabletStatus, Handle); // from bootqueue
        hFunc(TEvLocal::TEvRegisterNode, Handle); // from local
        hFunc(TEvBlobStorage::TEvControllerSelectGroupsResult, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(TEvTabletPipe::TEvServerConnected, Handle);
        hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        hFunc(TEvPrivate::TEvBootTablets, Handle);
        hFunc(TEvHive::TEvInitMigration, Handle);
        hFunc(TEvHive::TEvQueryMigration, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(TEvInterconnect::TEvNodeInfo, Handle);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvPrivate::TEvProcessBootQueue, Handle);
        hFunc(TEvPrivate::TEvPostponeProcessBootQueue, Handle);
        hFunc(TEvPrivate::TEvProcessPendingOperations, Handle);
        hFunc(TEvPrivate::TEvProcessDisconnectNode, Handle);
        hFunc(TEvLocal::TEvSyncTablets, Handle);
        hFunc(TEvPrivate::TEvKickTablet, Handle);
        hFunc(TEvHive::TEvTabletMetrics, Handle);
        hFunc(TEvTabletBase::TEvBlockBlobStorageResult, Handle);
        hFunc(TEvTabletBase::TEvDeleteTabletResult, Handle);
        hFunc(TEvHive::TEvReassignTablet, Handle);
        hFunc(TEvHive::TEvInitiateBlockStorage, Handle);
        hFunc(TEvHive::TEvDeleteTablet, Handle);
        hFunc(TEvHive::TEvDeleteOwnerTablets, Handle);
        hFunc(TEvHive::TEvRequestHiveInfo, Handle);
        hFunc(TEvHive::TEvLookupTablet, Handle);
        hFunc(TEvHive::TEvLookupChannelInfo, Handle);
        hFunc(TEvHive::TEvCutTabletHistory, Handle);
        hFunc(TEvHive::TEvDrainNode, Handle);
        hFunc(TEvHive::TEvFillNode, Handle);
        hFunc(TEvHive::TEvInitiateDeleteStorage, Handle);
        hFunc(TEvHive::TEvGetTabletStorageInfo, Handle);
        hFunc(TEvHive::TEvLockTabletExecution, Handle);
        hFunc(TEvHive::TEvUnlockTabletExecution, Handle);
        hFunc(TEvPrivate::TEvProcessTabletBalancer, Handle);
        hFunc(TEvPrivate::TEvUnlockTabletReconnectTimeout, Handle);
        hFunc(TEvHive::TEvInitiateTabletExternalBoot, Handle);
        hFunc(TEvHive::TEvRequestHiveDomainStats, Handle);
        hFunc(TEvHive::TEvRequestHiveNodeStats, Handle);
        hFunc(TEvHive::TEvRequestHiveStorageStats, Handle);
        hFunc(TEvHive::TEvInvalidateStoragePools, Handle);
        hFunc(TEvHive::TEvReassignOnDecommitGroup, Handle);
        hFunc(TEvHive::TEvRequestTabletIdSequence, Handle);
        hFunc(TEvHive::TEvResponseTabletIdSequence, Handle);
        hFunc(TEvHive::TEvSeizeTablets, Handle);
        hFunc(TEvHive::TEvSeizeTabletsReply, Handle);
        hFunc(TEvHive::TEvReleaseTablets, Handle);
        hFunc(TEvHive::TEvReleaseTabletsReply, Handle);
        hFunc(TEvSubDomain::TEvConfigure, Handle);
        hFunc(TEvHive::TEvConfigureHive, Handle);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
        hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, Handle);
        hFunc(NSysView::TEvSysView::TEvGetTabletIdsRequest, Handle);
        hFunc(NSysView::TEvSysView::TEvGetTabletsRequest, Handle);
        hFunc(TEvHive::TEvRequestTabletOwners, Handle);
        hFunc(TEvHive::TEvTabletOwnersReply, Handle);
        hFunc(TEvPrivate::TEvBalancerOut, Handle);
        hFunc(TEvHive::TEvUpdateTabletsObject, Handle);
        hFunc(TEvPrivate::TEvRefreshStorageInfo, Handle);
        hFunc(TEvPrivate::TEvLogTabletMoves, Handle);
        hFunc(TEvPrivate::TEvStartStorageBalancer, Handle);
        hFunc(TEvPrivate::TEvProcessStorageBalancer, Handle);
        hFunc(TEvHive::TEvUpdateDomain, Handle);
        hFunc(TEvPrivate::TEvDeleteNode, Handle);
        hFunc(TEvHive::TEvRequestTabletDistribution, Handle);
        hFunc(TEvPrivate::TEvUpdateDataCenterFollowers, Handle);
        hFunc(TEvHive::TEvRequestScaleRecommendation, Handle);
        hFunc(TEvPrivate::TEvRefreshScaleRecommendation, Handle)
    }
}

void THive::EnqueueIncomingEvent(STATEFN_SIG) {
    EventQueue.EnqueueIncomingEvent(ev);
    UpdateCounterEventQueueSize(+1);
}

STFUNC(THive::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvPrivate::TEvProcessBootQueue, HandleInit);
        hFunc(TEvPrivate::TEvProcessTabletBalancer, HandleInit);
        // We subscribe to config updates before hive is fully loaded
        hFunc(TEvPrivate::TEvProcessIncomingEvent, Handle);
        fFunc(NConsole::TEvConsole::TEvConfigNotificationRequest::EventType, EnqueueIncomingEvent);
        fFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::EventType, EnqueueIncomingEvent);
    default:
        StateInitImpl(ev, SelfId());
    }
}

STFUNC(THive::StateWork) {
    if (ResponsivenessPinger)
        ResponsivenessPinger->OnAnyEvent();

    switch (ev->GetTypeRewrite()) {
        fFunc(TEvHive::TEvCreateTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvAdoptTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvStopTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvBootTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvLocal::TEvStatus::EventType, EnqueueIncomingEvent);
        fFunc(TEvLocal::TEvTabletStatus::EventType, EnqueueIncomingEvent); // from bootqueue
        fFunc(TEvLocal::TEvRegisterNode::EventType, EnqueueIncomingEvent); // from local
        fFunc(TEvBlobStorage::TEvControllerSelectGroupsResult::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletPipe::TEvClientConnected::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletPipe::TEvClientDestroyed::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletPipe::TEvServerConnected::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletPipe::TEvServerDisconnected::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvBootTablets::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvInitMigration::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvQueryMigration::EventType, EnqueueIncomingEvent);
        fFunc(TEvInterconnect::TEvNodeConnected::EventType, EnqueueIncomingEvent);
        fFunc(TEvInterconnect::TEvNodeDisconnected::EventType, EnqueueIncomingEvent);
        fFunc(TEvInterconnect::TEvNodeInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvInterconnect::TEvNodesInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvents::TEvUndelivered::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvProcessBootQueue::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvPostponeProcessBootQueue::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvProcessPendingOperations::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvProcessDisconnectNode::EventType, EnqueueIncomingEvent);
        fFunc(TEvLocal::TEvSyncTablets::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvKickTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvTabletMetrics::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletBase::TEvBlockBlobStorageResult::EventType, EnqueueIncomingEvent);
        fFunc(TEvTabletBase::TEvDeleteTabletResult::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvReassignTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvInitiateBlockStorage::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvDeleteTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvDeleteOwnerTablets::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestHiveInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvLookupTablet::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvLookupChannelInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvCutTabletHistory::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvDrainNode::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvFillNode::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvInitiateDeleteStorage::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvGetTabletStorageInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvLockTabletExecution::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvUnlockTabletExecution::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvProcessTabletBalancer::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvUnlockTabletReconnectTimeout::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvInitiateTabletExternalBoot::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestHiveDomainStats::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestHiveNodeStats::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestHiveStorageStats::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvInvalidateStoragePools::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvReassignOnDecommitGroup::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestTabletIdSequence::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvResponseTabletIdSequence::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvSeizeTablets::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvSeizeTabletsReply::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvReleaseTablets::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvReleaseTabletsReply::EventType, EnqueueIncomingEvent);
        fFunc(TEvSubDomain::TEvConfigure::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvConfigureHive::EventType, EnqueueIncomingEvent);
        fFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType, EnqueueIncomingEvent);
        fFunc(NConsole::TEvConsole::TEvConfigNotificationRequest::EventType, EnqueueIncomingEvent);
        fFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::EventType, EnqueueIncomingEvent);
        fFunc(NSysView::TEvSysView::TEvGetTabletIdsRequest::EventType, EnqueueIncomingEvent);
        fFunc(NSysView::TEvSysView::TEvGetTabletsRequest::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestTabletOwners::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvTabletOwnersReply::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvBalancerOut::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvUpdateTabletsObject::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvRefreshStorageInfo::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvLogTabletMoves::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvStartStorageBalancer::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvUpdateDomain::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvProcessStorageBalancer::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvDeleteNode::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestTabletDistribution::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvUpdateDataCenterFollowers::EventType, EnqueueIncomingEvent);
        fFunc(TEvHive::TEvRequestScaleRecommendation::EventType, EnqueueIncomingEvent);
        fFunc(TEvPrivate::TEvRefreshScaleRecommendation::EventType, EnqueueIncomingEvent);
        hFunc(TEvPrivate::TEvProcessIncomingEvent, Handle);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            BLOG_W("THive::StateWork unhandled event type: " << ev->GetTypeRewrite()
                   << " event: " << ev->ToString());
        }
        break;
    }
}

void THive::KickTablet(const TTabletInfo& tablet) {
    Send(SelfId(), new TEvPrivate::TEvKickTablet(tablet));
}

void THive::Handle(TEvHive::TEvRequestTabletIdSequence::TPtr& ev) {
    Execute(CreateRequestTabletSequence(std::move(ev)));
}

void THive::Handle(TEvHive::TEvResponseTabletIdSequence::TPtr& ev) {
    Execute(CreateResponseTabletSequence(std::move(ev)));
}

void THive::RequestFreeSequence() {
    TTabletId rootHiveId = AppData()->DomainsInfo->GetHive();
    if (rootHiveId != TabletID()) {
        size_t sequenceIndex = Sequencer.NextFreeSequenceIndex();
        size_t sequenceSize = GetRequestSequenceSize();

        if (PendingCreateTablets.size() > sequenceSize) {
            size_t newSequenceSize = ((PendingCreateTablets.size() / sequenceSize) + 1) * sequenceSize;
            BLOG_W("Increasing sequence size from " << sequenceSize << " to " << newSequenceSize << " due to PendingCreateTablets.size() == " << PendingCreateTablets.size());
            sequenceSize = newSequenceSize;
        }

        BLOG_D("Requesting free sequence #" << sequenceIndex << " of " << sequenceSize << " from root hive");
        SendToRootHivePipe(new TEvHive::TEvRequestTabletIdSequence(TabletID(), sequenceIndex, sequenceSize));
        RequestingSequenceNow = true;
        RequestingSequenceIndex = sequenceIndex;
    } else {
        BLOG_ERROR("We ran out of tablet ids");
    }
}

void THive::ProcessPendingOperations() {
    Execute(CreateProcessPendingOperations());
}

void THive::Handle(TEvSubDomain::TEvConfigure::TPtr& ev) {
    BLOG_D("Handle TEvSubDomain::TEvConfigure(" << ev->Get()->Record.ShortDebugString() << ")");
    Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
}

void THive::Handle(TEvHive::TEvConfigureHive::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvConfigureHive(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateConfigureSubdomain(std::move(ev)));
}


void THive::Handle(NSysView::TEvSysView::TEvGetTabletIdsRequest::TPtr& ev) {
    const auto& request = ev->Get()->Record;
    auto fromId = request.GetFrom();
    auto toId = request.GetTo();

    auto response = MakeHolder<NSysView::TEvSysView::TEvGetTabletIdsResponse>();
    auto& record = response->Record;

    for (const auto& [tabletId, _] : Tablets) {
        if (tabletId >= fromId && tabletId <= toId) {
            record.AddTabletIds(tabletId);
        }
    }

    Send(ev->Sender, response.Release());
}

void THive::Handle(NSysView::TEvSysView::TEvGetTabletsRequest::TPtr& ev) {
    const auto& request = ev->Get()->Record;

    auto response = MakeHolder<NSysView::TEvSysView::TEvGetTabletsResponse>();
    auto& record = response->Record;

    auto limit = request.GetBatchSizeLimit();
    size_t count = 0;

    for (size_t i = 0; i < request.TabletIdsSize(); ++i) {
        auto tabletId = request.GetTabletIds(i);
        auto lookup = Tablets.find(tabletId);
        if (lookup == Tablets.end()) {
            continue;
        }

        auto* entry = record.AddEntries();
        ++count;

        const auto& tablet = lookup->second;
        auto tabletTypeName = TTabletTypes::TypeToStr(tablet.Type);

        entry->SetTabletId(tabletId);
        entry->SetFollowerId(0);

        entry->SetType(tabletTypeName);
        entry->SetState(ETabletStateName(tablet.State));
        entry->SetVolatileState(TTabletInfo::EVolatileStateName(tablet.GetVolatileState()));
        entry->SetBootState(tablet.BootState);
        entry->SetGeneration(tablet.KnownGeneration);
        entry->SetNodeId(tablet.NodeId);

        const auto& resourceValues = tablet.GetResourceValues();
        entry->SetCPU(resourceValues.GetCPU());
        entry->SetMemory(resourceValues.GetMemory());
        entry->SetNetwork(resourceValues.GetNetwork());

        for (const auto& follower : tablet.Followers) {
            auto* entry = record.AddEntries();
            ++count;

            entry->SetTabletId(tabletId);
            entry->SetFollowerId(follower.Id);

            entry->SetType(tabletTypeName);
            // state is null
            entry->SetVolatileState(TTabletInfo::EVolatileStateName(follower.GetVolatileState()));
            entry->SetBootState(follower.BootState);
            // generation is null
            entry->SetNodeId(follower.NodeId);

            const auto& resourceValues = follower.GetResourceValues();
            entry->SetCPU(resourceValues.GetCPU());
            entry->SetMemory(resourceValues.GetMemory());
            entry->SetNetwork(resourceValues.GetNetwork());
        }

        if (count >= limit && i < request.TabletIdsSize() - 1) {
            record.SetNextTabletId(request.GetTabletIds(i + 1));
            break;
        }
    }

    Send(ev->Sender, response.Release());
}

const TTabletMetricsAggregates& THive::GetDefaultResourceMetricsAggregates() const {
    return DefaultResourceMetricsAggregates;
}

bool THive::CheckForForwardTabletRequest(TTabletId tabletId, NKikimrHive::TForwardRequest& forwardRequest) {
    const TLeaderTabletInfo* tablet = FindTablet(tabletId);
    if (tablet == nullptr) {
        TOwnershipKeeper::TOwnerType owner = Keeper.GetOwner(UniqPartFromTabletID(tabletId));
        if (owner == TSequencer::NO_OWNER && AreWeSubDomainHive()) {
            owner = RootHiveId;
        }
        if (owner != TSequencer::NO_OWNER && owner != TabletID()) {
            BLOG_NOTICE("Forwarding TabletRequest(TabletID " << tabletId << ") to Hive " << owner);
            forwardRequest.SetHiveTabletId(owner);
            return true;
        }
    }
    return false;
}

TSubDomainKey THive::GetRootDomainKey() const {
    return RootDomainKey;
}

TSubDomainKey THive::GetMySubDomainKey() const {
    if (AreWeRootHive()) {
        return GetRootDomainKey();
    }
    if (PrimaryDomainKey) {
        return PrimaryDomainKey;
    }
    if (Domains.size() == 1) {
        // not very straight way to get our domain key
        return Domains.begin()->first;
    }
    if (!Tablets.empty()) {
        std::unordered_set<TSubDomainKey> objectDomains;
        for (const auto& [id, tablet] : Tablets) {
            objectDomains.insert(tablet.ObjectDomain);
        }
        if (objectDomains.size() == 1) {
            BLOG_W("GetMySubDomainKey() - guessed PrimaryDomainKey to " << *objectDomains.begin());
            return *objectDomains.begin();
        } else {
            BLOG_W("GetMySubDomainKey() - couldn't guess PrimaryDomainKey: " << objectDomains.size() << " object domains found");
        }
    }
    return {};
}

void THive::Handle(TEvHive::TEvSeizeTablets::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvSeizeTablets(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateSeizeTablets(ev));
}

void THive::Handle(TEvHive::TEvSeizeTabletsReply::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvSeizeTabletsReply(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateSeizeTabletsReply(ev));
}

void THive::Handle(TEvHive::TEvReleaseTablets::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvReleaseTablets(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateReleaseTablets(ev));
}

void THive::Handle(TEvHive::TEvReleaseTabletsReply::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvReleaseTabletsReply(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateReleaseTabletsReply(ev));
}

void THive::Handle(TEvHive::TEvRequestTabletOwners::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvRequestTabletOwners(" << ev->Get()->Record.ShortDebugString() << ")");
    Execute(CreateRequestTabletOwners(std::move(ev)));
}

void THive::Handle(TEvHive::TEvTabletOwnersReply::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvTabletOwnersReply()");
    Execute(CreateTabletOwnersReply(std::move(ev)));
}

void THive::Handle(TEvHive::TEvUpdateTabletsObject::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvUpdateTabletsObject");
    Execute(CreateUpdateTabletsObject(std::move(ev)));
}

void THive::Handle(TEvPrivate::TEvRefreshStorageInfo::TPtr&) {
    RequestPoolsInformation();
}

void THive::Handle(TEvPrivate::TEvLogTabletMoves::TPtr&) {
    LogTabletMovesScheduled = false;
    if (TabletMovesByTypeForLog.empty()) {
        return;
    }
    std::sort(TabletMoveSamplesForLog.begin(), TabletMoveSamplesForLog.end(), [](const TTabletMoveInfo& lhs, const TTabletMoveInfo& rhs) {
        return lhs.Timestamp < rhs.Timestamp;
    });
    TStringBuilder movesByTypeString;
    ui64 movesCount = 0;
    for (const auto& [type, cnt] : TabletMovesByTypeForLog) {
        if (!movesByTypeString.empty()) {
            movesByTypeString << ", ";
        }
        movesByTypeString << cnt << "x " << TTabletTypes::TypeToStr(type);
        movesCount += cnt;
    }
    BLOG_I("Made " << movesCount <<
           " tablet moves (" << movesByTypeString <<
           ") since " << LogTabletMovesSchedulingTime <<
           ", including:");
    for (const auto& moveInfo : TabletMoveSamplesForLog) {
        auto tablet = FindTablet(moveInfo.Tablet);
        BLOG_I("tablet " << (tablet ? tablet->ToString() : ToString(moveInfo.Tablet)) <<
               " from node " << moveInfo.From <<
               " to node " << moveInfo.To <<
               " at " << moveInfo.Timestamp);
    }
    TabletMoveSamplesForLog.clear();
    TabletMovesByTypeForLog.clear();
}

void THive::Handle(TEvPrivate::TEvDeleteNode::TPtr& ev) {
    auto node = FindNode(ev->Get()->NodeId);
    if (node == nullptr) {
        return;
    }
    node->DeletionScheduled = false;
    if (!node->IsAlive()) {
        TryToDeleteNode(node);
    }
}

void THive::Handle(TEvHive::TEvRequestTabletDistribution::TPtr& ev) {
    std::unordered_map<TNodeId, std::unordered_set<TTabletId>> distribution;
    auto& tabletIds = ev->Get()->Record.GetTabletIds();
    for (auto& tabletId : tabletIds) {
        TTabletInfo* tablet = FindTablet(tabletId);
        if (tablet == nullptr) {
            continue;
        }
        auto nodeId = tablet->NodeId;
        distribution[nodeId].insert(tabletId);
    }
    auto response = std::make_unique<TEvHive::TEvResponseTabletDistribution>();
    auto& record = response->Record;
    for (auto& [nodeId, tabletIds] : distribution) {
        auto* node = record.AddNodes();
        node->SetNodeId(nodeId);
        node->MutableTabletIds()->Reserve(tabletIds.size());
        for (auto& tabletId : tabletIds) {
            node->AddTabletIds(tabletId);
        }
    }
    Send(ev->Sender, response.release());
}

void THive::Handle(TEvPrivate::TEvUpdateDataCenterFollowers::TPtr& ev) {
    Execute(CreateUpdateDcFollowers(ev->Get()->DataCenter));
}

template<typename It>
ui32 THive::CalculateRecommendedNodes(It windowBegin, It windowEnd, size_t readyNodes, double target) {
    double maxOnWindow = *std::max_element(windowBegin, windowEnd);
    double ratio = maxOnWindow / target;
    return std::ceil(readyNodes * ratio);
}

void THive::MakeScaleRecommendation() {
    BLOG_D("[MSR] Started making scale recommendation");

    // TODO(pixcc): make following variables as configurable settings
    constexpr TDuration NODE_INITILIZATION_TIME = TDuration::Seconds(30);
    constexpr size_t MAX_HISTORY_SIZE = 15;
    constexpr size_t SCALE_IN_WINDOW_SIZE = 5;
    constexpr size_t SCALE_OUT_WINDOW_SIZE = 15;
    constexpr double TARGET_AVG_CPU_USAGE_PERCENT = 0.66;
    constexpr double CPU_USAGE_MARGIN = 0.2;

    double cpuUsageSum = 0;
    size_t readyNodesCount = 0;
    for (auto& [id, node] : Nodes) {
        if (!node.IsAlive()) {
            BLOG_TRACE("[MSR] Skip node " << id << ", not alive");
            continue;
        }

        if (node.StartTime + NODE_INITILIZATION_TIME > TActivationContext::Now()) {
            BLOG_TRACE("[MSR] Skip node " << id << ", in initialization");
            continue;
        }

        if (!node.AveragedNodeTotalCpuUsage.IsValueReady()) {
            BLOG_TRACE("[MSR] Skip node " << id << ", no CPU usage value");
            continue;
        }

        if (node.GetServicedDomain() != GetMySubDomainKey()) {
            BLOG_TRACE("[MSR] Skip node " << id << ", serviced domain doesn't match");
            continue;
        }

        double avgCpuUsage = node.AveragedNodeTotalCpuUsage.GetValue();
        BLOG_TRACE("[MSR] Node " << id << " is ready, Avg CPU Usage " << avgCpuUsage);
        ++readyNodesCount;

        cpuUsageSum += avgCpuUsage;
        node.AveragedNodeTotalCpuUsage.Clear();
    }

    auto& domain = Domains[GetMySubDomainKey()];
    auto& avgCpuUsageHistory = domain.AvgCpuUsageHistory;

    double avgCpuUsage = readyNodesCount != 0 ? cpuUsageSum / readyNodesCount : 0;
    BLOG_D("[MSR] Total Avg CPU Usage " << avgCpuUsage);

    avgCpuUsageHistory.push_back(avgCpuUsage);
    if (avgCpuUsageHistory.size() > MAX_HISTORY_SIZE) {
        avgCpuUsageHistory.pop_front();
    }

    ui32 recommendedNodes = 0;

    if (avgCpuUsageHistory.size() >= SCALE_IN_WINDOW_SIZE) {
        auto scaleInWindowBegin = avgCpuUsageHistory.end() - SCALE_IN_WINDOW_SIZE;
        auto scaleInWindowEnd = avgCpuUsageHistory.end();
        double usageBottomThreshold = TARGET_AVG_CPU_USAGE_PERCENT - CPU_USAGE_MARGIN;

        bool needScaleIn = std::all_of(
            scaleInWindowBegin,
            scaleInWindowEnd,
            [usageBottomThreshold](double value){ return value < usageBottomThreshold; }
        );

        if (needScaleIn) {
            recommendedNodes = CalculateRecommendedNodes(
                scaleInWindowBegin,
                scaleInWindowEnd,
                readyNodesCount,
                TARGET_AVG_CPU_USAGE_PERCENT
            );
            BLOG_D("[MSR] Need scale in: " << readyNodesCount << " -> " << recommendedNodes);
        }
    }

    if (avgCpuUsageHistory.size() >= SCALE_OUT_WINDOW_SIZE) {
        auto scaleOutWindowBegin = avgCpuUsageHistory.end() - SCALE_OUT_WINDOW_SIZE;
        auto scaleOutWindowEnd = avgCpuUsageHistory.end();

        bool needScaleOut = std::all_of(
            scaleOutWindowBegin,
            scaleOutWindowEnd,
            [](double value){ return value > TARGET_AVG_CPU_USAGE_PERCENT; }
        );

        if (needScaleOut) {
            recommendedNodes = CalculateRecommendedNodes(
                scaleOutWindowBegin,
                scaleOutWindowEnd,
                readyNodesCount,
                TARGET_AVG_CPU_USAGE_PERCENT
            );
            BLOG_D("[MSR] Need scale out: " << readyNodesCount << " -> " << recommendedNodes);
        }
    }

    if (recommendedNodes != 0) {
        domain.LastScaleRecommendation = TScaleRecommendation{
            .Nodes = recommendedNodes,
            .Timestamp = TActivationContext::Now()
        };
    }

    Schedule(GetScaleRecommendationRefreshFrequency(), new TEvPrivate::TEvRefreshScaleRecommendation());
}

void THive::Handle(TEvPrivate::TEvRefreshScaleRecommendation::TPtr&) {
    MakeScaleRecommendation();
}

void THive::Handle(TEvHive::TEvRequestScaleRecommendation::TPtr& ev) {
    BLOG_D("Handle TEvHive::TEvRequestScaleRecommendation(" << ev->Get()->Record.ShortDebugString() << ")");
    auto response = std::make_unique<TEvHive::TEvResponseScaleRecommendation>();

    const auto& record = ev->Get()->Record;
    if (!record.HasDomainKey()) {
        response->Record.SetStatus(NKikimrProto::ERROR);
        Send(ev->Sender, response.release());
        return;
    }

    const TSubDomainKey domainKey(ev->Get()->Record.GetDomainKey());
    if (!Domains.contains(domainKey)) {
        response->Record.SetStatus(NKikimrProto::ERROR);
        Send(ev->Sender, response.release());
        return;
    }

    const TDomainInfo& domainInfo = Domains[domainKey];
    if (domainInfo.LastScaleRecommendation.Empty()) {
        response->Record.SetStatus(NKikimrProto::NOTREADY);
        Send(ev->Sender, response.release());
        return;
    }

    response->Record.SetStatus(NKikimrProto::OK);
    response->Record.SetRecommendedNodes(domainInfo.LastScaleRecommendation->Nodes);
    Send(ev->Sender, response.release());
}

TVector<TNodeId> THive::GetNodesForWhiteboardBroadcast(size_t maxNodesToReturn) {
    TVector<TNodeId> nodes;
    TNodeId selfNodeId = SelfId().NodeId();
    nodes.emplace_back(selfNodeId);
    for (const auto& [nodeId, nodeInfo] : Nodes) {
        if (nodes.size() >= maxNodesToReturn) {
            break;
        }
        if (nodeId != selfNodeId && nodeInfo.IsAlive()) {
            nodes.emplace_back(nodeId);
        }
    }
    return nodes;
}

void THive::ReportStoppedToWhiteboard(const TLeaderTabletInfo& tablet) {
    ReportTabletStateToWhiteboard(tablet, NKikimrWhiteboard::TTabletStateInfo::Stopped);
}

void THive::ReportDeletedToWhiteboard(const TLeaderTabletInfo& tablet) {
    ReportTabletStateToWhiteboard(tablet, NKikimrWhiteboard::TTabletStateInfo::Deleted);
}

void THive::ReportTabletStateToWhiteboard(const TLeaderTabletInfo& tablet, NKikimrWhiteboard::TTabletStateInfo::ETabletState state) {
    ui32 generation = state == NKikimrWhiteboard::TTabletStateInfo::Deleted ? std::numeric_limits<ui32>::max() : tablet.KnownGeneration;
    TPathId pathId = tablet.GetTenant();
    TSubDomainKey tenantId(pathId.OwnerId, pathId.LocalPathId);
    for (TNodeId nodeId : GetNodesForWhiteboardBroadcast()) {
        TActorId whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
        THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate> event = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>();
        event->Record.SetTabletId(tablet.Id);
        event->Record.SetType(tablet.Type);
        event->Record.SetLeader(true);
        event->Record.SetGeneration(generation);
        event->Record.SetState(state);
        event->Record.SetHiveId(TabletID());
        event->Record.MutableTenantId()->CopyFrom(tenantId);
        Send(whiteboardId, event.Release());
        for (const TFollowerTabletInfo& follower : tablet.Followers) {
            THolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate> event = MakeHolder<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>();
            event->Record.SetTabletId(follower.LeaderTablet.Id);
            event->Record.SetFollowerId(follower.Id);
            event->Record.SetType(tablet.Type);
            event->Record.SetGeneration(generation);
            event->Record.SetState(state);
            event->Record.SetHiveId(TabletID());
            event->Record.MutableTenantId()->CopyFrom(tenantId);
            Send(whiteboardId, event.Release());
        }
    }
}

void THive::ActualizeRestartStatistics(google::protobuf::RepeatedField<google::protobuf::uint64>& array, ui64 barrier) {
    static constexpr decltype(array.size()) MAX_RESTARTS_PER_PERIOD = 128;
    auto begin = array.begin();
    auto end = array.end();
    auto it = begin;
    if (array.size() > MAX_RESTARTS_PER_PERIOD) {
        it = end - MAX_RESTARTS_PER_PERIOD;
    }
    while (it != end && *it < barrier) {
        ++it;
    }
    array.erase(begin, it);
}

ui64 THive::GetRestartsPerPeriod(const google::protobuf::RepeatedField<google::protobuf::uint64>& restartTimestamps, ui64 barrier) {
    auto it = std::lower_bound(restartTimestamps.begin(), restartTimestamps.end(), barrier);
    return restartTimestamps.end() - it;
}

bool THive::IsSystemTablet(TTabletTypes::EType type) {
    switch (type) {
        case TTabletTypes::Coordinator:
        case TTabletTypes::Mediator:
        case TTabletTypes::TxAllocator:
        //case TTabletTypes::SchemeShard:
            return true;
        default:
            return false;
    }
}

TString THive::GetLogPrefix() const {
    return TStringBuilder() << "HIVE#" << TabletID() << " ";
}

bool THive::IsItPossibleToStartBalancer(EBalancerType balancerType) {
    for (std::size_t balancer = 0; balancer < std::size(BalancerStats); ++balancer) {
        const auto& stats(BalancerStats[balancer]);
        if (stats.IsRunningNow) {
            EBalancerType type = static_cast<EBalancerType>(balancer);
            BLOG_D("It's not possible to start balancer " << EBalancerTypeName(balancerType) << " because balancer " << EBalancerTypeName(type) << " is already running");
            return false;
        }
    }
    return true;
}

} // NHive

IActor* CreateDefaultHive(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NHive::THive(info, tablet);
}

} // NKikimr
