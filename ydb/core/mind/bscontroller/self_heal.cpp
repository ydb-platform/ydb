#include "self_heal.h"
#include "impl.h"
#include "vdisk_status_tracker.h"
#include "config.h"
#include "group_geometry_info.h"
#include "group_layout_checker.h"
#include "layout_helpers.h"

#include <ydb/core/debug_tools/operation_log.h>

namespace NKikimr::NBsController {
    enum class EGroupRepairOperation {
        SelfHeal = 0,
        GroupLayoutSanitizer,
    };

    enum {
        EvReassignerDone = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    };

    struct TEvReassignerDone : TEventLocal<TEvReassignerDone, EvReassignerDone> {
        TGroupId GroupId;
        bool Success;
        EGroupRepairOperation Operation;
        TString ErrorReason;
        ui64 ConfigTxSeqNo;

        TEvReassignerDone(TGroupId groupId, bool success, EGroupRepairOperation operation, ui64 configTxSeqNo, TString errorReason = "")
            : GroupId(groupId)
            , Success(success)
            , Operation(operation)
            , ErrorReason(errorReason)
            , ConfigTxSeqNo(configTxSeqNo)
        {}
    };

    class TReassignerActor : public TActorBootstrapped<TReassignerActor> {
        const TActorId ControllerId;
        TActorId SelfHealId; // filled on bootstrap
        const TGroupId GroupId;
        const TEvControllerUpdateSelfHealInfo::TGroupContent Group;
        const std::optional<TVDiskID> VDiskToReplace;
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;
        TBlobStorageGroupInfo::TGroupVDisks FailedGroupDisks;
        const bool IsSelfHealReasonDecommit;
        THashSet<TVDiskID> PendingVDisks;
        THashMap<TActorId, TVDiskID> ActorToDiskMap;
        THashMap<TNodeId, TVector<TVDiskID>> NodeToDiskMap;

    public:
        TReassignerActor(TActorId controllerId, TGroupId groupId, TEvControllerUpdateSelfHealInfo::TGroupContent group,
                std::optional<TVDiskID> vdiskToReplace, std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology,
                bool isSelfHealReasonDecommit)
            : ControllerId(controllerId)
            , GroupId(groupId)
            , Group(std::move(group))
            , VDiskToReplace(vdiskToReplace)
            , Topology(std::move(topology))
            , FailedGroupDisks(Topology.get())
            , IsSelfHealReasonDecommit(isSelfHealReasonDecommit)
        {}

        void Bootstrap(const TActorId& parent) {
            SelfHealId = parent;
            Become(&TThis::StateFunc, TDuration::Seconds(60), new TEvents::TEvWakeup);

            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH01, "Reassigner starting", (GroupId, GroupId));

            for (const auto& [vdiskId, vdisk] : Group.VDisks) {
                if (VDiskToReplace && vdiskId == *VDiskToReplace) {
                    FailedGroupDisks |= {Topology.get(), vdiskId};
                    continue; // skip disk we are going to replcate -- it will be wiped out anyway
                }

                // send TEvVStatus message to disk
                const auto& l = vdisk.Location;
                const TActorId& vdiskActorId = MakeBlobStorageVDiskID(l.NodeId, l.PDiskId, l.VSlotId);
                Send(vdiskActorId, new TEvBlobStorage::TEvVStatus(vdiskId), IEventHandle::MakeFlags(0,
                    IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));
                ActorToDiskMap.emplace(vdiskActorId, vdiskId);
                NodeToDiskMap[l.NodeId].push_back(vdiskId);
                PendingVDisks.insert(vdiskId);
            }

            if (!PendingVDisks) {
                ProcessResult();
            }
        }

        void ProcessVDiskReply(const TVDiskID& vdiskId, bool diskIsOk) {
            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH02, "Reassigner ProcessVDiskReply", (GroupId, GroupId),
                (VDiskId, vdiskId), (DiskIsOk, diskIsOk));
            if (PendingVDisks.erase(vdiskId)) {
                if (!diskIsOk) {
                    FailedGroupDisks |= {Topology.get(), vdiskId};
                }
                if (!PendingVDisks) {
                    ProcessResult();
                }
            }
        }

        void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH03, "Reassigner TEvVStatusResult", (GroupId, GroupId),
                (Status, record.GetStatus()), (JoinedGroup, record.GetJoinedGroup()),
                (Replicated, record.GetReplicated()));

            bool diskIsOk = false;
            if (record.GetStatus() == NKikimrProto::RACE) {
                return Finish(false, 0, "Race occured"); // group reconfigured while we were querying it
            } else if (record.GetStatus() == NKikimrProto::OK) {
                diskIsOk = record.GetJoinedGroup() && record.GetReplicated();
            }
            ProcessVDiskReply(VDiskIDFromVDiskID(record.GetVDiskID()), diskIsOk);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {} // not interesting

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH04, "Reassigner TEvNodeDisconnected", (GroupId, GroupId),
                (NodeId, ev->Get()->NodeId));
            for (const auto& vdiskId : NodeToDiskMap[ev->Get()->NodeId]) {
                ProcessVDiskReply(vdiskId, false);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            auto it = ActorToDiskMap.find(ev->Sender);
            Y_VERIFY(it != ActorToDiskMap.end());
            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH05, "Reassigner TEvUndelivered", (GroupId, GroupId),
                (Sender, ev->Sender), (VDiskId, it->second));
            ProcessVDiskReply(it->second, false);
            ActorToDiskMap.erase(it);
        }

        void ProcessResult() {
            auto& checker = Topology->GetQuorumChecker();
            if (!checker.CheckFailModelForGroup(FailedGroupDisks)) {
                STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH06, "Reassigner ProcessResult quorum checker failed", (GroupId, GroupId));
                return Finish(false, 0, "Reassigner ProcessResult quorum checker failed"); // this change will render group unusable
            }

            if (!VDiskToReplace && FailedGroupDisks) {
                STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH10, "Cannot sanitize group with non-operational disks", (GroupId, GroupId));
                return Finish(false, 0, "Cannot sanitize group with non-operational disks");
            }

            auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            ev->SelfHeal = true;
            auto& record = ev->Record;
            auto *request = record.MutableRequest();
            request->SetIgnoreGroupReserve(true);
            request->SetSettleOnlyOnOperationalDisks(true);
            request->SetIsSelfHealReasonDecommit(IsSelfHealReasonDecommit);
            if (VDiskToReplace) {
                auto *cmd = request->AddCommand()->MutableReassignGroupDisk();
                cmd->SetGroupId(VDiskToReplace->GroupID);
                cmd->SetGroupGeneration(VDiskToReplace->GroupGeneration);
                cmd->SetFailRealmIdx(VDiskToReplace->FailRealm);
                cmd->SetFailDomainIdx(VDiskToReplace->FailDomain);
                cmd->SetVDiskIdx(VDiskToReplace->VDisk);
            } else {
                auto *cmd = request->AddCommand()->MutableSanitizeGroup();
                cmd->SetGroupId(GroupId);
            }

            Send(ControllerId, ev.Release());
        }

        void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            if (!record.GetResponse().GetSuccess()) {
                STLOG(PRI_WARN, BS_SELFHEAL, BSSH07, "Reassigner ReassignGroupDisk request failed", (GroupId, GroupId),
                    (VDiskToReplace, VDiskToReplace), (Response, record));
                Finish(false, 0, record.GetResponse().GetErrorDescription());
            } else {
                ui64 configTxSeqNo = record.GetResponse().GetConfigTxSeqNo();
                Y_VERIFY_DEBUG(configTxSeqNo != 0);
                TString items = "none";
                for (const auto& item : record.GetResponse().GetStatus(0).GetReassignedItem()) {
                    items = TStringBuilder() << VDiskIDFromVDiskID(item.GetVDiskId()) << ": "
                        << TVSlotId(item.GetFrom()) << " -> " << TVSlotId(item.GetTo());
                }
                STLOG(PRI_INFO, BS_SELFHEAL, BSSH09, "Reassigner succeeded", (GroupId, GroupId), (Items, items), (ConfigTxSeqNo, configTxSeqNo));
                Finish(true, configTxSeqNo);
            }
        }

        void Finish(bool success, ui64 configTxSeqNo, TString errorReason = "") {
            STLOG(PRI_DEBUG, BS_SELFHEAL, BSSH08, "Reassigner finished", (GroupId, GroupId), (Success, success));
            auto operation = VDiskToReplace ? EGroupRepairOperation::SelfHeal : EGroupRepairOperation::GroupLayoutSanitizer;
            Send(SelfHealId, new TEvReassignerDone(GroupId, success, operation, configTxSeqNo, errorReason));
            PassAway();
        }

        void HandleWakeup() {
            // actually it is watchdog timer for VDisk status query
            if (PendingVDisks) {
                Finish(false, 0, "VDisk status query timer expired");
            }
        }

        void PassAway() override {
            for (const auto& [nodeId, info] : NodeToDiskMap) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
            }
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        })
    };

    class TBlobStorageController::TSelfHealActor : public TActorBootstrapped<TSelfHealActor> {
        static constexpr TDuration MinRetryTimeout = TDuration::Seconds(1);
        static constexpr TDuration MaxRetryTimeout = TDuration::Seconds(60);

        struct TWithFaultyDisks {};
        struct TWithInvalidLayout {};

        struct TGroupRecord
            : TIntrusiveListItem<TGroupRecord, TWithFaultyDisks>
            , TIntrusiveListItem<TGroupRecord, TWithInvalidLayout>
        {
            const TGroupId GroupId;
            TEvControllerUpdateSelfHealInfo::TGroupContent Content;
            TActorId ReassignerActorId; // reassigner in flight
            TDuration RetryTimeout = MinRetryTimeout;
            TInstant NextRetryTimestamp = TInstant::Zero();
            THashMap<TVDiskID, TVDiskStatusTracker> VDiskStatus;
            std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;

            bool LayoutValid = true;
            TString LayoutError;

            ui64 ResponseConfigTxSeqNo = 0;
            ui64 UpdateConfigTxSeqNo = 0;

            TGroupRecord(TGroupId groupId) : GroupId(groupId) {}
        };

        const ui64 TabletId;
        TActorId ControllerId;
        THashMap<TGroupId, TGroupRecord> Groups;
        TIntrusiveList<TGroupRecord, TWithFaultyDisks> GroupsWithFaultyDisks;
        TIntrusiveList<TGroupRecord, TWithInvalidLayout> GroupsWithInvalidLayout;
        std::shared_ptr<std::atomic_uint64_t> UnreassignableGroups;
        bool GroupLayoutSanitizerEnabled = false;
        bool AllowMultipleRealmsOccupation = true;
        THostRecordMap HostRecords;

        using TTopologyDescr = std::tuple<TBlobStorageGroupType::EErasureSpecies, ui32, ui32, ui32>;
        THashMap<TTopologyDescr, std::shared_ptr<TBlobStorageGroupInfo::TTopology>> Topologies;

        static constexpr TDuration SelfHealWakeupPeriod = TDuration::Seconds(10);

        static constexpr uint32_t GroupLayoutSanitizerOperationLogSize = 128;
        TOperationLog<GroupLayoutSanitizerOperationLogSize> GroupLayoutSanitizerOperationLog;

    public:
        TSelfHealActor(ui64 tabletId, std::shared_ptr<std::atomic_uint64_t> unreassignableGroups, THostRecordMap hostRecords)
            : TabletId(tabletId)
            , UnreassignableGroups(std::move(unreassignableGroups))
            , HostRecords(std::move(hostRecords))
        {}

        void Bootstrap(const TActorId& parentId) {
            ControllerId = parentId;
            Become(&TThis::StateFunc);
            HandleWakeup();
        }

        void Handle(TEvControllerUpdateSelfHealInfo::TPtr& ev) {
            const TInstant now = TActivationContext::Now();
            if (const auto& setting = ev->Get()->GroupLayoutSanitizerEnabled) {
                bool previousSetting = std::exchange(GroupLayoutSanitizerEnabled, *setting);
                if (!previousSetting && GroupLayoutSanitizerEnabled) {
                    UpdateLayoutInformationForAllGroups();
                }
            }
            if (const auto& setting = ev->Get()->AllowMultipleRealmsOccupation) {
                bool previousSetting = std::exchange(AllowMultipleRealmsOccupation, *setting);
                if (previousSetting != AllowMultipleRealmsOccupation) {
                    UpdateLayoutInformationForAllGroups();
                }
            }
            bool groupsDeleted = false;
            for (const auto& [groupId, data] : ev->Get()->GroupsToUpdate) {
                if (data) {
                    if (!data->VDisks) {
                        continue; // virtual-only group
                    }

                    const auto [it, inserted] = Groups.try_emplace(groupId, groupId);
                    auto& g = it->second;
                    bool hasFaultyDisks = false;
                    
                    g.Content = std::move(*data);

                    if (GroupLayoutSanitizerEnabled) {
                        UpdateGroupLayoutInformation(g);
                    }

                    ui32 numFailRealms = 0;
                    ui32 numFailDomainsPerFailRealm = 0;
                    ui32 numVDisksPerFailDomain = 0;

                    for (const auto& [vdiskId, vdisk] : g.Content.VDisks) {
                        g.VDiskStatus[vdiskId].Update(vdisk.VDiskStatus, now);
                        hasFaultyDisks |= vdisk.Faulty;
                        numFailRealms = Max<ui32>(numFailRealms, 1 + vdiskId.FailRealm);
                        numFailDomainsPerFailRealm = Max<ui32>(numFailDomainsPerFailRealm, 1 + vdiskId.FailDomain);
                        numVDisksPerFailDomain = Max<ui32>(numVDisksPerFailDomain, 1 + vdiskId.VDisk);
                    }
                    for (auto it = g.VDiskStatus.begin(); it != g.VDiskStatus.end(); ) {
                        if (g.Content.VDisks.count(it->first)) {
                            ++it;
                        } else {
                            g.VDiskStatus.erase(it++);
                        }
                    }
                    if (hasFaultyDisks) {
                        GroupsWithFaultyDisks.PushBack(&g);
                    } else {
                        GroupsWithFaultyDisks.Remove(&g);
                    }

                    Y_VERIFY(numFailRealms && numFailDomainsPerFailRealm && numVDisksPerFailDomain);
                    TTopologyDescr descr(g.Content.Type.GetErasure(), numFailRealms, numFailDomainsPerFailRealm,
                        numVDisksPerFailDomain);
                    auto& topology = Topologies[descr];
                    if (!topology) {
                        topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(std::get<0>(descr),
                            std::get<1>(descr), std::get<2>(descr), std::get<3>(descr), true);
                    }
                    g.Topology = topology;
                    g.UpdateConfigTxSeqNo = ev->Get()->ConfigTxSeqNo;
                } else {
                    // find the group to delete
                    const auto it = Groups.find(groupId);
                    if (it == Groups.end()) {
                        continue; // TODO(alexvru): this should not happen
                    }
                    Y_VERIFY(it != Groups.end());
                    TGroupRecord& group = it->second;

                    // kill reassigner, if it is working
                    if (group.ReassignerActorId) {
                        Send(group.ReassignerActorId, new TEvents::TEvPoison);
                    }

                    // remove the group
                    Groups.erase(it);

                    groupsDeleted = true;
                }
            }
            if (groupsDeleted) {
                for (auto it = Topologies.begin(); it != Topologies.end(); ) {
                    if (it->second.use_count() == 1) {
                        Topologies.erase(it++);
                    } else {
                        ++it;
                    }
                }
            }
            for (const auto& [vdiskId, status, onlyPhantomsRemain] : ev->Get()->VDiskStatusUpdate) {
                if (const auto it = Groups.find(vdiskId.GroupID); it != Groups.end()) {
                    auto& group = it->second;
                    if (const auto it = group.Content.VDisks.find(vdiskId); it != group.Content.VDisks.end()) {
                        it->second.VDiskStatus = status;
                        it->second.OnlyPhantomsRemain = onlyPhantomsRemain;
                        group.VDiskStatus[vdiskId].Update(status, now);
                    }
                }
            }
            for (auto& [vdiskId, isReady] : ev->Get()->VDiskIsReadyUpdate) {
                if (const auto it = Groups.find(vdiskId.GroupID); it != Groups.end()) {
                    auto& group = it->second;
                    if (const auto it = group.Content.VDisks.find(vdiskId); it != group.Content.VDisks.end() &&
                            vdiskId.GroupGeneration == it->first.GroupGeneration) {
                        it->second.IsReady = isReady;
                    }
                }
            }
            CheckGroups();
        }

        void CheckGroups() {
            const TInstant now = TActivationContext::Now();

            ui64 counter = 0;

            for (TGroupRecord& group : GroupsWithFaultyDisks) {
                if (group.ReassignerActorId || now < group.NextRetryTimestamp) {
                    continue; // we are already running reassigner for this group
                }

                if (group.UpdateConfigTxSeqNo < group.ResponseConfigTxSeqNo) {
                    continue; // response from bsc was received before selfheal info update
                }

                // check if it is possible to move anything out
                bool isSelfHealReasonDecommit;
                if (const auto v = FindVDiskToReplace(group.VDiskStatus, group.Content, now, group.Topology.get(),
                        &isSelfHealReasonDecommit)) {
                    group.ReassignerActorId = Register(new TReassignerActor(ControllerId, group.GroupId, group.Content,
                        *v, group.Topology, isSelfHealReasonDecommit));
                } else {
                    ++counter; // this group can't be reassigned right now
                }
            }

            if (GroupLayoutSanitizerEnabled) {
                for (auto it = GroupsWithInvalidLayout.begin(); it != GroupsWithInvalidLayout.end(); ) {
                    TGroupRecord& group = *it++;
                    if (group.UpdateConfigTxSeqNo < group.ResponseConfigTxSeqNo) {
                        continue; // response from bsc was received before selfheal info update
                    }

                    bool allDisksAreFullyOperational = true;
                    for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                        if (vdisk.Bad || vdisk.Faulty || !vdisk.IsReady) {
                            // don't sanitize groups with non-operational or replicating disks
                            allDisksAreFullyOperational = false;
                            break;
                        }
                    }

                    if (!allDisksAreFullyOperational) {
                        continue;
                    }

                    Y_VERIFY(!group.LayoutValid);
                    if (group.ReassignerActorId || now < group.NextRetryTimestamp) {
                        // nothing to do
                    } else {
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(GroupLayoutSanitizerOperationLog,
                                "Start sanitizing GroupId# " << group.GroupId << " GroupGeneration# " << group.Content.Generation);
                        group.ReassignerActorId = Register(new TReassignerActor(ControllerId, group.GroupId, group.Content,
                            std::nullopt, group.Topology, false /*isSelfHealReasonDecommit*/));
                    }
                }
            }

            UnreassignableGroups->store(counter);
        }

        void UpdateGroupLayoutInformation(TGroupRecord& group) {
            NLayoutChecker::TDomainMapper domainMapper;
            Y_VERIFY(group.Content.Geometry);
            Y_VERIFY(HostRecords);
            auto groupDef = MakeGroupDefinition(group.Content.VDisks, *group.Content.Geometry);

            std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdisks;
            for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                ui32 nodeId = vdisk.Location.NodeId;
                TPDiskId pdiskId = vdisk.Location.ComprisingPDiskId();
                if (HostRecords->GetHostId(nodeId)) {
                    pdisks[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper,
                            HostRecords->GetLocation(nodeId),
                            pdiskId,
                            *group.Content.Geometry);
                } else {
                    // Node location cannot be obtained, assume group layout is valid
                    return;
                }
            }

            TString error;
            bool isValid = CheckLayoutByGroupDefinition(groupDef, pdisks, *group.Content.Geometry,
                    AllowMultipleRealmsOccupation, error);

            if (group.LayoutValid && !isValid) {
                group.LayoutError = error;
                GroupsWithInvalidLayout.PushBack(&group);
            } else if (!group.LayoutValid && isValid) {
                group.LayoutError.clear();
                GroupsWithInvalidLayout.Remove(&group);
            }
            group.LayoutValid = isValid;
        }

        void UpdateLayoutInformationForAllGroups() {
            for (auto& [_, group] : Groups) {
                UpdateGroupLayoutInformation(group);
            }
        }

        std::optional<TVDiskID> FindVDiskToReplace(const THashMap<TVDiskID, TVDiskStatusTracker>& tracker,
                const TEvControllerUpdateSelfHealInfo::TGroupContent& content, TInstant now,
                TBlobStorageGroupInfo::TTopology *topology, bool *isSelfHealReasonDecommit) {
            // main idea of selfhealing is step-by-step healing of bad group; we can allow healing of group with more
            // than one disk missing, but we should not move next faulty disk until previous one is replicated, at least
            // partially (meaning only phantoms left)

            // so, first we check that we have no replicating or starting disk in the group; but we allow one
            // semi-replicated disk to prevent selfheal blocking
            TBlobStorageGroupInfo::TGroupVDisks failedByReadiness(topology);
            TBlobStorageGroupInfo::TGroupVDisks failedByBadness(topology);
            ui32 numReplicatingWithPhantomsOnly = 0;
            for (const auto& [vdiskId, vdisk] : content.VDisks) {
                switch (vdisk.VDiskStatus) {
                    case NKikimrBlobStorage::EVDiskStatus::REPLICATING:
                        if (vdisk.OnlyPhantomsRemain && !numReplicatingWithPhantomsOnly) {
                            ++numReplicatingWithPhantomsOnly;
                            break;
                        }
                        [[fallthrough]];
                    case NKikimrBlobStorage::EVDiskStatus::INIT_PENDING:
                        return std::nullopt; // don't touch group with replicating disks

                    default:
                        break;
                }

                auto it = tracker.find(vdiskId);
                Y_VERIFY(it != tracker.end());
                if (it->second.GetStatus(now) != NKikimrBlobStorage::EVDiskStatus::READY) {
                    failedByReadiness |= {topology, vdiskId};
                }
                if (vdisk.Bad) {
                    failedByBadness |= {topology, vdiskId};
                }
            }

            const auto& checker = topology->GetQuorumChecker();
            const auto failed = failedByReadiness | failedByBadness; // assume disks marked as Bad may become non-ready any moment now

            for (const auto& [vdiskId, vdisk] : content.VDisks) {
                if (vdisk.Faulty) {
                    const auto newFailed = failed | TBlobStorageGroupInfo::TGroupVDisks(topology, vdiskId);
                    if (!checker.CheckFailModelForGroup(newFailed)) {
                        continue; // healing this disk would break the group
                    } else if (checker.IsDegraded(failed) < checker.IsDegraded(newFailed)) {
                        continue; // this group will become degraded when applying self-heal logic, skip disk
                    }
                    *isSelfHealReasonDecommit = vdisk.IsSelfHealReasonDecommit;
                    return vdiskId;
                }
            }

            // no options for this group
            return std::nullopt;
        }

        void Handle(TEvReassignerDone::TPtr& ev) {
            if (const auto it = Groups.find(ev->Get()->GroupId); it != Groups.end() && it->second.ReassignerActorId == ev->Sender) {
                auto& group = it->second;
                group.ReassignerActorId = {};

                const TInstant now = TActivationContext::Now();
                if (ev->Get()->Success) {
                    group.NextRetryTimestamp = now;
                    group.RetryTimeout = MinRetryTimeout;
                    group.ResponseConfigTxSeqNo = ev->Get()->ConfigTxSeqNo;
                    if (ev->Get()->Operation == EGroupRepairOperation::GroupLayoutSanitizer) {
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(GroupLayoutSanitizerOperationLog,
                                "Sanitizing succeeded GroupId# " << group.GroupId);
                    }
                } else {
                    group.NextRetryTimestamp = now + group.RetryTimeout;
                    group.RetryTimeout = std::min(MaxRetryTimeout, group.RetryTimeout * 3 / 2);
                    if (ev->Get()->Operation == EGroupRepairOperation::GroupLayoutSanitizer) {
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(GroupLayoutSanitizerOperationLog,
                                "Sanitizing failed GroupId# " << group.GroupId << " ErrorReason# " << ev->Get()->ErrorReason);
                    }
                }

                CheckGroups();
            }
        }

        using TVDiskInfo = TEvControllerUpdateSelfHealInfo::TGroupContent::TVDiskInfo;
        TGroupMapper::TGroupDefinition MakeGroupDefinition(const TMap<TVDiskID, TVDiskInfo>& vdisks, 
                const TGroupGeometryInfo& geom) {
            TGroupMapper::TGroupDefinition groupDefinition;
            geom.ResizeGroup(groupDefinition);

            for (const auto& [vdiskId, vdisk] : vdisks) {
                if (!vdisk.Decommitted) {
                    groupDefinition[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] = vdisk.Location.ComprisingPDiskId();
                }
            }

            return std::move(groupDefinition);
        }

        void HandleWakeup() {
            CheckGroups();
            Schedule(SelfHealWakeupPeriod, new TEvents::TEvWakeup());
        }

        void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
            TStringStream str;
            RenderMonPage(str, ev->Cookie);
            Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        }

        void RenderMonPage(IOutputStream& out, bool selfHealEnabled) {
            const TInstant now = TActivationContext::Now();

            HTML(out) {
                TAG(TH2) {
                    out << "BlobStorage Controller";
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Self-Heal status";
                    }
                    DIV_CLASS("panel-body") {
                        out << (selfHealEnabled ? "Enabled" : "Disabled");
                        if (selfHealEnabled) {
                            out << "<br/>" << Endl;
                            out << "<form method='POST'>" << Endl;
                            out << "<input type='hidden' name='TabletID' value='" << TabletId << "'>" << Endl;
                            out << "<input type='hidden' name='page' value='SelfHeal'>" << Endl;
                            out << "<input type='hidden' name='disable' value='1'>" << Endl;
                            out << "<input type='hidden' name='action' value='disableSelfHeal'>" << Endl;
                            out << "<input class='btn btn-primary' type='submit' value='DISABLE NOW'/>" << Endl;
                            out << "</form>";
                        }
                    }
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "VDisk states";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "State"; }
                                    TABLEH() { out << "Description"; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { out << "ERROR"; }
                                    TABLED() { out << "VDisk is not available now or is in error state"; }
                                }
                                TABLER() {
                                    TABLED() { out << "INIT_PENDING"; }
                                    TABLED() { out << "VDisk is being initialized or synced with other disks in the group"; }
                                }
                                TABLER() {
                                    TABLED() { out << "REPLICATING"; }
                                    TABLED() { out << "VDisk is being currently replicated"; }
                                }
                                TABLER() {
                                    TABLED() { out << "READY"; }
                                    TABLED() { out << "VDisk is replicated and ready"; }
                                }
                            }
                        }
                    }
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Broken groups";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table-sortable table") {
                            TABLEHEAD() {
                                ui32 numCols = 0;
                                for (const TGroupRecord& group : GroupsWithFaultyDisks) {
                                    numCols = Max<ui32>(numCols, group.Content.VDisks.size());
                                }

                                TABLER() {
                                    TABLEH() { out << "GroupId:Gen"; }
                                    for (ui32 i = 0; i < numCols; ++i) {
                                        TABLEH() { out << "OrderNum# " << i; }
                                    }
                                }
                            }
                            TABLEBODY() {
                                for (const TGroupRecord& group : GroupsWithFaultyDisks) {
                                    TABLER() {
                                        out << "<td rowspan='2'><a href='?TabletID=" << TabletId
                                            << "&page=GroupDetail&GroupId=" << group.GroupId << "'>"
                                            << group.GroupId << "</a>:" << group.Content.Generation << "</td>";

                                        for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                                            TABLED() {
                                                out << vdiskId.ToString();
                                                out << "<br/>";
                                                out << vdisk.VDiskStatus;
                                                out << "<br/><strong>";
                                                if (const auto it = group.VDiskStatus.find(vdiskId); it != group.VDiskStatus.end()) {
                                                    if (const auto& status = it->second.GetStatus(now)) {
                                                        out << *status;
                                                    } else {
                                                        out << "unsure";
                                                    }
                                                } else {
                                                    out << "?";
                                                }
                                                out << "</strong>";
                                            }
                                        }
                                    }
                                    TABLER() {
                                        for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                                            TABLED() {
                                                const auto& l = vdisk.Location;
                                                if (vdisk.Faulty) {
                                                    out << "<strong>";
                                                }
                                                if (vdisk.Bad) {
                                                    out << "<font color='red'>";
                                                }
                                                out << "[" << l.NodeId << ":" << l.PDiskId << ":" << l.VSlotId << "]";
                                                if (vdisk.Bad) {
                                                    out << "</font>";
                                                }
                                                if (vdisk.Faulty) {
                                                    out << "</strong>";
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Group Layout Sanitizer";
                    }
                    DIV_CLASS("panel-body") {
                        out << "Status: " << (GroupLayoutSanitizerEnabled ? "enabled" : "disabled");

                        out << "<br/>";

                        out << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px' \
                            data-target='#operationLogCollapse'>Operation Log</button>";
                        out << "<div id='operationLogCollapse' class='collapse'>";
                        TABLE_CLASS("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "Index"; }
                                    TABLEH() { out << "Record"; }
                                }
                            }

                            ui32 logSize = GroupLayoutSanitizerOperationLog.Size();
                            TABLEBODY() {
                                for (ui32 i = 0; i < logSize; ++i) {
                                    TABLER() {
                                        TABLED() {
                                            out << i;
                                        }
                                        TABLED() { 
                                            auto record = GroupLayoutSanitizerOperationLog.BorrowByIdx(i);
                                            if (record) {
                                                out << *record;
                                                GroupLayoutSanitizerOperationLog.ReturnBorrowedRecord(record);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        out << "</div>";
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Groups with invalid layout";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table-sortable table") {
                            TABLEHEAD() {
                                ui32 numCols = 0;
                                for (const TGroupRecord& group : GroupsWithInvalidLayout) {
                                    numCols = Max<ui32>(numCols, group.Content.VDisks.size());
                                }

                                TABLER() {
                                    TABLEH() { out << "GroupId:Gen"; }
                                    for (ui32 i = 0; i < numCols; ++i) {
                                        TABLEH() { out << "OrderNum# " << i; }
                                    }
                                    TABLEH() { out << "LayoutErrorReason# "; }
                                }
                            }
                            TABLEBODY() {
                                for (const TGroupRecord& group : GroupsWithInvalidLayout) {
                                    TABLER() {
                                        out << "<td rowspan='2'><a href='?TabletID=" << TabletId
                                            << "&page=GroupDetail&GroupId=" << group.GroupId << "'>"
                                            << group.GroupId << "</a>:" << group.Content.Generation << "</td>";

                                        for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                                            TABLED() {
                                                out << vdiskId.ToString();
                                                out << "<br/>";
                                                out << vdisk.VDiskStatus;
                                                out << "<br/><strong>";
                                                if (const auto it = group.VDiskStatus.find(vdiskId); it != group.VDiskStatus.end()) {
                                                    if (const auto& status = it->second.GetStatus(now)) {
                                                        out << *status;
                                                    } else {
                                                        out << "unsure";
                                                    }
                                                } else {
                                                    out << "?";
                                                }
                                                out << "</strong>";
                                            }
                                        }

                                        out << "<td rowspan='2'>" << group.LayoutError << "</td>";
                                    }
                                    TABLER() {
                                        for (const auto& [vdiskId, vdisk] : group.Content.VDisks) {
                                            TABLED() {
                                                const auto& l = vdisk.Location;
                                                if (vdisk.Faulty) {
                                                    out << "<strong>";
                                                }
                                                if (vdisk.Bad) {
                                                    out << "<font color='red'>";
                                                }
                                                out << "[" << l.NodeId << ":" << l.PDiskId << ":" << l.VSlotId << "]";
                                                if (vdisk.Bad) {
                                                    out << "</font>";
                                                }
                                                if (vdisk.Faulty) {
                                                    out << "</strong>";
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        void Handle(TEvPrivate::TEvUpdateHostRecords::TPtr ev) {
            HostRecords = std::move(ev->Get()->HostRecords);
        }

        STRICT_STFUNC(StateFunc, {
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(TEvControllerUpdateSelfHealInfo, Handle);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            hFunc(TEvReassignerDone, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            hFunc(TEvPrivate::TEvUpdateHostRecords, Handle);
        })
    };

    IActor *TBlobStorageController::CreateSelfHealActor() {
        Y_VERIFY(HostRecords);
        return new TSelfHealActor(TabletID(), SelfHealUnreassignableGroups, HostRecords);
    }

    void TBlobStorageController::InitializeSelfHealState() {
        auto ev = MakeHolder<TEvControllerUpdateSelfHealInfo>();
        for (const auto& [groupId, group] : GroupMap) {
            ev->GroupsToUpdate.emplace(groupId, TEvControllerUpdateSelfHealInfo::TGroupContent());
        }
        FillInSelfHealGroups(*ev, nullptr);
        ev->GroupLayoutSanitizerEnabled = GroupLayoutSanitizerEnabled;
        Send(SelfHealId, ev.Release());
    }

    void TBlobStorageController::FillInSelfHealGroups(TEvControllerUpdateSelfHealInfo& msg, TConfigState *state) {
        THashMap<TBoxStoragePoolId, std::shared_ptr<TGroupGeometryInfo>> geomCache;

        for (auto& [groupId, group] : msg.GroupsToUpdate) {
            if (!group) {
                continue;
            }

            const TGroupInfo *p = state ? state->Groups.Find(groupId) : FindGroup(groupId);
            Y_VERIFY(p);

            group->Generation = p->Generation;
            group->Type = TBlobStorageGroupType(p->ErasureSpecies);

            if (auto it = geomCache.find(p->StoragePoolId); it != geomCache.end()) {
                group->Geometry = it->second;
            } else {
                const TMap<TBoxStoragePoolId, TStoragePoolInfo>& storagePools = state
                    ? state->StoragePools.Get()
                    : StoragePools;
                const auto spIt = storagePools.find(p->StoragePoolId);
                Y_VERIFY(spIt != storagePools.end());
                group->Geometry = std::make_unique<TGroupGeometryInfo>(group->Type, spIt->second.GetGroupGeometry());
                geomCache.emplace(p->StoragePoolId, group->Geometry);
            }

            for (const TVSlotInfo *slot : p->VDisksInGroup) {
                group->VDisks[slot->GetVDiskId()] = {
                    slot->VSlotId,
                    slot->PDisk->ShouldBeSettledBySelfHeal(),
                    slot->PDisk->BadInTermsOfSelfHeal(),
                    slot->PDisk->Decommitted(),
                    slot->PDisk->IsSelfHealReasonDecommit(),
                    slot->OnlyPhantomsRemain,
                    slot->IsReady,
                    slot->Status,
                };
            }
        }
    }

    void TBlobStorageController::ProcessVDiskStatus(
            const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus>& s) {
        THashSet<TGroupInfo*> groups;
        const TInstant now = TActivationContext::Now();
        const TMonotonic mono = TActivationContext::Monotonic();
        std::vector<TVDiskAvailabilityTiming> timingQ;

        std::unique_ptr<TEvPrivate::TEvDropDonor> dropDonorEv;

        auto ev = MakeHolder<TEvControllerUpdateSelfHealInfo>();
        for (const auto& m : s) {
            const TVSlotId vslotId(m.GetNodeId(), m.GetPDiskId(), m.GetVSlotId());
            const auto vdiskId = VDiskIDFromVDiskID(m.GetVDiskId());
            if (TVSlotInfo *slot = FindVSlot(vslotId); slot && !slot->IsBeingDeleted() &&
                    slot->PDisk->Guid == m.GetPDiskGuid() && vdiskId.SameExceptGeneration(slot->GetVDiskId())) {
                const bool was = slot->IsOperational();
                if (const TGroupInfo *group = slot->Group) {
                    const bool wasReady = slot->IsReady;
                    if (slot->Status != m.GetStatus() || slot->OnlyPhantomsRemain != m.GetOnlyPhantomsRemain()) {
                        slot->SetStatus(m.GetStatus(), mono, now, m.GetOnlyPhantomsRemain());
                        if (slot->IsReady != wasReady) {
                            ScrubState.UpdateVDiskState(slot);
                            ev->VDiskIsReadyUpdate.emplace_back(vdiskId, slot->IsReady);
                            if (wasReady) {
                                NotReadyVSlotIds.insert(slot->VSlotId);
                            }
                        }
                        timingQ.emplace_back(*slot);
                    }
                    ev->VDiskStatusUpdate.emplace_back(vdiskId, m.GetStatus(), m.GetOnlyPhantomsRemain());
                    if (!was && slot->IsOperational() && !group->SeenOperational) {
                        groups.insert(const_cast<TGroupInfo*>(group));
                    }
                }
                if (slot->Status == NKikimrBlobStorage::EVDiskStatus::READY) {
                    // we can release donor slots without further notice then the VDisk is completely replicated; we
                    // intentionally use GetStatus() here instead of IsReady() to prevent waiting
                    for (const TVSlotId& donorVSlotId : slot->Donors) {
                        if (!dropDonorEv) {
                            dropDonorEv.reset(new TEvPrivate::TEvDropDonor);
                        }
                        dropDonorEv->VSlotIds.push_back(donorVSlotId);
                    }
                }
            }
            if (const auto it = StaticVSlots.find(vslotId); it != StaticVSlots.end() && it->second.VDiskId == vdiskId) {
                it->second.VDiskStatus = m.GetStatus();
            }
        }

        if (dropDonorEv) {
            Send(SelfId(), dropDonorEv.release());
        }

        // issue updated statuses to self-healer
        if (ev->VDiskStatusUpdate) {
            Send(SelfHealId, ev.Release());
        }

        // update operational status for groups
        TVector<TGroupId> groupIds;
        for (TGroupInfo *group : groups) {
            group->UpdateSeenOperational();
            if (group->SeenOperational) {
                groupIds.push_back(group->ID);
            }
        }
        if (groupIds) {
            Execute(CreateTxUpdateSeenOperational(std::move(groupIds)));
        }

        if (!timingQ.empty()) {
            Execute(CreateTxUpdateLastSeenReady(std::move(timingQ)));
        }

        ScheduleVSlotReadyUpdate();
    }

    void TBlobStorageController::UpdateSelfHealCounters() {
        // WARNING: keep this logic consistent with updateSelfHealCounters flag calculation in CommitConfigUpdates
        const TInstant now = TActivationContext::Now();
        bool reschedule = false;

        auto updateDiskCounters = [&](
                NKikimrBlobStorage::EDriveStatus status,
                NBlobStorageController::EPercentileCounters histCounter,
                NBlobStorageController::ESimpleCounters groups,
                NBlobStorageController::ESimpleCounters slots,
                NBlobStorageController::ESimpleCounters bytes) {

            // build histogram of PDisks in faulty state with VSlots over 'em
            auto& histo = TabletCounters->Percentile()[histCounter];
            histo.Clear();
            const auto& ranges = histo.GetRanges(); // a sorted vector of ranges
            for (const auto& [pdiskId, pdisk] : PDisks) {
               if (pdisk->Status == status && pdisk->NumActiveSlots) {
                    const ui64 passed = (now - pdisk->StatusTimestamp).Seconds();
                    auto comp = [](const ui64 value, const auto& range) { return value < range.RangeVal; };
                    const size_t idx = std::upper_bound(ranges.begin(), ranges.end(), passed, comp) - ranges.begin() - 1;
                    histo.IncrementForRange(idx);
                    reschedule = true;
                }
            }

            // calculate some simple counters
            ui64 vslotsOnFaultyPDisks = 0;
            ui64 bytesOnFaultyPDisks = 0;
            std::unordered_set<TGroupId> groupsWithSlotsOnFaultyPDisks;
            for (const auto& [vslotId, vslot] : VSlots) {
                if (!vslot->IsBeingDeleted() && vslot->PDisk->Status == status) {
                    ++vslotsOnFaultyPDisks;
                    bytesOnFaultyPDisks += vslot->Metrics.GetAllocatedSize();
                    groupsWithSlotsOnFaultyPDisks.insert(vslot->GroupId);
                }
            }
            auto& s = TabletCounters->Simple();
            s[groups].Set(groupsWithSlotsOnFaultyPDisks.size());
            s[slots].Set(vslotsOnFaultyPDisks);
            s[bytes].Set(bytesOnFaultyPDisks);
        };

        updateDiskCounters(
            NKikimrBlobStorage::EDriveStatus::FAULTY,
            NBlobStorageController::COUNTER_FAULTY_USETTLED_PDISKS,
            NBlobStorageController::COUNTER_GROUPS_WITH_SLOTS_ON_FAULTY_DISKS,
            NBlobStorageController::COUNTER_SLOTS_ON_FAULTY_DISKS,
            NBlobStorageController::COUNTER_BYTES_ON_FAULTY_DISKS
        );

        updateDiskCounters(
            NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED,
            NBlobStorageController::COUNTER_TO_BE_REMOVED_USETTLED_PDISKS,
            NBlobStorageController::COUNTER_GROUPS_WITH_SLOTS_ON_TO_BE_REMOVED_DISKS,
            NBlobStorageController::COUNTER_SLOTS_ON_TO_BE_REMOVED_DISKS,
            NBlobStorageController::COUNTER_BYTES_ON_TO_BE_REMOVED_DISKS
        );

        TabletCounters->Simple()[NBlobStorageController::COUNTER_SELF_HEAL_UNREASSIGNABLE_GROUPS] = SelfHealUnreassignableGroups->load();

        Schedule(TDuration::Seconds(15), new TEvPrivate::TEvUpdateSelfHealCounters);
    }

} // NKikimr::NBsController

template<>
void Out<std::nullopt_t>(IOutputStream& out, const std::nullopt_t&) {
    out << "null";
}

template<>
void Out<std::optional<NKikimrBlobStorage::EVDiskStatus>>(IOutputStream& out, const std::optional<NKikimrBlobStorage::EVDiskStatus>& s) {
    if (s) {
        out << *s;
    } else {
        out << std::nullopt;
    }
}
