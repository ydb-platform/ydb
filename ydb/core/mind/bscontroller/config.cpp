#include "impl.h"
#include "config.h"
#include "diff.h"
#include "table_merger.h"

namespace NKikimr::NBsController {

        class TBlobStorageController::TNodeWardenUpdateNotifier {
            TBlobStorageController *Self;
            TConfigState &State;
            THashMap<TNodeId, NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate> Services;
            THashSet<TPDiskId> DeletedPDiskIds;

        public:
            TNodeWardenUpdateNotifier(TBlobStorageController *self, TConfigState &state)
                : Self(self)
                , State(state)
            {}

            void Execute() {
                ApplyUpdates();

                for (auto &pair : Services) {
                    const TNodeId &nodeId = pair.first;

                    if (TNodeInfo *node = Self->FindNode(nodeId); node && node->ConnectedServerId) {
                        auto event = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
                        auto& record = event->Record;
                        pair.second.Swap(&record);
                        record.SetStatus(NKikimrProto::OK);
                        record.SetNodeID(nodeId);
                        record.SetInstanceId(Self->InstanceId);
                        record.SetAvailDomain(AppData()->DomainsInfo->GetDomain()->DomainUid);
                        State.Outbox.emplace_back(nodeId, std::move(event), 0);
                    }
                }
            }

        private:
            void ApplyUpdates() {
                for (auto&& [base, overlay] : State.PDisks.Diff()) {
                    if (!overlay->second) {
                        ApplyPDiskDeleted(overlay->first, *base->second);
                    } else if (!base) {
                        ApplyPDiskCreated(overlay->first, *overlay->second);
                    } else {
                        ApplyPDiskDiff(overlay->first, *base->second, *overlay->second);
                    }
                }
                for (auto&& [base, overlay] : State.VSlots.Diff()) {
                    if (!overlay->second) {
                        ApplyVSlotDeleted(overlay->first, *base->second);
                    } else if (!base) {
                        ApplyVSlotCreated(overlay->first, *overlay->second);
                    } else {
                        ApplyVSlotDiff(overlay->first, *base->second, *overlay->second);
                    }
                }
                for (auto&& [base, overlay] : State.Groups.Diff()) {
                    if (!overlay->second) {
                        ApplyGroupDeleted(overlay->first, *base->second);
                    } else if (!base) {
                        ApplyGroupCreated(overlay->first, *overlay->second);
                    } else {
                        ApplyGroupDiff(overlay->first, *base->second, *overlay->second);
                    }
                }
            }

            void ApplyPDiskCreated(const TPDiskId &pdiskId, const TPDiskInfo &pdiskInfo) {
                NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk *pdisk = CreatePDiskEntry(pdiskId, pdiskInfo);
                pdisk->SetEntityStatus(NKikimrBlobStorage::CREATE);
            }

            void ApplyPDiskDiff(const TPDiskId &pdiskId, const TPDiskInfo &prev, const TPDiskInfo &cur) {
                if (prev.Mood != cur.Mood) {
                    // PDisk's mood has changed
                    CreatePDiskEntry(pdiskId, cur);
                } else if (prev.PDiskConfig != cur.PDiskConfig) {
                    // PDisk's config has changed
                    NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk *pdisk = CreatePDiskEntry(pdiskId, cur);
                    pdisk->SetEntityStatus(NKikimrBlobStorage::RESTART);
                }
            }

            void ApplyPDiskDeleted(const TPDiskId &pdiskId, const TPDiskInfo &pdiskInfo) {
                DeletedPDiskIds.insert(pdiskId);
                TNodeInfo *nodeInfo = Self->FindNode(pdiskId.NodeId);
                if (!State.StaticPDisks.count(pdiskId) || (nodeInfo && nodeInfo->DeclarativePDiskManagement)) {
                    NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk *pdisk = CreatePDiskEntry(pdiskId, pdiskInfo);
                    pdisk->SetEntityStatus(NKikimrBlobStorage::DESTROY);
                }
            }

            NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk *CreatePDiskEntry(const TPDiskId &fullPDiskId,
                    const TPDiskInfo &pdiskInfo) {
                const ui32 nodeId = fullPDiskId.NodeId;
                const ui32 pdiskId = fullPDiskId.PDiskId;

                NKikimrBlobStorage::TNodeWardenServiceSet &service = *Services[nodeId].MutableServiceSet();
                NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk *pdisk = service.AddPDisks();
                pdisk->SetNodeID(nodeId);
                pdisk->SetPDiskID(pdiskId);
                if (pdiskInfo.Path) {
                    pdisk->SetPath(pdiskInfo.Path);
                } else if (pdiskInfo.LastSeenPath) {
                    pdisk->SetPath(pdiskInfo.LastSeenPath);
                }
                pdisk->SetPDiskGuid(pdiskInfo.Guid);
                pdisk->SetPDiskCategory(pdiskInfo.Kind.GetRaw());
                pdisk->SetExpectedSerial(pdiskInfo.ExpectedSerial);
                pdisk->SetManagementStage(Self->SerialManagementStage);
                if (pdiskInfo.PDiskConfig && !pdisk->MutablePDiskConfig()->ParseFromString(pdiskInfo.PDiskConfig)) {
                    // TODO(alexvru): report this somehow
                }
                pdisk->SetSpaceColorBorder(Self->PDiskSpaceColorBorder);

                switch (pdiskInfo.Mood) {
                    case NBsController::TPDiskMood::EValue::Normal:
                        break;
                    case NBsController::TPDiskMood::EValue::Restarting:
                        pdisk->SetEntityStatus(NKikimrBlobStorage::RESTART);
                        break;
                }

                return pdisk;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // VSLOT OPERATIONS
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            void AddVSlotToProtobuf(const TVSlotId &vslotId, const TVSlotInfo &vslotInfo, TMood::EValue mood,
                    TMaybe<NKikimrBlobStorage::EEntityStatus> status = Nothing()) {
                NKikimrBlobStorage::TNodeWardenServiceSet &service = *Services[vslotId.NodeId].MutableServiceSet();

                NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk &item = *service.AddVDisks();

                // fill in VDiskID for this new VSlot
                VDiskIDFromVDiskID(vslotInfo.GetVDiskId(), item.MutableVDiskID());

                // fill in VDiskLocation
                Serialize(item.MutableVDiskLocation(), vslotInfo);

                // set up kind
                item.SetVDiskKind(vslotInfo.Kind);

                // set destroy/donor flag if needed
                switch (mood) {
                    case TMood::Delete:
                        item.SetDoDestroy(true);
                        item.SetEntityStatus(NKikimrBlobStorage::DESTROY); // set explicitly
                        Y_ABORT_UNLESS(!status);
                        break;

                    case TMood::Donor:
                        Y_ABORT_UNLESS(!status);
                        break;

                    case TMood::Normal:
                        if (status) {
                            item.SetEntityStatus(*status);
                        }
                        break;

                    case TMood::Wipe:
                        item.SetDoWipe(true);
                        break;

                    case TMood::ReadOnly:
                        item.SetReadOnly(true);
                        break;

                    default:
                        Y_ABORT();
                }

                if (const TGroupInfo *group = State.Groups.Find(vslotInfo.GroupId); group && mood != TMood::Delete) {
                    item.SetStoragePoolName(State.StoragePools.Get().at(group->StoragePoolId).Name);

                    const TVSlotFinder vslotFinder{[this](TVSlotId vslotId, auto&& callback) {
                        if (const TVSlotInfo *vslot = State.VSlots.Find(vslotId)) {
                            callback(*vslot);
                        }
                    }};

                    SerializeDonors(&item, vslotInfo, *group, vslotFinder);
                } else {
                    Y_ABORT_UNLESS(mood != TMood::Donor);
                }
            }

            void ApplyVSlotCreated(const TVSlotId &vslotId, const TVSlotInfo &vslotInfo) {
                AddVSlotToProtobuf(vslotId, vslotInfo, TMood::Normal, NKikimrBlobStorage::CREATE);
            }

            void ApplyVSlotDeleted(const TVSlotId& vslotId, const TVSlotInfo& vslotInfo) {
                if (DeletedPDiskIds.count(vslotId.ComprisingPDiskId()) && vslotInfo.IsBeingDeleted()) {
                    // the slot has been deleted along with its PDisk; although it is useless to slay slots over PDisk
                    // that is being stopped, we issue this command to terminate VDisk actors correctly
                    AddVSlotToProtobuf(vslotId, vslotInfo, TMood::Delete);
                }
            }

            void ApplyVSlotDiff(const TVSlotId &vslotId, const TVSlotInfo &prev, const TVSlotInfo &cur) {
                if (!prev.IsBeingDeleted() && cur.IsBeingDeleted()) {
                    // the slot has started deletion during this update
                    AddVSlotToProtobuf(vslotId, prev, TMood::Delete);
                } else if (prev.Mood != cur.Mood) {
                    // the slot mood has changed
                    AddVSlotToProtobuf(vslotId, cur, static_cast<TMood::EValue>(cur.Mood));
                } else if (prev.GroupGeneration != cur.GroupGeneration) {
                    // the slot generation has changed
                    AddVSlotToProtobuf(vslotId, cur, TMood::Normal);
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // GROUP OPERATIONS
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            void ApplyGroupCreated(const TGroupId& groupId, const TGroupInfo &groupInfo) {
                if (!groupInfo.VDisksInGroup && groupInfo.VirtualGroupState != NKikimrBlobStorage::EVirtualGroupState::WORKING) {
                    return; // do not report virtual groups that are not properly created yet
                }

                // create ordered map of VDisk entries for group
                THashSet<TNodeId> nodes;
                for (const TVSlotInfo *vslot : groupInfo.VDisksInGroup) {
                    Y_ABORT_UNLESS(vslot->GroupGeneration == groupInfo.Generation);
                    nodes.insert(vslot->VSlotId.NodeId);
                }
                for (auto it = Self->GroupToNode.lower_bound(std::make_tuple(groupId, Min<TNodeId>()));
                        it != Self->GroupToNode.end() && *it <= std::make_tuple(groupId, Max<TNodeId>()); ++it) {
                    const auto [groupId, nodeId] = *it;
                    nodes.insert(nodeId);
                }

                // check tenant id, if necessary
                TMaybe<TKikimrScopeId> scopeId;
                const TStoragePoolInfo& info = State.StoragePools.Get().at(groupInfo.StoragePoolId);
                if (info.SchemeshardId && info.PathItemId) {
                    scopeId = TKikimrScopeId(*info.SchemeshardId, *info.PathItemId);
                } else {
                    Y_ABORT_UNLESS(!info.SchemeshardId && !info.PathItemId);
                }
                const TString storagePoolName = info.Name;

                // push group information to each node that will receive VDisk status update
                for (TNodeId nodeId : nodes) {
                    NKikimrBlobStorage::TNodeWardenServiceSet *service = Services[nodeId].MutableServiceSet();
                    SerializeGroupInfo(service->AddGroups(), groupInfo, storagePoolName, scopeId);
                }

                // push group state notification to NodeWhiteboard (for virtual groups only)
                if (groupInfo.VirtualGroupState) {
                    TBlobStorageGroupInfo::TDynamicInfo dynInfo(groupInfo.ID, groupInfo.Generation);
                    for (const auto& vdisk : groupInfo.VDisksInGroup) {
                        const auto& id = vdisk->VSlotId;
                        dynInfo.PushBackActorId(MakeBlobStorageVDiskID(id.NodeId, id.PDiskId, id.VSlotId));
                    }
                    State.NodeWhiteboardOutbox.emplace_back(new NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate(
                        MakeIntrusive<TBlobStorageGroupInfo>(groupInfo.Topology, std::move(dynInfo), storagePoolName,
                        scopeId, NPDisk::DEVICE_TYPE_UNKNOWN)));
                }
            }

            void ApplyGroupDeleted(const TGroupId &groupId, const TGroupInfo& /*groupInfo*/) {
                for (const auto &kv : State.Nodes.Get()) {
                    const TNodeId nodeId = kv.first;
                    NKikimrBlobStorage::TNodeWardenServiceSet &service = *Services[nodeId].MutableServiceSet();
                    NKikimrBlobStorage::TGroupInfo &item = *service.AddGroups();
                    item.SetGroupID(groupId.GetRawId());
                    item.SetEntityStatus(NKikimrBlobStorage::DESTROY);
                }
            }

            void ApplyGroupDiff(const TGroupId &groupId, const TGroupInfo &prev, const TGroupInfo &cur) {
                if (prev.Generation != cur.Generation) {
                    ApplyGroupCreated(groupId, cur);
                }
                Y_ABORT_UNLESS(prev.VDisksInGroup.size() == cur.VDisksInGroup.size() ||
                    (cur.VDisksInGroup.empty() && cur.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE));
                for (size_t i = 0; i < cur.VDisksInGroup.size(); ++i) {
                    const TVSlotInfo& prevSlot = *prev.VDisksInGroup[i];
                    const TVSlotInfo& curSlot = *cur.VDisksInGroup[i];
                    if (prevSlot.VSlotId != curSlot.VSlotId) {
                        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA05, "VDisk moved",
                            (UniqueId, State.UniqueId),
                            (PrevSlot, prevSlot.VSlotId),
                            (CurSlot, curSlot.VSlotId),
                            (VDiskId, curSlot.GetVDiskId()));
                    }
                }
            }
        };

        bool TBlobStorageController::CommitConfigUpdates(TConfigState& state, bool suppressFailModelChecking,
                bool suppressDegradedGroupsChecking, bool suppressDisintegratedGroupsChecking,
                TTransactionContext& txc, TString *errorDescription, NKikimrBlobStorage::TConfigResponse *response) {
            NIceDb::TNiceDb db(txc.DB);

            for (TGroupId groupId : state.GroupContentChanged) {
                TGroupInfo *group = state.Groups.FindForUpdate(groupId);
                Y_ABORT_UNLESS(group);
                ++group->Generation;
                for (const TVSlotInfo *slot : group->VDisksInGroup) {
                    if (slot->GroupGeneration != group->Generation) {
                        TVSlotInfo *mutableSlot = state.VSlots.FindForUpdate(slot->VSlotId);
                        Y_ABORT_UNLESS(mutableSlot);
                        mutableSlot->GroupGeneration = group->Generation;
                    }
                }
            }

            bool errors = false;
            std::vector<TGroupId> disintegratedByExpectedStatus;
            std::vector<TGroupId> disintegrated;
            std::vector<TGroupId> degraded;

            if (!suppressDisintegratedGroupsChecking) {
                for (auto&& [base, overlay] : state.Groups.Diff()) {
                    if (base && overlay->second) {
                        const TGroupInfo::TGroupStatus& prev = base->second->Status;
                        const TGroupInfo::TGroupStatus& status = overlay->second->Status;
                        if (status.ExpectedStatus == NKikimrBlobStorage::TGroupStatus::DISINTEGRATED &&
                                status.ExpectedStatus != prev.ExpectedStatus) { // status did really change
                            disintegratedByExpectedStatus.push_back(overlay->first);
                            errors = true;
                        }
                     }
                }
            }

            // check that group modification would not degrade failure model
            if (!suppressFailModelChecking) {
                for (TGroupId groupId : state.GroupFailureModelChanged) {
                    if (const TGroupInfo *group = state.Groups.Find(groupId); group && group->VDisksInGroup) {
                        // process only groups with changed content; create topology for group
                        auto& topology = *group->Topology;
                        // fill in vector of failed disks (that are not fully operational)
                        TBlobStorageGroupInfo::TGroupVDisks failed(&topology);
                        for (const TVSlotInfo *slot : group->VDisksInGroup) {
                            if (!slot->IsReady) {
                                failed |= {&topology, slot->GetShortVDiskId()};
                            }
                        }
                        // check the failure model
                        auto& checker = *topology.QuorumChecker;
                        if (!checker.CheckFailModelForGroup(failed)) {
                            disintegrated.push_back(groupId);
                            errors = true;
                        } else if (!suppressDegradedGroupsChecking && checker.IsDegraded(failed)) {
                            degraded.push_back(groupId);
                            errors = true;
                        }
                    } else {
                        Y_ABORT_UNLESS(group); // group must exist
                    }
                }
            }

            if (errors) {
                TStringStream msg;
                if (!degraded.empty()) {
                    msg << "Degraded GroupIds# " << FormatList(degraded) << ' ';
                    if (response) {
                        for (const auto& id: degraded) { 
                            response->MutableGroupsGetDegraded()->Add(id.GetRawId());
                        }
                    }
                }
                if (!disintegrated.empty()) {
                    msg << "Disintegrated GroupIds# " << FormatList(disintegrated) << ' ';
                    if (response) {
                        for (const auto& id: disintegrated) {
                            response->MutableGroupsGetDisintegrated()->Add(id.GetRawId());
                        }
                    }
                }
                if (!disintegratedByExpectedStatus.empty()) {
                    msg << "DisintegratedByExpectedStatus GroupIds# " << FormatList(disintegratedByExpectedStatus) << ' ';
                    if (response) {
                        for (const auto& id: disintegratedByExpectedStatus) {
                            response->MutableGroupsGetDisintegratedByExpectedStatus()->Add(id.GetRawId());
                        }
                    }
                }
                *errorDescription = msg.Str();
                errorDescription->pop_back();
                return false;
            }

            // trim PDisks awaiting deletion
            for (const TPDiskId& pdiskId : state.PDisksToRemove) {
                TPDiskInfo *pdiskInfo = state.PDisks.FindForUpdate(pdiskId);
                Y_ABORT_UNLESS(pdiskInfo);
                if (pdiskInfo->NumActiveSlots) {
                    *errorDescription = TStringBuilder() << "failed to remove PDisk# " << pdiskId << " as it has active VSlots";
                    return false;
                }
                for (const auto& [vslotId, vslot] : std::exchange(pdiskInfo->VSlotsOnPDisk, {})) {
                    Y_ABORT_UNLESS(vslot->IsBeingDeleted());
                    state.DeleteDestroyedVSlot(vslot);
                }
                state.PDisks.DeleteExistingEntry(pdiskId);
            }

            if (state.HostConfigs.Changed()) {
                MakeTableMerger<Schema::HostConfig>(&HostConfigs, &state.HostConfigs.Get(), this)(txc);
            }
            if (state.Boxes.Changed()) {
                MakeTableMerger<Schema::Box>(&Boxes, &state.Boxes.Get(), this)(txc);
            }
            if (state.StoragePools.Changed()) {
                MakeTableMerger<Schema::BoxStoragePool>(&StoragePools, &state.StoragePools.Get(), this)(txc);
            }
            if (state.Nodes.Changed()) {
                MakeTableMerger<Schema::Node>(&Nodes, &state.Nodes.Get(), this)(txc);
            }
            if (state.BlobDepotDeleteQueue.Changed()) {
                MakeTableMerger<Schema::BlobDepotDeleteQueue>(&BlobDepotDeleteQueue, &state.BlobDepotDeleteQueue.Get(), this)(txc);
            }

            // apply overlay maps to their respective tables
            state.PDisks.ApplyToTable(this, txc);
            state.VSlots.ApplyToTable(this, txc);
            state.Groups.ApplyToTable(this, txc);
            state.DrivesSerials.ApplyToTable(this, txc);

            // apply group to storage pool mapping
            for (auto&& [base, overlay] : state.Groups.Diff()) {
                using Table = Schema::GroupStoragePool;
                if (!overlay->second) {
                    db.Table<Table>().Key(overlay->first.GetRawId()).Delete();
                } else if (!base || base->second->StoragePoolId != overlay->second->StoragePoolId) {
                    db.Table<Table>().Key(overlay->first.GetRawId()).Update<Table::BoxId, Table::StoragePoolId>(
                        std::get<0>(overlay->second->StoragePoolId),
                        std::get<1>(overlay->second->StoragePoolId));
                }
            }

            // trim the PDiskMetrics table
            for (auto&& [base, overlay] : state.PDisks.Diff()) {
                if (!overlay->second) {
                    db.Table<Schema::PDiskMetrics>().Key(overlay->first.GetKey()).Delete();
                }
            }

            // remove unused group latency and vdisk metrics records
            for (auto&& [base, overlay] : state.Groups.Diff()) {
                if (!overlay->second) {
                    const TGroupId groupId = overlay->first;
                    db.Table<Schema::GroupLatencies>().Key(groupId.GetRawId()).Delete();
                }
            }

            // remove unused vdisk metrics for either deleted vslots or with changed generation
            for (auto&& [base, overlay] : state.VSlots.Diff()) {
                if (!overlay->second || (base && overlay->second->GroupGeneration != base->second->GroupGeneration)) {
                    const TVDiskID& vdiskId = base->second->GetVDiskId();
                    db.Table<Schema::VDiskMetrics>().Key(vdiskId.GroupID.GetRawId(), vdiskId.GroupGeneration, vdiskId.FailRealm,
                        vdiskId.FailDomain, vdiskId.VDisk).Delete();
                }
            }

            // write down NextGroupId if it has changed
            if (state.NextGroupId.Changed()) {
                db.Table<Schema::State>().Key(true).Update<Schema::State::NextGroupID>(state.NextGroupId.Get().GetRawId());
            }
            if (state.NextStoragePoolId.Changed()) {
                db.Table<Schema::State>().Key(true).Update<Schema::State::NextStoragePoolId>(state.NextStoragePoolId.Get());
            }
            if (state.SerialManagementStage.Changed()) {
                db.Table<Schema::State>().Key(true).Update<Schema::State::SerialManagementStage>(state.SerialManagementStage.Get());
            }
            if (state.NextVirtualGroupId.Changed()) {
                db.Table<Schema::State>().Key(true).Update<Schema::State::NextVirtualGroupId>(state.NextVirtualGroupId.Get().GetRawId());
            }

            CommitSelfHealUpdates(state);
            CommitScrubUpdates(state, txc);
            CommitStoragePoolStatUpdates(state);
            CommitSysViewUpdates(state);
            CommitVirtualGroupUpdates(state);

            // add updated and remove deleted vslots from VSlotReadyTimestampQ
            const TMonotonic now = TActivationContext::Monotonic();
            for (auto&& [base, overlay] : state.VSlots.Diff()) {
                if (!overlay->second || !overlay->second->Group) { // deleted one
                    (overlay->second ? overlay->second : base->second)->DropFromVSlotReadyTimestampQ();
                    NotReadyVSlotIds.erase(overlay->first);
                } else if (overlay->second->Status != NKikimrBlobStorage::EVDiskStatus::READY) {
                    overlay->second->DropFromVSlotReadyTimestampQ();
                } else if (!base || base->second->Status != NKikimrBlobStorage::EVDiskStatus::READY) {
                    overlay->second->PutInVSlotReadyTimestampQ(now);
                } else {
                    Y_DEBUG_ABORT_UNLESS(overlay->second->IsReady || overlay->second->IsInVSlotReadyTimestampQ());
                }
            }

            for (auto&& [base, overlay] : state.Groups.Diff()) {
                if (!overlay->second) { // deleted group
                    auto begin = GroupToNode.lower_bound(std::make_tuple(overlay->first, Min<TNodeId>()));
                    auto end = GroupToNode.upper_bound(std::make_tuple(overlay->first, Max<TNodeId>()));
                    for (auto it = begin; it != end; ++it) {
                        const auto [groupId, nodeId] = *it;
                        GetNode(nodeId).GroupsRequested.erase(groupId);
                    }
                    GroupToNode.erase(begin, end);
                }
            }

            TNodeWardenUpdateNotifier(this, state).Execute();

            state.CheckConsistency();
            state.Commit();
            ValidateInternalState();

            ScheduleVSlotReadyUpdate();

            return true;
        }

        void TBlobStorageController::CommitSelfHealUpdates(TConfigState& state) {
            auto ev = std::make_unique<TEvControllerNotifyGroupChange>();
            auto sh = MakeHolder<TEvControllerUpdateSelfHealInfo>();

            for (auto&& [base, overlay] : state.Groups.Diff()) {
                const TGroupId groupId = overlay->first;
                if (!overlay->second) { // item was deleted, drop it from the cache
                    const ui32 erased = GroupLookup.erase(groupId);
                    Y_ABORT_UNLESS(erased);
                    sh->GroupsToUpdate[groupId].reset();
                    ev->Deleted.push_back(groupId);
                } else if (base) { // item was overwritten, just update pointer in the lookup cache
                    const auto it = GroupLookup.find(groupId);
                    Y_ABORT_UNLESS(it != GroupLookup.end());
                    TGroupInfo *prev = std::exchange(it->second, overlay->second.Get());
                    Y_ABORT_UNLESS(prev == base->second.Get());
                    if (base->second->Generation != overlay->second->Generation) {
                        sh->GroupsToUpdate[groupId].emplace();
                    }
                } else { // a new item was inserted
                    auto&& [it, inserted] = GroupLookup.emplace(groupId, overlay->second.Get());
                    Y_ABORT_UNLESS(inserted);
                    sh->GroupsToUpdate[groupId].emplace();
                    ev->Created.push_back(groupId);
                }
            }
            for (auto&& [base, overlay] : state.PDisks.Diff()) {
                if (!overlay->second) {
                    continue; // ignore cases with PDisk was deleted -- groups over this disk must be deleted too
                } else if (base && base->second->GetSelfHealStatusTuple() == overlay->second->GetSelfHealStatusTuple()) {
                    continue; // nothing changed for this PDisk
                } else {
                    for (const auto& [id, slot] : overlay->second->VSlotsOnPDisk) {
                        if (slot->Group) {
                            sh->GroupsToUpdate[slot->GroupId].emplace();
                        }
                    }
                    if (StaticPDisks.contains(overlay->first)) {
                        state.PushStaticGroupsToSelfHeal = true;
                    }
                }
            }

            if (ev->Created || ev->Deleted) {
                state.StatProcessorOutbox.push_back(std::move(ev));
            }
            if (sh->GroupsToUpdate) {
                FillInSelfHealGroups(*sh, &state);
                state.UpdateSelfHealInfoMsg = std::move(sh);
            }
        }

        void TBlobStorageController::CommitScrubUpdates(TConfigState& state, TTransactionContext& txc) {
            // remove scrubbing entries
            for (auto&& [base, overlay] : state.PDisks.Diff()) {
                if (!overlay->second) { // PDisk deleted
                    ScrubState.OnDeletePDisk(overlay->first);
                }
            }
            for (auto&& [base, overlay] : state.VSlots.Diff()) {
                if (!overlay->second) {
                    ScrubState.OnDeleteVSlot(overlay->first, txc);
                } else if (!base) {
                    Y_DEBUG_ABORT_UNLESS(!overlay->second->IsBeingDeleted());
                    ScrubState.UpdateVDiskState(&*overlay->second);
                } else if (overlay->second->IsBeingDeleted() && !base->second->IsBeingDeleted()) {
                    ScrubState.OnDeleteVSlot(overlay->first, txc);
                }
            }
            for (auto&& [base, overlay] : state.Groups.Diff()) {
                if (!overlay->second) { // group deleted
                    ScrubState.OnDeleteGroup(overlay->first);
                }
            }
        }

        void TBlobStorageController::CommitStoragePoolStatUpdates(TConfigState& state) {
            // scan for created/renamed storage pools
            for (const auto& [prev, cur] : Diff(&StoragePools, &state.StoragePools.Get())) {
                if (!prev) { // created storage pool
                    StoragePoolStat->AddStoragePool(TStoragePoolStat::ConvertId(cur->first), cur->second.Name, 0);
                } else if (cur && prev->second.Name != cur->second.Name) { // renamed storage pool
                    StoragePoolStat->RenameStoragePool(TStoragePoolStat::ConvertId(cur->first), cur->second.Name);
                }
            }

            // apply created/deleted groups
            for (const auto& [base, overlay] : state.Groups.Diff()) {
                if (!base) { // newly created group
                    overlay->second->StatusFlags = overlay->second->GetStorageStatusFlags();
                    StoragePoolStat->Update(TStoragePoolStat::ConvertId(overlay->second->StoragePoolId),
                        std::nullopt, overlay->second->StatusFlags);
                } else if (!overlay->second) { // deleted group
                    if (state.StoragePools.Get().count(base->second->StoragePoolId)) {
                        StoragePoolStat->Update(TStoragePoolStat::ConvertId(base->second->StoragePoolId),
                            base->second->StatusFlags, std::nullopt);
                    }
                }
            }

            // apply VDisks going for destruction
            for (const auto& [base, overlay] : state.VSlots.Diff()) {
                if (overlay->second && overlay->second->DeletedFromStoragePoolId && overlay->second->Metrics.GetAllocatedSize()) {
                    StoragePoolStat->UpdateAllocatedSize(TStoragePoolStat::ConvertId(*overlay->second->DeletedFromStoragePoolId),
                        -overlay->second->Metrics.GetAllocatedSize());
                }
            }

            // scan for deleted storage pools
            for (const auto& [prev, cur] : Diff(&StoragePools, &state.StoragePools.Get())) {
                if (prev && !cur) {
                    StoragePoolStat->DeleteStoragePool(TStoragePoolStat::ConvertId(prev->first));
                }
            }
        }

        void TBlobStorageController::CommitSysViewUpdates(TConfigState& state) {
            for (const auto& [base, overlay] : state.PDisks.Diff()) {
                SysViewChangedPDisks.insert(overlay->first);
            }
            for (const auto& [base, overlay] : state.VSlots.Diff()) {
                SysViewChangedPDisks.insert(overlay->first.ComprisingPDiskId());
                SysViewChangedVSlots.insert(overlay->first);
                SysViewChangedGroups.insert(overlay->second ? overlay->second->GroupId : base->second->GroupId);
            }
            for (const auto& [base, overlay] : state.Groups.Diff()) {
                SysViewChangedGroups.insert(overlay->first);
            }
            for (const auto& [prev, cur] : Diff(&StoragePools, &state.StoragePools.Get())) {
                SysViewChangedStoragePools.insert(cur ? cur->first : prev->first);
            }
        }

        ui64 TBlobStorageController::TConfigState::ApplyConfigUpdates() {
            for (auto& [nodeId, ev, cookie] : Outbox) {
                Self.SendToWarden(nodeId, std::move(ev), cookie);
            }
            for (auto& ev : StatProcessorOutbox) {
                Self.SelfId().Send(Self.StatProcessorActorId, ev.release());
            }
            for (auto& ev : NodeWhiteboardOutbox) {
                Self.SelfId().Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(Self.SelfId().NodeId()), ev.release());
            }

            if (UpdateSelfHealInfoMsg) {
                UpdateSelfHealInfoMsg->ConfigTxSeqNo = Self.NextConfigTxSeqNo;
                TActivationContext::Send(std::make_unique<IEventHandle>(Self.SelfHealId, Self.SelfId(), UpdateSelfHealInfoMsg.Release()));
            }

            for (auto& fn : Callbacks) {
                fn();
            }
            if (PushStaticGroupsToSelfHeal) {
                Self.PushStaticGroupsToSelfHeal();
            }
            return Self.NextConfigTxSeqNo++;
        }

        void TBlobStorageController::TConfigState::DestroyVSlot(TVSlotId vslotId, const TVSlotInfo *ensureAcceptorSlot) {
            // obtain mutable slot pointer
            TVSlotInfo *mutableSlot = VSlots.FindForUpdate(vslotId);
            Y_ABORT_UNLESS(mutableSlot);

            // ensure it hasn't started deletion yet
            Y_ABORT_UNLESS(!mutableSlot->IsBeingDeleted());

            if (mutableSlot->Mood == TMood::Donor) {
                // this is the donor disk and it is being deleted; here we have to inform the acceptor disk of changed
                // donor set by simply removing the donor disk
                const TGroupInfo *group = Groups.Find(mutableSlot->GroupId);
                Y_ABORT_UNLESS(group);
                const ui32 orderNumber = group->Topology->GetOrderNumber(mutableSlot->GetShortVDiskId());
                const TVSlotInfo *acceptor = group->VDisksInGroup[orderNumber];
                Y_ABORT_UNLESS(acceptor);
                Y_ABORT_UNLESS(!acceptor->IsBeingDeleted());
                Y_ABORT_UNLESS(acceptor->Mood != TMood::Donor);
                Y_ABORT_UNLESS(mutableSlot->GroupId == acceptor->GroupId && mutableSlot->GroupGeneration < acceptor->GroupGeneration &&
                    mutableSlot->GetShortVDiskId() == acceptor->GetShortVDiskId());

                TVSlotInfo *mutableAcceptor = VSlots.FindForUpdate(acceptor->VSlotId);
                Y_ABORT_UNLESS(mutableAcceptor);
                Y_VERIFY_S(!ensureAcceptorSlot || ensureAcceptorSlot == mutableAcceptor,
                    "EnsureAcceptor# " << ensureAcceptorSlot->VSlotId << ':' << ensureAcceptorSlot->GetVDiskId()
                    << " MutableAcceptor# " << mutableAcceptor->VSlotId << ':' << mutableAcceptor->GetVDiskId()
                    << " Slot# " << mutableSlot->VSlotId << ':' << mutableSlot->GetVDiskId());

                auto& donors = mutableAcceptor->Donors;
                const size_t numErased = donors.erase(vslotId);
                Y_ABORT_UNLESS(numErased == 1);
            } else {
                Y_ABORT_UNLESS(!ensureAcceptorSlot);
            }

            // this is the acceptor disk and we have to delete all the donors as they are not needed anymore
            for (auto& donors = mutableSlot->Donors; !donors.empty(); ) {
                DestroyVSlot(*donors.begin(), mutableSlot);
            }

            // remove slot info from the PDisk
            TPDiskInfo *pdisk = PDisks.FindForUpdate(vslotId.ComprisingPDiskId());
            Y_ABORT_UNLESS(pdisk);
            --pdisk->NumActiveSlots;

            if (UncommittedVSlots.erase(vslotId)) {
                const ui32 erased = pdisk->VSlotsOnPDisk.erase(vslotId.VSlotId);
                Y_ABORT_UNLESS(erased);
                VSlots.DeleteExistingEntry(vslotId); // this slot hasn't been created yet and can be deleted safely
            } else {
                TGroupInfo *group = Groups.FindForUpdate(mutableSlot->GroupId);
                Y_ABORT_UNLESS(group);
                group->VSlotsBeingDeleted.insert(vslotId);
                mutableSlot->ScheduleForDeletion(group->StoragePoolId);
            }
        }

        void TBlobStorageController::TConfigState::DeleteDestroyedVSlot(const TVSlotInfo *vslot) {
            if (TGroupInfo *group = Groups.FindForUpdate(vslot->GroupId)) {
                const size_t num = group->VSlotsBeingDeleted.erase(vslot->VSlotId);
                Y_ABORT_UNLESS(num);
            }
            VSlots.DeleteExistingEntry(vslot->VSlotId);
        }

        void TBlobStorageController::TConfigState::CheckConsistency() const {
#ifndef NDEBUG
            PDisks.ForEach([&](const auto& pdiskId, const auto& pdisk) {
                ui32 numActiveSlots = 0;
                for (const auto& [vslotId, vslot] : pdisk.VSlotsOnPDisk) {
                    const TVSlotInfo *vslotInTable = VSlots.Find(TVSlotId(pdiskId, vslotId));
                    Y_ABORT_UNLESS(vslot == vslotInTable);
                    Y_ABORT_UNLESS(vslot->PDisk == &pdisk);
                    numActiveSlots += !vslot->IsBeingDeleted();
                }
                Y_ABORT_UNLESS(pdisk.NumActiveSlots == numActiveSlots);
            });
            VSlots.ForEach([&](const auto& vslotId, const auto& vslot) {
                Y_ABORT_UNLESS(vslot.VSlotId == vslotId);
                const TPDiskInfo *pdisk = PDisks.Find(vslot.VSlotId.ComprisingPDiskId());
                Y_ABORT_UNLESS(vslot.PDisk == pdisk);
                const auto it = vslot.PDisk->VSlotsOnPDisk.find(vslotId.VSlotId);
                Y_ABORT_UNLESS(it != vslot.PDisk->VSlotsOnPDisk.end());
                Y_ABORT_UNLESS(it->second == &vslot);
                const TGroupInfo *group = Groups.Find(vslot.GroupId);
                if (!vslot.IsBeingDeleted() && vslot.Mood != TMood::Donor) {
                    Y_ABORT_UNLESS(group);
                    Y_ABORT_UNLESS(vslot.Group == group);
                } else {
                    Y_ABORT_UNLESS(!vslot.Group);
                }
                if (vslot.Mood == TMood::Donor) {
                    Y_ABORT_UNLESS(vslot.Donors.empty());
                    Y_ABORT_UNLESS(group);
                    const ui32 orderNumber = group->Topology->GetOrderNumber(vslot.GetShortVDiskId());
                    const TVSlotInfo *acceptor = group->VDisksInGroup[orderNumber];
                    Y_ABORT_UNLESS(acceptor);
                    Y_ABORT_UNLESS(!acceptor->IsBeingDeleted());
                    Y_ABORT_UNLESS(acceptor->Mood != TMood::Donor);
                    Y_ABORT_UNLESS(acceptor->Donors.contains(vslotId));
                }
                for (const TVSlotId& donorVSlotId : vslot.Donors) {
                    const TVSlotInfo *donor = VSlots.Find(donorVSlotId);
                    Y_ABORT_UNLESS(donor);
                    Y_ABORT_UNLESS(donor->Mood == TMood::Donor);
                    Y_ABORT_UNLESS(donor->GroupId == vslot.GroupId);
                    Y_ABORT_UNLESS(donor->GroupGeneration < vslot.GroupGeneration + GroupContentChanged.count(vslot.GroupId));
                    Y_ABORT_UNLESS(donor->GetShortVDiskId() == vslot.GetShortVDiskId());
                }
            });
            Groups.ForEach([&](const auto& groupId, const auto& group) {
                Y_ABORT_UNLESS(groupId == group.ID);
                for (const TVSlotInfo *vslot : group.VDisksInGroup) {
                    Y_ABORT_UNLESS(VSlots.Find(vslot->VSlotId) == vslot);
                    Y_ABORT_UNLESS(vslot->Group == &group);
                    Y_ABORT_UNLESS(vslot->GroupId == groupId);
                    Y_ABORT_UNLESS(vslot->GroupGeneration == group.Generation);
                }
            });
#endif
        }

        void TBlobStorageController::TPDiskInfo::OnCommit() {
            for (const auto& [id, slot] : VSlotsOnPDisk) {
                slot.Mutable().PDisk = this;
            }
        }

        void TBlobStorageController::TVSlotInfo::OnCommit() {
            PDisk.Mutable().VSlotsOnPDisk[VSlotId.VSlotId] = this;
            if (Group) {
                const ui32 index = Group->Topology->GetOrderNumber(GetShortVDiskId());
                Group.Mutable().VDisksInGroup[index] = this;
            }
            if (VSlotReadyTimestampIter != TVSlotReadyTimestampQ::iterator()) {
                VSlotReadyTimestampIter->second = this;
            }
            DeletedFromStoragePoolId.reset();
        }

        void TBlobStorageController::TGroupInfo::OnCommit() {
            for (const auto& slot : VDisksInGroup) {
                slot.Mutable().Group = this;
            }
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TDefineHostConfig *pb, const THostConfigId &id,
                const THostConfigInfo &hostConfig) {
            pb->SetHostConfigId(id);
            pb->SetName(hostConfig.Name);
            pb->SetItemConfigGeneration(hostConfig.Generation.GetOrElse(1));
            for (const auto& [key, value] : hostConfig.Drives) {
                auto &drive = *pb->AddDrive();
                drive.SetPath(key.Path);
                drive.SetType(value.Type);
                drive.SetSharedWithOs(value.SharedWithOs);
                drive.SetReadCentric(value.ReadCentric);
                drive.SetKind(value.Kind);

                if (const auto& config = value.PDiskConfig) {
                    NKikimrBlobStorage::TPDiskConfig& pb = *drive.MutablePDiskConfig();
                    if (!pb.ParseFromString(*config)) {
                        throw TExError() << "HostConfigId# " << id << " undeserializable PDiskConfig string"
                            << " Path# " << key.Path;
                    }
                }
            }
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TDefineBox *pb, const TBoxId &id, const TBoxInfo &box) {
            pb->SetBoxId(id);
            pb->SetName(box.Name);
            pb->SetItemConfigGeneration(box.Generation.GetOrElse(1));
            for (const auto &userId : box.UserIds) {
                pb->AddUserId(std::get<1>(userId));
            }
            for (const auto &kv : box.Hosts) {
                auto *host = pb->AddHost();
                host->SetHostConfigId(kv.second.HostConfigId);
                host->SetEnforcedNodeId(kv.second.EnforcedNodeId.GetOrElse(0));
                auto *key = host->MutableKey();
                key->SetFqdn(kv.first.Fqdn);
                key->SetIcPort(kv.first.IcPort);
            }
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TDefineStoragePool *pb, const TBoxStoragePoolId &id, const TStoragePoolInfo &pool) {
            pb->SetBoxId(std::get<0>(id));
            pb->SetStoragePoolId(std::get<1>(id));
            pb->SetName(pool.Name);
            pb->SetItemConfigGeneration(pool.Generation.GetOrElse(1));
            pb->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(pool.ErasureSpecies));
            pb->SetEncryptionMode(pool.EncryptionMode.GetOrElse(0));
            auto* geometry = pb->MutableGeometry();
            if (pool.RealmLevelBegin) {
                geometry->SetRealmLevelBegin(*pool.RealmLevelBegin);
            }
            if (pool.RealmLevelEnd) {
                geometry->SetRealmLevelEnd(*pool.RealmLevelEnd);
            }
            if (pool.DomainLevelBegin) {
                geometry->SetDomainLevelBegin(*pool.DomainLevelBegin);
            }
            if (pool.DomainLevelEnd) {
                geometry->SetDomainLevelEnd(*pool.DomainLevelEnd);
            }
            if (pool.NumFailRealms) {
                geometry->SetNumFailRealms(*pool.NumFailRealms);
            }
            if (pool.NumFailDomainsPerFailRealm) {
                geometry->SetNumFailDomainsPerFailRealm(*pool.NumFailDomainsPerFailRealm);
            }
            if (pool.NumVDisksPerFailDomain) {
                geometry->SetNumVDisksPerFailDomain(*pool.NumVDisksPerFailDomain);
            }
            if (pool.SchemeshardId && pool.PathItemId) {
                auto *x = pb->MutableScopeId();
                x->SetX1(*pool.SchemeshardId);
                x->SetX2(*pool.PathItemId);
            }

            // group geometry
            if (pool.HasGroupGeometry()) {
                pb->MutableGeometry()->CopyFrom(pool.GetGroupGeometry());
            }

            // serialize VDiskKind as a string
            {
                const google::protobuf::EnumDescriptor *descr = NKikimrBlobStorage::TVDiskKind::EVDiskKind_descriptor();
                const google::protobuf::EnumValueDescriptor *value = descr->FindValueByNumber(pool.VDiskKind);
                if (!value) {
                    throw TExError() << "invalid VDiskKind# " << pool.VDiskKind;
                }
                pb->SetVDiskKind(value->name());
            }

            // usage pattern
            if (pool.HasUsagePattern()) {
                pb->MutableUsagePattern()->CopyFrom(pool.GetUsagePattern());
            }

            pb->SetKind(pool.Kind);
            pb->SetNumGroups(pool.NumGroups);
            pb->SetRandomizeGroupMapping(pool.RandomizeGroupMapping);

            for (const auto &userId : pool.UserIds) {
                pb->AddUserId(std::get<2>(userId));
            }

            for (const TStoragePoolInfo::TPDiskFilter &filter : pool.PDiskFilters) {
                Serialize(pb->AddPDiskFilter(), filter);
            }
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TPDiskFilter *pb, const TStoragePoolInfo::TPDiskFilter &filter) {
#define SERIALIZE_VARIABLE(NAME) (filter.NAME && (pb->AddProperty()->Set##NAME(*filter.NAME), true))
            SERIALIZE_VARIABLE(Type);
            SERIALIZE_VARIABLE(SharedWithOs);
            SERIALIZE_VARIABLE(ReadCentric);
            SERIALIZE_VARIABLE(Kind);
#undef SERIALIZE_VARIABLE
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TBaseConfig::TPDisk *pb, const TPDiskId &id, const TPDiskInfo &pdisk) {
            const TPDiskCategory category(pdisk.Kind);

            pb->SetNodeId(id.NodeId);
            pb->SetPDiskId(id.PDiskId);
            pb->SetPath(pdisk.Path);
            pb->SetType(PDiskTypeToPDiskType(category.Type()));
            pb->SetSharedWithOs(!pdisk.SharedWithOs ? NKikimrBlobStorage::ETriStateBool::kNotSet
                : *pdisk.SharedWithOs ? NKikimrBlobStorage::ETriStateBool::kTrue
                : NKikimrBlobStorage::ETriStateBool::kFalse);
            pb->SetReadCentric(!pdisk.ReadCentric ? NKikimrBlobStorage::ETriStateBool::kNotSet
                : *pdisk.ReadCentric ? NKikimrBlobStorage::ETriStateBool::kTrue
                : NKikimrBlobStorage::ETriStateBool::kFalse);
            pb->SetKind(category.Kind());
            pb->SetGuid(pdisk.Guid);
            if (pdisk.PDiskConfig && !pb->MutablePDiskConfig()->ParseFromString(pdisk.PDiskConfig)) {
                throw TExError() << "failed to parse PDiskConfig for PDisk# " << id;
            }
            pb->SetBoxId(pdisk.BoxId);
            pb->SetNumStaticSlots(pdisk.StaticSlotUsage);
            pb->SetDriveStatus(pdisk.Status);
            pb->SetExpectedSlotCount(pdisk.ExpectedSlotCount);
            pb->SetDriveStatusChangeTimestamp(pdisk.StatusTimestamp.GetValue());
            pb->SetDecommitStatus(pdisk.DecommitStatus);
            pb->MutablePDiskMetrics()->CopyFrom(pdisk.Metrics);
            pb->MutablePDiskMetrics()->ClearPDiskId();
            pb->SetExpectedSerial(pdisk.ExpectedSerial);
            pb->SetLastSeenSerial(pdisk.LastSeenSerial);
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TVSlotId *pb, TVSlotId id) {
            id.Serialize(pb);
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TVDiskLocation *pb, const TVSlotInfo& vslot) {
            pb->SetNodeID(vslot.VSlotId.NodeId);
            pb->SetPDiskID(vslot.VSlotId.PDiskId);
            pb->SetVDiskSlotID(vslot.VSlotId.VSlotId);
            pb->SetPDiskGuid(vslot.PDisk->Guid);
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TVDiskLocation *pb, const TVSlotId& vslotId) {
            pb->SetNodeID(vslotId.NodeId);
            pb->SetPDiskID(vslotId.PDiskId);
            pb->SetVDiskSlotID(vslotId.VSlotId);
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TBaseConfig::TVSlot *pb, const TVSlotInfo &vslot,
                const TVSlotFinder& finder) {
            Serialize(pb->MutableVSlotId(), vslot.VSlotId);
            pb->SetGroupId(vslot.GroupId.GetRawId());
            pb->SetGroupGeneration(vslot.GroupGeneration);
            pb->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(vslot.Kind));
            pb->SetFailRealmIdx(vslot.RingIdx);
            pb->SetFailDomainIdx(vslot.FailDomainIdx);
            pb->SetVDiskIdx(vslot.VDiskIdx);
            pb->SetAllocatedSize(vslot.Metrics.GetAllocatedSize());
            pb->MutableVDiskMetrics()->CopyFrom(vslot.Metrics);
            pb->MutableVDiskMetrics()->ClearVDiskId();
            pb->SetStatus(NKikimrBlobStorage::EVDiskStatus_Name(vslot.Status));
            for (const TVSlotId& vslotId : vslot.Donors) {
                auto *item = pb->AddDonors();
                Serialize(item->MutableVSlotId(), vslotId);
                finder(vslotId, [item](const TVSlotInfo& vslot) {
                    VDiskIDFromVDiskID(vslot.GetVDiskId(), item->MutableVDiskId());
                    item->MutableVDiskMetrics()->CopyFrom(vslot.Metrics);
                    item->MutableVDiskMetrics()->ClearVDiskId();
                });
            }
            pb->SetReady(vslot.IsReady);
            pb->SetReadOnly(vslot.Mood == TMood::ReadOnly);
        }

        void TBlobStorageController::Serialize(NKikimrBlobStorage::TBaseConfig::TGroup *pb, const TGroupInfo &group) {
            pb->SetGroupId(group.ID.GetRawId());
            pb->SetGroupGeneration(group.Generation);
            pb->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(group.ErasureSpecies));
            for (const TVSlotInfo *vslot : group.VDisksInGroup) {
                Serialize(pb->AddVSlotId(), vslot->VSlotId);
            }
            pb->SetBoxId(std::get<0>(group.StoragePoolId));
            pb->SetStoragePoolId(std::get<1>(group.StoragePoolId));
            pb->SetSeenOperational(group.SeenOperational);

            const auto& status = group.Status;
            pb->SetOperatingStatus(status.OperatingStatus);
            pb->SetExpectedStatus(status.ExpectedStatus);

            if (group.DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE || group.VirtualGroupState) {
                auto *vgi = pb->MutableVirtualGroupInfo();
                if (group.VirtualGroupState) {
                    vgi->SetState(*group.VirtualGroupState);
                }
                if (group.VirtualGroupName) {
                    vgi->SetName(*group.VirtualGroupName);
                }
                if (group.BlobDepotId) {
                    vgi->SetBlobDepotId(*group.BlobDepotId);
                }
                if (group.ErrorReason) {
                    vgi->SetErrorReason(*group.ErrorReason);
                }
                vgi->SetDecommitStatus(group.DecommitStatus);
            }
        }

        void TBlobStorageController::SerializeDonors(NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk *vdisk,
                const TVSlotInfo& vslot, const TGroupInfo& group, const TVSlotFinder& finder) {
            if (vslot.Mood == TMood::Donor) {
                ui32 numFailRealms = 0, numFailDomainsPerFailRealm = 0, numVDisksPerFailDomain = 0;
                for (const TVSlotInfo *slot : group.VDisksInGroup) {
                    numFailRealms = Max(numFailRealms, slot->RingIdx + 1);
                    numFailDomainsPerFailRealm = Max(numFailDomainsPerFailRealm, slot->FailDomainIdx + 1);
                    numVDisksPerFailDomain = Max(numVDisksPerFailDomain, slot->VDiskIdx + 1);
                }
                Y_ABORT_UNLESS(numFailRealms * numFailDomainsPerFailRealm * numVDisksPerFailDomain == group.VDisksInGroup.size());
                auto *pb = vdisk->MutableDonorMode();
                pb->SetNumFailRealms(numFailRealms);
                pb->SetNumFailDomainsPerFailRealm(numFailDomainsPerFailRealm);
                pb->SetNumVDisksPerFailDomain(numVDisksPerFailDomain);
                pb->SetErasureSpecies(group.ErasureSpecies);
            }

            std::vector<std::pair<TVDiskID, TVSlotId>> donors;
            for (const TVSlotId& donorVSlotId : vslot.Donors) {
                std::optional<TVDiskID> vdiskId;
                finder(donorVSlotId, [&](const TVSlotInfo& donor) {
                    vdiskId.emplace(donor.GetVDiskId());
                });
                Y_ABORT_UNLESS(vdiskId);
                donors.emplace_back(*vdiskId, donorVSlotId);
            }

            std::sort(donors.begin(), donors.end());
            if (!donors.empty()) {
                for (size_t i = 0; i < donors.size() - 1; ++i) {
                    const auto& x = donors[i].first;
                    const auto& y = donors[i + 1].first;
                    Y_ABORT_UNLESS(x.GroupID == y.GroupID);
                    Y_ABORT_UNLESS(x.GroupGeneration < y.GroupGeneration);
                    Y_ABORT_UNLESS(TVDiskIdShort(x) == TVDiskIdShort(y));
                }
            }

            for (const auto& [donorVDiskId, donorVSlotId] : donors) {
                auto *pb = vdisk->AddDonors();
                VDiskIDFromVDiskID(donorVDiskId, pb->MutableVDiskId());
                Serialize(pb->MutableVDiskLocation(), donorVSlotId);
            }
        }

        void TBlobStorageController::SerializeGroupInfo(NKikimrBlobStorage::TGroupInfo *group, const TGroupInfo& groupInfo,
                const TString& storagePoolName, const TMaybe<TKikimrScopeId>& scopeId) {
            group->SetGroupID(groupInfo.ID.GetRawId());
            group->SetGroupGeneration(groupInfo.Generation);
            group->SetStoragePoolName(storagePoolName);

            group->SetEncryptionMode(groupInfo.EncryptionMode.GetOrElse(0));
            group->SetLifeCyclePhase(groupInfo.LifeCyclePhase.GetOrElse(0));
            group->SetMainKeyId(groupInfo.MainKeyId.GetOrElse(""));
            group->SetEncryptedGroupKey(groupInfo.EncryptedGroupKey.GetOrElse(""));
            group->SetGroupKeyNonce(groupInfo.GroupKeyNonce.GetOrElse(0));
            group->SetMainKeyVersion(groupInfo.MainKeyVersion.GetOrElse(0));

            if (scopeId) {
                auto *pb = group->MutableAcceptedScope();
                const TScopeId& x = scopeId->GetInterconnectScopeId();
                pb->SetX1(x.first);
                pb->SetX2(x.second);
            }

            if (groupInfo.VDisksInGroup) {
                group->SetErasureSpecies(groupInfo.ErasureSpecies);
                group->SetDeviceType(PDiskTypeToPDiskType(groupInfo.GetCommonDeviceType()));

                std::vector<std::pair<TVDiskID, const TVSlotInfo*>> vdisks;
                for (const auto& vslot : groupInfo.VDisksInGroup) {
                    vdisks.emplace_back(vslot->GetVDiskId(), vslot);
                }
                auto comp = [](const auto& x, const auto& y) { return x.first < y.first; };
                std::sort(vdisks.begin(), vdisks.end(), comp);

                TVDiskID prevVDiskId;
                NKikimrBlobStorage::TGroupInfo::TFailRealm *realm = nullptr;
                NKikimrBlobStorage::TGroupInfo::TFailRealm::TFailDomain *domain = nullptr;
                for (const auto& [vdiskId, vslot] : vdisks) {
                    if (!realm || prevVDiskId.FailRealm != vdiskId.FailRealm) {
                        realm = group->AddRings();
                        domain = nullptr;
                    }
                    if (!domain || prevVDiskId.FailDomain != vdiskId.FailDomain) {
                        Y_ABORT_UNLESS(realm);
                        domain = realm->AddFailDomains();
                    }
                    prevVDiskId = vdiskId;

                    Serialize(domain->AddVDiskLocations(), *vslot);
                }
            }

            if (groupInfo.VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::WORKING) {
                Y_ABORT_UNLESS(groupInfo.BlobDepotId);
                group->SetBlobDepotId(*groupInfo.BlobDepotId);
            } else if (groupInfo.VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED) {
                group->SetBlobDepotId(0);
            }

            if (groupInfo.DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
                group->SetDecommitStatus(groupInfo.DecommitStatus);
            }
        }

        void TBlobStorageController::SerializeSettings(NKikimrBlobStorage::TUpdateSettings *settings) {
            settings->AddDefaultMaxSlots(DefaultMaxSlots);
            settings->AddEnableSelfHeal(SelfHealEnable);
            settings->AddEnableDonorMode(DonorMode);
            settings->AddScrubPeriodicitySeconds(ScrubPeriodicity.Seconds());
            settings->AddPDiskSpaceMarginPromille(PDiskSpaceMarginPromille);
            settings->AddGroupReserveMin(GroupReserveMin);
            settings->AddGroupReservePartPPM(GroupReservePart);
            settings->AddMaxScrubbedDisksAtOnce(MaxScrubbedDisksAtOnce);
            settings->AddPDiskSpaceColorBorder(PDiskSpaceColorBorder);
            settings->AddEnableGroupLayoutSanitizer(GroupLayoutSanitizerEnabled);
            // TODO: settings->AddSerialManagementStage(SerialManagementStage);
            settings->AddAllowMultipleRealmsOccupation(AllowMultipleRealmsOccupation);
            settings->AddUseSelfHealLocalPolicy(UseSelfHealLocalPolicy);
            settings->AddTryToRelocateBrokenDisksLocallyFirst(TryToRelocateBrokenDisksLocallyFirst);
        }

} // NKikimr::NBsController
