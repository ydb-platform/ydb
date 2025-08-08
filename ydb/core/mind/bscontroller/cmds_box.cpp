#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineBox& cmd, TStatus& /*status*/) {
        const TBoxId &id = cmd.GetBoxId();
        const ui64 nextGen = CheckGeneration(cmd, Boxes.Get(), id);

        TBoxInfo box;
        box.Name = cmd.GetName();
        box.Generation = nextGen;
        for (const auto &userId : cmd.GetUserId()) {
            box.UserIds.emplace(id, userId);
        }
        THashSet<TNodeId> usedNodes;
        for (const auto &host : cmd.GetHost()) {
            TBoxInfo::THostKey hostKey;
            hostKey.BoxId = id;

            NKikimrBlobStorage::THostKey key = NormalizeHostKey(host.GetKey());
            hostKey.Fqdn = key.GetFqdn();
            hostKey.IcPort = key.GetIcPort();

            TBoxInfo::THostInfo info;
            info.HostConfigId = host.GetHostConfigId();
            if (const ui32 nodeId = host.GetEnforcedNodeId()) {
                info.EnforcedNodeId = nodeId;
            }

            const auto &hostConfigs = HostConfigs.Get();
            if (!hostConfigs.count(info.HostConfigId)) {
                throw TExHostConfigNotFound(info.HostConfigId);
            }

            const auto [it, inserted] = box.Hosts.emplace(std::move(hostKey), std::move(info));
            if (!inserted) {
                throw TExError() << "duplicate HostKey" << TErrorParams::BoxId(id) << TErrorParams::Fqdn(it->first.Fqdn)
                    << TErrorParams::IcPort(it->first.IcPort);
            }
        }

        auto &boxes = Boxes.Unshare();
        boxes[id] = std::move(box);
        Fit.Boxes.insert(id);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadBox& cmd, TStatus& status) {
        TSet<TBoxId> queryIds;
        if (cmd.BoxIdSize()) {
            const auto &ids = cmd.GetBoxId();
            queryIds.insert(ids.begin(), ids.end());
        } else {
            for (const auto &kv : Boxes.Get()) {
                queryIds.insert(kv.first);
            }
        }

        const auto &boxes = Boxes.Get();
        for (const TBoxId &id : queryIds) {
            auto it = boxes.find(id);
            if (it == boxes.end()) {
                throw TExError() << "BoxId# " << id << " not found";
            }
            Serialize(status.AddBox(), it->first, it->second);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteBox& cmd, TStatus& /*status*/) {
        const TBoxId &id = cmd.GetBoxId();
        CheckGeneration(cmd, Boxes.Get(), id);

        auto &boxes = Boxes.Unshare();
        auto it = boxes.find(id);
        if (it == boxes.end()) {
            throw TExError() << "BoxId# " << id << " not found";
        }

        const auto &storagePools = StoragePools.Get();
        using T = std::tuple_element<1, TBoxStoragePoolId>::type;
        const auto min = TBoxStoragePoolId(id, std::numeric_limits<T>::min());
        const auto max = TBoxStoragePoolId(id, std::numeric_limits<T>::max());
        if (storagePools.lower_bound(min) != storagePools.upper_bound(max)) {
            throw TExError() << "BoxId# " << id << " has storage pools";
        }

        boxes.erase(it);
        Fit.Boxes.insert(id);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMergeBoxes& cmd, TStatus& /*status*/) {
        auto& boxes = Boxes.Unshare();

        auto getBox = [&](ui64 boxId, ui64 boxGeneration, const char *name) -> TBoxInfo& {
            auto it = boxes.find(boxId);
            if (it == boxes.end()) {
                throw TExError() << name << " BoxId# " << boxId << " not found";
            }
            TBoxInfo& box = it->second;
            const ui64 generation = box.Generation.GetOrElse(1);
            if (generation != boxGeneration) {
                throw TExError() << name << " BoxId# " << boxId << " Generation# " << generation
                    << " does not match expected Generation# " << boxGeneration;
            }
            box.Generation = generation + 1;
            return box;
        };

        TBoxInfo& origin = getBox(cmd.GetOriginBoxId(), cmd.GetOriginBoxGeneration(), "origin");
        TBoxInfo& target = getBox(cmd.GetTargetBoxId(), cmd.GetTargetBoxGeneration(), "target");

        while (origin.Hosts) {
            auto node = origin.Hosts.extract(origin.Hosts.begin());
            node.key().BoxId = cmd.GetTargetBoxId();
            if (!target.Hosts.insert(std::move(node)).inserted) {
                throw TExError() << "duplicate hosts in merged box";
            }
        }

        // spin the generation
        target.Generation = target.Generation.GetOrElse(1) + 1;

        auto& storagePools = StoragePools.Unshare();
        auto& storagePoolGroups = StoragePoolGroups.Unshare();

        for (const auto& item : cmd.GetStoragePoolIdMap()) {
            const TBoxStoragePoolId origin(cmd.GetOriginBoxId(), item.GetOriginStoragePoolId());
            auto it = storagePools.find(origin);
            if (it == storagePools.end()) {
                throw TExError() << "origin StoragePoolId# " << origin << " not found";
            }

            const TBoxStoragePoolId target(cmd.GetTargetBoxId(), item.GetTargetStoragePoolId());
            if (storagePools.count(target)) {
                throw TExError() << "target StoragePoolId# " << target << " is duplicate";
            }

            // update the key for the storage pool; also update PDiskFilters which contain BoxId/StoragePoolId
            auto node = storagePools.extract(it);
            node.key() = target;
            TStoragePoolInfo& sp = node.mapped();

            // fix PDisk filters
            TStoragePoolInfo::TPDiskFilters filters;
            while (sp.PDiskFilters) {
                auto node = sp.PDiskFilters.extract(sp.PDiskFilters.begin());
                node.value().BoxId = cmd.GetTargetBoxId();
                node.value().StoragePoolId = item.GetTargetStoragePoolId();
                filters.insert(filters.end(), std::move(node));
            }
            filters.swap(sp.PDiskFilters);

            // fix user ids
            TStoragePoolInfo::TUserIds userIds;
            while (sp.UserIds) {
                auto node = sp.UserIds.extract(sp.UserIds.begin());
                node.value() = std::tuple_cat(target, std::tie(std::get<2>(node.value())));
                userIds.insert(userIds.end(), std::move(node));
            }
            userIds.swap(sp.UserIds);

            storagePools.insert(std::move(node));

            // process storage pool to group mapping
            for (;;) {
                auto node = storagePoolGroups.extract(origin);
                if (node.empty()) {
                    break;
                }

                // update storage pool id mapping in group itself
                TGroupInfo *group = Groups.FindForUpdate(node.mapped());
                Y_ABORT_UNLESS(group);
                Y_ABORT_UNLESS(group->StoragePoolId == origin);
                group->StoragePoolId = target;

                // update the key and insert item back into map
                node.key() = target;
                storagePoolGroups.insert(std::move(node));
            }
        }

        auto it = storagePools.lower_bound(TBoxStoragePoolId(cmd.GetOriginBoxId(), 0));
        if (it != storagePools.end() && std::get<0>(it->first) == cmd.GetOriginBoxId()) {
            throw TExError() << "not all storage pools from origin Box are enlisted in StoragePoolIdMap field";
        }

        // process PDisks of this box
        PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
            if (pdiskInfo.BoxId == cmd.GetOriginBoxId()) {
                TPDiskInfo *mut = PDisks.FindForUpdate(pdiskId);
                Y_ABORT_UNLESS(mut);
                mut->BoxId = cmd.GetTargetBoxId();
            }
        });

        // drop origin box
        boxes.erase(cmd.GetOriginBoxId());
    }

    template <class T>
    TPDiskId GetPDiskId(const TBlobStorageController::TConfigState& state, const T& command) {
        if (command.HasTargetPDiskId() && command.HasTargetPDiskLocation()) {
            throw TExError() << "Only one of TargetPDiskId or PDiskLocation can be specified";
        }

        if (command.HasTargetPDiskId()) {
            const NKikimrBlobStorage::TPDiskId& pdiskId = command.GetTargetPDiskId();
            ui32 targetNodeId = pdiskId.GetNodeId();
            ui32 targetPDiskId = pdiskId.GetPDiskId();
            if (const auto& hostId = state.HostRecords->GetHostId(targetNodeId)) {
                TPDiskId target(targetNodeId, targetPDiskId);
                if (state.PDisks.Find(target) && !state.PDisksToRemove.count(target)) {
                    return target;
                }
                throw TExPDiskNotFound(targetNodeId, targetPDiskId);
            }
            throw TExHostNotFound(targetNodeId);
        } else if (command.HasTargetPDiskLocation()) {
            const NKikimrBlobStorage::TPDiskLocation& pdiskLocation = command.GetTargetPDiskLocation();
            const TString& targetFqdn = pdiskLocation.GetFqdn();
            const TString& targetDiskPath = pdiskLocation.GetPath();

            auto range = state.HostRecords->ResolveNodeId(targetFqdn);

            for (auto it = range.first; it != range.second; ++it) {
                const TNodeId nodeId = it->second;
                if (const auto& pdiskId = state.FindPDiskByLocation(nodeId, targetDiskPath)) {
                    return *pdiskId;
                }
            }
            
            throw TExPDiskNotFound(targetFqdn, targetDiskPath);
        }
        throw TExError() << "Either TargetPDiskId or PDiskLocation must be specified";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TRestartPDisk& cmd, TStatus& /*status*/) {
        TPDiskId pdiskId = GetPDiskId(*this, cmd);

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);

        if (!pdisk) {
            throw TExPDiskNotFound(pdiskId.NodeId, pdiskId.PDiskId);
        }

        pdisk->Mood = TPDiskMood::Restarting;

        for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
            if (slot->Group) {
                auto *m = VSlots.FindForUpdate(slot->VSlotId);
                m->VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
                m->IsReady = false;
                TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
                GroupFailureModelChanged.insert(slot->Group->ID);
                group->CalculateGroupStatus();
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TSetPDiskReadOnly& cmd, TStatus& /*status*/) {
        TPDiskId pdiskId = GetPDiskId(*this, cmd);

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);

        if (!pdisk) {
            throw TExPDiskNotFound(pdiskId.NodeId, pdiskId.PDiskId);
        }

        if (cmd.GetValue()) {
            pdisk->Mood = TPDiskMood::ReadOnly;

            for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
                if (slot->Group) {
                    auto *m = VSlots.FindForUpdate(slot->VSlotId);
                    m->VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
                    m->IsReady = false;
                    TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
                    GroupFailureModelChanged.insert(slot->Group->ID);
                    group->CalculateGroupStatus();
                }
            }
        } else {
            pdisk->Mood = TPDiskMood::Normal;
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TStopPDisk& cmd, TStatus& /*status*/) {
        TPDiskId pdiskId = GetPDiskId(*this, cmd);

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);

        if (!pdisk) {
            throw TExPDiskNotFound(pdiskId.NodeId, pdiskId.PDiskId);
        }

        pdisk->Mood = TPDiskMood::Stop;

        for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
            if (slot->Group) {
                auto *m = VSlots.FindForUpdate(slot->VSlotId);
                m->VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
                m->IsReady = false;
                TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
                GroupFailureModelChanged.insert(slot->Group->ID);
                group->CalculateGroupStatus();
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMovePDisk& cmd, TStatus& /*status*/) {
        TPDiskId sourcePDiskId = GetPDiskId(*this, cmd.GetSourcePDisk());
        TPDiskId destinationPDiskId = GetPDiskId(*this, cmd.GetDestinationPDisk());

        TPDiskInfo *sourcePDisk = PDisks.FindForUpdate(sourcePDiskId);
        TPDiskInfo *destinationPDisk = PDisks.FindForUpdate(destinationPDiskId);

        if (!sourcePDisk) {
            throw TExPDiskNotFound(sourcePDiskId.NodeId, sourcePDiskId.PDiskId);
        }

        if (!destinationPDisk) {
            throw TExPDiskNotFound(destinationPDiskId.NodeId, destinationPDiskId.PDiskId);
        }

        if (sourcePDiskId == destinationPDiskId) {
            throw TExError() << "Source and destination PDiskIds are the same: " << sourcePDiskId;
        }

        if (destinationPDisk->Mood != TPDiskMood::Stop) {
            // It's a loose check, but actually destination disk can't start because the drive is locked by
            // the source disk, so we can safely assume that it is stopped if mood is Stop.
            throw TExError() << "Destination PDisk is not stopped: " << destinationPDiskId;
        }

        // Destination PDisk's GUID is not the same as source PDisk's GUID yet,
        // set it to the source PDisk's GUID, so that it can start.
        destinationPDisk->Guid = sourcePDisk->Guid;

        std::unordered_set<TGroupId> changedGroups;

        for (const auto& [id, srcSlot] : sourcePDisk->VSlotsOnPDisk) {
            if (srcSlot->Group) {
                TVDiskIdShort diskId = srcSlot->GetShortVDiskId();
                
                TVSlotId newSlotId(destinationPDiskId, srcSlot->VSlotId.VSlotId);

                auto* group = Groups.FindForUpdate(srcSlot->Group->ID);
                
                // Remove source slot from the group, so ConstructInplaceNewEntry can populate it with the new slot.
                const ui32 orderNumber = group->Topology->GetOrderNumber(diskId);
                group->VDisksInGroup[orderNumber] = nullptr;

                if (changedGroups.insert(group->ID).second) {
                    // VSlots "moving" requires generation change.
                    // Need to only update group's generation once, since in some setups
                    // one disk can have multiple vdisks of the same group.
                    group->Generation++;
                }

                // Update all vslots in the group to the new generation.
                auto it = group->VDisksInGroup.begin();
                for (; it != group->VDisksInGroup.end(); ++it) {
                    if ((*it)) {
                        auto slot = VSlots.FindForUpdate((*it)->VSlotId);
                        slot->GroupGeneration = group->Generation;
                    }
                }
                
                // Create a new slot on the destination PDisk.
                TVSlotInfo *dstSlot = VSlots.ConstructInplaceNewEntry(newSlotId, newSlotId, destinationPDisk,
                    srcSlot->GroupId, srcSlot->GroupGeneration, group->Generation, srcSlot->Kind, srcSlot->RingIdx,
                    srcSlot->FailDomainIdx, srcSlot->VDiskIdx, TMood::Normal, group, &Self.VSlotReadyTimestampQ,
                    TInstant::Zero(), TDuration::Zero());

                dstSlot->VDiskStatusTimestamp = Mono;

                UncommittedVSlots.insert(newSlotId);
                
                // Remove old slot from the source PDisk.
                VSlots.DeleteExistingEntry(srcSlot->VSlotId);
            }
        }

        // "Move" all slots from source PDisk to destination PDisk.
        //destinationPDisk->VSlotsOnPDisk.insert(sourcePDisk->VSlotsOnPDisk.begin(), sourcePDisk->VSlotsOnPDisk.end());

        for (const auto& [id, slot] : destinationPDisk->VSlotsOnPDisk) {
            TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
            GroupFailureModelChanged.insert(slot->Group->ID);
            group->CalculateGroupStatus();
        }

        // Adjust active slots on destination PDisk.
        destinationPDisk->NumActiveSlots = sourcePDisk->NumActiveSlots;
        destinationPDisk->Mood = TPDiskMood::Restarting;

        // And remove old pdisk altogether.
        // It will shutdown some time in the future, this can cause a race condition if destination PDisk
        // is restarted before the source PDisk is stopped, but nothing we can do here.
        PDisks.DeleteExistingEntry(sourcePDiskId);
    }

} // namespace NKikimr::NBsController
