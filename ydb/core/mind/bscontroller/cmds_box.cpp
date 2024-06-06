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

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TRestartPDisk& cmd, TStatus& /*status*/) {
        auto targetPDiskId = cmd.GetTargetPDiskId();

        TPDiskId pdiskId(targetPDiskId.GetNodeId(), targetPDiskId.GetPDiskId());

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);

        if (!pdisk) {
            throw TExPDiskNotFound(pdiskId.NodeId, pdiskId.PDiskId);
        }

        pdisk->Mood = TPDiskMood::Restarting;

        for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
            if (slot->Group) {
                auto *m = VSlots.FindForUpdate(slot->VSlotId);
                m->Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
                m->IsReady = false;
                TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
                GroupFailureModelChanged.insert(slot->Group->ID);
                group->CalculateGroupStatus();
            }
        }
    }

} // namespace NKikimr::NBsController
