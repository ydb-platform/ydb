#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TUpdateBridgeGroupInfo& cmd, TStatus& /*status*/) {
        // first of all, find the group
        TGroupInfo *group = Groups.FindForUpdate(TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TUpdateBridgeGroupInfo::GetGroupId));
        if (!group) {
            throw TExGroupNotFound(cmd.GetGroupId());
        } else if (group->Generation != cmd.GetGroupGeneration()) {
            throw TExGroupGenerationMismatch(cmd.GetGroupId(), cmd.GetGroupGeneration(), group->Generation);
        } else if (!group->BridgeGroupInfo) {
            throw TExError() << "group is not a bridged one" << TErrorParams::GroupId(cmd.GetGroupId());
        } else if (!cmd.HasBridgeGroupInfo()) {
            throw TExError() << "missing mandatory TUpdateBridgeGroupInfo.BridgeGroupInfo field in request";
        } else if (!cmd.GetBridgeGroupInfo().HasBridgeGroupState()) {
            throw TExError() << "missing mandatory TGroupInfo.GroupState field in request";
        }

        const auto& current = group->BridgeGroupInfo->GetBridgeGroupState();
        const auto& updated = cmd.GetBridgeGroupInfo().GetBridgeGroupState();

        if (current.PileSize() != updated.PileSize()) {
            throw TExError() << "number of piles in GroupState is not allowed to be changed";
        }

        bool stageChanged = false;
        for (size_t pileIndex = 0; pileIndex < current.PileSize(); ++pileIndex) {
            const auto& curPile = current.GetPile(pileIndex);
            const auto& updatedPile = updated.GetPile(pileIndex);
            if (curPile.GetAbsent() != updatedPile.GetAbsent()) {
                throw TExError() << "can't change set of piles for group state";
            } else if (curPile.GetAbsent()) {
                continue; // the group has no part in this pile
            } else if (curPile.GetGroupId() != updatedPile.GetGroupId() ||
                    curPile.GetGroupGeneration() != updatedPile.GetGroupGeneration() ||
                    curPile.GetBecameUnsyncedGeneration() != updatedPile.GetBecameUnsyncedGeneration()) {
                throw TExError() << "can't change group id for group state";
            } else if (const TGroupInfo *pileGroup = Groups.Find(TGroupId::FromProto(&updatedPile,
                    &NKikimrBridge::TGroupState::TPile::GetGroupId)); !pileGroup) {
                throw TExGroupNotFound(updatedPile.GetGroupId());
            } else if (pileGroup->Generation != updatedPile.GetGroupGeneration()) {
                throw TExGroupGenerationMismatch(updatedPile.GetGroupId(), updatedPile.GetGroupGeneration(),
                    pileGroup->Generation);
            } else {
                stageChanged |= curPile.GetStage() != updatedPile.GetStage();
            }
        }

        group->BridgeGroupInfo->MutableBridgeGroupState()->CopyFrom(updated);
        if (stageChanged) {
            GroupContentChanged.insert(group->ID); // pile groups get new generation along with the bridge proxy group
        }
    }

} // namespace NKikimr::NBsController
