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

        auto *groupState = group->BridgeGroupInfo->MutableBridgeGroupState();
        groupState->CopyFrom(updated);

        for (size_t pileIndex = 0; pileIndex < current.PileSize(); ++pileIndex) {
            const auto& curPile = current.GetPile(pileIndex);
            const auto& updatedPile = groupState->GetPile(pileIndex);
            if (curPile.GetGroupId() != updatedPile.GetGroupId() ||
                    curPile.GetGroupGeneration() != updatedPile.GetGroupGeneration() ||
                    curPile.GetBecameUnsyncedGeneration() != updatedPile.GetBecameUnsyncedGeneration()) {
                throw TExError() << "can't change group id for group state";
            } else if (const TGroupInfo *group = Groups.Find(TGroupId::FromProto(&updatedPile,
                    &NKikimrBridge::TGroupState::TPile::GetGroupId)); !group) {
                throw TExGroupNotFound(updatedPile.GetGroupId());
            } else if (group->Generation != updatedPile.GetGroupGeneration()) {
                throw TExGroupGenerationMismatch(updatedPile.GetGroupId(), updatedPile.GetGroupGeneration(), group->Generation);
            } else if (curPile.GetStage() != updatedPile.GetStage()) {
                GroupContentChanged.insert(group->ID);
            }
        }
    }

} // namespace NKikimr::NBsController
