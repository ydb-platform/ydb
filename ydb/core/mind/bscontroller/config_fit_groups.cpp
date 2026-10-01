#include "impl.h"
#include "config.h"
#include "group_mapper.h"
#include "group_geometry_info.h"
#include "layout_helpers.h"

namespace NKikimr {
    namespace NBsController {

        class TBlobStorageController::TGroupFitter {
            TConfigState& State;
            const ui32 AvailabilityDomainId;
            const bool IgnoreGroupSanityChecks;
            const bool IgnoreGroupFailModelChecks;
            const TGroupLayoutPolicy LayoutPolicy;
            const bool IgnoreDegradedGroupsChecks;
            const bool IgnoreVSlotQuotaCheck;
            const bool AllowUnusableDisks;
            const bool SettleOnlyOnOperationalDisks;
            const bool IsSelfHealReasonDecommit;
            THashSet<TPDiskId> RequiredPDisks;
            const bool DDisk;
            std::deque<ui64> ExpectedSlotSize;
            const ui32 PDiskSpaceMarginPromille;
            const TGroupGeometryInfo Geometry;
            const TBoxStoragePoolId StoragePoolId;
            const TStoragePoolInfo& StoragePool;
            std::optional<TGroupMapper> Mapper;
            NKikimrBlobStorage::TConfigResponse::TStatus& Status;
            TVSlotReadyTimestampQ& VSlotReadyTimestampQ;

        public:
            TGroupFitter(TConfigState& state, ui32 availabilityDomainId, const NKikimrBlobStorage::TConfigRequest& cmd,
                    std::deque<ui64>& expectedSlotSize, const TVector<TGroupId>& groupsToProcess,
                    ui32 pdiskSpaceMarginPromille,
                    const TBoxStoragePoolId& storagePoolId, const TStoragePoolInfo& storagePool,
                    NKikimrBlobStorage::TConfigResponse::TStatus& status, TVSlotReadyTimestampQ& vslotReadyTimestampQ, bool requireCorrectLayout)
                : State(state)
                , AvailabilityDomainId(availabilityDomainId)
                , IgnoreGroupSanityChecks(cmd.GetIgnoreGroupSanityChecks())
                , IgnoreGroupFailModelChecks(cmd.GetIgnoreGroupFailModelChecks())
                , LayoutPolicy(TGroupLayoutPolicy::FromFlags(cmd.GetIgnoreGroupLayoutChecks(), requireCorrectLayout))
                , IgnoreDegradedGroupsChecks(cmd.GetIgnoreDegradedGroupsChecks())
                , IgnoreVSlotQuotaCheck(cmd.GetIgnoreVSlotQuotaCheck())
                , AllowUnusableDisks(cmd.GetAllowUnusableDisks())
                , SettleOnlyOnOperationalDisks(cmd.GetSettleOnlyOnOperationalDisks())
                , IsSelfHealReasonDecommit(cmd.GetIsSelfHealReasonDecommit())
                , DDisk(storagePool.DDisk)
                , ExpectedSlotSize(expectedSlotSize)
                , PDiskSpaceMarginPromille(pdiskSpaceMarginPromille)
                , Geometry(TBlobStorageGroupType(storagePool.ErasureSpecies), storagePool.GetGroupGeometry())
                , StoragePoolId(storagePoolId)
                , StoragePool(storagePool)
                , Status(status)
                , VSlotReadyTimestampQ(vslotReadyTimestampQ)
            {
                if (AllowUnusableDisks) {
                    for (const TGroupId groupId : groupsToProcess) {
                        if (const TGroupInfo *group = State.Groups.Find(groupId)) {
                            for (const TVSlotInfo *vslot : group->VDisksInGroup) {
                                RequiredPDisks.insert(vslot->VSlotId.ComprisingPDiskId());
                                if (const auto it = State.ExplicitReconfigureMap.find(vslot->VSlotId);
                                    it != State.ExplicitReconfigureMap.end() && it->second != TPDiskId()) {
                                    RequiredPDisks.insert(it->second);
                                }
                            }
                        }
                    }
                }
            }

            void CheckReserve(ui32 total, ui32 min, ui32 part) {
                // number of reserved groups is min + part * maxGroups in cluster
                for (ui64 reserve = 0; reserve < min || (reserve - min) * 1000000 / Max<ui64>(1, total) < part; ++reserve, ++total) {
                    TGroupMapper::TGroupDefinition group;
                    try {
                        AllocateNewGroup(TGroupId::Zero(), group, 1u, 0, {});
                    } catch (const TExFitGroupError&) {
                        throw TExError() << "group reserve constraint hit";
                    }
                }
            }

            TGroupId AllocateGroupId() {
                TGroupId groupId;
                for (;;) {
                    // obtain group local id
                    auto& nextGroupId = State.NextGroupId.Unshare();
                    const ui32 groupLocalId = nextGroupId.GetRawId() ? TGroupID(nextGroupId).GroupLocalID() : 0;

                    // create new full group id
                    TGroupID fullGroupId(EGroupConfigurationType::Dynamic, AvailabilityDomainId, groupLocalId);
                    TGroupID nextFullGroupId = fullGroupId;
                    ++nextFullGroupId;

                    // write down NextGroupId
                    nextGroupId = TGroupId::FromValue(nextFullGroupId.GetRaw());

                    // exit if there is no collision
                    groupId = TGroupId::FromValue(fullGroupId.GetRaw());
                    if (!State.Groups.Find(groupId)) {
                        break;
                    }
                }

                return groupId;
            }

            void CreateGroup() {
                if (StoragePool.BridgeMode) {
                    const TGroupId mainGroupId = AllocateGroupId();

                    TGroupInfo *groupInfo = State.Groups.ConstructInplaceNewEntry(
                        mainGroupId,
                        mainGroupId, /* id */
                        1, /* generation */
                        0, /* owner */
                        TBlobStorageGroupType::ErasureNone, /* erasureSpecies */
                        0, /* desiredPDiskCategory */
                        NKikimrBlobStorage::TVDiskKind::Default, /* desiredVDiskCategory */
                        0, /* encryptionMode */
                        0, /* lifeCyclePhase */
                        TString(), /* mainKeyId */
                        TString(), /* encryptedGroupKey */
                        mainGroupId.GetRawId(), /* groupKeyNonce */
                        0, /* mainKeyVersion */
                        false, /* down */
                        false, /* seenOperational */
                        0, /* groupSizeInUnits */
                        TBridgePileId(), /* bridgePileId */
                        StoragePoolId, /* storagePoolId */
                        0, /* numFailRealms */
                        0, /* numFailDomainsPerFailRealm */
                        0, /* numVDisksPerFailDomain */
                        false); /* ddisk */

                    // bind group to storage pool
                    State.StoragePoolGroups.Unshare().emplace(StoragePoolId, mainGroupId);

                    const TGroupSpecies species = groupInfo->GetGroupSpecies();
                    auto& index = State.IndexGroupSpeciesToGroup.Unshare();
                    index[species].push_back(mainGroupId);

                    NKikimrBlobStorage::TGroupInfo& mainGroup = groupInfo->BridgeGroupInfo.emplace();
                    NKikimrBridge::TGroupState *mainGroupState = mainGroup.MutableBridgeGroupState();
                    const auto& bridgeInfo = State.BridgeInfo;
                    Y_ABORT_UNLESS(bridgeInfo);
                    bridgeInfo->ForEachPile([&](TBridgePileId bridgePileId) {
                        const TGroupId groupId = CreateGroup(bridgePileId, mainGroupId);
                        NKikimrBridge::TGroupState::TPile *pile = mainGroupState->AddPile();
                        groupId.CopyToProto(pile, &NKikimrBridge::TGroupState::TPile::SetGroupId);
                        pile->SetGroupGeneration(1);
                        pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
                    });
                } else {
                    CreateGroup(TBridgePileId(), std::nullopt); // regular single group
                }
            }

            TGroupId CreateGroup(TBridgePileId bridgePileId, std::optional<TGroupId> bridgeProxyGroupId) {
                ////////////////////////////////////////////////////////////////////////////////////////////
                // ALLOCATE GROUP ID FOR THE NEW GROUP
                ////////////////////////////////////////////////////////////////////////////////////////////
                TGroupId groupId = AllocateGroupId();

                ////////////////////////////////////////////////////////////////////////////////////////////////
                // CREATE MORE GROUPS
                ////////////////////////////////////////////////////////////////////////////////////////////////
                TGroupMapper::TGroupDefinition group;
                i64 requiredSpace = Min<i64>();
                if (!ExpectedSlotSize.empty()) {
                    requiredSpace = ExpectedSlotSize.front();
                    ExpectedSlotSize.pop_front();
                }
                ui32 groupSizeInUnits = StoragePool.DefaultGroupSizeInUnits;
                AllocateNewGroup(groupId, group, groupSizeInUnits, requiredSpace, bridgePileId);

                // scan all comprising PDisks for PDiskCategory
                TMaybe<TPDiskCategory> desiredPDiskCategory;

                for (const auto& realm : group) {
                    for (const auto& domain : realm) {
                        for (const TPDiskId& disk : domain) {
                            if (const TPDiskInfo *pdisk = State.PDisks.Find(disk); pdisk && !State.PDisksToRemove.find(disk)) {
                                if (requiredSpace != Min<i64>() && !pdisk->SlotSpaceEnforced(State.Self)) {
                                    Mapper->AdjustSpaceAvailable(disk, -requiredSpace);
                                }
                                if (!desiredPDiskCategory) {
                                    desiredPDiskCategory = pdisk->Kind;
                                } else if (*desiredPDiskCategory != pdisk->Kind) {
                                    desiredPDiskCategory = 0;
                                }
                            } else {
                                throw TExFitGroupError() << "can't find PDisk# " << disk;
                            }
                        }
                    }
                }

                // create group info
                const ui64 MainKeyVersion = 0;
                ui32 lifeCyclePhase = 0;
                TString mainKeyId = "";
                TString encryptedGroupKey = "";
                ui64 groupKeyNonce = groupId.GetRawId(); // For the first time use groupId, then use low 32 bits of the
                                              // NextGroupKeyNonce to produce high 32 bits of the groupKeyNonce.

                TGroupInfo *groupInfo = State.Groups.ConstructInplaceNewEntry(groupId, groupId, 1,
                    0, Geometry.GetErasure(), desiredPDiskCategory.GetOrElse(0), StoragePool.VDiskKind,
                    StoragePool.EncryptionMode.GetOrElse(0), lifeCyclePhase, mainKeyId, encryptedGroupKey,
                    groupKeyNonce, MainKeyVersion, false, false, groupSizeInUnits, bridgePileId, StoragePoolId,
                    Geometry.GetNumFailRealms(), Geometry.GetNumFailDomainsPerFailRealm(), Geometry.GetNumVDisksPerFailDomain(),
                    DDisk);

                groupInfo->BridgeProxyGroupId = bridgeProxyGroupId;

                // bind group to storage pool
                State.StoragePoolGroups.Unshare().emplace(StoragePoolId, groupId);

                const TGroupSpecies species = groupInfo->GetGroupSpecies();
                auto& index = State.IndexGroupSpeciesToGroup.Unshare();
                index[species].push_back(groupId);

                // create VSlots
                CreateVSlotsForGroup(groupInfo, group, {});

                return groupId;
            }

            void FitExistingGroup(TGroupId groupId, bool allocate) {
                const TGroupInfo *groupInfo = State.Groups.Find(groupId);
                if (!groupInfo) {
                    throw TExFitGroupError() << "GroupId# " << groupId << " not found";
                }

                if (allocate) {
                    TGroupInfo *groupInfo = State.Groups.FindForUpdate(groupId);
                    Y_ABORT_UNLESS(groupInfo);
                    groupInfo->Topology = Geometry.CreateTopology();
                    groupInfo->Topology->FinalizeConstruction();
                    groupInfo->VDisksInGroup.resize(groupInfo->Topology->GetTotalVDisksNum());

                    // TODO(alexvru): calculate required space
                    TGroupMapper::TGroupDefinition group;
                    Geometry.ResizeGroup(group);
                    AllocateNewGroup(groupId, group, groupInfo->GroupSizeInUnits, Min<i64>(), groupInfo->BridgePileId);
                    CreateVSlotsForGroup(groupInfo, group, {});
                    return;
                }

                auto reassignment = PrepareGroupReassignment(*groupInfo);
                const bool layoutRepairRequested = State.SanitizingRequests.contains(groupId);
                if (layoutRepairRequested) {
                    if (groupInfo->Topology->QuorumChecker->OneStepFromDegradedOrWorse(reassignment.NonOperationalVDisks)) {
                        throw TExFitGroupError() << "Sanitizing requst was blocked, group is one step from DEGRADED or worse";
                    }
                    if (groupInfo->VDisksInGroup.empty()) {
                        throw TExFitGroupError() << "Group has been decommitted and cannot be sanitized";
                    }
                }

                if (!reassignment.ReplacedSlots.empty() || layoutRepairRequested) {
                    TGroupInfo *mutableGroup = State.Groups.FindForUpdate(groupId);
                    auto placement = AllocateGroupPlacement(*mutableGroup, reassignment, layoutRepairRequested);
                    ApplyGroupReassignment(mutableGroup, reassignment, placement);
                }
                State.CheckConsistency();
            }

        private:
            struct TGroupReassignment {
                TVector<TGroupMapper::TVDiskPlacement> Disks;
                TMap<TVDiskIdShort, TVSlotId> ReplacedSlots;
                TBlobStorageGroupInfo::TGroupVDisks NonOperationalVDisks;

                explicit TGroupReassignment(const TBlobStorageGroupInfo::TTopology *topology)
                    : NonOperationalVDisks(topology)
                {}
            };

            struct TGroupPlacement {
                TGroupMapper::TGroupDefinition Group;
                i64 RequiredSpace = Min<i64>();
                bool AccountSpace = false;
            };

            TGroupReassignment PrepareGroupReassignment(const TGroupInfo& groupInfo) const {
                TGroupReassignment result(groupInfo.Topology.get());
                for (const TVSlotInfo *vslot : groupInfo.VDisksInGroup) {
                    if (!vslot->IsOperational()) {
                        result.NonOperationalVDisks |= {groupInfo.Topology.get(), vslot->GetShortVDiskId()};
                    }

                    const auto it = State.ExplicitReconfigureMap.find(vslot->VSlotId);
                    bool replace = it != State.ExplicitReconfigureMap.end();
                    const TPDiskId targetPDiskId = replace ? it->second : TPDiskId();
                    if (!replace) {
                        switch (vslot->PDisk->Status) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::FAULTY:
                            case NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED:
                                break;
                            case NKikimrBlobStorage::EDriveStatus::BROKEN:
                                replace = true;
                                break;
                            default:
                                Y_ABORT("unexpected drive status");
                        }
                    }

                    auto& disk = result.Disks.emplace_back();
                    disk.VDiskId = vslot->GetShortVDiskId();
                    disk.PDiskId = vslot->VSlotId.ComprisingPDiskId();
                    if (vslot->Metrics.HasAllocatedSize()) {
                        disk.AllocatedSize = static_cast<i64>(vslot->Metrics.GetAllocatedSize());
                    }
                    if (!replace) {
                        continue;
                    }

                    if (targetPDiskId == TPDiskId()) {
                        disk.Reassignment = TGroupMapper::TReplaceVDisk{
                            .RequireSameNode = State.Self.UseSelfHealLocalPolicy
                                               && it != State.ExplicitReconfigureMap.end(),
                        };
                    } else if (IgnoreGroupSanityChecks) {
                        disk.Reassignment = TGroupMapper::TForceVDiskOnPDisk{targetPDiskId};
                    } else {
                        disk.Reassignment = TGroupMapper::TReplaceVDiskOnPDisk{targetPDiskId};
                    }
                    result.ReplacedSlots.emplace(disk.VDiskId, vslot->VSlotId);
                }
                return result;
            }

            TGroupMapper::TForbiddenPDisks CollectForbiddenPDisks(const TGroupInfo& groupInfo,
                                                                  const TGroupMapper::TGroupDefinition& group) const {
                auto needsPlacement = [&](TVDiskIdShort id) {
                    return group[id.FailRealm][id.FailDomain][id.VDisk] == TPDiskId();
                };
                TGroupMapper::TForbiddenPDisks forbidden;
                for (const TVSlotInfo *vslot : groupInfo.VDisksInGroup) {
                    if (needsPlacement(vslot->GetShortVDiskId())) {
                        for (const TVSlotId& donor : vslot->Donors) {
                            forbidden.insert(donor.ComprisingPDiskId());
                        }
                    }
                }
                for (const TVSlotId& id : groupInfo.VSlotsBeingDeleted) {
                    const TVSlotInfo *vslot = State.VSlots.Find(id);
                    if (needsPlacement(vslot->GetShortVDiskId())) {
                        forbidden.insert(id.ComprisingPDiskId());
                    }
                }
                return forbidden;
            }

            TGroupPlacement AllocateGroupPlacement(const TGroupInfo& groupInfo, TGroupReassignment& reassignment,
                                                   bool layoutRepairRequested) {
                TGroupPlacement placement;
                auto& group = placement.Group;
                Geometry.ResizeGroup(group);
                NLayoutChecker::TDomainMapper domainMapper;
                std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdiskLocations;
                for (const TVSlotInfo *vslot : groupInfo.VDisksInGroup) {
                    const TPDiskId pdiskId = vslot->VSlotId.ComprisingPDiskId();
                    group[vslot->RingIdx][vslot->FailDomainIdx][vslot->VDiskIdx] = pdiskId;
                    pdiskLocations[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(
                        domainMapper, State.HostRecords->GetLocation(pdiskId.NodeId), vslot->PDisk->DiskScope, pdiskId, Geometry);
                }
                TString errorReason;
                const bool layoutIsValid = CheckLayoutByGroupDefinition(group, pdiskLocations, Geometry,
                                                                        State.Self.AllowMultipleRealmsOccupation, errorReason);
                for (const auto& disk : reassignment.Disks) {
                    auto& pdiskId = group[disk.VDiskId.FailRealm][disk.VDiskId.FailDomain][disk.VDiskId.VDisk];
                    if (const auto *force = std::get_if<TGroupMapper::TForceVDiskOnPDisk>(&disk.Reassignment)) {
                        pdiskId = force->PDiskId;
                    } else if (!std::holds_alternative<TGroupMapper::TKeepVDisk>(disk.Reassignment)) {
                        pdiskId = TPDiskId();
                    }
                }

                bool hasMissingSlots = false;
                TGroupMapper::Traverse(group, [&](TVDiskIdShort, TPDiskId pdiskId) {
                    hasMissingSlots |= pdiskId == TPDiskId();
                });
                if (!hasMissingSlots && IgnoreGroupSanityChecks) {
                    return placement;
                }

                auto forbidden = CollectForbiddenPDisks(groupInfo, group);
                if (ShouldRepairLayout(reassignment, layoutRepairRequested, layoutIsValid)) {
                    RepairGroupLayout(groupInfo, reassignment, placement, std::move(forbidden));
                } else {
                    AllocateReplacementSlots(groupInfo, reassignment, placement, std::move(forbidden));
                }
                placement.AccountSpace = !IgnoreVSlotQuotaCheck;
                return placement;
            }

            bool ShouldRepairLayout(const TGroupReassignment& reassignment, bool layoutRepairRequested,
                                    bool layoutIsValid) const {
                if (reassignment.ReplacedSlots.empty()) {
                    return layoutRepairRequested;
                }
                if (!State.Self.IsGroupLayoutSanitizerEnabled() || LayoutPolicy.AllowsRelaxedPlacement()
                    || layoutIsValid || reassignment.ReplacedSlots.size() != 1) {
                    return false;
                }

                // Layout repair chooses its own destination and cannot satisfy target constraints.
                return std::ranges::any_of(reassignment.Disks, [](const auto& disk) {
                    const auto *automatic = std::get_if<TGroupMapper::TReplaceVDisk>(&disk.Reassignment);
                    return automatic && !automatic->RequireSameNode;
                });
            }

            void RepairGroupLayout(const TGroupInfo& groupInfo, TGroupReassignment& reassignment,
                                   TGroupPlacement& placement, TGroupMapper::TForbiddenPDisks forbidden) {
                YDB_LOG_INFO_COMP(BS_CONTROLLER, "Attempt to sanitize group layout",
                    {"marker", "BSCFG01"},
                    {"groupId", groupInfo.ID});
                EnsureGroupMapper();
                placement.RequiredSpace = TGroupMapper::CalculateRequiredSpace(reassignment.Disks);
                const TVDiskIdShort replacedVDisk = Geometry.RepairGroupLayout(*Mapper, groupInfo.ID, placement.Group,
                                                                             std::move(forbidden), groupInfo.GroupSizeInUnits,
                                                                             placement.RequiredSpace, groupInfo.BridgePileId);
                if (reassignment.ReplacedSlots.empty()) {
                    const TVSlotInfo *slot = groupInfo.VDisksInGroup[groupInfo.Topology->GetOrderNumber(replacedVDisk)];
                    reassignment.ReplacedSlots.emplace(replacedVDisk, slot->VSlotId);
                }
            }

            void AllocateReplacementSlots(const TGroupInfo& groupInfo, TGroupReassignment& reassignment,
                                          TGroupPlacement& placement, TGroupMapper::TForbiddenPDisks forbidden) {
                TGroupMapper::TReassignmentRequest request;
                request.GroupId = groupInfo.ID.GetRawId();
                request.GroupGeneration = groupInfo.Generation;
                request.VDisks = std::move(reassignment.Disks);
                request.ForbiddenPDisks = std::move(forbidden);
                request.GroupSizeInUnits = groupInfo.GroupSizeInUnits;
                request.TryToRelocateLocallyFirst = State.Self.TryToRelocateBrokenDisksLocallyFirst;
                request.IgnoreGroupLayoutChecks = LayoutPolicy.AllowsRelaxedPlacement();
                request.BridgePileId = groupInfo.BridgePileId;

                EnsureGroupMapper();
                auto outcome = Mapper->AllocateGroupReassignment(std::move(request));
                if (!outcome.Success) {
                    TExFitGroupError errorException;
                    errorException << "failed to allocate group: " << outcome.Error.ErrorMessage;
                    errorException.GroupMapperError = std::move(outcome.Error);
                    throw errorException;
                }
                placement.RequiredSpace = outcome.RequiredSpace;
                placement.Group = std::move(outcome.Group);
            }

            void ApplyGroupReassignment(TGroupInfo *groupInfo, const TGroupReassignment& reassignment,
                                        const TGroupPlacement& placement) {
                TMap<TVDiskID, TVSlotId> preservedSlots;
                for (const TVSlotInfo *slot : groupInfo->VDisksInGroup) {
                    if (!reassignment.ReplacedSlots.contains(slot->GetShortVDiskId())) {
                        preservedSlots.emplace(slot->GetVDiskId(), slot->VSlotId);
                    }
                }
                const auto& group = placement.Group;
                if (placement.AccountSpace) {
                    for (const auto& [pos, vslotId] : reassignment.ReplacedSlots) {
                        const TPDiskId pdiskId = group[pos.FailRealm][pos.FailDomain][pos.VDisk];
                        const TPDiskInfo *pdisk = State.PDisks.Find(pdiskId);
                        if (placement.RequiredSpace != Min<i64>() && !pdisk->SlotSpaceEnforced(State.Self)) {
                            Mapper->AdjustSpaceAvailable(pdiskId, -placement.RequiredSpace);
                        }
                    }
                }

                std::vector<TVSlotId> donors;
                for (const auto& [vdiskId, vslotId] : reassignment.ReplacedSlots) {
                    const bool suppressDonorMode = State.SuppressDonorMode.contains(vslotId);
                    if (State.DonorMode && !suppressDonorMode && !State.UncommittedVSlots.count(vslotId)) {
                        donors.push_back(vslotId);
                    } else {
                        if (placement.AccountSpace) {
                            const TVSlotInfo *slot = State.VSlots.Find(vslotId);
                            Y_ABORT_UNLESS(slot);
                            if (!slot->PDisk->SlotSpaceEnforced(State.Self)) {
                                Mapper->AdjustSpaceAvailable(vslotId.ComprisingPDiskId(), slot->Metrics.GetAllocatedSize());
                            }
                        }
                        State.DestroyVSlot(vslotId);
                    }
                }

                auto newSlots = CreateVSlotsForGroup(groupInfo, group, preservedSlots);
                State.GroupContentChanged.insert(groupInfo->ID);
                State.GroupFailureModelChanged.insert(groupInfo->ID);
                if (!reassignment.ReplacedSlots.empty()) {
                    CheckGroupReassignment(*groupInfo, reassignment.NonOperationalVDisks);
                    CheckAndReportReassignment(groupInfo, reassignment.ReplacedSlots, group);
                }

                for (const TVSlotId& vslotId : donors) {
                    TVSlotInfo *mutableSlot = State.VSlots.FindForUpdate(vslotId);
                    Y_ABORT_UNLESS(mutableSlot);
                    const auto it = newSlots.find(mutableSlot->GetShortVDiskId());
                    Y_ABORT_UNLESS(it != newSlots.end());
                    mutableSlot->MakeDonorFor(it->second);
                }
            }

            void CheckGroupReassignment(const TGroupInfo& groupInfo,
                                        const TBlobStorageGroupInfo::TGroupVDisks& nonOperationalVDisks) const {
                if (!LayoutPolicy.Accepts(groupInfo.LayoutCorrect)) {
                    throw TExGroupLayoutIncorrect(groupInfo.ID.GetRawId());
                }
                if (!IgnoreGroupFailModelChecks) {
                    const auto& checker = *groupInfo.Topology->QuorumChecker;
                    if (!checker.CheckFailModelForGroup(nonOperationalVDisks)) {
                        throw TExMayLoseData(groupInfo.ID.GetRawId());
                    } else if (!IgnoreDegradedGroupsChecks && checker.IsDegraded(nonOperationalVDisks)) {
                        throw TExMayGetDegraded(groupInfo.ID.GetRawId());
                    }
                }
            }

            void CheckReassignmentOccupancy(TPDiskId fromPDiskId, TPDiskId toPDiskId,
                                            const TPDiskInfo *fromPDisk, const TPDiskInfo *toPDisk) const {
                if (!State.Fit.OnlyToLessOccupiedPDisk || !fromPDisk || !toPDisk) {
                    return;
                }
                const size_t fromPDiskSlots = fromPDisk->VSlotsOnPDisk.size() - 1; // -1 because we are removing the slot
                const size_t toPDiskSlots = toPDisk->VSlotsOnPDisk.size();
                if (toPDiskSlots > fromPDiskSlots) {
                    throw TExReassignNotViable() << "Reassignment from PDisk# " << fromPDiskId
                                                 << " to PDisk# " << toPDiskId
                                                 << " is not allowed with OnlyToLessOccupiedPDisk=true, target PDisk will have " << toPDiskSlots
                                                 << " slot(s), while source PDisk will have " << fromPDiskSlots << " slot(s)";
                }
            }

            void CheckAndReportReassignment(const TGroupInfo *groupInfo, const TMap<TVDiskIdShort, TVSlotId>& replacedSlots,
                                            const TGroupMapper::TGroupDefinition& group) {
                for (const TVSlotInfo *slot : groupInfo->VDisksInGroup) {
                    const TVDiskIdShort pos(slot->RingIdx, slot->FailDomainIdx, slot->VDiskIdx);
                    if (const auto it = replacedSlots.find(pos); it != replacedSlots.end()) {
                        TVSlotId fromVSlotId = it->second;
                        TVSlotId toVSlotId = slot->VSlotId;

                        TPDiskId fromPDiskId = fromVSlotId.ComprisingPDiskId();
                        TPDiskId toPDiskId = toVSlotId.ComprisingPDiskId();

                        auto *fromPDisk = State.PDisks.Find(fromPDiskId);
                        auto *toPDisk = State.PDisks.Find(toPDiskId);

                        CheckReassignmentOccupancy(fromPDiskId, toPDiskId, fromPDisk, toPDisk);

                        auto *item = Status.AddReassignedItem();
                        VDiskIDFromVDiskID(TVDiskID(groupInfo->ID, groupInfo->Generation, pos), item->MutableVDiskId());

                        Serialize(item->MutableFrom(), fromVSlotId);
                        Serialize(item->MutableTo(), toVSlotId);
                        if (fromPDisk) {
                            item->SetFromFqdn(std::get<0>(fromPDisk->HostId));
                            item->SetFromPath(fromPDisk->Path);
                        }
                        if (toPDisk) {
                            item->SetToFqdn(std::get<0>(toPDisk->HostId));
                            item->SetToPath(toPDisk->Path);
                        }
                    }
                }

                auto makeReplacements = [&] {
                    TStringBuilder s;
                    s << "[";
                    bool first = true;
                    for (const auto& kv : replacedSlots) {
                        s << (std::exchange(first, false) ? "" : " ")
                          << "{" << kv.first << " from# " << kv.second << " to# "
                          << group[kv.first.FailRealm][kv.first.FailDomain][kv.first.VDisk] << "}";
                    }
                    return static_cast<TString>(s << "]");
                };
                YDB_LOG_INFO_COMP(BS_CONTROLLER_AUDIT, "ReconfigGroup",
                    {"marker", "BSCA04"},
                    {"uniqueId", State.UniqueId},
                    {"groupId", groupInfo->ID},
                    {"groupGeneration", groupInfo->Generation},
                    {"replacements", makeReplacements()});
            }

            void EnsureGroupMapper() {
                if (!Mapper) {
                    Mapper.emplace(Geometry, TGroupMapper::TOptions{
                        .Randomize = StoragePool.RandomizeGroupMapping,
                        .PreferLessOccupiedRack = State.Fit.PreferLessOccupiedRack,
                        .WithAttentionToReplication = State.Fit.WithAttentionToReplication,
                        .IgnoreVSlotQuotaCheck = IgnoreVSlotQuotaCheck,
                        .SettleOnlyOnOperationalDisks = SettleOnlyOnOperationalDisks,
                        .IsSelfHealReasonDecommit = IsSelfHealReasonDecommit,
                        .SpaceColorBorder = State.Self.PDiskSpaceColorBorder,
                        .SpaceMarginPromille = PDiskSpaceMarginPromille,
                    });
                    PopulateGroupMapper();
                }
            }

            void AllocateNewGroup(TGroupId groupId, TGroupMapper::TGroupDefinition& group, ui32 groupSizeInUnits,
                                  i64 requiredSpace, TBridgePileId bridgePileId) {
                EnsureGroupMapper();
                TGroupMapper::TGroupConstraintsDefinition constraints;
                Geometry.AllocateGroup(*Mapper, groupId, group, constraints, {}, {}, groupSizeInUnits, requiredSpace, bridgePileId);
            }

            void PopulateGroupMapper() {
                const TBoxId boxId = std::get<0>(StoragePoolId);
                TGroupMapper::TPlacementBuilder builder(*Mapper);
                THashSet<std::pair<ui32, ui32>> capturedGroups;

                if (State.Fit.WithAttentionToReplication) {
                    TPDiskSlotTracker tracker;
                    State.VSlots.ForEach([&](const TVSlotId& id, const TVSlotInfo& info) {
                        if (!info.IsBeingDeleted()
                            && info.GetStatus() == NKikimrBlobStorage::EVDiskStatus::REPLICATING) {
                            tracker.AddReplicatingVSlot(id.ComprisingPDiskId());
                        }
                    });
                    builder.SetPrecomputedReplicationTracker(std::move(tracker));
                }

                State.PDisks.ForEach([&](const TPDiskId& id, const TPDiskInfo& info) {
                    bool usable = info.BoxId == boxId && !State.PDisksToRemove.contains(id);
                    if (usable) {
                        usable = false;
                        for (const auto& filter : StoragePool.PDiskFilters) {
                            if (filter.MatchPDisk(info)) {
                                usable = true;
                                break;
                            }
                        }
                    }
                    if (!usable && !RequiredPDisks.contains(id)) {
                        return;
                    }

                    TGroupMapper::TPDiskState pdisk;
                    pdisk.PDiskId = id;
                    pdisk.Location = State.HostRecords->GetLocation(id.NodeId);
                    pdisk.Usable = usable;
                    pdisk.NumActiveSlots = info.NumActiveDynamicSlots + info.StaticSlotUsage;
                    info.ExtractInferredPDiskSettings(pdisk.ExpectedSlotCount, pdisk.SlotSizeInUnits);
                    pdisk.SlotSizeInBytes = info.GetEffectiveExpectedSlotSize();
                    pdisk.Space = TGroupMapper::CapturePDiskSpace(info.Metrics);
                    pdisk.Operational = info.Operational;
                    pdisk.DriveStatus = info.Status;
                    pdisk.MaintenanceStatus = info.MaintenanceStatus;
                    pdisk.DecommitStatus = info.DecommitStatus;
                    if (!usable) {
                        pdisk.WhyUnusable += 'X';
                    }
                    pdisk.DiskScope = info.DiskScope;

                    if (const auto& bridgeInfo = State.BridgeInfo) {
                        if (const TBridgeInfo::TPile *pile = bridgeInfo->GetPileForNode(id.NodeId)) {
                            pdisk.BridgePileId = pile->BridgePileId;
                        } else {
                            Y_DEBUG_ABORT_S("can't find pile for NodeId# " << id.NodeId);
                        }
                    }

                    builder.AddPDisk(std::move(pdisk));

                    for (const auto& [_, item] : info.VSlotsOnPDisk) {
                        const TVSlotInfo& vslotInfo = *item;
                        if (vslotInfo.Group) {
                            const TGroupInfo& group = *vslotInfo.Group;
                            const auto groupKey = std::make_pair(group.ID.GetRawId(), group.Generation);
                            if (capturedGroups.insert(groupKey).second) {
                                i64 maxVDiskAllocatedSize = 0;
                                for (const TVSlotInfo *peer : group.VDisksInGroup) {
                                    maxVDiskAllocatedSize = Max(maxVDiskAllocatedSize,
                                                                static_cast<i64>(peer->Metrics.GetAllocatedSize()));
                                }
                                builder.AddGroup({
                                    .GroupId = group.ID.GetRawId(),
                                    .GroupGeneration = group.Generation,
                                    .GroupSizeInUnits = group.GroupSizeInUnits,
                                    .MaxVDiskAllocatedSize = maxVDiskAllocatedSize,
                                });
                            }
                        }

                        TGroupMapper::TVSlotState vslot{
                            .VSlotId = vslotInfo.VSlotId,
                            .PDiskId = vslotInfo.VSlotId.ComprisingPDiskId(),
                            .GroupId = vslotInfo.GroupId.GetRawId(),
                            .GroupGeneration = vslotInfo.GroupGeneration,
                            .VDiskId = vslotInfo.GetShortVDiskId(),
                            .CountedInNumActiveSlots = false,
                            .OccupiedByGroup = !vslotInfo.IsBeingDeleted(),
                            .Ready = vslotInfo.GetStatus() == NKikimrBlobStorage::EVDiskStatus::READY,
                            .Replicating = !vslotInfo.IsBeingDeleted()
                                           && vslotInfo.GetStatus() == NKikimrBlobStorage::EVDiskStatus::REPLICATING,
                        };
                        if (vslotInfo.Group) {
                            vslot.SpaceUsed = static_cast<i64>(vslotInfo.Metrics.GetAllocatedSize());
                        }
                        builder.AddVSlot(vslot);
                    }
                });

                builder.Finish();
            }

            std::map<TVDiskIdShort, TVSlotInfo*> CreateVSlotsForGroup(TGroupInfo *groupInfo,
                    const TGroupMapper::TGroupDefinition& group, const TMap<TVDiskID, TVSlotId>& preservedSlots) {
                std::map<TVDiskIdShort, TVSlotInfo*> res;

                // reset group contents as we are going to fill it right now
                groupInfo->ClearVDisksInGroup();

                for (ui32 failRealmIdx = 0; failRealmIdx < group.size(); ++failRealmIdx) {
                    const auto& realm = group[failRealmIdx];
                    for (ui32 failDomainIdx = 0; failDomainIdx < realm.size(); ++failDomainIdx) {
                        const auto& domain = realm[failDomainIdx];
                        for (ui32 vdiskIdx = 0; vdiskIdx < domain.size(); ++vdiskIdx) {
                            const TVDiskID vdiskId(groupInfo->ID, groupInfo->Generation, failRealmIdx, failDomainIdx, vdiskIdx);
                            if (auto it = preservedSlots.find(vdiskId); it != preservedSlots.end()) {
                                const TVSlotInfo *vslotInfo = State.VSlots.Find(it->second);
                                Y_ABORT_UNLESS(vslotInfo);
                                groupInfo->AddVSlot(vslotInfo);
                            } else {
                                const TPDiskId pdiskId = domain[vdiskIdx];
                                Y_ABORT_UNLESS(!State.PDisksToRemove.count(pdiskId));
                                TPDiskInfo *pdiskInfo = State.PDisks.FindForUpdate(pdiskId);
                                Y_ABORT_UNLESS(pdiskInfo);
                                TVSlotId vslotId;

                                // allocate new VSlot id; avoid collisions
                                for (;;) {
                                    const auto currentVSlotId = pdiskInfo->NextVSlotId;
                                    pdiskInfo->NextVSlotId = currentVSlotId + 1;
                                    vslotId = TVSlotId(pdiskId, currentVSlotId);
                                    if (!State.VSlots.Find(vslotId)) {
                                        break;
                                    }
                                }

                                // insert new VSlot
                                TVSlotInfo *vslotInfo = State.VSlots.ConstructInplaceNewEntry(vslotId, vslotId, pdiskInfo,
                                    groupInfo->ID, 0, groupInfo->Generation, StoragePool.VDiskKind, failRealmIdx,
                                    failDomainIdx, vdiskIdx, TMood::Normal, groupInfo, &VSlotReadyTimestampQ,
                                    TInstant::Zero(), TDuration::Zero(), 0, 0);
                                vslotInfo->VDiskStatusTimestamp = State.Mono;

                                // mark as uncommitted
                                State.UncommittedVSlots.insert(vslotId);

                                // remember newly created slot
                                res.emplace(vdiskId, vslotInfo);
                            }
                        }
                    }
                }

                groupInfo->FinishVDisksInGroup();
                groupInfo->CalculateGroupStatus();
                groupInfo->CalculateLayoutStatus(&State.Self, groupInfo->Topology.get(), [&] {
                    const auto& pools = State.StoragePools.Get();
                    if (const auto it = pools.find(groupInfo->StoragePoolId); it != pools.end()) {
                        return TGroupGeometryInfo(groupInfo->Topology->GType, it->second.GetGroupGeometry());
                    }
                    Y_DEBUG_ABORT(); // this can't normally happen
                    return TGroupGeometryInfo();
                });

                return res;
            }
        };

        void FillGroupMapperError(NKikimrBlobStorage::TGroupMapperError& groupMapperErrorProto, const TGroupMapperError& error) {
            auto fillStats = [](NKikimrBlobStorage::TGroupMapperError::TStats& statsProto, const TGroupMapperError::TStats& stats) {
                statsProto.SetDomain(stats.Domain);
                statsProto.SetAllSlotsAreOccupied(stats.AllSlotsAreOccupied);
                statsProto.SetNotEnoughSpace(stats.NotEnoughSpace);
                statsProto.SetNotAcceptingNewSlots(stats.NotAcceptingNewSlots);
                statsProto.SetNotOperational(stats.NotOperational);
                statsProto.SetDecommission(stats.Decommission);
            };
            fillStats(*groupMapperErrorProto.MutableTotalStats(), error.TotalStats);
            for (const auto& domainStat : error.MatchingDomainsStats) {
                auto* domainStatsProto = groupMapperErrorProto.AddMatchingDomainsStats();
                fillStats(*domainStatsProto, domainStat);
            }
            groupMapperErrorProto.SetMissingFailRealmsCount(error.MissingFailRealmsCount);
            groupMapperErrorProto.SetFailRealmsWithMissingDomainsCount(error.FailRealmsWithMissingDomainsCount);
            groupMapperErrorProto.SetOkDisksCount(error.OkDisksCount);
            groupMapperErrorProto.SetRealmLocationKey(error.RealmLocationKey);
            groupMapperErrorProto.SetDomainLocationKey(error.DomainLocationKey);
        }

        void TBlobStorageController::FitGroupsForUserConfig(TConfigState& state, ui32 availabilityDomainId,
                const NKikimrBlobStorage::TConfigRequest& cmd, std::deque<ui64> expectedSlotSize,
                NKikimrBlobStorage::TConfigResponse::TStatus& status, bool requireCorrectLayout) {
            Y_DEFER {
                // reset Fit options so they do not affect further commands
                state.Fit.OnlyToLessOccupiedPDisk = false;
                state.Fit.PreferLessOccupiedRack = false;
                state.Fit.WithAttentionToReplication = false;
                state.Fit.GroupsToAllocate.clear();
            };

            auto poolsAndGroups = std::exchange(state.Fit.PoolsAndGroups, {});
            if (poolsAndGroups.empty()) {
                return; // nothing to do
            }

            std::unordered_map<TString, std::pair<ui32, TBoxStoragePoolId>> filterMap;
            std::unordered_set<TString> changedFilters;

            // scan through all storage pools and fit the number of groups to desired one
            auto processSingleStoragePool = [&](TBoxStoragePoolId storagePoolId, const TStoragePoolInfo& storagePool,
                    bool createNewGroups, const auto& enumerateGroups) {
                TVector<TGroupId> groupIds;
                enumerateGroups([&](TGroupId groupId) {
                    groupIds.push_back(groupId);
                });

                TGroupFitter fitter(state, availabilityDomainId, cmd, expectedSlotSize, groupIds, PDiskSpaceMarginPromille,
                    storagePoolId, storagePool, status, VSlotReadyTimestampQ, requireCorrectLayout);

                ui32 numActualGroups = 0;

                try {
                    TStringBuilder identifier;
                    identifier << "Erasure# " << storagePool.ErasureSpecies
                        << " Geometry# " << storagePool.RealmLevelBegin << "," << storagePool.RealmLevelEnd
                        << "," << storagePool.DomainLevelBegin << "," << storagePool.DomainLevelEnd
                        << "," << storagePool.NumFailRealms << "," << storagePool.NumFailDomainsPerFailRealm
                        << "," << storagePool.NumVDisksPerFailDomain;
                    for (const auto& filter : storagePool.PDiskFilters) {
                        identifier << " Filter# " << filter.Type << "," << filter.SharedWithOs
                            << "," << filter.ReadCentric << "," << filter.Kind;
                    }
                    auto& [numGroups, id] = filterMap[identifier];
                    numGroups += storagePool.NumGroups;
                    id = storagePoolId;

                    for (const TGroupId groupId : groupIds) {
                        fitter.FitExistingGroup(groupId, state.Fit.GroupsToAllocate.contains(groupId));
                        if (const TGroupInfo *group = state.Groups.Find(groupId); group && !group->BridgePileId) {
                            ++numActualGroups;
                        }
                    }
                    if (createNewGroups) {
                        if (numActualGroups < storagePool.NumGroups) {
                            changedFilters.insert(identifier);
                        }
                        for (; numActualGroups < storagePool.NumGroups; ++numActualGroups) {
                            fitter.CreateGroup();
                        }
                    }
                } catch (const TExFitGroupError& ex) {
                    TExError err;
                    err << "Group fit error"
                        << " BoxId# " << std::get<0>(storagePoolId)
                        << " StoragePoolId# " << std::get<1>(storagePoolId)
                        << " Error# " << ex.what();
                    if (ex.GroupMapperError) {
                        auto& failParam = err.FailParams.emplace_back();
                        FillGroupMapperError(*failParam.MutableGroupMapperError(), *ex.GroupMapperError);
                    }
                    throw err;
                }
                if (storagePool.NumGroups < numActualGroups) {
                    throw TExError() << "Storage pool modification error"
                        << " BoxId# " << std::get<0>(storagePoolId)
                        << " StoragePoolId# " << std::get<1>(storagePoolId)
                        << " impossible to reduce number of groups";
                }
            };

            const auto& storagePools = state.StoragePools.Get();
            for (auto it = poolsAndGroups.begin(); it != poolsAndGroups.end(); ) {
                const auto& [storagePoolId, groupId] = *it;
                const auto spIt = storagePools.find(storagePoolId);
                Y_ABORT_UNLESS(spIt != storagePools.end());
                if (!groupId) {
                    // process all groups in this pool and skip the rest
                    processSingleStoragePool(spIt->first, spIt->second, true, [&](const auto& callback) {
                        const auto& storagePoolGroups = state.StoragePoolGroups.Get();
                        for (auto it = storagePoolGroups.lower_bound({spIt->first, Min<TGroupId>()});
                                it != storagePoolGroups.end() && it->first == spIt->first; ++it) {
                            callback(it->second);
                        }
                    });
                    for (; it != poolsAndGroups.end() && std::get<0>(*it) == storagePoolId; ++it)
                    {}
                } else {
                    // process explicit group set
                    processSingleStoragePool(spIt->first, spIt->second, false, [&](const auto& callback) {
                        for (; it != poolsAndGroups.end() && std::get<0>(*it) == spIt->first; ++it) {
                            callback(*std::get<1>(*it));
                        }
                    });
                }
            }

            if (!cmd.GetIgnoreGroupReserve()) {
                for (const auto& identifier : changedFilters) {
                    auto& [numGroups, storagePoolId] = filterMap.at(identifier);
                    const auto& storagePool = state.StoragePools.Get().at(storagePoolId);
                    TGroupFitter fitter(state, availabilityDomainId, cmd, expectedSlotSize, {}, PDiskSpaceMarginPromille,
                        storagePoolId, storagePool, status, VSlotReadyTimestampQ, requireCorrectLayout);
                    fitter.CheckReserve(numGroups, GroupReserveMin, GroupReservePart);
                }
            }

            state.CheckConsistency();
        }

    } // NBsController
} // NKikimr
