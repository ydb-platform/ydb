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
            const bool IgnoreDegradedGroupsChecks;
            const bool IgnoreVSlotQuotaCheck;
            const bool AllowUnusableDisks;
            const bool SettleOnlyOnOperationalDisks;
            const bool IsSelfHealReasonDecommit;
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
                    std::deque<ui64>& expectedSlotSize, ui32 pdiskSpaceMarginPromille,
                    const TBoxStoragePoolId& storagePoolId, const TStoragePoolInfo& storagePool,
                    NKikimrBlobStorage::TConfigResponse::TStatus& status, TVSlotReadyTimestampQ& vslotReadyTimestampQ)
                : State(state)
                , AvailabilityDomainId(availabilityDomainId)
                , IgnoreGroupSanityChecks(cmd.GetIgnoreGroupSanityChecks())
                , IgnoreGroupFailModelChecks(cmd.GetIgnoreGroupFailModelChecks())
                , IgnoreDegradedGroupsChecks(cmd.GetIgnoreDegradedGroupsChecks())
                , IgnoreVSlotQuotaCheck(cmd.GetIgnoreVSlotQuotaCheck())
                , AllowUnusableDisks(cmd.GetAllowUnusableDisks())
                , SettleOnlyOnOperationalDisks(cmd.GetSettleOnlyOnOperationalDisks())
                , IsSelfHealReasonDecommit(cmd.GetIsSelfHealReasonDecommit())
                , ExpectedSlotSize(expectedSlotSize)
                , PDiskSpaceMarginPromille(pdiskSpaceMarginPromille)
                , Geometry(TBlobStorageGroupType(storagePool.ErasureSpecies), storagePool.GetGroupGeometry())
                , StoragePoolId(storagePoolId)
                , StoragePool(storagePool)
                , Status(status)
                , VSlotReadyTimestampQ(vslotReadyTimestampQ)
            {}

            void CheckReserve(ui32 total, ui32 min, ui32 part) {
                // number of reserved groups is min + part * maxGroups in cluster
                for (ui64 reserve = 0; reserve < min || (reserve - min) * 1000000 / Max<ui64>(1, total) < part; ++reserve, ++total) {
                    TGroupMapper::TGroupDefinition group;
                    try {
                        AllocateOrSanitizeGroup(TGroupId::Zero(), group, {}, {}, 0, false, &TGroupGeometryInfo::AllocateGroup);
                    } catch (const TExFitGroupError&) {
                        throw TExError() << "group reserve constraint hit";
                    }
                }
            }

            void CreateGroup() {
                ////////////////////////////////////////////////////////////////////////////////////////////
                // ALLOCATE GROUP ID FOR THE NEW GROUP
                ////////////////////////////////////////////////////////////////////////////////////////////
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

                ////////////////////////////////////////////////////////////////////////////////////////////////
                // CREATE MORE GROUPS
                ////////////////////////////////////////////////////////////////////////////////////////////////
                TGroupMapper::TGroupDefinition group;
                i64 requiredSpace = Min<i64>();
                if (!ExpectedSlotSize.empty()) {
                    requiredSpace = ExpectedSlotSize.front();
                    ExpectedSlotSize.pop_front();
                }
                AllocateOrSanitizeGroup(groupId, group, {}, {}, requiredSpace, false, &TGroupGeometryInfo::AllocateGroup);

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
                    groupKeyNonce, MainKeyVersion, false, false, StoragePoolId, Geometry.GetNumFailRealms(),
                    Geometry.GetNumFailDomainsPerFailRealm(), Geometry.GetNumVDisksPerFailDomain());

                // bind group to storage pool
                State.StoragePoolGroups.Unshare().emplace(StoragePoolId, groupId);

                const TGroupSpecies species = groupInfo->GetGroupSpecies();
                auto& index = State.IndexGroupSpeciesToGroup.Unshare();
                index[species].push_back(groupId);

                // create VSlots
                CreateVSlotsForGroup(groupInfo, group, {});
            }

            void CheckExistingGroup(TGroupId groupId) {
                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // extract TGroupInfo for specified group
                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                const TGroupInfo *groupInfo = State.Groups.Find(groupId);
                if (!groupInfo) {
                    throw TExFitGroupError() << "GroupId# " << groupId << " not found";
                }

                TGroupMapper::TGroupDefinition group;
                TGroupMapper::TGroupConstraintsDefinition softConstraints, hardConstraints;
                bool layoutIsValid = true;

                auto getGroup = [&]() -> TGroupMapper::TGroupDefinition& {
                    if (!group) {
                        NLayoutChecker::TDomainMapper domainMapper;
                        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdiskLocations;

                        Geometry.ResizeGroup(group);
                        Geometry.ResizeGroup(softConstraints);
                        Geometry.ResizeGroup(hardConstraints);
                        for (const TVSlotInfo *vslot : groupInfo->VDisksInGroup) {
                            const TPDiskId& pdiskId = vslot->VSlotId.ComprisingPDiskId();
                            group[vslot->RingIdx][vslot->FailDomainIdx][vslot->VDiskIdx] = pdiskId;
                            if (State.Self.TryToRelocateBrokenDisksLocallyFirst) {
                                softConstraints[vslot->RingIdx][vslot->FailDomainIdx][vslot->VDiskIdx].NodeId = pdiskId.NodeId;
                            }
                            const auto loc = State.HostRecords->GetLocation(pdiskId.NodeId);

                            pdiskLocations[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper, loc, pdiskId, Geometry);
                        }

                        TString errorReason;
                        if (!CheckLayoutByGroupDefinition(group, pdiskLocations, Geometry,
                                State.Self.AllowMultipleRealmsOccupation, errorReason)) {
                            layoutIsValid = false;
                        }
                    }

                    return group;
                };

                // a set of preserved slots (which are not changed during this transaction)
                TMap<TVDiskID, TVSlotId> preservedSlots;

                // mapping for audit log
                TMap<TVDiskIdShort, TVSlotId> replacedSlots;
                i64 requiredSpace = Min<i64>();

                bool sanitizingRequest = (State.SanitizingRequests.find(groupId) != State.SanitizingRequests.end());

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // scan through all VSlots and find matching PDisks
                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // create topology for group
                auto& topology = *groupInfo->Topology;
                // fill in vector of failed disks (that are not fully operational)
                TBlobStorageGroupInfo::TGroupVDisks failed(&topology);
                auto& checker = *topology.QuorumChecker;
                for (const TVSlotInfo *vslot : groupInfo->VDisksInGroup) {
                    if (!vslot->IsOperational()) {
                        failed |= {&topology, vslot->GetShortVDiskId()};
                    }

                    const auto it = State.ExplicitReconfigureMap.find(vslot->VSlotId);
                    bool replace = it != State.ExplicitReconfigureMap.end();
                    const TPDiskId targetPDiskId = replace ? it->second : TPDiskId();

                    if (!replace) {
                        // check status
                        switch (vslot->PDisk->Status) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                                // nothing to do, it's okay
                                break;

                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                                // nothing to do as well, new groups are not created on this drive, but existing ones continue
                                // to function as expected
                                break;

                            case NKikimrBlobStorage::EDriveStatus::BROKEN:
                                // reconfigure group
                                replace = true;
                                break;

                            case NKikimrBlobStorage::EDriveStatus::FAULTY:
                            case NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED:
                                // groups are moved out asynchronously
                                break;

                            default:
                                Y_ABORT("unexpected drive status");
                        }
                    }

                    if (replace) {
                        auto& g = getGroup();
                        // get the current PDisk in the desired slot and replace it with the target one; if the target
                        // PDisk id is zero, then new PDisk will be picked up automatically
                        g[vslot->RingIdx][vslot->FailDomainIdx][vslot->VDiskIdx] = targetPDiskId;
                        if (State.Self.UseSelfHealLocalPolicy && it != State.ExplicitReconfigureMap.end()) {
                            hardConstraints[vslot->RingIdx][vslot->FailDomainIdx][vslot->VDiskIdx].NodeId = vslot->VSlotId.ComprisingPDiskId().NodeId;
                        }
                        replacedSlots.emplace(vslot->GetShortVDiskId(), vslot->VSlotId);
                    } else {
                        preservedSlots.emplace(vslot->GetVDiskId(), vslot->VSlotId);
                        auto& m = vslot->Metrics;
                        if (m.HasAllocatedSize() && !IgnoreVSlotQuotaCheck) {
                            // calculate space as the maximum of allocated sizes of untouched vdisks
                            requiredSpace = Max<i64>(requiredSpace, m.GetAllocatedSize());
                        }
                    }
                }

                if (sanitizingRequest) {
                    if (checker.OneStepFromDegradedOrWorse(failed)) {
                        throw TExFitGroupError() << "Sanitizing requst was blocked, group is one step from DEGRADED or worse";
                    }
                    if (groupInfo->VDisksInGroup.empty()) {
                        throw TExFitGroupError() << "Group has been decommitted and cannot be sanitized";
                    }
                    getGroup();
                }

                if (group) {
                    TGroupInfo *groupInfo = State.Groups.FindForUpdate(groupId);

                    // we have something changed in the group; initiate remapping
                    bool hasMissingSlots = false;
                    bool adjustSpaceAvailable = false;
                    for (const auto& realm : group) {
                        for (const auto& domain : realm) {
                            for (const TPDiskId& pdisk : domain) {
                                if (pdisk == TPDiskId()) {
                                    hasMissingSlots = true;
                                }
                            }
                        }
                    }
                    if (hasMissingSlots || !IgnoreGroupSanityChecks) {
                        TGroupMapper::TForbiddenPDisks forbid;
                        for (const auto& vslot : groupInfo->VDisksInGroup) {
                            const TVDiskIdShort vdiskId = vslot->GetShortVDiskId();
                            for (const TVSlotId& vslotId : vslot->Donors) {
                                if (group[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] == TPDiskId()) {
                                    forbid.insert(vslotId.ComprisingPDiskId());
                                }
                            }
                        }
                        for (const auto& vslotId : groupInfo->VSlotsBeingDeleted) {
                            const TVSlotInfo *vslot = State.VSlots.Find(vslotId);
                            const TVDiskIdShort& vdiskId = vslot->GetShortVDiskId();
                            if (group[vdiskId.FailRealm][vdiskId.FailDomain][vdiskId.VDisk] == TPDiskId()) {
                                forbid.insert(vslotId.ComprisingPDiskId());
                            }
                        }

                        if ((State.Self.IsGroupLayoutSanitizerEnabled() && replacedSlots.size() == 1 && hasMissingSlots && !layoutIsValid) ||
                                (replacedSlots.empty() && sanitizingRequest)) {

                            STLOG(PRI_INFO, BS_CONTROLLER, BSCFG01, "Attempt to sanitize group layout", (GroupId, groupId));
                            // Use group layout sanitizing algorithm on direct requests or when initial group layout is invalid
                            auto result = AllocateOrSanitizeGroup(groupId, group, {}, std::move(forbid), requiredSpace,
                                AllowUnusableDisks, &TGroupGeometryInfo::SanitizeGroup);

                            if (replacedSlots.empty()) {
                                // update information about replaced disks
                                for (const TVSlotInfo *vslot : groupInfo->VDisksInGroup) {
                                    if (vslot->GetShortVDiskId() == result.first) {
                                        auto it = preservedSlots.find(vslot->GetVDiskId());
                                        Y_ABORT_UNLESS(it != preservedSlots.end());
                                        preservedSlots.erase(it);
                                        replacedSlots.emplace(result.first, vslot->VSlotId);
                                        break;
                                    }
                                }
                            }
                        } else {
                            THashMap<TVDiskIdShort, TPDiskId> replacedDisks;
                            for (const auto& [vdiskId, vslotId] : replacedSlots) {
                                replacedDisks.emplace(vdiskId, vslotId.ComprisingPDiskId());
                            }
                            try {
                                TGroupMapper::MergeTargetDiskConstraints(hardConstraints, softConstraints);
                                AllocateOrSanitizeGroup(groupId, group, softConstraints, replacedDisks, std::move(forbid), requiredSpace,
                                    AllowUnusableDisks, &TGroupGeometryInfo::AllocateGroup);
                            } catch (const TExFitGroupError& ex) {
                                AllocateOrSanitizeGroup(groupId, group, hardConstraints, replacedDisks, std::move(forbid), requiredSpace,
                                    AllowUnusableDisks, &TGroupGeometryInfo::AllocateGroup);
                            }
                        }
                        if (!IgnoreVSlotQuotaCheck) {
                            adjustSpaceAvailable = true;
                            for (const auto& [pos, vslotId] : replacedSlots) {
                                const TPDiskId& pdiskId = group[pos.FailRealm][pos.FailDomain][pos.VDisk];
                                const TPDiskInfo *pdisk = State.PDisks.Find(pdiskId);
                                if (requiredSpace != Min<i64>() && !pdisk->SlotSpaceEnforced(State.Self)) {
                                    Mapper->AdjustSpaceAvailable(pdiskId, -requiredSpace);
                                }
                            }
                        }
                    }

                    // make list of donors and dispose unused slots
                    std::vector<TVSlotId> donors;
                    for (const auto& [vdiskId, vslotId] : replacedSlots) {
                        const bool suppressDonorMode = State.SuppressDonorMode.contains(vslotId);
                        if (State.DonorMode && !suppressDonorMode && !State.UncommittedVSlots.count(vslotId)) {
                            donors.push_back(vslotId);
                        } else {
                            if (adjustSpaceAvailable) {
                                const TVSlotInfo *slot = State.VSlots.Find(vslotId);
                                Y_ABORT_UNLESS(slot);
                                if (!slot->PDisk->SlotSpaceEnforced(State.Self)) {
                                    // mark the space from destroyed slot as available
                                    Mapper->AdjustSpaceAvailable(vslotId.ComprisingPDiskId(), slot->Metrics.GetAllocatedSize());
                                }
                            }

                            State.DestroyVSlot(vslotId);
                        }
                    }

                    // create slots for the new group
                    auto newSlots = CreateVSlotsForGroup(groupInfo, group, preservedSlots);
                    State.GroupContentChanged.insert(groupId);
                    State.GroupFailureModelChanged.insert(groupId);

                    if (replacedSlots) {
                        if (!IgnoreGroupFailModelChecks) {
                            // process only groups with changed content; check the failure model
                            if (!checker.CheckFailModelForGroup(failed)) {
                                throw TExMayLoseData(groupId.GetRawId());
                            } else if (!IgnoreDegradedGroupsChecks && checker.IsDegraded(failed)) {
                                throw TExMayGetDegraded(groupId.GetRawId());
                            }
                        }

                        for (const TVSlotInfo *slot : groupInfo->VDisksInGroup) {
                            const TVDiskIdShort pos(slot->RingIdx, slot->FailDomainIdx, slot->VDiskIdx);
                            if (const auto it = replacedSlots.find(pos); it != replacedSlots.end()) {
                                auto *item = Status.AddReassignedItem();
                                VDiskIDFromVDiskID(TVDiskID(groupInfo->ID, groupInfo->Generation, pos), item->MutableVDiskId());
                                Serialize(item->MutableFrom(), it->second);
                                Serialize(item->MutableTo(), slot->VSlotId);
                                if (auto *pdisk = State.PDisks.Find(it->second.ComprisingPDiskId())) {
                                    item->SetFromFqdn(std::get<0>(pdisk->HostId));
                                    item->SetFromPath(pdisk->Path);
                                }
                                if (auto *pdisk = State.PDisks.Find(slot->VSlotId.ComprisingPDiskId())) {
                                    item->SetToFqdn(std::get<0>(pdisk->HostId));
                                    item->SetToPath(pdisk->Path);
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
                        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA04, "ReconfigGroup", (UniqueId, State.UniqueId),
                            (GroupId, groupInfo->ID),
                            (GroupGeneration, groupInfo->Generation),
                            (Replacements, makeReplacements()));
                    }

                    for (const TVSlotId& vslotId : donors) {
                        TVSlotInfo *mutableSlot = State.VSlots.FindForUpdate(vslotId);
                        Y_ABORT_UNLESS(mutableSlot);
                        const auto it = newSlots.find(mutableSlot->GetShortVDiskId());
                        Y_ABORT_UNLESS(it != newSlots.end());
                        mutableSlot->MakeDonorFor(it->second);
                    }
                }

                State.CheckConsistency();
            }

        private:            
            template<typename T>
            std::invoke_result_t<T, TGroupGeometryInfo&, TGroupMapper&, TGroupId, TGroupMapper::TGroupDefinition&, TGroupMapper::TGroupConstraintsDefinition&,
                    const THashMap<TVDiskIdShort, TPDiskId>&, TGroupMapper::TForbiddenPDisks, i64> AllocateOrSanitizeGroup(
                    TGroupId groupId, TGroupMapper::TGroupDefinition& group, TGroupMapper::TGroupConstraintsDefinition& constraints,
                    const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TGroupMapper::TForbiddenPDisks forbid,
                    i64 requiredSpace, bool addExistingDisks, T&& func) {
                if (!Mapper) {
                    Mapper.emplace(Geometry, StoragePool.RandomizeGroupMapping);
                    PopulateGroupMapper();
                }
                TStackVec<TPDiskId, 32> removeQ;
                if (addExistingDisks) {
                    for (const auto& realm : group) {
                        for (const auto& domain : realm) {
                            for (const TPDiskId id : domain) {
                                if (id != TPDiskId()) {
                                    if (auto *info = State.PDisks.Find(id); info && RegisterPDisk(id, *info, false, "X")) {
                                        removeQ.push_back(id);
                                    }
                                }
                            }
                        }
                    }
                }
                struct TUnregister {
                    TGroupMapper& Mapper;
                    TStackVec<TPDiskId, 32>& RemoveQ;
                    ~TUnregister() {
                        for (const TPDiskId pdiskId : RemoveQ) {
                            Mapper.UnregisterPDisk(pdiskId);
                        }
                    }
                } unregister{*Mapper, removeQ};
                return std::invoke(func, Geometry, *Mapper, groupId, group, constraints, replacedDisks, std::move(forbid), requiredSpace);
            }

            template<typename T>
            std::invoke_result_t<T, TGroupGeometryInfo&, TGroupMapper&, TGroupId, TGroupMapper::TGroupDefinition&, TGroupMapper::TGroupConstraintsDefinition&,
                    const THashMap<TVDiskIdShort, TPDiskId>&, TGroupMapper::TForbiddenPDisks, i64> AllocateOrSanitizeGroup(
                    TGroupId groupId, TGroupMapper::TGroupDefinition& group,
                    const THashMap<TVDiskIdShort, TPDiskId>& replacedDisks, TGroupMapper::TForbiddenPDisks forbid,
                    i64 requiredSpace, bool addExistingDisks, T&& func) {
                TGroupMapper::TGroupConstraintsDefinition emptyConstraints;
                return AllocateOrSanitizeGroup(groupId, group, emptyConstraints, replacedDisks, forbid, requiredSpace, addExistingDisks, func);
            }

            void PopulateGroupMapper() {
                const TBoxId boxId = std::get<0>(StoragePoolId);

                State.PDisks.ForEach([&](const TPDiskId& id, const TPDiskInfo& info) {
                    if (info.BoxId != boxId) {
                        return; // ignore disks not from desired box
                    }

                    if (State.PDisksToRemove.count(id)) {
                        return; // this PDisk is scheduled for removal
                    }

                    for (const auto& filter : StoragePool.PDiskFilters) {
                        if (filter.MatchPDisk(info)) {
                            const bool inserted = RegisterPDisk(id, info, true);
                            Y_ABORT_UNLESS(inserted);
                            break;
                        }
                    }
                });
            }

            bool RegisterPDisk(TPDiskId id, const TPDiskInfo& info, bool usable, TString whyUnusable = {}) {
                // calculate number of used slots on this PDisk, also counting the static ones
                ui32 numSlots = info.NumActiveSlots + info.StaticSlotUsage;

                // create a set of groups residing on this PDisk
                TStackVec<ui32, 16> groups;
                for (const auto& [vslotId, vslot] : info.VSlotsOnPDisk) {
                    if (!vslot->IsBeingDeleted()) {
                        groups.push_back(vslot->GroupId.GetRawId());
                    }
                }

                // calculate vdisk space quota (or amount of available space when no enforcement is enabled)
                i64 availableSpace = Max<i64>();
                if (usable && !IgnoreVSlotQuotaCheck) {
                    if (info.SlotSpaceEnforced(State.Self)) {
                        availableSpace = info.Metrics.GetEnforcedDynamicSlotSize() * (1000 - PDiskSpaceMarginPromille) / 1000;
                    } else {
                        // here we assume that no space enforcement takes place and we have to calculate available space
                        // for this disk; we take it as available space and keep in mind that PDisk must have at least
                        // PDiskSpaceMarginPromille space remaining
                        availableSpace = info.Metrics.GetAvailableSize() - info.Metrics.GetTotalSize() * PDiskSpaceMarginPromille / 1000;

                        // also we have to find replicating VSlots on this PDisk and assume they consume up to
                        // max(vslotSize for every slot in group), not their actual AllocatedSize
                        for (const auto& [id, slot] : info.VSlotsOnPDisk) {
                            if (slot->Group && slot->Status != NKikimrBlobStorage::EVDiskStatus::READY) {
                                ui64 maxGroupSlotSize = 0;
                                for (const TVSlotInfo *peer : slot->Group->VDisksInGroup) {
                                    maxGroupSlotSize = Max(maxGroupSlotSize, peer->Metrics.GetAllocatedSize());
                                }
                                // return actually used space to available pool
                                availableSpace += slot->Metrics.GetAllocatedSize();
                                // and consume expected slot size after replication finishes
                                availableSpace -= maxGroupSlotSize;
                            }
                        }
                    }
                }

                if (!info.AcceptsNewSlots()) {
                    usable = false;
                    whyUnusable.append('S');
                }

                if (SettleOnlyOnOperationalDisks && !info.Operational) {
                    usable = false;
                    whyUnusable.append('O');
                }

                if (!info.UsableInTermsOfDecommission(IsSelfHealReasonDecommit)) {
                    usable = false;
                    whyUnusable.append('D');
                }

                // register PDisk in the mapper
                return Mapper->RegisterPDisk({
                    .PDiskId = id,
                    .Location = State.HostRecords->GetLocation(id.NodeId),
                    .Usable = usable,
                    .NumSlots = numSlots,
                    .MaxSlots = info.ExpectedSlotCount,
                    .Groups = std::move(groups),
                    .SpaceAvailable = availableSpace,
                    .Operational = info.Operational,
                    .Decommitted = info.Decommitted(),
                    .WhyUnusable = std::move(whyUnusable),
                });
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
                                    TInstant::Zero(), TDuration::Zero());

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

                return res;
            }
        };

        void TBlobStorageController::FitGroupsForUserConfig(TConfigState& state, ui32 availabilityDomainId,
                const NKikimrBlobStorage::TConfigRequest& cmd, std::deque<ui64> expectedSlotSize,
                NKikimrBlobStorage::TConfigResponse::TStatus& status) {
            auto poolsAndGroups = std::exchange(state.Fit.PoolsAndGroups, {});
            if (poolsAndGroups.empty()) {
                return; // nothing to do
            }

            std::unordered_map<TString, std::pair<ui32, TBoxStoragePoolId>> filterMap;
            std::unordered_set<TString> changedFilters;

            // scan through all storage pools and fit the number of groups to desired one
            auto processSingleStoragePool = [&](TBoxStoragePoolId storagePoolId, const TStoragePoolInfo& storagePool,
                    bool createNewGroups, const auto& enumerateGroups) {
                TGroupFitter fitter(state, availabilityDomainId, cmd, expectedSlotSize, PDiskSpaceMarginPromille,
                    storagePoolId, storagePool, status, VSlotReadyTimestampQ);

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

                    enumerateGroups([&](TGroupId groupId) {
                        fitter.CheckExistingGroup(groupId);
                        ++numActualGroups;
                    });
                    if (createNewGroups) {
                        if (numActualGroups < storagePool.NumGroups) {
                            changedFilters.insert(identifier);
                        }
                        for (; numActualGroups < storagePool.NumGroups; ++numActualGroups) {
                            fitter.CreateGroup();
                        }
                    }
                } catch (const TExFitGroupError& ex) {
                    throw TExError() << "Group fit error"
                        << " BoxId# " << std::get<0>(storagePoolId)
                        << " StoragePoolId# " << std::get<1>(storagePoolId)
                        << " Error# " << ex.what();
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
                        for (auto [begin, end] = storagePoolGroups.equal_range(spIt->first); begin != end; ++begin) {
                            callback(begin->second);
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
                    TGroupFitter fitter(state, availabilityDomainId, cmd, expectedSlotSize, PDiskSpaceMarginPromille,
                        storagePoolId, storagePool, status, VSlotReadyTimestampQ);
                    fitter.CheckReserve(numGroups, GroupReserveMin, GroupReservePart);
                }
            }

            state.CheckConsistency();
        }

    } // NBsController
} // NKikimr
