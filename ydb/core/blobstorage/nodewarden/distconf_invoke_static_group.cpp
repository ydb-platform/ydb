#include "distconf_invoke.h"

#include <util/stream/output.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd) {
        RunCommonChecks();

        if (cmd.GetFromSelfHeal() && !Self->StorageConfig->GetSelfManagementConfig().GetAutomaticStaticGroupManagement()) {
            throw TExError() << "Operation forbidden: automatic static group management disabled";
        }

        bool found = false;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDisk request"
            << " SelfId# " << SelfId()
            << " VDiskId# " << vdiskId
            << " FromSelfHeal# " << cmd.GetFromSelfHeal()
            << " ConvertToDonor# " << cmd.GetConvertToDonor()
            << " IsSelfHealReasonDecommit# " << cmd.GetIsSelfHealReasonDecommit()
            << " IgnoreVSlotQuotaCheck# " << cmd.GetIgnoreVSlotQuotaCheck()
            << " HasTargetPDisk# " << cmd.HasPDiskId();
        if (cmd.HasPDiskId()) {
            Cerr << " TargetPDisk# [" << cmd.GetPDiskId().GetNodeId() << ':' << cmd.GetPDiskId().GetPDiskId() << ']';
        }
        Cerr << Endl;
        for (const auto& group : Self->StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDisk group found"
                    << " GroupId# " << group.GetGroupID()
                    << " GroupGeneration# " << group.GetGroupGeneration()
                    << " RequestedVDisk# " << vdiskId
                    << Endl;
                if (group.GetGroupGeneration() != vdiskId.GroupGeneration) {
                    throw TExError() << "Group generation mismatch"
                        << " GroupId# " << group.GetGroupID()
                        << " Generation# " << group.GetGroupGeneration()
                        << " VDiskId# " << vdiskId;
                }
                found = true;
                if (!cmd.GetIgnoreGroupFailModelChecks()) {
                    IssueVStatusQueries(group);
                }
                break;
            }
        }
        if (!found) {
            throw TExError() << "GroupId# " << vdiskId.GroupID << " not found";
        }

        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
    }

    void TInvokeRequestHandlerActor::IssueVStatusQueries(const NKikimrBlobStorage::TGroupInfo& group) {
        TStringStream err;
        GroupInfo = TBlobStorageGroupInfo::Parse(group, nullptr, &err);
        if (!GroupInfo) {
            throw TExError() << "Failed to parse group info: " << err.Str();
        }
        SuccessfulVDisks.emplace(&GroupInfo->GetTopology());

        for (ui32 i = 0, num = GroupInfo->GetTotalVDisksNum(); i < num; ++i) {
            const TVDiskID vdiskId = GroupInfo->GetVDiskId(i);
            const TActorId actorId = GroupInfo->GetActorId(i);
            const ui32 flags = IEventHandle::FlagTrackDelivery |
                (actorId.NodeId() == SelfId().NodeId() ? 0 : IEventHandle::FlagSubscribeOnSession);
            STLOG(PRI_DEBUG, BS_NODE, NWDC73, "sending TEvVStatus", (SelfId, SelfId()), (VDiskId, vdiskId),
                (ActorId, actorId));
            Send(actorId, new TEvBlobStorage::TEvVStatus(vdiskId), flags);
            if (actorId.NodeId() != SelfId().NodeId()) {
                NodeToVDisk.emplace(actorId.NodeId(), vdiskId);
                Subscriptions.try_emplace(actorId.NodeId());
            }
            ActorToVDisk.emplace(actorId, vdiskId);
            PendingVDiskIds.emplace(vdiskId);
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        STLOG(PRI_DEBUG, BS_NODE, NWDC74, "TEvVStatusResult", (SelfId, SelfId()), (Record, record), (VDiskId, vdiskId));
        if (!PendingVDiskIds.erase(vdiskId)) {
            throw TExError() << "TEvVStatusResult VDiskID# " << vdiskId << " is unexpected";
        }
        if (record.GetJoinedGroup() && record.GetReplicated()) {
            *SuccessfulVDisks |= {&GroupInfo->GetTopology(), vdiskId};
        }
        CheckReassignGroupDisk();
    }

    void TInvokeRequestHandlerActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (const auto it = ActorToVDisk.find(ev->Sender); it != ActorToVDisk.end()) {
            Y_ABORT_UNLESS(ev->Get()->SourceType == TEvBlobStorage::EvVStatus);
            OnVStatusError(it->second);
        }
    }

    void TInvokeRequestHandlerActor::OnVStatusError(TVDiskID vdiskId) {
        PendingVDiskIds.erase(vdiskId);
        CheckReassignGroupDisk();
    }

    void TInvokeRequestHandlerActor::Handle(TEvNodeWardenBaseConfig::TPtr ev) {
        BaseConfig.emplace(std::move(ev->Get()->BaseConfig));
        CheckReassignGroupDisk();
    }

    void TInvokeRequestHandlerActor::CheckReassignGroupDisk() {
        if (BaseConfig && PendingVDiskIds.empty()) {
            ReassignGroupDiskExecute();
        }
    }

    void TInvokeRequestHandlerActor::ReassignGroupDiskExecute() {
        RunCommonChecks();

        if (!Self->SelfManagementEnabled) {
            throw TExError() << "Self-management is not enabled";
        }

        auto *op = std::get_if<TInvokeExternalOperation>(&Query);
        Y_ABORT_UNLESS(op);
        const auto& record = op->Command;
        const auto& cmd = record.GetReassignGroupDisk();

        STLOG(PRI_DEBUG, BS_NODE, NWDC75, "ReassignGroupDiskExecute", (SelfId, SelfId()));

        const auto& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute begin"
            << " SelfId# " << SelfId()
            << " VDiskId# " << vdiskId
            << " FromSelfHeal# " << cmd.GetFromSelfHeal()
            << " ConvertToDonor# " << cmd.GetConvertToDonor()
            << " IsSelfHealReasonDecommit# " << cmd.GetIsSelfHealReasonDecommit()
            << Endl;

        ui64 maxSlotSize = 0;

        if (SuccessfulVDisks) {
            const auto& checker = GroupInfo->GetQuorumChecker();

            auto check = [&](auto failedVDisks, const char *base) {
                bool wasDegraded = checker.IsDegraded(failedVDisks) && checker.CheckFailModelForGroup(failedVDisks);
                failedVDisks |= {&GroupInfo->GetTopology(), vdiskId};

                if (!checker.CheckFailModelForGroup(failedVDisks)) {
                    throw TExError() << "ReassignGroupDisk would render group inoperable (" << base << ')';
                } else if (!cmd.GetIgnoreDegradedGroupsChecks() && !wasDegraded && checker.IsDegraded(failedVDisks)) {
                    throw TExError() << "ReassignGroupDisk would drive group into degraded state (" << base << ')';
                }
            };

            check(~SuccessfulVDisks.value(), "polling");

            // scan failed disks according to BS_CONTROLLER's data
            TBlobStorageGroupInfo::TGroupVDisks failedVDisks(&GroupInfo->GetTopology());
            for (const auto& vslot : BaseConfig->GetVSlot()) {
                if (vslot.GetGroupId() != vdiskId.GroupID.GetRawId() || vslot.GetGroupGeneration() != vdiskId.GroupGeneration) {
                    continue;
                }
                if (!vslot.GetReady()) {
                    auto groupId = TGroupId::FromProto(&vslot, &NKikimrBlobStorage::TBaseConfig::TVSlot::GetGroupId);
                    const TVDiskID vdiskId(groupId, vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
                        vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                    failedVDisks |= {&GroupInfo->GetTopology(), vdiskId};
                }
                if (vslot.HasVDiskMetrics()) {
                    const auto& m = vslot.GetVDiskMetrics();
                    if (m.HasAllocatedSize()) {
                        maxSlotSize = Max(maxSlotSize, m.GetAllocatedSize());
                    }
                }
            }

            check(failedVDisks, "BS_CONTROLLER state");
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        if (!config.HasBlobStorageConfig()) {
            throw TExError() << "No BlobStorageConfig defined";
        }
        const auto& bsConfig = config.GetBlobStorageConfig();

        if (!bsConfig.HasServiceSet()) {
            throw TExError() << "No ServiceSet defined";
        }
        const auto& ss = bsConfig.GetServiceSet();

        const auto& smConfig = config.GetSelfManagementConfig();
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute serviceset"
            << " Groups# " << ss.GroupsSize()
            << " VDisks# " << ss.VDisksSize()
            << " PDisks# " << ss.PDisksSize()
            << Endl;

        THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
        NBsController::TGroupMapper::TForbiddenPDisks forbid;
        for (const auto& vdisk : ss.GetVDisks()) {
            const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                continue;
            }
            const auto& loc = vdisk.GetVDiskLocation();
            Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute matching VDisk"
                << " CurrentVDiskId# " << currentVDiskId
                << " TargetVDiskId# " << vdiskId
                << " Location# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ':' << loc.GetVDiskSlotID() << ']'
                << " EntityStatus# " << static_cast<int>(vdisk.GetEntityStatus())
                << " HasDonorMode# " << vdisk.HasDonorMode()
                << Endl;
            if (currentVDiskId == vdiskId) {
                NBsController::TPDiskId pdiskId;
                if (cmd.HasPDiskId()) {
                    const auto& target = cmd.GetPDiskId();
                    pdiskId = {target.GetNodeId(), target.GetPDiskId()};
                }
                replacedDisks.emplace(vdiskId, pdiskId);
                Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute replaced disk"
                    << " VDiskId# " << vdiskId
                    << " TargetPDisk# " << pdiskId
                    << Endl;
            } else {
                Y_DEBUG_ABORT_UNLESS(vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY ||
                    vdisk.HasDonorMode());
                forbid.emplace(loc.GetNodeID(), loc.GetPDiskID());
                Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute forbid obsolete disk"
                    << " VDiskId# " << currentVDiskId
                    << " PDisk# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ']'
                    << Endl;
            }
        }

        for (const auto& group : ss.GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                try {
                    const auto bridgePileId = TBridgePileId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId);
                    std::optional<TGroupId> bridgeProxyGroupId = group.HasBridgeProxyGroupId()
                        ? std::make_optional(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId))
                        : std::nullopt;
                    Self->AllocateStaticGroup(&config, vdiskId.GroupID, vdiskId.GroupGeneration + 1,
                        TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies()),
                        smConfig.GetGeometry(), smConfig.GetPDiskFilter(),
                        smConfig.HasPDiskType() ? std::make_optional(smConfig.GetPDiskType()) : std::nullopt,
                        replacedDisks, forbid, maxSlotSize,
                        &BaseConfig.value(), cmd.GetConvertToDonor(), cmd.GetIgnoreVSlotQuotaCheck(),
                        cmd.GetIsSelfHealReasonDecommit(), bridgePileId, bridgeProxyGroupId);
                    Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW ReassignGroupDiskExecute allocated"
                        << " GroupId# " << vdiskId.GroupID
                        << " OldGeneration# " << vdiskId.GroupGeneration
                        << " NewGeneration# " << vdiskId.GroupGeneration + 1
                        << Endl;
                } catch (const TExConfigError& ex) {
                    STLOG(PRI_NOTICE, BS_NODE, NWDC76, "ReassignGroupDisk failed to allocate group", (SelfId, SelfId()),
                        (Config, config),
                        (BaseConfig, *BaseConfig),
                        (Error, ex.what()));
                    throw TExError() << "Failed to allocate group: " << ex.what();
                }

                return StartProposition(&config);
            }
        }

        throw TExError() << "Group not found";
    }

    void TInvokeRequestHandlerActor::StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd) {
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW StaticVDiskSlain"
            << " SelfId# " << SelfId()
            << " VDiskId# " << VDiskIDFromVDiskID(cmd.GetVDiskId())
            << " VSlot# [" << cmd.GetVSlotId().GetNodeId() << ':' << cmd.GetVSlotId().GetPDiskId()
            << ':' << cmd.GetVSlotId().GetVSlotId() << ']'
            << Endl;
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), false);
    }

    void TInvokeRequestHandlerActor::DropDonor(const TQuery::TDropDonor& cmd) {
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW DropDonor"
            << " SelfId# " << SelfId()
            << " VDiskId# " << VDiskIDFromVDiskID(cmd.GetVDiskId())
            << " VSlot# [" << cmd.GetVSlotId().GetNodeId() << ':' << cmd.GetVSlotId().GetPDiskId()
            << ':' << cmd.GetVSlotId().GetVSlotId() << ']'
            << Endl;
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), true);
    }

    void TInvokeRequestHandlerActor::HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor) {
        RunCommonChecks();
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain begin"
            << " SelfId# " << SelfId()
            << " VDiskId# " << vdiskId
            << " VSlot# [" << vslotId.GetNodeId() << ':' << vslotId.GetPDiskId() << ':' << vslotId.GetVSlotId() << ']'
            << " IsDropDonor# " << isDropDonor
            << Endl;

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        if (!config.HasBlobStorageConfig()) {
            throw TExError() << "No BlobStorageConfig defined";
        }
        auto *bsConfig = config.MutableBlobStorageConfig();

        if (!bsConfig->HasServiceSet()) {
            throw TExError() << "No ServiceSet defined";
        }
        auto *ss = bsConfig->MutableServiceSet();

        bool changes = false;

        ui32 actualGroupGeneration = 0;
        for (const auto& group : ss->GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                actualGroupGeneration = group.GetGroupGeneration();
                break;
            }
        }
        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain group generation"
            << " VDiskId# " << vdiskId
            << " ActualGroupGeneration# " << actualGroupGeneration
            << Endl;
        Y_ABORT_UNLESS(0 < actualGroupGeneration && vdiskId.GroupGeneration < actualGroupGeneration);

        for (size_t i = 0; i < ss->VDisksSize(); ++i) {
            if (const auto& vdisk = ss->GetVDisks(i); vdisk.HasVDiskID() && vdisk.HasVDiskLocation()) {
                const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                    continue; // definitely incorrect group or position in group
                }
                if (isDropDonor && vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                    continue; // dropping donor and this entity is already destroyed
                }

                if (isDropDonor && !vdisk.HasDonorMode()) {
                    // this is the active disk and we want to drop its donor
                    Y_ABORT_UNLESS(currentVDiskId.GroupGeneration == actualGroupGeneration);
                    auto *m = ss->MutableVDisks(i);
                    if (vdiskId.GroupGeneration) { // drop specific donor
                        for (size_t k = 0; k < m->DonorsSize(); ++k) {
                            const auto& donor = m->GetDonors(k);
                            const auto& loc = donor.GetVDiskLocation();
                            if (VDiskIDFromVDiskID(donor.GetVDiskId()) == vdiskId && loc.GetNodeID() == vslotId.GetNodeId() &&
                                    loc.GetPDiskID() == vslotId.GetPDiskId() && loc.GetVDiskSlotID() == vslotId.GetVSlotId()) {
                                m->MutableDonors()->DeleteSubrange(k, 1);
                                changes = true;
                                break;
                            }
                        }
                    } else { // drop all of them
                        m->ClearDonors();
                        changes = true;
                    }
                    continue;
                }

                const auto& loc = vdisk.GetVDiskLocation();
                Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain candidate"
                    << " CurrentVDiskId# " << currentVDiskId
                    << " Location# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ':' << loc.GetVDiskSlotID() << ']'
                    << " EntityStatus# " << static_cast<int>(vdisk.GetEntityStatus())
                    << " HasDonorMode# " << vdisk.HasDonorMode()
                    << Endl;
                if (loc.GetNodeID() != vslotId.GetNodeId() || loc.GetPDiskID() != vslotId.GetPDiskId() ||
                        loc.GetVDiskSlotID() != vslotId.GetVSlotId()) {
                    Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain skip candidate location mismatch"
                        << " CurrentVDiskId# " << currentVDiskId
                        << Endl;
                    continue; // doesn't match our pdisk
                }

                // this must be the obsolete disk (donor has generation from the past, destroyed disks too)
                Y_ABORT_UNLESS(currentVDiskId.GroupGeneration < actualGroupGeneration);

                if (!isDropDonor) {
                    // destroying slot on this disk
                    Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain delete VDisk"
                        << " CurrentVDiskId# " << currentVDiskId
                        << " Location# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ':' << loc.GetVDiskSlotID() << ']'
                        << Endl;
                    ss->MutableVDisks()->DeleteSubrange(i--, 1);
                    changes = true;
                } else {
                    Y_ABORT_UNLESS(vdisk.HasDonorMode()); // we have already checked this case by now
                    if (vdiskId.GroupGeneration == 0 || vdiskId.GroupGeneration == currentVDiskId.GroupGeneration) {
                        // sign up this entity for destruction
                        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain mark donor DESTROY"
                            << " CurrentVDiskId# " << currentVDiskId
                            << " Location# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ':' << loc.GetVDiskSlotID() << ']'
                            << Endl;
                        auto *m = ss->MutableVDisks(i);
                        m->ClearDonorMode();
                        m->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                        changes = true;
                    }
                }
            }
        }

        bool unusedPDisk = true;
        for (const auto& vdisk : ss->GetVDisks()) {
            if (vdisk.HasVDiskLocation()) {
                const auto& loc = vdisk.GetVDiskLocation();
                if (loc.GetNodeID() == vslotId.GetNodeId() && loc.GetPDiskID() == vslotId.GetPDiskId()) {
                    unusedPDisk = false;
                    Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain PDisk still used"
                        << " BlockingVDiskId# " << VDiskIDFromVDiskID(vdisk.GetVDiskID())
                        << " Location# [" << loc.GetNodeID() << ':' << loc.GetPDiskID() << ':' << loc.GetVDiskSlotID() << ']'
                        << " EntityStatus# " << static_cast<int>(vdisk.GetEntityStatus())
                        << " HasDonorMode# " << vdisk.HasDonorMode()
                        << Endl;
                    break;
                }
            }
        }
        if (unusedPDisk) {
            for (size_t i = 0; i < ss->PDisksSize(); ++i) {
                if (const auto& pdisk = ss->GetPDisks(i); pdisk.HasNodeID() && pdisk.HasPDiskID() &&
                        pdisk.GetNodeID() == vslotId.GetNodeId() && pdisk.GetPDiskID() == vslotId.GetPDiskId()) {
                    Y_ABORT_UNLESS(changes);
                    Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain delete PDisk"
                        << " PDisk# [" << pdisk.GetNodeID() << ':' << pdisk.GetPDiskID() << ']'
                        << " Path# " << pdisk.GetPath()
                        << Endl;
                    ss->MutablePDisks()->DeleteSubrange(i, 1);
                    break;
                }
            }
        }

        if (!changes) {
            Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain no changes"
                << " VDiskId# " << vdiskId
                << " IsDropDonor# " << isDropDonor
                << Endl;
            return Finish(TResult::OK, std::nullopt);
        }

        Cerr << "DISTCONF_STATIC_SELFHEAL_DEBUG NW HandleDropDonorAndSlain propose changes"
            << " VDiskId# " << vdiskId
            << " IsDropDonor# " << isDropDonor
            << " UnusedPDisk# " << unusedPDisk
            << Endl;
        StartProposition(&config);
    }

} // NKikimr::NStorage
