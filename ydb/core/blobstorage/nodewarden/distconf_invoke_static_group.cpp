#include "distconf_invoke.h"

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd) {
        if (!RunCommonChecks()) {
            return;
        }

        bool found = false;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        for (const auto& group : Self->StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                if (group.GetGroupGeneration() != vdiskId.GroupGeneration) {
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "group generation mismatch"
                        << " GroupId# " << group.GetGroupID()
                        << " Generation# " << group.GetGroupGeneration()
                        << " VDiskId# " << vdiskId);
                }
                found = true;
                if (!cmd.GetIgnoreGroupFailModelChecks()) {
                    IssueVStatusQueries(group);
                }
                break;
            }
        }
        if (!found) {
            return FinishWithError(TResult::ERROR, TStringBuilder() << "GroupId# " << vdiskId.GroupID << " not found");
        }

        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
    }

    void TInvokeRequestHandlerActor::IssueVStatusQueries(const NKikimrBlobStorage::TGroupInfo& group) {
        TStringStream err;
        GroupInfo = TBlobStorageGroupInfo::Parse(group, nullptr, &err);
        if (!GroupInfo) {
            return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to parse group info: " << err.Str());
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
            return FinishWithError(TResult::ERROR, TStringBuilder() << "TEvVStatusResult VDiskID# " << vdiskId
                << " is unexpected");
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
        const auto& record = Event->Get()->Record;
        const auto& cmd = record.GetReassignGroupDisk();

        if (!RunCommonChecks()) {
            return;
        } else if (!Self->SelfManagementEnabled) {
            return FinishWithError(TResult::ERROR, "self-management is not enabled");
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC75, "ReassignGroupDiskExecute", (SelfId, SelfId()));

        const auto& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

        ui64 maxSlotSize = 0;

        if (SuccessfulVDisks) {
            const auto& checker = GroupInfo->GetQuorumChecker();

            auto check = [&](auto failedVDisks, const char *base) {
                bool wasDegraded = checker.IsDegraded(failedVDisks) && checker.CheckFailModelForGroup(failedVDisks);
                failedVDisks |= {&GroupInfo->GetTopology(), vdiskId};

                if (!checker.CheckFailModelForGroup(failedVDisks)) {
                    FinishWithError(TResult::ERROR, TStringBuilder()
                        << "ReassignGroupDisk would render group inoperable (" << base << ')');
                } else if (!cmd.GetIgnoreDegradedGroupsChecks() && !wasDegraded && checker.IsDegraded(failedVDisks)) {
                    FinishWithError(TResult::ERROR, TStringBuilder()
                        << "ReassignGroupDisk would drive group into degraded state (" << base << ')');
                } else {
                    return true;
                }

                return false;
            };

            if (!check(~SuccessfulVDisks.value(), "polling")) {
                return;
            }

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

            if (!check(failedVDisks, "BS_CONTROLLER state")) {
                return;
            }
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        if (!config.HasBlobStorageConfig()) {
            return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
        }
        const auto& bsConfig = config.GetBlobStorageConfig();

        if (!bsConfig.HasServiceSet()) {
            return FinishWithError(TResult::ERROR, "no ServiceSet defined");
        }
        const auto& ss = bsConfig.GetServiceSet();

        const auto& smConfig = config.GetSelfManagementConfig();

        THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
        NBsController::TGroupMapper::TForbiddenPDisks forbid;
        for (const auto& vdisk : ss.GetVDisks()) {
            const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                continue;
            }
            if (currentVDiskId == vdiskId) {
                NBsController::TPDiskId pdiskId;
                if (cmd.HasPDiskId()) {
                    const auto& target = cmd.GetPDiskId();
                    pdiskId = {target.GetNodeId(), target.GetPDiskId()};
                }
                replacedDisks.emplace(vdiskId, pdiskId);
            } else {
                Y_DEBUG_ABORT_UNLESS(vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY ||
                    vdisk.HasDonorMode());
                const auto& loc = vdisk.GetVDiskLocation();
                forbid.emplace(loc.GetNodeID(), loc.GetPDiskID());
            }
        }

        for (const auto& group : ss.GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                try {
                    Self->AllocateStaticGroup(&config, vdiskId.GroupID.GetRawId(), vdiskId.GroupGeneration + 1,
                        TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies()),
                        smConfig.GetGeometry(), smConfig.GetPDiskFilter(),
                        smConfig.HasPDiskType() ? std::make_optional(smConfig.GetPDiskType()) : std::nullopt,
                        replacedDisks, forbid, maxSlotSize,
                        &BaseConfig.value(), cmd.GetConvertToDonor(), cmd.GetIgnoreVSlotQuotaCheck(),
                        cmd.GetIsSelfHealReasonDecommit());
                } catch (const TExConfigError& ex) {
                    STLOG(PRI_NOTICE, BS_NODE, NWDC76, "ReassignGroupDisk failed to allocate group", (SelfId, SelfId()),
                        (Config, config),
                        (BaseConfig, *BaseConfig),
                        (Error, ex.what()));
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to allocate group: " << ex.what());
                }

                config.SetGeneration(config.GetGeneration() + 1);
                return StartProposition(&config);
            }
        }

        return FinishWithError(TResult::ERROR, TStringBuilder() << "group not found");
    }

    void TInvokeRequestHandlerActor::StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd) {
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), false);
    }

    void TInvokeRequestHandlerActor::DropDonor(const TQuery::TDropDonor& cmd) {
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), true);
    }

    void TInvokeRequestHandlerActor::HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor) {
        if (!RunCommonChecks()) {
            return;
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        if (!config.HasBlobStorageConfig()) {
            return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
        }
        auto *bsConfig = config.MutableBlobStorageConfig();

        if (!bsConfig->HasServiceSet()) {
            return FinishWithError(TResult::ERROR, "no ServiceSet defined");
        }
        auto *ss = bsConfig->MutableServiceSet();

        bool changes = false;
        ui32 pdiskUsageCount = 0;

        ui32 actualGroupGeneration = 0;
        for (const auto& group : ss->GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                actualGroupGeneration = group.GetGroupGeneration();
                break;
            }
        }
        Y_ABORT_UNLESS(0 < actualGroupGeneration && vdiskId.GroupGeneration < actualGroupGeneration);

        for (size_t i = 0; i < ss->VDisksSize(); ++i) {
            if (const auto& vdisk = ss->GetVDisks(i); vdisk.HasVDiskID() && vdisk.HasVDiskLocation()) {
                const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (!currentVDiskId.SameExceptGeneration(vdiskId) ||
                        vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                    continue;
                }

                if (isDropDonor && !vdisk.HasDonorMode()) {
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
                if (loc.GetNodeID() != vslotId.GetNodeId() || loc.GetPDiskID() != vslotId.GetPDiskId()) {
                    continue;
                }
                ++pdiskUsageCount;

                if (loc.GetVDiskSlotID() != vslotId.GetVSlotId()) {
                    continue;
                }

                Y_ABORT_UNLESS(currentVDiskId.GroupGeneration < actualGroupGeneration);

                if (!isDropDonor) {
                    --pdiskUsageCount;
                    ss->MutableVDisks()->DeleteSubrange(i--, 1);
                    changes = true;
                } else if (vdisk.HasDonorMode()) {
                    if (currentVDiskId == vdiskId || vdiskId.GroupGeneration == 0) {
                        auto *m = ss->MutableVDisks(i);
                        m->ClearDonorMode();
                        m->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                        changes = true;
                    }
                }
            }
        }

        if (!isDropDonor && !pdiskUsageCount) {
            for (size_t i = 0; i < ss->PDisksSize(); ++i) {
                if (const auto& pdisk = ss->GetPDisks(i); pdisk.HasNodeID() && pdisk.HasPDiskID() &&
                        pdisk.GetNodeID() == vslotId.GetNodeId() && pdisk.GetPDiskID() == vslotId.GetPDiskId()) {
                    ss->MutablePDisks()->DeleteSubrange(i, 1);
                    changes = true;
                    break;
                }
            }
        }

        if (!changes) {
            return Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
        }

        config.SetGeneration(config.GetGeneration() + 1);
        StartProposition(&config);
    }

} // NKikimr::NStorage
