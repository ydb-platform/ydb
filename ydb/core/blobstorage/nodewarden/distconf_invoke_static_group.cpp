#include "distconf_invoke.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd) {
        RunCommonChecks();

        if (cmd.GetFromSelfHeal() && !Self->StorageConfig->GetSelfManagementConfig().GetAutomaticStaticGroupManagement()) {
            throw TExError() << "Operation forbidden: automatic static group management disabled";
        }

        bool found = false;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        for (const auto& group : Self->StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                if (group.GetGroupGeneration() != vdiskId.GroupGeneration) {
                    throw TExError() << "Group generation mismatch"
                        << " GroupId# " << group.GetGroupID()
                        << " Generation# " << group.GetGroupGeneration()
                        << " VDiskId# " << vdiskId;
                }
                if (vdiskId.FailRealm >= group.RingsSize() || vdiskId.FailDomain >= group.GetRings(vdiskId.FailRealm).FailDomainsSize()
                    || vdiskId.VDisk >= group.GetRings(vdiskId.FailRealm)
                            .GetFailDomains(vdiskId.FailDomain).VDiskLocationsSize()) {
                    throw TExError() << "VDiskId# " << vdiskId << " not found in group";
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

        TVector<ui32> nodesToConnect;
        for (ui32 i = 0, num = GroupInfo->GetTotalVDisksNum(); i < num; ++i) {
            const TVDiskID vdiskId = GroupInfo->GetVDiskId(i);
            const TActorId actorId = GroupInfo->GetActorId(i);
            const ui32 nodeId = actorId.NodeId();
            if (nodeId == SelfId().NodeId()) {
                SendVStatusQuery(actorId, vdiskId);
            } else {
                NodeToVDisk.emplace(nodeId, vdiskId);
                const auto [it, inserted] = Subscriptions.try_emplace(nodeId);
                if (it->second) {
                    SendVStatusQuery(actorId, vdiskId, it->second);
                } else {
                    VStatusQueriesAwaitingConnection[nodeId].emplace_back(actorId, vdiskId);
                }
                if (inserted) {
                    nodesToConnect.push_back(nodeId);
                }
            }
            ActorToVDisk.emplace(actorId, vdiskId);
            PendingVDiskIds.emplace(vdiskId);
        }

        for (const ui32 nodeId : nodesToConnect) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvInterconnect::TEvConnectNode);
        }
    }

    void TInvokeRequestHandlerActor::SendVStatusQuery(TActorId actorId, TVDiskID vdiskId, TActorId sessionId) {
        YDB_LOG_DEBUG("Sending TEvVStatus",
            {"marker", "NWDC73"},
            {"selfId", SelfId()},
            {"VDiskId", vdiskId},
            {"actorId", actorId});

        auto ev = std::make_unique<IEventHandle>(actorId, SelfId(), new TEvBlobStorage::TEvVStatus(vdiskId), IEventHandle::FlagTrackDelivery);
        if (sessionId) {
            ev->Rewrite(TEvInterconnect::EvForward, sessionId);
        }
        TActivationContext::Send(ev.release());
    }

    void TInvokeRequestHandlerActor::SendPendingVStatusQueries(ui32 nodeId, TActorId sessionId) {
        if (const auto it = VStatusQueriesAwaitingConnection.find(nodeId); it != VStatusQueriesAwaitingConnection.end()) {
            auto queries = std::move(it->second);
            VStatusQueriesAwaitingConnection.erase(it);
            for (const auto& [actorId, vdiskId] : queries) {
                SendVStatusQuery(actorId, vdiskId, sessionId);
            }
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        YDB_LOG_DEBUG("TEvVStatusResult",
            {"marker", "NWDC74"},
            {"selfId", SelfId()},
            {"record", record},
            {"VDiskId", vdiskId});
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
        if (!PendingVDiskIds.erase(vdiskId)) {
            return;
        }
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

        YDB_LOG_DEBUG("ReassignGroupDiskExecute",
            {"marker", "NWDC75"},
            {"selfId", SelfId()});

        const auto& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

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
                TDistributedConfigKeeper::TStaticGroupReassignments reassignments;
                try {
                    const auto bridgePileId = TBridgePileId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId);
                    std::optional<TGroupId> bridgeProxyGroupId = group.HasBridgeProxyGroupId()
                        ? std::make_optional(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId))
                        : std::nullopt;
                    Self->AllocateStaticGroup({
                        .Config = &config,
                        .GroupId = vdiskId.GroupID,
                        .GroupGeneration = vdiskId.GroupGeneration + 1,
                        .GroupType = TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies()),
                        .ReplacedDisks = std::move(replacedDisks),
                        .ForbiddenPDisks = std::move(forbid),
                        .RequiredSpace = static_cast<i64>(maxSlotSize),
                        .BaseConfig = &BaseConfig.value(),
                        .ConvertToDonor = cmd.GetConvertToDonor(),
                        .IgnoreVSlotQuotaCheck = cmd.GetIgnoreVSlotQuotaCheck(),
                        .AllowUnusableDisks = cmd.GetAllowUnusableDisks(),
                        .SettleOnlyOnOperationalDisks = cmd.GetSettleOnlyOnOperationalDisks(),
                        .IsSelfHealReasonDecommit = cmd.GetIsSelfHealReasonDecommit(),
                        .BridgePileId = bridgePileId,
                        .BridgeProxyGroupId = bridgeProxyGroupId,
                        .ApplySelfHealNodeAllowList = cmd.GetFromSelfHeal(),
                        .Reassignments = &reassignments,
                    });
                } catch (const TExConfigError& ex) {
                    YDB_LOG_NOTICE("ReassignGroupDisk failed to allocate group",
                        {"marker", "NWDC76"},
                        {"selfId", SelfId()},
                        {"config", config},
                        {"baseConfig", *BaseConfig},
                        {"error", ex.what()});
                    throw TExError() << "Failed to allocate group: " << ex.what();
                }

                const auto it = reassignments.find(vdiskId);
                if (it == reassignments.end() || !it->second.SourceSlotId || !it->second.TargetSlotId) {
                    throw TExError() << "Failed to obtain reassigned VSlotIds";
                }
                auto& result = ReassignGroupDiskResult.emplace();
                result.MutableSourceSlotId()->CopyFrom(*it->second.SourceSlotId);
                result.MutableTargetSlotId()->CopyFrom(*it->second.TargetSlotId);

                return StartProposition(&config);
            }
        }

        throw TExError() << "Group not found";
    }

    void TInvokeRequestHandlerActor::StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd) {
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), false);
    }

    void TInvokeRequestHandlerActor::DropDonor(const TQuery::TDropDonor& cmd) {
        HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), true);
    }

    void TInvokeRequestHandlerActor::HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor) {
        RunCommonChecks();

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
                if (loc.GetNodeID() != vslotId.GetNodeId() || loc.GetPDiskID() != vslotId.GetPDiskId() ||
                        loc.GetVDiskSlotID() != vslotId.GetVSlotId()) {
                    continue; // doesn't match our pdisk
                }

                // this must be the obsolete disk (donor has generation from the past, destroyed disks too)
                Y_ABORT_UNLESS(currentVDiskId.GroupGeneration < actualGroupGeneration);

                if (!isDropDonor) {
                    // destroying slot on this disk
                    ss->MutableVDisks()->DeleteSubrange(i--, 1);
                    changes = true;
                } else {
                    Y_ABORT_UNLESS(vdisk.HasDonorMode()); // we have already checked this case by now
                    if (vdiskId.GroupGeneration == 0 || vdiskId.GroupGeneration == currentVDiskId.GroupGeneration) {
                        // sign up this entity for destruction
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
                    break;
                }
            }
        }
        if (unusedPDisk) {
            for (size_t i = 0; i < ss->PDisksSize(); ++i) {
                if (const auto& pdisk = ss->GetPDisks(i); pdisk.HasNodeID() && pdisk.HasPDiskID() &&
                        pdisk.GetNodeID() == vslotId.GetNodeId() && pdisk.GetPDiskID() == vslotId.GetPDiskId()) {
                    Y_ABORT_UNLESS(changes);
                    ss->MutablePDisks()->DeleteSubrange(i, 1);
                    break;
                }
            }
        }

        if (!changes) {
            return Finish(TResult::OK, std::nullopt);
        }

        StartProposition(&config);
    }

} // NKikimr::NStorage
