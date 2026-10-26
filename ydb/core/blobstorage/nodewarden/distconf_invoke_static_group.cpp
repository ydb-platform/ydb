#include "distconf_invoke.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    namespace {

        // Propagate new generation of the group to the bridge group state referencing it; the referencing bridge proxy
        // group gets its generation bumped too, unless this is an inline pile referencing itself.
        void UpdateBridgePileGeneration(NKikimrBlobStorage::TNodeWardenServiceSet *ss, TGroupId groupId) {
            std::optional<ui32> generation;
            for (const auto& group : ss->GetGroups()) {
                if (TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID) == groupId) {
                    generation.emplace(group.GetGroupGeneration());
                    break;
                }
            }
            Y_ABORT_UNLESS(generation);

            THashMap<TGroupId, ui32> bumpedInlineGroups;
            for (auto& group : *ss->MutableGroups()) {
                if (!group.HasBridgeGroupState()) {
                    continue;
                }
                bool updated = false;
                NBridge::ForEachGroupPile(group.MutableBridgeGroupState(), [&](TBridgePileId,
                        NKikimrBridge::TGroupState::TPile& pile) {
                    if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == groupId) {
                        pile.SetGroupGeneration(*generation);
                        updated = true;
                    }
                });
                if (updated) {
                    BumpBridgeProxyGeneration(&group, [&](TGroupId id) { return id == groupId; });
                    // The proxy may still contain the original pile's VDisks. Reassigning another pile bumps
                    // this physical group's generation too, including its self-reference and live VDisks.
                    const auto proxyId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
                    if (auto *inlinePile = NBridge::FindGroupPileByGroupId(group.MutableBridgeGroupState(), proxyId);
                            inlinePile && proxyId != groupId) {
                        inlinePile->SetGroupGeneration(group.GetGroupGeneration());
                        bumpedInlineGroups.emplace(proxyId, group.GetGroupGeneration());
                    }
                }
            }
            UpdateStaticVDiskGenerations(ss, bumpedInlineGroups);
        }

    }

    TDistributedConfigKeeper::TAllocateStaticGroupParams
    TDistributedConfigKeeper::BuildStaticGroupReassignParams(NKikimrBlobStorage::TStorageConfig *config,
                                                             const NKikimrBlobStorage::TBaseConfig *baseConfig,
                                                             const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TReassignGroupDisk& command,
                                                             const NKikimrBlobStorage::TGroupInfo& group,
                                                             const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet,
                                                             TStaticGroupReassignments *reassignments) {
        const TVDiskID vdiskId = VDiskIDFromVDiskID(command.GetVDiskId());
        THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
        NBsController::TGroupMapper::TForbiddenPDisks forbiddenPDisks;

        for (const auto& vdisk : serviceSet.GetVDisks()) {
            const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                continue;
            }
            if (currentVDiskId == vdiskId) {
                NBsController::TPDiskId pdiskId;
                if (command.HasPDiskId()) {
                    const auto& target = command.GetPDiskId();
                    pdiskId = {target.GetNodeId(), target.GetPDiskId()};
                }
                replacedDisks.emplace(vdiskId, pdiskId);
            } else {
                Y_DEBUG_ABORT_UNLESS(vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY || vdisk.HasDonorMode());
                const auto& location = vdisk.GetVDiskLocation();
                forbiddenPDisks.emplace(location.GetNodeID(), location.GetPDiskID());
            }
        }

        const TBridgePileId bridgePileId = TBridgePileId::FromProto(&group,
                                                                    &NKikimrBlobStorage::TGroupInfo::GetBridgePileId);
        const std::optional<TGroupId> bridgeProxyGroupId = group.HasBridgeProxyGroupId()
                                                           ? std::make_optional(TGroupId::FromProto(&group,
                                                                                                   &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId))
                                                           : std::nullopt;

        return {
            .Config = config,
            .GroupId = vdiskId.GroupID,
            .GroupGeneration = vdiskId.GroupGeneration + 1,
            .GroupType = TBlobStorageGroupType(static_cast<TBlobStorageGroupType::EErasureSpecies>(group.GetErasureSpecies())),
            .ReplacedDisks = std::move(replacedDisks),
            .ForbiddenPDisks = std::move(forbiddenPDisks),
            .BaseConfig = baseConfig,
            .ConvertToDonor = command.GetConvertToDonor(),
            .IgnoreVSlotQuotaCheck = command.GetIgnoreVSlotQuotaCheck(),
            .LayoutPolicy = NBsController::TGroupLayoutPolicy::FromFlags(command.GetIgnoreGroupLayoutChecks(),
                                                                         !command.GetFromSelfHeal()),
            .AllowUnusableDisks = command.GetAllowUnusableDisks(),
            .SettleOnlyOnOperationalDisks = command.GetSettleOnlyOnOperationalDisks(),
            .IsSelfHealReasonDecommit = command.GetIsSelfHealReasonDecommit(),
            .PreferLessOccupiedRack = command.GetPreferLessOccupiedRack(),
            .WithAttentionToReplication = command.GetWithAttentionToReplication(),
            .UseSelfHealLocalPolicy = command.GetUseSelfHealLocalPolicy(),
            .TryToRelocateBrokenDisksLocallyFirst = command.GetTryToRelocateBrokenDisksLocallyFirst(),
            .BridgePileId = bridgePileId,
            .BridgeProxyGroupId = bridgeProxyGroupId,
            .ApplySelfHealNodeAllowList = command.GetFromSelfHeal(),
            .Reassignments = reassignments,
        };
    }

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

        TActivationContext::Schedule(TDuration::Seconds(30), new IEventHandle(TEvents::TSystem::Wakeup, 0,
            SelfId(), {}, nullptr, VStatusTimeoutCookie));
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
            return;
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
        if (const auto& error = ev->Get()->ErrorReason) {
            // static groups can't be placed without knowing PDisks of BSC, they would get conflicting ones otherwise
            throw TExError() << "Can't get base config: " << *error;
        }
        BaseConfig.emplace(std::move(ev->Get()->BaseConfig));
        if (BridgePileOpPileId) {
            AddBridgePileWithBaseConfig();
        } else {
            CheckReassignGroupDisk();
        }
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

        const TVDiskID vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

        if (SuccessfulVDisks) {
            TBlobStorageGroupInfo::TGroupVDisks failedVDisks(&GroupInfo->GetTopology());
            for (const auto& vslot : BaseConfig->GetVSlot()) {
                if (vslot.GetGroupId() != vdiskId.GroupID.GetRawId() || vslot.GetGroupGeneration() != vdiskId.GroupGeneration) {
                    continue;
                }
                if (!vslot.GetReady()) {
                    const auto groupId = TGroupId::FromProto(&vslot, &NKikimrBlobStorage::TBaseConfig::TVSlot::GetGroupId);
                    const TVDiskID failedVDiskId(groupId, vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
                        vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                    failedVDisks |= {&GroupInfo->GetTopology(), failedVDiskId};
                }
            }
            const auto& checker = GroupInfo->GetQuorumChecker();

            auto check = [&](auto failed, const char *base) {
                const bool wasDegraded = checker.IsDegraded(failed) && checker.CheckFailModelForGroup(failed);
                failed |= {&GroupInfo->GetTopology(), vdiskId};

                if (!checker.CheckFailModelForGroup(failed)) {
                    throw TExError() << "ReassignGroupDisk would render group inoperable (" << base << ')';
                } else if (!cmd.GetIgnoreDegradedGroupsChecks() && !wasDegraded && checker.IsDegraded(failed)) {
                    throw TExError() << "ReassignGroupDisk would drive group into degraded state (" << base << ')';
                }
            };

            check(~SuccessfulVDisks.value(), "polling");
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

        for (const auto& group : ss.GetGroups()) {
            if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                CheckBridgedPartDataRestorable(config, group, vdiskId);

                TDistributedConfigKeeper::TStaticGroupReassignments reassignments;
                try {
                    Self->AllocateStaticGroup(BuildStaticGroupReassignParams(&config, &BaseConfig.value(), cmd, group, ss,
                                                                             &reassignments));
                } catch (const TExConfigError& ex) {
                    YDB_LOG_NOTICE("ReassignGroupDisk failed to allocate group",
                        {"marker", "NWDC76"},
                        {"selfId", SelfId()},
                        {"config", config},
                        {"baseConfig", *BaseConfig},
                        {"error", ex.what()});
                    throw TExError() << "Failed to allocate group: " << ex.what();
                }

                UpdateBridgePileGeneration(config.MutableBlobStorageConfig()->MutableServiceSet(), vdiskId.GroupID);

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

    void TInvokeRequestHandlerActor::CheckBridgedPartDataRestorable(const NKikimrBlobStorage::TStorageConfig& config,
            const NKikimrBlobStorage::TGroupInfo& group, TVDiskID vdiskId) {
        if (!config.HasClusterState()) {
            return;
        }
        const auto& clusterState = config.GetClusterState();
        const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
        std::optional<TBridgePileId> bridgePileId;
        const NKikimrBridge::TGroupState *proxyState = nullptr;
        for (const auto& proxyGroup : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (proxyGroup.HasBridgeGroupState()) {
                NBridge::FindGroupPile(proxyGroup.GetBridgeGroupState(), [&](TBridgePileId id,
                        const NKikimrBridge::TGroupState::TPile& pile) {
                    if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == groupId) {
                        bridgePileId.emplace(id);
                        proxyState = &proxyGroup.GetBridgeGroupState();
                        return true;
                    }
                    return false;
                });
            }
        }
        if (!bridgePileId) {
            return; // not a part of the bridged group
        }
        // the part of the pile taking part in data quorum keeps its sync stage, and the pile its state, whatever
        // happens to its VDisks, so it is trusted with the data it has lost then; the part of the pile out of data
        // quorum gets reset and resynced when the pile gets back
        const auto state = NBridge::GetPileState(clusterState, *bridgePileId);
        if (!NBridge::PileStateTraits(state).RequiresDataQuorum) {
            return;
        }
        // the data lost by the part is lost by the group when there is no other part to resync it from, like in the
        // cluster of a single pile; the usual checks of the request decide then
        if (!NBridge::FindGroupPile(*proxyState, [&](TBridgePileId id, const NKikimrBridge::TGroupState::TPile& part) {
                    return id != *bridgePileId &&
                        NBridge::GetPileState(clusterState, id) == NKikimrBridge::TClusterState::SYNCHRONIZED &&
                        part.GetStage() == NKikimrBridge::TGroupState::SYNCED;
                })) {
            return;
        }

        TStringStream err;
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, &err);
        if (!info) {
            throw TExError() << "Failed to parse group info: " << err.Str();
        }
        const auto& topology = info->GetTopology();
        // the replacing VDisk has no data of its own, even with the donor: the one being replaced may be unreadable;
        // the VDisks not ready may miss data too
        TBlobStorageGroupInfo::TGroupVDisks dataless(&topology, vdiskId);
        Y_ABORT_UNLESS(BaseConfig);
        for (const auto& vslot : BaseConfig->GetVSlot()) {
            if (vslot.GetGroupId() == vdiskId.GroupID.GetRawId() && vslot.GetGroupGeneration() == vdiskId.GroupGeneration &&
                    !vslot.GetReady()) {
                dataless |= {&topology, TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(),
                    vslot.GetVDiskIdx())};
            }
        }
        if (!info->GetQuorumChecker().CheckFailModelForGroup(dataless)) {
            const TString pileName = Self->BridgeInfo && bridgePileId->GetPileIndex() < Self->BridgeInfo->Piles.size()
                ? Self->BridgeInfo->Piles[bridgePileId->GetPileIndex()].Name
                : TString();
            throw TExError() << "GroupId# " << groupId << " is the part of the bridged group in the '" << pileName
                << "' pile being " << NKikimrBridge::TClusterState::EPileState_Name(state)
                << ", the reassignment would leave it unable to restore its data from its pile."
                " Take the pile out and rejoin it to resync the part from another pile";
        }
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
        // the group may have been removed along with the pile it was a part of, then its VDisks are destroyed ones
        const bool groupRemoved = !actualGroupGeneration;
        if (groupRemoved && isDropDonor) {
            throw TExError() << "Group not found";
        }
        if (!groupRemoved && vdiskId.GroupGeneration >= actualGroupGeneration) {
            // A node may have applied the replacement config before the newly elected root has recovered it.
            // Keep the slot intact and let the sender retry after the root catches up.
            throw TExRace() << "Static VDisk report is ahead of current group configuration VDiskId# " << vdiskId
                << " ActualGroupGeneration# " << actualGroupGeneration;
        }

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

                if (groupRemoved && vdisk.GetEntityStatus() != NKikimrBlobStorage::EEntityStatus::DESTROY) {
                    continue; // VDisk of the removed group can only be a destroyed one
                }

                // this must be the obsolete disk (donor has generation from the past, destroyed disks too)
                Y_ABORT_UNLESS(groupRemoved || currentVDiskId.GroupGeneration < actualGroupGeneration);

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
