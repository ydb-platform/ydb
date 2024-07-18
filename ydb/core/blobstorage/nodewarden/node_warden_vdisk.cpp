#include "node_warden_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <util/string/split.h>

namespace NKikimr::NStorage {

    void TNodeWarden::DestroyLocalVDisk(TVDiskRecord& vdisk) {
        STLOG(PRI_INFO, BS_NODE, NW35, "DestroyLocalVDisk", (VDiskId, vdisk.GetVDiskId()), (VSlotId, vdisk.GetVSlotId()));
        Y_ABORT_UNLESS(!vdisk.RuntimeData);

        const TVSlotId vslotId = vdisk.GetVSlotId();
        StopAggregator(MakeBlobStorageVDiskID(vslotId.NodeId, vslotId.PDiskId, vslotId.VDiskSlotId));

        if (vdisk.WhiteboardVDiskId) {
            Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateDelete(*vdisk.WhiteboardVDiskId));
            vdisk.WhiteboardVDiskId.reset();
        }
    }

    void TNodeWarden::PoisonLocalVDisk(TVDiskRecord& vdisk) {
        STLOG(PRI_INFO, BS_NODE, NW00, "PoisonLocalVDisk", (VDiskId, vdisk.GetVDiskId()), (VSlotId, vdisk.GetVSlotId()),
            (RuntimeData, vdisk.RuntimeData.has_value()));

        if (vdisk.RuntimeData) {
            vdisk.TIntrusiveListItem<TVDiskRecord, TGroupRelationTag>::Unlink();
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, vdisk.GetVDiskServiceId(), {}, nullptr, 0));
            vdisk.RuntimeData.reset();
        }

        switch (vdisk.ScrubState) {
            case TVDiskRecord::EScrubState::IDLE: // no scrub in progress at all
            case TVDiskRecord::EScrubState::QUERY_START_QUANTUM: // VDisk was waiting for quantum and didn't get it
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED: // quantum finished, no work is in progress
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE: // like QUERY_START_QUANTUM
                break;

            case TVDiskRecord::EScrubState::IN_PROGRESS: { // scrub is in progress, report scrub stop to BS_CONTROLLER
                const TVSlotId vslotId = vdisk.GetVSlotId();
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQuantumFinished>(vslotId.NodeId,
                    vslotId.PDiskId, vslotId.VDiskSlotId));
                break;
            }
        }
        vdisk.ScrubState = TVDiskRecord::EScrubState::IDLE;
        vdisk.QuantumFinished.Clear();
        vdisk.ScrubCookie = 0; // disable reception of Scrub messages from this disk
        vdisk.ScrubCookieForController = 0; // and from controller too
        vdisk.Status = NKikimrBlobStorage::EVDiskStatus::ERROR;
        vdisk.ShutdownPending = true;
        VDiskStatusChanged = true;
    }

    void TNodeWarden::StartLocalVDiskActor(TVDiskRecord& vdisk) {
        const TVSlotId vslotId = vdisk.GetVSlotId();
        const ui64 pdiskGuid = vdisk.Config.GetVDiskLocation().GetPDiskGuid();
        const bool donorMode = vdisk.Config.HasDonorMode();
        const bool readOnly = vdisk.Config.GetReadOnly();
        Y_VERIFY_S(!donorMode || !readOnly, "Only one of modes should be enabled: donorMode " << donorMode << ", readOnly " << readOnly);

        STLOG(PRI_DEBUG, BS_NODE, NW23, "StartLocalVDiskActor", (SlayInFlight, SlayInFlight.contains(vslotId)),
            (VDiskId, vdisk.GetVDiskId()), (VSlotId, vslotId), (PDiskGuid, pdiskGuid), (DonorMode, donorMode));

        if (SlayInFlight.contains(vslotId)) {
            return;
        }

        if (PDiskRestartInFlight.contains(vslotId.PDiskId)) {
            return;
        }

        if (vdisk.ShutdownPending) {
            vdisk.RestartAfterShutdown = true;
            return;
        }

        // find underlying PDisk and determine its media type
        auto pdiskIt = LocalPDisks.find({vslotId.NodeId, vslotId.PDiskId});
        Y_VERIFY_S(pdiskIt != LocalPDisks.end(), "PDiskId# " << vslotId.NodeId << ":" << vslotId.PDiskId << " not found");
        auto& pdisk = pdiskIt->second;
        Y_VERIFY_S(pdisk.Record.GetPDiskGuid() == pdiskGuid, "PDiskId# " << vslotId.NodeId << ":" << vslotId.PDiskId << " PDiskGuid mismatch");
        const NPDisk::EDeviceType deviceType = TPDiskCategory(pdisk.Record.GetPDiskCategory()).Type();

        const TActorId pdiskServiceId = MakeBlobStoragePDiskID(vslotId.NodeId, vslotId.PDiskId);
        const TActorId vdiskServiceId = MakeBlobStorageVDiskID(vslotId.NodeId, vslotId.PDiskId, vslotId.VDiskSlotId);

        // generate correct VDiskId (based on relevant generation of containing group) and groupInfo pointer
        Y_ABORT_UNLESS(!vdisk.RuntimeData);
        TVDiskID vdiskId = vdisk.GetVDiskId();
        TIntrusivePtr<TBlobStorageGroupInfo> groupInfo;

        if (!donorMode) {
            // find group containing VDisk being started
            const auto it = Groups.find(vdisk.GetGroupId());
            if (it == Groups.end()) {
                STLOG_DEBUG_FAIL(BS_NODE, NW09, "group not found while starting VDisk actor",
                    (GroupId, vdisk.GetGroupId()), (VDiskId, vdiskId), (Config, vdisk.Config));
                return;
            }
            auto& group = it->second;

            // ensure the group has correctly filled protobuf (in case when there is no relevant info pointer)
            if (!group.Group) {
                STLOG_DEBUG_FAIL(BS_NODE, NW13, "group configuration does not contain protobuf to start VDisk",
                    (GroupId, it->first), (VDiskId, vdiskId), (Config, vdisk.Config));
                return;
            }

            // obtain group info pointer
            if (group.Info) {
                groupInfo = group.Info;
            } else {
                TStringStream err;
                groupInfo = TBlobStorageGroupInfo::Parse(*group.Group, nullptr, nullptr);
            }

            // check that VDisk belongs to active VDisks of the group
            const ui32 orderNumber = groupInfo->GetOrderNumber(TVDiskIdShort(vdiskId));
            if (groupInfo->GetActorId(orderNumber) != vdiskServiceId) {
                return;
            }

            // generate correct VDisk id
            vdiskId = TVDiskID(groupInfo->GroupID, groupInfo->GroupGeneration, vdiskId);

            // entangle VDisk with the group
            group.VDisksOfGroup.PushBack(&vdisk);
        } else {
            const auto& dm = vdisk.Config.GetDonorMode();
            TBlobStorageGroupType type(static_cast<TBlobStorageGroupType::EErasureSpecies>(dm.GetErasureSpecies()));

            // generate desired group topology
            TBlobStorageGroupInfo::TTopology topology(type, dm.GetNumFailRealms(), dm.GetNumFailDomainsPerFailRealm(),
                dm.GetNumVDisksPerFailDomain());

            // fill in dynamic info
            TBlobStorageGroupInfo::TDynamicInfo dyn(vdiskId.GroupID, vdiskId.GroupGeneration);
            for (ui32 i = dm.GetNumFailRealms() * dm.GetNumFailDomainsPerFailRealm() * dm.GetNumVDisksPerFailDomain(); i; --i) {
                dyn.PushBackActorId(TActorId());
            }

            // create fake group info
            groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(std::move(topology), std::move(dyn),
                vdisk.Config.GetStoragePoolName(), Nothing(), deviceType);
        }

        const auto kind = vdisk.Config.HasVDiskKind() ? vdisk.Config.GetVDiskKind() : NKikimrBlobStorage::TVDiskKind::Default;

        // just some random number of indicate VDisk actor restart
        const ui64 whiteboardInstanceGuid = RandomNumber<ui64>();

        const ui64 scrubCookie = ++LastScrubCookie;

        std::vector<std::pair<TVDiskID, TActorId>> donorDiskIds;
        std::vector<NKikimrBlobStorage::TVSlotId> donors;
        for (const auto& donor : vdisk.Config.GetDonors()) {
            const auto& location = donor.GetVDiskLocation();
            const TVSlotId donorSlot(location);
            donorDiskIds.emplace_back(VDiskIDFromVDiskID(donor.GetVDiskId()), donorSlot.GetVDiskServiceId());
            NKikimrBlobStorage::TVSlotId slotId;
            slotId.SetNodeId(location.GetNodeID());
            slotId.SetPDiskId(location.GetPDiskID());
            slotId.SetVSlotId(location.GetVDiskSlotID());
            donors.emplace_back(slotId);
        }

        TVDiskConfig::TBaseInfo baseInfo(vdiskId, pdiskServiceId, pdiskGuid, vslotId.PDiskId, deviceType,
            vslotId.VDiskSlotId, kind, NextLocalPDiskInitOwnerRound(), groupInfo->GetStoragePoolName(), donorMode,
            donorDiskIds, scrubCookie, whiteboardInstanceGuid, readOnly);

        baseInfo.ReplPDiskReadQuoter = pdiskIt->second.ReplPDiskReadQuoter;
        baseInfo.ReplPDiskWriteQuoter = pdiskIt->second.ReplPDiskWriteQuoter;
        baseInfo.ReplNodeRequestQuoter = ReplNodeRequestQuoter;
        baseInfo.ReplNodeResponseQuoter = ReplNodeResponseQuoter;
        baseInfo.YardInitDelay = VDiskCooldownTimeout;

        TIntrusivePtr<TVDiskConfig> vdiskConfig = Cfg->AllVDiskKinds->MakeVDiskConfig(baseInfo);
        vdiskConfig->EnableVDiskCooldownTimeout = Cfg->EnableVDiskCooldownTimeout;
        vdiskConfig->ReplPausedAtStart = Cfg->VDiskReplPausedAtStart;
        vdiskConfig->EnableVPatch = EnableVPatch;
        vdiskConfig->DefaultHugeGarbagePerMille = DefaultHugeGarbagePerMille;

        vdiskConfig->EnableLocalSyncLogDataCutting = EnableLocalSyncLogDataCutting;
        if (deviceType == NPDisk::EDeviceType::DEVICE_TYPE_ROT) {
            vdiskConfig->EnableSyncLogChunkCompression = EnableSyncLogChunkCompressionHDD;
            vdiskConfig->MaxSyncLogChunksInFlight = MaxSyncLogChunksInFlightHDD;
        } else {
            vdiskConfig->EnableSyncLogChunkCompression = EnableSyncLogChunkCompressionSSD;
            vdiskConfig->MaxSyncLogChunksInFlight = MaxSyncLogChunksInFlightSSD;
        }

        vdiskConfig->CostMetricsParametersByMedia = CostMetricsParametersByMedia;

        vdiskConfig->FeatureFlags = Cfg->FeatureFlags;

        if (StorageConfig.HasBlobStorageConfig() && StorageConfig.GetBlobStorageConfig().HasVDiskPerformanceSettings()) {
            for (auto &type : StorageConfig.GetBlobStorageConfig().GetVDiskPerformanceSettings().GetVDiskTypes()) {
                if (type.HasPDiskType() && deviceType == PDiskTypeToPDiskType(type.GetPDiskType())) {
                    if (type.HasMinHugeBlobSizeInBytes()) {
                        vdiskConfig->MinHugeBlobInBytes = type.GetMinHugeBlobSizeInBytes();
                    }
                }
            }
        }

        // issue initial report to whiteboard before creating actor to avoid races
        Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate(vdiskId, groupInfo->GetStoragePoolName(),
            vslotId.PDiskId, vslotId.VDiskSlotId, pdiskGuid, kind, donorMode, whiteboardInstanceGuid, std::move(donors)));
        vdisk.WhiteboardVDiskId.emplace(vdiskId);
        vdisk.WhiteboardInstanceGuid = whiteboardInstanceGuid;

        // create an actor
        auto *as = TActivationContext::ActorSystem();
        TActorId actorId = as->Register(CreateVDisk(vdiskConfig, groupInfo, AppData()->Counters),
            TMailboxType::Revolving, AppData()->SystemPoolId);
        as->RegisterLocalService(vdiskServiceId, actorId);
        VDiskIdByActor.try_emplace(actorId, vslotId);

        STLOG(PRI_DEBUG, BS_NODE, NW24, "StartLocalVDiskActor done", (VDiskId, vdisk.GetVDiskId()), (VSlotId, vslotId),
            (PDiskGuid, pdiskGuid));

        // for dynamic groups -- start state aggregator
        if (TGroupID(groupInfo->GroupID).ConfigurationType() == EGroupConfigurationType::Dynamic) {
            StartAggregator(vdiskServiceId, groupInfo->GroupID.GetRawId());
        }

        Y_ABORT_UNLESS(vdisk.ScrubState == TVDiskRecord::EScrubState::IDLE);
        Y_ABORT_UNLESS(!vdisk.QuantumFinished.ByteSizeLong());
        Y_ABORT_UNLESS(!vdisk.ScrubCookie);

        vdisk.RuntimeData.emplace(TVDiskRecord::TRuntimeData{
            .GroupInfo = groupInfo,
            .OrderNumber = groupInfo->GetOrderNumber(TVDiskIdShort(vdiskId)),
            .DonorMode = donorMode,
            .ReadOnly = readOnly,
        });

        vdisk.Status = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
        vdisk.ReportedVDiskStatus.reset();
        vdisk.ScrubCookie = scrubCookie;
        VDiskStatusChanged = true;
    }

    void TNodeWarden::HandleGone(STATEFN_SIG) {
        if (const auto it = VDiskIdByActor.find(ev->Sender); it != VDiskIdByActor.end()) {
            if (const auto jt = LocalVDisks.find(it->second); jt != LocalVDisks.end()) {
                TVDiskRecord& vdisk = jt->second;
                Y_ABORT_UNLESS(vdisk.ShutdownPending);
                vdisk.ShutdownPending = false;
                if (std::exchange(vdisk.RestartAfterShutdown, false)) {
                    StartLocalVDiskActor(vdisk);
                }
            }
            VDiskIdByActor.erase(it);
        }
    }

    void TNodeWarden::ApplyServiceSetVDisks(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet) {
        for (const auto& vdisk : serviceSet.GetVDisks()) {
            ApplyLocalVDiskInfo(vdisk);
        }
    }

    void TNodeWarden::ApplyLocalVDiskInfo(const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk& vdisk) {
        // ApplyLocalVDiskInfo invocation may occur only in the following cases:
        //
        // 1. Starting and reading data from cache.
        // 2. Receiving RegisterNode response with comprehensive configuration.
        // 3. Processing GroupReconfigurationWipe command.
        // 4. Deleting VSlot during group reconfiguration or donor termination.
        // 5. Making VDisk a donor one.
        // 6. Updating VDisk generation when modifying group.
        // 7. Putting VDisk into or out of read-only
        //
        // The main idea of this command is when VDisk is created, it does not change its configuration. It may be
        // wiped out several times, it may become a donor or read-only and then it may be destroyed. That is a possible life cycle
        // of a VDisk in the occupied slot.

        if (!vdisk.HasVDiskID() || !vdisk.HasVDiskLocation()) {
            STLOG_DEBUG_FAIL(BS_NODE, NW30, "weird VDisk configuration", (Record, vdisk));
            return;
        }

        const auto& loc = vdisk.GetVDiskLocation();
        if (loc.GetNodeID() != LocalNodeId) {
            if (TGroupID(vdisk.GetVDiskID().GetGroupID()).ConfigurationType() != EGroupConfigurationType::Static) {
                STLOG_DEBUG_FAIL(BS_NODE, NW31, "incorrect NodeId in VDisk configuration", (Record, vdisk), (NodeId, LocalNodeId));
            }
            return;
        }

        const TVSlotId vslotId(loc);
        const auto [it, inserted] = LocalVDisks.try_emplace(vslotId);
        auto& record = it->second;
        if (!inserted) {
            // -- check that configuration did not change
        }
        record.Config.CopyFrom(vdisk);

        if (vdisk.GetDoDestroy() || vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
            if (record.UnderlyingPDiskDestroyed) {
                PoisonLocalVDisk(record);
                SendVDiskReport(vslotId, record.GetVDiskId(), NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED);
                record.UnderlyingPDiskDestroyed = false;
            } else {
                Slay(record);
            }
            DestroyLocalVDisk(record);
            LocalVDisks.erase(it);
            ApplyServiceSetPDisks(); // destroy unneeded PDisk actors
        } else if (vdisk.GetDoWipe()) {
            Slay(record);
        } else if (!record.RuntimeData) {
            StartLocalVDiskActor(record);
        } else if (record.RuntimeData->DonorMode < record.Config.HasDonorMode() || record.RuntimeData->ReadOnly != record.Config.GetReadOnly()) {
            PoisonLocalVDisk(record);
            StartLocalVDiskActor(record);
        }
    }

    void TNodeWarden::Slay(TVDiskRecord& vdisk) {
        const TVSlotId vslotId = vdisk.GetVSlotId();
        STLOG(PRI_INFO, BS_NODE, NW33, "Slay", (VDiskId, vdisk.GetVDiskId()), (VSlotId, vdisk.GetVSlotId()),
            (SlayInFlight, SlayInFlight.contains(vslotId)));
        if (!SlayInFlight.contains(vslotId)) {
            PoisonLocalVDisk(vdisk);
            const TVSlotId vslotId = vdisk.GetVSlotId();
            const TActorId pdiskServiceId = MakeBlobStoragePDiskID(vslotId.NodeId, vslotId.PDiskId);
            const ui64 round = NextLocalPDiskInitOwnerRound();
            Send(pdiskServiceId, new NPDisk::TEvSlay(vdisk.GetVDiskId(), round, vslotId.PDiskId, vslotId.VDiskSlotId));
            SlayInFlight.emplace(vslotId, round);
        }
    }

    void TNodeWarden::Handle(TEvBlobStorage::TEvAskRestartVDisk::TPtr ev) {
        const auto& [pDiskId, vDiskId] = *ev->Get();
        const auto nodeId = SelfId().NodeId();  // Skeleton and NodeWarden are on the same node
        TVSlotId slotId(nodeId, pDiskId, 0);

        for (auto it = LocalVDisks.lower_bound(slotId); it != LocalVDisks.end() && it->first.NodeId == nodeId && it->first.PDiskId == pDiskId; ++it) {
            auto& record = it->second;
            if (record.GetVDiskId() == vDiskId) {
                PoisonLocalVDisk(record);
                StartLocalVDiskActor(record);
                break;
            }
        }
    }

    void TNodeWarden::Handle(TEvBlobStorage::TEvDropDonor::TPtr ev) {
        auto *msg = ev->Get();
        const TVSlotId vslotId(msg->NodeId, msg->PDiskId, msg->VSlotId);
        STLOG(PRI_INFO, BS_NODE, NW34, "TEvDropDonor", (VSlotId, vslotId), (VDiskId, msg->VDiskId));
        SendDropDonorQuery(msg->NodeId, msg->PDiskId, msg->VSlotId, msg->VDiskId);

        if (const auto it = LocalVDisks.find(vslotId); it != LocalVDisks.end()) {
            const auto& vdisk = it->second;
            if (vdisk.WhiteboardVDiskId) {
                NKikimrBlobStorage::TVSlotId id;
                vslotId.Serialize(&id);
                Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvVDiskDropDonors(*vdisk.WhiteboardVDiskId,
                    vdisk.WhiteboardInstanceGuid, {id}));
            }
        }
    }

    void TNodeWarden::UpdateGroupInfoForDisk(TVDiskRecord& vdisk, const TIntrusivePtr<TBlobStorageGroupInfo>& newInfo) {
        if (!vdisk.RuntimeData) {
            return;
        }
        if (newInfo->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE) {
            return; // group is decomitted, VDisks will be deleted soon
        }

        TIntrusivePtr<TBlobStorageGroupInfo>& currentInfo = vdisk.RuntimeData->GroupInfo;
        Y_ABORT_UNLESS(newInfo->GroupID == currentInfo->GroupID);

        const ui32 orderNumber = vdisk.RuntimeData->OrderNumber;
        const TActorId vdiskServiceId = vdisk.GetVDiskServiceId();

        if (newInfo->GetActorId(orderNumber) != vdiskServiceId) {
            // this disk is in donor mode, we don't care about generation change; donor modes are operated by BSC solely
            return;
        }

        // update generation and send update message
        currentInfo = newInfo;
        const TVDiskID newVDiskId = currentInfo->GetVDiskId(orderNumber);
        vdisk.WhiteboardVDiskId.emplace(newVDiskId);
        Send(vdiskServiceId, new TEvVGenerationChange(newVDiskId, currentInfo));
    }

} // NKikimr::NStorage
