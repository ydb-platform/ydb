#include "cluster_balancing.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/sys_view/common/events.h>

namespace NKikimr::NBsController {

    enum {
        EvBscResponse = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvResume,
    };

    // See TODO about coroutine timeout.
    // const TDuration TIMEOUT = TDuration::Seconds(10);

    struct TEvResume : TEventLocal<TEvResume, EvResume> {};

    struct TPoison {};

    struct TBscResponse : TEventLocal<TBscResponse, EvBscResponse> {

        NKikimrSysView::TEvGetVSlotsResponse VSlotsResponse;
        NKikimrBlobStorage::TEvControllerConfigResponse ConfigResponse;
        bool Success;

        TBscResponse(NKikimrSysView::TEvGetVSlotsResponse&& vSlotsResponse,
                       NKikimrBlobStorage::TEvControllerConfigResponse&& configResponse)
            : VSlotsResponse(std::move(vSlotsResponse))
            , ConfigResponse(std::move(configResponse))
            , Success(true) {}

        TBscResponse() : Success(false) {}
    };

    class TBscRequestActor : public TActorBootstrapped<TBscRequestActor> {
        ui64 NextConfigCookie = 1;
        const TActorId ControllerId;
        ui32 Requests = 0;
        TActorId ParentActorId;

        std::optional<NKikimrSysView::TEvGetVSlotsResponse> VSlotsResponse;
        std::optional<NKikimrBlobStorage::TEvControllerConfigResponse> ConfigResponse;

    public:
        TBscRequestActor(const TActorId& ControllerId) : ControllerId(ControllerId) {}

        void Bootstrap(const TActorId parentId) {
            Become(&TThis::StateFunc);
            ParentActorId = parentId;

            {
                Requests++;
                auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
                request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
            
                Send(ControllerId, request.release(), 0, NextConfigCookie++);
            }

            {
                Requests++;
                auto request = std::make_unique<NSysView::TEvSysView::TEvGetVSlotsRequest>();
                Send(ControllerId, request.release(), 0, NextConfigCookie++);
            }
        }

        void ReplyUnsuccessfullAndPassAway() {
            Send(ParentActorId, new TBscResponse());
            PassAway();
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB01, "TBscRequestActor TEvUndelivered", (Sender, ev->Sender));
            ReplyUnsuccessfullAndPassAway();
        }

        void CheckDone() {
            Requests--;
            if (Requests == 0) {
                Send(ParentActorId, new TBscResponse(std::move(*VSlotsResponse), std::move(*ConfigResponse)));
                PassAway();
            }
        }

        void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            const auto& response = record.GetResponse();
            
            if (!response.GetSuccess() || !response.StatusSize() || !response.GetStatus(0).GetSuccess()) {
                STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB02, "Failed to get BSC config", (Response, record));
                ReplyUnsuccessfullAndPassAway();
                return;
            } else {
                ConfigResponse = std::move(ev->Get()->Record);
                CheckDone();
            }
        }

        void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
            VSlotsResponse = std::move(ev->Get()->Record);
            CheckDone();
        }

        STFUNC(StateFunc) {
            STRICT_STFUNC_BODY(
                hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
                hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);

                hFunc(TEvents::TEvUndelivered, Handle);
            )
        }
    };

    class TBalancingActor : public TActorCoroImpl {
    private:
        using TGroupId = ui32;
        using TVSlot = NKikimrBlobStorage::TBaseConfig_TVSlot;

        const TActorId ControllerId;
        const TClusterBalancingSettings Settings;

        struct TStorageInfo {
            std::unordered_set<TGroupId> HealthyGroups;
            std::unordered_map<TPDiskId, ui32> PDiskUsageMap;
            ui32 ReplicatingPDisks;
            ui32 ReplicatingVDisks;
        };

        enum class ReassignCheckResult {
            BscIssue,
            ReassignNotViable,
            CanReassign,
        };

        enum class ReassignResult {
            BscIssue,
            FailedToReassign,
            Reassigned,
        };

        bool IsDynamicGroup(TGroupId groupId) {
            return groupId & 0x80000000;
        }

        TStorageInfo BuildStorageInfo(const NKikimrBlobStorage::TBaseConfig& config, const NKikimrSysView::TEvGetVSlotsResponse& vSlotsResponse) {
            TStorageInfo storageInfo;

            // First, iterate over PDisk and initialize the map.
            for (const auto& pdisk : config.pdisk()) {
                TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                storageInfo.PDiskUsageMap[pdiskId] = pdisk.GetNumStaticSlots(); // initialize with static groups
            }

            ui32 replicatingVDisks = 0;
            std::unordered_set<TPDiskId> replicatingPDisks;

            THashMap<TVSlotId, const TVSlot*> vslotMap;
            for (const auto& vslot : config.vslot()) {
                auto key = TVSlotId(vslot.GetVSlotId());
                vslotMap[key] = &vslot;

                if (!IsDynamicGroup(vslot.GetGroupId())) { // don't count vslots from static groups twice
                    continue;
                }

                const auto& statusStr = vslot.GetStatus();
                NKikimrBlobStorage::EVDiskStatus status;
                NKikimrBlobStorage::EVDiskStatus_Parse(statusStr, &status);

                switch (status) {
                    case NKikimrBlobStorage::ERROR:
                    case NKikimrBlobStorage::READY:
                        break;
                    case NKikimrBlobStorage::INIT_PENDING:
                    case NKikimrBlobStorage::REPLICATING:
                        replicatingPDisks.insert(TPDiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId()));
                        replicatingVDisks++;
                        break;
                }

                TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
                auto it = storageInfo.PDiskUsageMap.find(pdiskId);
                if (it == storageInfo.PDiskUsageMap.end()) {
                    continue;
                }
                it->second += 1;
            }

            storageInfo.ReplicatingPDisks = replicatingPDisks.size();
            storageInfo.ReplicatingVDisks = replicatingVDisks;

            std::unordered_map<TGroupId, ui32> goodSlotsByGroup;
            for (const auto& vslotEntry : vSlotsResponse.entries()) {
                const auto& info = vslotEntry.info();

                NKikimrWhiteboard::EVDiskState state;
                NKikimrWhiteboard::EVDiskState_Parse(info.state(), &state);

                if (info.replicated() && state == NKikimrWhiteboard::EVDiskState::OK) {
                    goodSlotsByGroup[info.GetGroupId()]++;
                }
            }

            for (const auto& group : config.group()) {                
                if (!IsDynamicGroup(group.GetGroupId())) {
                    continue;
                }
                
                bool isHealthy = true;
                ui32 slotCount = 0;

                for (const auto& vslotId : group.GetVSlotId()) {
                    auto key = TVSlotId(vslotId);
                    auto it = vslotMap.find(key);
                    if (it != vslotMap.end()) {
                        const auto& vslot = it->second;
                        const auto& statusStr = vslot->GetStatus();
                        NKikimrBlobStorage::EVDiskStatus status;
                        NKikimrBlobStorage::EVDiskStatus_Parse(statusStr, &status);

                        slotCount++;

                        if (!vslot->GetReady() || status != NKikimrBlobStorage::EVDiskStatus::READY) {
                            isHealthy = false;
                            break;
                        }
                    }
                }

                if (isHealthy && goodSlotsByGroup[group.GetGroupId()] == slotCount) {
                    storageInfo.HealthyGroups.insert(group.GetGroupId());
                }
            }

            return storageInfo;
        }

        std::vector<std::vector<const TVSlot*>> OrderVSlotsByPDiskUsage(
            const std::vector<const TVSlot*>& vslots,
            const TStorageInfo& storageInfo
        ) {
            std::map<ui32, std::vector<const TVSlot*>, std::greater<>> vslotsByPDiskSlotUsage;

            for (const auto* vslot : vslots) {
                TPDiskId pdiskId(vslot->GetVSlotId().GetNodeId(), vslot->GetVSlotId().GetPDiskId());
                auto it = storageInfo.PDiskUsageMap.find(pdiskId);
                if (it != storageInfo.PDiskUsageMap.end()) {
                    ui32 usage = it->second;
                    vslotsByPDiskSlotUsage[usage].push_back(vslot);
                }
            }

            std::vector<std::vector<const TVSlot*>> result;
            for (const auto& [usage, group] : vslotsByPDiskSlotUsage) {
                result.push_back(group);
            }

            return result;
        }

        static void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
            switch (const ui32 type = ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                   throw TPoison();
                default:
                    Y_DEBUG_ABORT_S("unexpected event " << ev->GetTypeName());
            }
        }

        THolder<TEvBlobStorage::TEvControllerConfigRequest> CreateReassignRequest(const TVSlot* vslot, const bool rollback) {
            auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            auto& record = ev->Record;
            auto *request = record.MutableRequest();

            auto *cmd = request->AddCommand()->MutableReassignGroupDisk();

            cmd->SetGroupId(vslot->GetGroupId());
            cmd->SetGroupGeneration(vslot->GetGroupGeneration());
            cmd->SetFailRealmIdx(vslot->GetFailRealmIdx());
            cmd->SetFailDomainIdx(vslot->GetFailDomainIdx());
            cmd->SetVDiskIdx(vslot->GetVDiskIdx());
            request->SetRollback(rollback);

            return ev;
        }

        ReassignCheckResult CheckCanReassign(const TVSlot* vslot, const TStorageInfo& storageInfo, const ui64 expectedConfigTxSeqNo) {
            TVSlotId vslotId(vslot->GetVSlotId());
            ui32 groupId = vslot->GetGroupId();
            ui32 vdiskIdx = vslot->GetVDiskIdx();

            auto request = CreateReassignRequest(vslot, true);

            Send(ControllerId, request.Release());

            auto ev = WaitForResponse<TEvBlobStorage::TEvControllerConfigResponse>();

            // See TODO about coroutine timeout.
            // if (!ev) {
            //     STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB03, "Failed to get response for reassign check", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId));
            //     return ReassignCheckResult::BscIssue;
            // }

            const auto& record = ev->Get()->Record;
            const auto& response = record.GetResponse();

            if (!response.StatusSize() || !response.GetStatus(0).GetSuccess()) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB04, "Failed to find where to move VDisk", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (Record, record));
                return ReassignCheckResult::BscIssue;
            }

            const auto& status = response.GetStatus(0);
            
            ui64 actualConfigTxSeqNo = response.GetConfigTxSeqNo();
            if (expectedConfigTxSeqNo != actualConfigTxSeqNo) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB05, "Can't proceed, BS config might have changed", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (ExpectedConfigTxSeqNo, expectedConfigTxSeqNo), (ActualConfigTxSeqNo, actualConfigTxSeqNo));
                return ReassignCheckResult::BscIssue;
            }

            const auto& reassigned = status.GetReassignedItem(0);

            TPDiskId pdiskFrom(reassigned.GetFrom().GetNodeId(), reassigned.GetFrom().GetPDiskId());
            TPDiskId pdiskTo(reassigned.GetTo().GetNodeId(), reassigned.GetTo().GetPDiskId());

            const auto itFrom = storageInfo.PDiskUsageMap.find(pdiskFrom);
            ui32 usageFrom = (itFrom != storageInfo.PDiskUsageMap.end()) ? itFrom->second : 0;

            const auto itTo = storageInfo.PDiskUsageMap.find(pdiskTo);
            ui32 usageTo = (itTo != storageInfo.PDiskUsageMap.end()) ? itTo->second : 0;

            if (usageTo + 1 > usageFrom - 1) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB06, "Moving VDisk is not viable", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (PDiskFrom, pdiskFrom), (usageFrom, usageFrom), (PDiskTo, pdiskTo), (usageTo, usageTo));
                return ReassignCheckResult::ReassignNotViable;
            }

            return ReassignCheckResult::CanReassign;
        }

        ReassignResult TryReassign(const TVSlot* vslot, const TStorageInfo& storageInfo, const ui64 expectedConfigTxSeqNo) {
            TVSlotId vslotId(vslot->GetVSlotId());
            ui32 groupId = vslot->GetGroupId();
            ui32 vdiskIdx = vslot->GetVDiskIdx();

            STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB07, "Trying to move VDisk", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId));

            switch (CheckCanReassign(vslot, storageInfo, expectedConfigTxSeqNo)) {
                case ReassignCheckResult::BscIssue:
                    return ReassignResult::BscIssue;

                case ReassignCheckResult::ReassignNotViable:
                    return ReassignResult::FailedToReassign;

                case ReassignCheckResult::CanReassign:
                    break;
            }

            auto request = CreateReassignRequest(vslot, false);

            Send(ControllerId, request.Release());

            auto ev = WaitForResponse<TEvBlobStorage::TEvControllerConfigResponse>();

            // See TODO about coroutine timeout.
            // if (!ev) {
            //     STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB08, "Failed to get response for reassign", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId));
            //     return ReassignResult::BscIssue;
            // }

            const auto& record = ev->Get()->Record;
            const auto& response = record.GetResponse();
            if (!response.GetSuccess() || !response.StatusSize() || !response.GetStatus(0).GetSuccess()) {
                STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB09, "Failed to move VDisk", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (Response, record));
                return ReassignResult::BscIssue;
            }

            ui64 actualConfigTxSeqNo = response.GetConfigTxSeqNo();
            ui32 newConfigTxSeqNo = expectedConfigTxSeqNo + 1;
            // TODO: Add expectedConfigTxSeqNo as a parameter for ReassignRequest, so that BSC only proceed if
            // expectedConfigTxSeqNo is equal to NextConfigTxSeqNo.
            if (newConfigTxSeqNo != actualConfigTxSeqNo) {
                STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB10, "BS config might have changed during balancing iteration",
                    (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (ExpectedConfigTxSeqNo, newConfigTxSeqNo), (ActualConfigTxSeqNo, actualConfigTxSeqNo));
            }

            const auto& status = response.GetStatus(0);
            const auto& reassigned = status.GetReassignedItem(0);

            TPDiskId pdiskFrom(reassigned.GetFrom().GetNodeId(), reassigned.GetFrom().GetPDiskId());
            TPDiskId pdiskTo(reassigned.GetTo().GetNodeId(), reassigned.GetTo().GetPDiskId());

            STLOG(PRI_INFO, BS_CLUSTER_BALANCING, BSCB11, "Moving VDisk succeeded", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (PDiskTo, pdiskTo), (PDiskFrom, pdiskFrom));

            return ReassignResult::Reassigned;
        }

        template <typename TEventType>
        THolder<typename TEventType::THandle> WaitForResponse() {
            // TODO: Wait with deadline when CoroActor's deadline is fixed
            // For now this doesn't return nullptr, hence this function doesn't return an empty holder.
            return WaitForSpecificEvent<TEventType>(&ProcessUnexpectedEvent/*, NActors::TMonotonic::Now() + TIMEOUT*/);
        }

        void Yield(ui64 timeoutMs = 0) {
            auto* event = new TEvResume();

            if (timeoutMs > 0) {
                Schedule(TDuration::MilliSeconds(timeoutMs), event);
            } else {
                Send(SelfActorId, event);
            }

            // TODO: Wait with deadline when CoroActor's deadline is fixed
            auto ev = WaitForSpecificEvent([](IEventHandle& ev) { return ev.Type == EvResume; }, &ProcessUnexpectedEvent/*, NActors::TMonotonic::Now() + TIMEOUT*/);

            // if (!ev) {
            //     // This would be a WTF alright
            //     return;
            // }
        }

        void RunBalancing() {
            RegisterWithSameMailbox(new TBscRequestActor(ControllerId));
            auto ev = WaitForResponse<TBscResponse>();

            // See TODO about coroutine timeout.
            // if (!ev) {
            //     STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB12, "Failed to get BSC response");
            //     return;
            // }

            const TBscResponse* bscResponse = ev->Get();

            if (!bscResponse->Success) {
                return;
            }

            const auto& configResponse = bscResponse->ConfigResponse.GetResponse();
            const auto& config = configResponse.GetStatus(0).GetBaseConfig();

            const auto storageInfo = BuildStorageInfo(config, bscResponse->VSlotsResponse);

            if (storageInfo.ReplicatingPDisks > Settings.MaxReplicatingPDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB13, "Skip balancing, too many replicating PDisks", (ReplicatingPDisks, storageInfo.ReplicatingPDisks));
                return;
            }

            if (storageInfo.ReplicatingVDisks > Settings.MaxReplicatingVDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB14, "Skip balancing, too many replicating VDisks", (ReplicatingVDisks, storageInfo.ReplicatingVDisks));
                return;
            }

            std::vector<const TVSlot*> candidateVSlots;

            for (const auto& vslot : config.vslot()) {
                if (storageInfo.HealthyGroups.contains(vslot.GetGroupId())) {
                    candidateVSlots.push_back(&vslot);
                }
            }

            auto groupSlotsOrdered = OrderVSlotsByPDiskUsage(candidateVSlots, storageInfo);

            if (groupSlotsOrdered.empty()) {
                // No groups to balance.
                return;
            }

            // Reading the config also increments the config transaction sequence number.
            // We need to increment it again to get the next one. Reassignment check 
            // and actual reassignment will use this sequence number. 
            // Reassignment check doesn't increment the sequence number because this transaction
            // rolls back.
            ui64 expectedConfigTxSeqNo = configResponse.GetConfigTxSeqNo();

            for (auto& groupSlots : groupSlotsOrdered) {
                std::random_shuffle(groupSlots.begin(), groupSlots.end());
                for (const auto& vslot : groupSlots) {
                    switch (TryReassign(vslot, storageInfo, expectedConfigTxSeqNo)) {
                        case ReassignResult::FailedToReassign:
                            // Skip this VDisk, try next one.
                            break;
                        case ReassignResult::BscIssue:
                        case ReassignResult::Reassigned:
                            // Move to the next balancing iteration.
                            return;
                    }
                    Yield();
                }
            }
        }

    public:
        TBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings)
        : TActorCoroImpl(/* stackSize */ 640_KB, /* allowUnhandledDtor */ false)
        , ControllerId(controllerId)
        , Settings(settings) {}

        void Run() override {
            try {
                while (true) {
                    RunBalancing();
                    
                    Yield(Settings.IterationIntervalMs);
                }
            } catch (const TDtorException&) {
                return; // actor system terminated
            } catch (const TPoison&) {
                return; // coroutine actor terminated
            } catch (...) {
                Y_DEBUG_ABORT("unhandled exception");
            }
        }
    };

    TClusterBalancingSettings ParseClusterBalancingSettings(const NKikimrBlobStorage::TStorageConfig& storageConfig) {
        TClusterBalancingSettings settings;

        if (!storageConfig.HasBlobStorageConfig()) {
            return settings;
        }

        const auto& bsConfig = storageConfig.GetBlobStorageConfig();
        
        if (!bsConfig.HasBscSettings()) {
            return settings;
        }

        const auto& bscSettings = bsConfig.GetBscSettings();

        if (!bscSettings.HasClusterBalancingSettings()) {
            return settings;
        }

        const auto& clusterBalancingSettings = bscSettings.GetClusterBalancingSettings();

        if (clusterBalancingSettings.HasEnable()) {
            settings.Enable = clusterBalancingSettings.GetEnable();   
        }
        if (clusterBalancingSettings.HasIterationIntervalMs()) {
            settings.IterationIntervalMs = clusterBalancingSettings.GetIterationIntervalMs();
        }
        if (clusterBalancingSettings.HasMaxReplicatingPDisks()) {
            settings.MaxReplicatingPDisks = clusterBalancingSettings.GetMaxReplicatingPDisks();
        }
        if (clusterBalancingSettings.HasMaxReplicatingVDisks()) {
            settings.MaxReplicatingVDisks = clusterBalancingSettings.GetMaxReplicatingVDisks();
        }

        return settings;
    }

    IActor* CreateClusterBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings) {
        return new TActorCoro(
            MakeHolder<TBalancingActor>(controllerId, settings)
        );
    }

}
