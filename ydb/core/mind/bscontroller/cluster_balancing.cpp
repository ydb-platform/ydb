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
        EvResume = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    };

    // See TODO about coroutine timeout.
    // const TDuration TIMEOUT = TDuration::Seconds(10);

    struct TEvResume : TEventLocal<TEvResume, EvResume> {};

    struct TPoison {};

    class TClusterBalancingActor : public TActorCoroImpl {
    private:
        using TGroupId = NKikimr::TGroupId;
        using TVSlot = NKikimrBlobStorage::TBaseConfig_TVSlot;

        const TActorId ControllerId;
        const TClusterBalancingSettings Settings;

        struct TStorageInfo {
            std::unordered_set<TGroupId> HealthyGroups;
            std::unordered_map<TPDiskId, ui32> PDiskUsageMap;
            ui32 PDisksWithReplicatingVDisks;
            ui32 ReplicatingVDisks;
        };

        enum class ReassignResult {
            BscIssue,
            FailedToReassign,
            Reassigned,
        };

        TStorageInfo BuildStorageInfo(const NKikimrBlobStorage::TBaseConfig& config) {
            TStorageInfo storageInfo;

            // First, iterate over PDisk and initialize the map.
            for (const auto& pdisk : config.GetPDisk()) {
                TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                storageInfo.PDiskUsageMap[pdiskId] = pdisk.GetNumStaticSlots(); // initialize with static groups
            }

            ui32 replicatingVDisks = 0;
            std::unordered_set<TPDiskId> pdisksWithReplicatingVDisks;

            THashMap<TVSlotId, const TVSlot*> vslotMap;
            for (const auto& vslot : config.GetVSlot()) {
                auto key = TVSlotId(vslot.GetVSlotId());
                vslotMap[key] = &vslot;

                TPDiskId pdiskId = key.ComprisingPDiskId();

                if (!NKikimr::IsDynamicGroup(TGroupId::FromValue(vslot.GetGroupId()))) { // don't count vslots from static groups twice
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
                        pdisksWithReplicatingVDisks.insert(pdiskId);
                        replicatingVDisks++;
                        break;
                }

                auto it = storageInfo.PDiskUsageMap.find(pdiskId);
                if (it == storageInfo.PDiskUsageMap.end()) {
                    continue;
                }
                it->second += 1;
            }

            storageInfo.PDisksWithReplicatingVDisks = pdisksWithReplicatingVDisks.size();
            storageInfo.ReplicatingVDisks = replicatingVDisks;

            for (const auto& group : config.GetGroup()) {
                if (!NKikimr::IsDynamicGroup(TGroupId::FromValue(group.GetGroupId()))) {
                    continue;
                }
                
                bool isHealthy = true;

                for (const auto& vslotId : group.GetVSlotId()) {
                    auto key = TVSlotId(vslotId);
                    auto it = vslotMap.find(key);
                    if (it != vslotMap.end()) {
                        const auto& vslot = it->second;

                        if (!vslot->GetReady()) {
                            isHealthy = false;
                            break;
                        }
                    } else {
                        Y_DEBUG_ABORT_UNLESS(false, "%s", (TStringBuilder() << "VSlotId not found in vslotMap: " << key.ToString()).c_str());
                        isHealthy = false;
                        break;
                    }
                }

                if (isHealthy) {
                    storageInfo.HealthyGroups.insert(TGroupId::FromValue(group.GetGroupId()));
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
                TVSlotId vslotId(vslot->GetVSlotId());
                TPDiskId pdiskId = vslotId.ComprisingPDiskId();
                auto it = storageInfo.PDiskUsageMap.find(pdiskId);
                if (it != storageInfo.PDiskUsageMap.end()) {
                    ui32 usage = it->second;
                    vslotsByPDiskSlotUsage[usage].push_back(vslot);
                }
            }

            std::vector<std::vector<const TVSlot*>> result;
            for (const auto& [_, slots] : vslotsByPDiskSlotUsage) {
                result.push_back(slots);
            }

            return result;
        }

        static void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
            switch (const ui32 type = ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                   throw TPoison();
                default:
                    Y_DEBUG_ABORT_UNLESS(false, "%s", (TStringBuilder() << "unexpected event " << ev->GetTypeName()).c_str());
            }
        }

        THolder<TEvBlobStorage::TEvControllerConfigRequest> CreateQueryConfigRequest() {
            auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            auto& record = ev->Record;
            auto *request = record.MutableRequest();
            request->AddCommand()->MutableQueryBaseConfig();
            return ev;
        }

        THolder<TEvBlobStorage::TEvControllerConfigRequest> CreateReassignRequest(const TVSlot* vslot) {
            auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            auto& record = ev->Record;
            auto *request = record.MutableRequest();

            auto *cmd = request->AddCommand()->MutableReassignGroupDisk();

            cmd->SetGroupId(vslot->GetGroupId());
            cmd->SetGroupGeneration(vslot->GetGroupGeneration());
            cmd->SetFailRealmIdx(vslot->GetFailRealmIdx());
            cmd->SetFailDomainIdx(vslot->GetFailDomainIdx());
            cmd->SetVDiskIdx(vslot->GetVDiskIdx());
            cmd->SetOnlyToLessOccupiedPDisk(true);
            cmd->SetPreferLessOccupiedRack(Settings.PreferLessOccupiedRack);
            cmd->SetWithAttentionToReplication(Settings.WithAttentionToReplication);

            return ev;
        }

        ReassignResult TryReassign(const TVSlot* vslot, const TStorageInfo& storageInfo, const ui64 expectedConfigTxSeqNo) {
            Y_UNUSED(storageInfo);

            TVSlotId vslotId(vslot->GetVSlotId());
            ui32 groupId = vslot->GetGroupId();
            ui32 vdiskIdx = vslot->GetVDiskIdx();

            STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB07, "Trying to move VDisk", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId));

            auto request = CreateReassignRequest(vslot);

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
                if (response.GetStatus(0).GetFailReason() != NKikimrBlobStorage::TConfigResponse::TStatus::kReassignNotViable) {
                    // This is a real BSC error.
                    STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB09, "Failed to move VDisk", (GroupId, groupId), (VDiskIdx, vdiskIdx), (VSlotId, vslotId), (Response, record));
                    return ReassignResult::BscIssue;
                }
                // This means that there was no better PDisk to reassign the VDisk to.
                return ReassignResult::FailedToReassign;
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

        void Yield(TDuration timeout = TDuration::Zero()) {
            auto* event = new TEvResume();

            if (timeout > TDuration::Zero()) {
                Schedule(timeout, event);
            } else {
                Send(SelfActorId, event);
            }

            // TODO: Wait with a deadline set when CoroActor's deadline handling is fixed
            auto ev = WaitForSpecificEvent([](IEventHandle& ev) { return ev.Type == EvResume; }, &ProcessUnexpectedEvent/*, NActors::TMonotonic::Now() + TIMEOUT*/);

            // if (!ev) {
            //     // This would be a WTF alright
            //     return;
            // }
        }

        void RunBalancing() {
            auto request = CreateQueryConfigRequest();
            Send(ControllerId, request.Release());
            // Wait for the response from BSC.
            auto bscResponse = WaitForResponse<TEvBlobStorage::TEvControllerConfigResponse>();

            // See TODO about coroutine timeout.
            // if (!ev) {
            //     STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB12, "Failed to get BSC config response");
            //     return;
            // }

            const auto& configResponse = bscResponse->Get()->Record.GetResponse();
            const auto& config = configResponse.GetStatus(0).GetBaseConfig();

            const auto storageInfo = BuildStorageInfo(config);

            if (storageInfo.PDisksWithReplicatingVDisks > Settings.MaxReplicatingPDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB13, "Skip balancing, too many replicating PDisks", (ReplicatingPDisks, storageInfo.PDisksWithReplicatingVDisks));
                return;
            }

            if (storageInfo.ReplicatingVDisks > Settings.MaxReplicatingVDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB14, "Skip balancing, too many replicating VDisks", (ReplicatingVDisks, storageInfo.ReplicatingVDisks));
                return;
            }

            std::vector<const TVSlot*> candidateVSlots;

            for (const auto& vslot : config.GetVSlot()) {
                if (storageInfo.HealthyGroups.contains(TGroupId::FromValue(vslot.GetGroupId()))) {
                    candidateVSlots.push_back(&vslot);
                }
            }

            auto groupSlotsOrdered = OrderVSlotsByPDiskUsage(candidateVSlots, storageInfo);

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
        TClusterBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings)
        : TActorCoroImpl(/* stackSize */ 64_KB, /* allowUnhandledDtor */ false)
        , ControllerId(controllerId)
        , Settings(settings) {}

        void Run() override {
            try {
                while (true) {
                    RunBalancing();
                    
                    Yield(TDuration::MilliSeconds(Settings.IterationIntervalMs));
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
        if (clusterBalancingSettings.HasPreferLessOccupiedRack()) {
            settings.PreferLessOccupiedRack = clusterBalancingSettings.GetPreferLessOccupiedRack();
        }
        if (clusterBalancingSettings.HasWithAttentionToReplication()) {
            settings.WithAttentionToReplication = clusterBalancingSettings.GetWithAttentionToReplication();
        }

        return settings;
    }

    IActor* CreateClusterBalancingActor(const TActorId& controllerId, const TClusterBalancingSettings& settings) {
        return new TActorCoro(
            MakeHolder<TClusterBalancingActor>(controllerId, settings)
        );
    }

}
