#include "cluster_balancing.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/sys_view/common/events.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

        struct TPDiskUsage {
            ui32 NumSlots = 0;
            ui32 MaxSlots = 1;
        };

        struct TPoolKey {
            ui64 BoxId = 0;
            ui64 StoragePoolId = 0;

            friend bool operator <(const TPoolKey& left, const TPoolKey& right) {
                return std::tie(left.BoxId, left.StoragePoolId) < std::tie(right.BoxId, right.StoragePoolId);
            }
        };

        struct TStorageInfo {
            std::unordered_set<TGroupId> MovableGroups;
            std::unordered_map<TGroupId, TPoolKey> GroupPoolKeys;
            std::unordered_map<TPDiskId, TPDiskUsage> PDiskUsageMap;
            std::map<TPoolKey, TPDiskUsage> BestTargetUsageAfterMoveByPool;
            ui32 PDisksWithReplicatingVDisks;
            ui32 ReplicatingVDisks;
        };

        struct TPoolUsageSummary {
            TPDiskUsage BestTargetUsageAfterMove;
            TPDiskUsage MaxSourceUsage;
            bool HasBestTargetUsageAfterMove = false;
            bool HasMaxSourceUsage = false;
        };

        enum class ReassignResult {
            BscIssue,
            FailedToReassign,
            Reassigned,
        };

        // Compare PDisks by their occupied-slot ratio without rounding. This keeps
        // balancing fair when PDisks in the same pool have different expected sizes.
        static int CompareUsage(const TPDiskUsage& left, const TPDiskUsage& right) {
            const ui64 leftProduct = ui64(left.NumSlots) * right.MaxSlots;
            const ui64 rightProduct = ui64(right.NumSlots) * left.MaxSlots;
            if (leftProduct < rightProduct) {
                return -1;
            } else if (leftProduct > rightProduct) {
                return 1;
            }
            return 0;
        }

        static bool IsUsageLess(const TPDiskUsage& left, const TPDiskUsage& right) {
            return CompareUsage(left, right) < 0;
        }

        static bool CanImproveByMovingFrom(const TPDiskUsage& source, const TPDiskUsage& bestTargetUsageAfterMove) {
            return source.NumSlots && CompareUsage(source, bestTargetUsageAfterMove) > 0;
        }

        struct TPDiskUsageGreater {
            bool operator()(const TPDiskUsage& left, const TPDiskUsage& right) const {
                return CompareUsage(left, right) > 0;
            }
        };

        static bool MatchTriStateBool(NKikimrBlobStorage::ETriStateBool actual, bool expected) {
            return expected
                ? actual == NKikimrBlobStorage::ETriStateBool::kTrue
                : actual == NKikimrBlobStorage::ETriStateBool::kFalse;
        }

        static bool MatchPDiskFilter(
            const NKikimrBlobStorage::TPDiskFilter& filter,
            const NKikimrBlobStorage::TBaseConfig_TPDisk& pdisk
        ) {
            for (const auto& property : filter.GetProperty()) {
                switch (property.GetPropertyCase()) {
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kType:
                        if (property.GetType() != pdisk.GetType()) {
                            return false;
                        }
                        break;
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kSharedWithOs:
                        if (!MatchTriStateBool(pdisk.GetSharedWithOs(), property.GetSharedWithOs())) {
                            return false;
                        }
                        break;
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kReadCentric:
                        if (!MatchTriStateBool(pdisk.GetReadCentric(), property.GetReadCentric())) {
                            return false;
                        }
                        break;
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::kKind:
                        if (property.GetKind() != pdisk.GetKind()) {
                            return false;
                        }
                        break;
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::PROPERTY_NOT_SET:
                        return false;
                }
            }

            return true;
        }

        static bool MatchStoragePool(
            const NKikimrBlobStorage::TDefineStoragePool& pool,
            const NKikimrBlobStorage::TBaseConfig_TPDisk& pdisk
        ) {
            if (!pool.HasBoxId() || pool.GetBoxId() != pdisk.GetBoxId()) {
                return false;
            }

            for (const auto& filter : pool.GetPDiskFilter()) {
                if (MatchPDiskFilter(filter, pdisk)) {
                    return true;
                }
            }

            return false;
        }

        std::map<TPoolKey, TPDiskUsage> BuildImprovablePoolTargets(
            const NKikimrBlobStorage::TBaseConfig& config,
            const NKikimrBlobStorage::TConfigResponse::TStatus& storagePoolsStatus,
            const TStorageInfo& storageInfo
        ) {
            std::map<TPoolKey, TPoolUsageSummary> poolUsageSummaries;

            std::unordered_map<ui64, std::vector<const NKikimrBlobStorage::TDefineStoragePool*>> storagePoolsByBox;
            for (const auto& pool : storagePoolsStatus.GetStoragePool()) {
                if (!pool.HasBoxId()) {
                    continue;
                }
                storagePoolsByBox[pool.GetBoxId()].push_back(&pool);
            }

            for (const auto& pdisk : config.GetPDisk()) {
                const auto poolsIt = storagePoolsByBox.find(pdisk.GetBoxId());
                if (poolsIt == storagePoolsByBox.end()) {
                    continue;
                }

                const TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                const auto usageIt = storageInfo.PDiskUsageMap.find(pdiskId);
                if (usageIt == storageInfo.PDiskUsageMap.end()) {
                    continue;
                }

                const TPDiskUsage& usage = usageIt->second;

                for (const auto* pool : poolsIt->second) {
                    if (!MatchStoragePool(*pool, pdisk)) {
                        continue;
                    }

                    const TPoolKey poolKey{
                        .BoxId = pool->GetBoxId(),
                        .StoragePoolId = pool->GetStoragePoolId(),
                    };

                    TPoolUsageSummary& summary = poolUsageSummaries[poolKey];
                    if (usage.NumSlots && (!summary.HasMaxSourceUsage || IsUsageLess(summary.MaxSourceUsage, usage))) {
                        summary.MaxSourceUsage = usage;
                        summary.HasMaxSourceUsage = true;
                    }
                    if (usage.NumSlots < usage.MaxSlots) {
                        // Targets are evaluated in the state they would have after
                        // accepting one VDisk; otherwise a move to an almost-full
                        // PDisk could look better than it actually is.
                        const TPDiskUsage usageAfterMove{
                            .NumSlots = usage.NumSlots + 1,
                            .MaxSlots = usage.MaxSlots,
                        };
                        if (!summary.HasBestTargetUsageAfterMove || IsUsageLess(usageAfterMove, summary.BestTargetUsageAfterMove)) {
                            summary.BestTargetUsageAfterMove = usageAfterMove;
                            summary.HasBestTargetUsageAfterMove = true;
                        }
                    }
                }
            }

            std::map<TPoolKey, TPDiskUsage> bestTargetUsageAfterMoveByPool;
            for (const auto& [poolKey, summary] : poolUsageSummaries) {
                // A pool is worth scanning only when its most loaded source PDisk
                // would still be worse than the best target after that target
                // receives the moved VDisk.
                if (summary.HasBestTargetUsageAfterMove && summary.HasMaxSourceUsage &&
                        CanImproveByMovingFrom(summary.MaxSourceUsage, summary.BestTargetUsageAfterMove)) {
                    bestTargetUsageAfterMoveByPool.emplace(poolKey, summary.BestTargetUsageAfterMove);
                }
            }

            return bestTargetUsageAfterMoveByPool;
        }

        TStorageInfo BuildStorageInfo(
            const NKikimrBlobStorage::TBaseConfig& config,
            const NKikimrBlobStorage::TConfigResponse::TStatus& storagePoolsStatus
        ) {
            TStorageInfo storageInfo;

            // First, iterate over PDisk and initialize the map.
            for (const auto& pdisk : config.GetPDisk()) {
                TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
                storageInfo.PDiskUsageMap[pdiskId] = {
                    .NumSlots = pdisk.GetNumStaticSlots(), // initialize with static groups
                    .MaxSlots = std::max<ui32>(pdisk.GetExpectedSlotCount(), 1),
                };
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
                it->second.NumSlots += 1;
            }

            storageInfo.PDisksWithReplicatingVDisks = pdisksWithReplicatingVDisks.size();
            storageInfo.ReplicatingVDisks = replicatingVDisks;
            storageInfo.BestTargetUsageAfterMoveByPool = BuildImprovablePoolTargets(config, storagePoolsStatus, storageInfo);

            for (const auto& group : config.GetGroup()) {
                if (!NKikimr::IsDynamicGroup(TGroupId::FromValue(group.GetGroupId()))) {
                    continue;
                }

                const TGroupId groupId = TGroupId::FromValue(group.GetGroupId());
                const TPoolKey poolKey{
                    .BoxId = group.GetBoxId(),
                    .StoragePoolId = group.GetStoragePoolId(),
                };

                // Healthy groups from non-improvable pools are skipped to avoid
                // issuing reassign checks that can only churn data inside the pool.
                if (storageInfo.BestTargetUsageAfterMoveByPool.find(poolKey) == storageInfo.BestTargetUsageAfterMoveByPool.end()) {
                    continue;
                }

                storageInfo.GroupPoolKeys.emplace(groupId, poolKey);

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
                        Y_DEBUG_ABORT_S("VSlotId not found in vslotMap: " << key.ToString());
                        isHealthy = false;
                        break;
                    }
                }

                if (isHealthy) {
                    storageInfo.MovableGroups.insert(groupId);
                }
            }

            return storageInfo;
        }

        std::vector<std::vector<const TVSlot*>> OrderVSlotsByPDiskUsage(
            const std::vector<const TVSlot*>& vslots,
            const TStorageInfo& storageInfo
        ) {
            // Buckets are source-PDisk usage levels. The cap on failed reassigns is
            // applied per bucket, so a bad top bucket should not block lower buckets.
            std::map<TPDiskUsage, std::vector<const TVSlot*>, TPDiskUsageGreater> vslotsByPDiskSlotUsage;

            for (const auto* vslot : vslots) {
                const TGroupId groupId = TGroupId::FromValue(vslot->GetGroupId());
                const auto groupPoolIt = storageInfo.GroupPoolKeys.find(groupId);
                if (groupPoolIt == storageInfo.GroupPoolKeys.end()) {
                    continue;
                }

                const auto bestTargetUsageIt = storageInfo.BestTargetUsageAfterMoveByPool.find(groupPoolIt->second);
                if (bestTargetUsageIt == storageInfo.BestTargetUsageAfterMoveByPool.end()) {
                    continue;
                }

                TVSlotId vslotId(vslot->GetVSlotId());
                TPDiskId pdiskId = vslotId.ComprisingPDiskId();
                auto it = storageInfo.PDiskUsageMap.find(pdiskId);
                if (it != storageInfo.PDiskUsageMap.end() && CanImproveByMovingFrom(it->second, bestTargetUsageIt->second)) {
                    vslotsByPDiskSlotUsage[it->second].push_back(vslot);
                }
            }

            std::vector<std::vector<const TVSlot*>> result;
            for (const auto& [_, slots] : vslotsByPDiskSlotUsage) {
                result.push_back(slots);
            }

            return result;
        }

        static void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                   throw TPoison();
                default:
                    Y_DEBUG_ABORT_S("unexpected event " << ev->GetTypeName());
            }
        }

        THolder<TEvBlobStorage::TEvControllerConfigRequest> CreateQueryConfigRequest() {
            auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
            auto& record = ev->Record;
            auto *request = record.MutableRequest();
            request->AddCommand()->MutableQueryBaseConfig();
            // Read all pools; pool filters are needed to decide whether a PDisk can
            // actually be a balancing target for a group's storage pool.
            request->AddCommand()->MutableReadStoragePool()->SetBoxId(std::numeric_limits<ui64>::max());
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
            if (configResponse.StatusSize() < 2 || !configResponse.GetStatus(0).GetSuccess() || !configResponse.GetStatus(1).GetSuccess()) {
                STLOG(PRI_WARN, BS_CLUSTER_BALANCING, BSCB17, "Failed to read base config or storage pools for cluster balancing", (Response, configResponse));
                return;
            }
            const auto& config = configResponse.GetStatus(0).GetBaseConfig();
            const auto& storagePoolsStatus = configResponse.GetStatus(1);

            const auto storageInfo = BuildStorageInfo(config, storagePoolsStatus);

            if (storageInfo.PDisksWithReplicatingVDisks > Settings.MaxReplicatingPDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB13, "Skip balancing, too many replicating PDisks", (ReplicatingPDisks, storageInfo.PDisksWithReplicatingVDisks));
                return;
            }

            if (storageInfo.ReplicatingVDisks > Settings.MaxReplicatingVDisks) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB14, "Skip balancing, too many replicating VDisks", (ReplicatingVDisks, storageInfo.ReplicatingVDisks));
                return;
            }

            if (storageInfo.BestTargetUsageAfterMoveByPool.empty()) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB15, "Skip balancing, no storage pool has a PDisk that can accept a moved VDisk");
                return;
            }

            std::vector<const TVSlot*> candidateVSlots;

            for (const auto& vslot : config.GetVSlot()) {
                if (storageInfo.MovableGroups.contains(TGroupId::FromValue(vslot.GetGroupId()))) {
                    candidateVSlots.push_back(&vslot);
                }
            }

            auto groupSlotsOrdered = OrderVSlotsByPDiskUsage(candidateVSlots, storageInfo);
            if (groupSlotsOrdered.empty()) {
                STLOG(PRI_DEBUG, BS_CLUSTER_BALANCING, BSCB16, "Skip balancing, cluster is balanced enough");
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
                ui32 reassignAttempts = 0;
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

                    if (++reassignAttempts >= Settings.MaxReassignAttemptsPerBucketPerIteration) {
                        // Move to the next source-usage bucket, keeping this cap
                        // local to the bucket instead of the whole iteration.
                        break;
                    }

                    // Avoid DoSing BSC.
                    Yield(TDuration::MilliSeconds(100));
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

    TClusterBalancingSettings ParseClusterBalancingSettings(const std::shared_ptr<const NKikimrBlobStorage::TStorageConfig> storageConfig) {
        TClusterBalancingSettings settings;

        if (!storageConfig->HasBlobStorageConfig()) {
            return settings;
        }

        const auto& bsConfig = storageConfig->GetBlobStorageConfig();

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
        if (clusterBalancingSettings.HasMaxReassignAttemptsPerBucketPerIteration()) {
            settings.MaxReassignAttemptsPerBucketPerIteration = clusterBalancingSettings.GetMaxReassignAttemptsPerBucketPerIteration();
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
