#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/iterator/enumerate.h>

struct TTestEnv {
    TTestEnv(ui32 nodeCount, TBlobStorageGroupType erasure, ui32 pdiskPerNode, ui32 groupCount)
    : Env({
        .NodeCount = nodeCount,
        .VDiskReplPausedAtStart = false,
        .Erasure = erasure,
        .ConfigPreprocessor = [](ui32, TNodeWardenConfig& conf) {
            auto* bscSettings = conf.BlobStorageConfig.MutableBscSettings();
            auto* clusterBalancingSettings = bscSettings->MutableClusterBalancingSettings();

            clusterBalancingSettings->SetEnable(true);
            clusterBalancingSettings->SetMaxReplicatingPDisks(100);
            clusterBalancingSettings->SetMaxReplicatingVDisks(800);
            clusterBalancingSettings->SetIterationIntervalMs(TDuration::Seconds(1).MilliSeconds());
            clusterBalancingSettings->SetPreferLessOccupiedRack(true);
            clusterBalancingSettings->SetWithAttentionToReplication(true);
        },
    })
    {
        Env.CreateBoxAndPool(pdiskPerNode, groupCount);
        Env.Sim(TDuration::Minutes(1));
        // Uncomment to see cluster rebalancing logs
        // Env.Runtime->SetLogPriority(NKikimrServices::BS_CLUSTER_BALANCING, NActors::NLog::PRI_DEBUG);
    }

    bool IsDynamicGroup(ui32 groupId) {
        return groupId & 0x80000000;
    }

    std::unordered_map<TPDiskId, ui32> BuildPDiskUsageMap() {
        auto config = Env.FetchBaseConfig();

        std::unordered_map<TPDiskId, ui32> pdiskUsageMap;

        for (const auto& pdisk : config.pdisk()) {
            TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
            pdiskUsageMap[pdiskId] = pdisk.GetNumStaticSlots();
        }

        for (const auto& vslot : config.vslot()) {
            if (!IsDynamicGroup(vslot.GetGroupId())) {
                continue;
            }

            TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
            auto it = pdiskUsageMap.find(pdiskId);
            if (it == pdiskUsageMap.end()) {
                continue;
            }
            it->second += 1;
        }

        return pdiskUsageMap;
    }

    bool EachPDiskHasNVDisks(ui32 n) {
        auto usageMap = BuildPDiskUsageMap();

        return std::all_of(usageMap.begin(), usageMap.end(), [&](std::pair<TPDiskId, ui32> val) { return val.second == n; });
    }

    template<class TCondition>
    bool WaitFor(TCondition&& condition, size_t maxAttempts = 1) {
        for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
            if (condition()) {
                return true;
            }
            Env.Sim(TDuration::Minutes(1));
        }
        return false;
    }

    TEnvironmentSetup* operator->() {
        return &Env;
    }

    TEnvironmentSetup Env;
};

Y_UNIT_TEST_SUITE(ClusterBalancing) {

    Y_UNIT_TEST(ClusterBalancingEvenDistribution) {
        TTestEnv env(8, TBlobStorageGroupType::Erasure4Plus2Block, 2, 4);

        UNIT_ASSERT(env.EachPDiskHasNVDisks(2));

        env->AlterBox(1, 4);

        bool seenParameters = false;

        auto catchReassigns = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) { 
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                for (const auto& command : request.GetCommand()) {
                    if (command.GetCommandCase() == NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                        auto& reassignCommand = command.GetReassignGroupDisk();
                        if (reassignCommand.GetPreferLessOccupiedRack() && reassignCommand.GetWithAttentionToReplication()) {
                            seenParameters = true;
                        }
                    }
                }
            }
            return true;
        };

        env->Runtime->FilterFunction = catchReassigns;

        bool success = env.WaitFor([&] {
            return env.EachPDiskHasNVDisks(1);
        }, 10);

        UNIT_ASSERT(seenParameters);
        UNIT_ASSERT(success);
    }

    Y_UNIT_TEST(ClusterBalancingEvenDistributionNotPossible) {
        TTestEnv env(8, TBlobStorageGroupType::Erasure4Plus2Block, 1, 3);

        UNIT_ASSERT(env.EachPDiskHasNVDisks(3));

        env->AlterBox(1, 2);

        auto check = [&] {
            auto usageMap = env.BuildPDiskUsageMap();
            std::unordered_map<ui32, ui32> countByUsage;
            for (const auto& [pdiskId, usage] : usageMap) {
                countByUsage[usage]++;
            }
            // There is now total of 16 PDisks, 8 of them should be used by 2 VDisks and 8 of them should be used by 1 VDisk.
            // This is the best distribution possible.
            return countByUsage[1] == 8 && countByUsage[2] == 8;
        };

        bool success = env.WaitFor(check, 10);

        UNIT_ASSERT(success);

        auto usageMap1 = env.BuildPDiskUsageMap();

        env.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigResponse::EventType) {
                const auto& response = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.GetResponse();
                    
                const auto& status = response.GetStatus(0);                

                UNIT_ASSERT_VALUES_EQUAL(0, status.ReassignedItemSize());
            }
            return true;
        };

        env.Env.Sim(TDuration::Seconds(5));

        // Check that the cluster balancing doesn't do anything now.
        // Optimal distribution is already achieved, any reassignment will only move data around for no reason.
        UNIT_ASSERT(check());

        auto usageMap2 = env.BuildPDiskUsageMap();

        UNIT_ASSERT(usageMap1 == usageMap2);
    }
}
