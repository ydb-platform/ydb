#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/random.h>

#include <library/cpp/iterator/enumerate.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <unordered_map>
#include <vector>

struct TTestEnv {
    using TVDiskPositionKey = std::tuple<ui32, ui32, ui32, ui32>;

    TTestEnv(
        ui32 nodeCount,
        TBlobStorageGroupType erasure,
        ui32 pdiskPerNode,
        ui32 groupCount,
        ui32 maxReassignAttemptsPerBucketPerIteration = 10,
        TDuration iterationInterval = TDuration::Seconds(1)
    )
    : Env({
        .NodeCount = nodeCount,
        .VDiskReplPausedAtStart = false,
        .Erasure = erasure,
        .ConfigPreprocessor = [maxReassignAttemptsPerBucketPerIteration, iterationInterval](ui32, TNodeWardenConfig& conf) {
            auto* bscSettings = conf.BlobStorageConfig.MutableBscSettings();
            auto* clusterBalancingSettings = bscSettings->MutableClusterBalancingSettings();

            clusterBalancingSettings->SetEnable(true);
            clusterBalancingSettings->SetMaxReplicatingPDisks(100);
            clusterBalancingSettings->SetMaxReplicatingVDisks(800);
            clusterBalancingSettings->SetIterationIntervalMs(iterationInterval.MilliSeconds());
            clusterBalancingSettings->SetMaxReassignAttemptsPerBucketPerIteration(maxReassignAttemptsPerBucketPerIteration);
            clusterBalancingSettings->SetPreferLessOccupiedRack(true);
            clusterBalancingSettings->SetWithAttentionToReplication(true);
        },
    })
    {
        Env.CreateBoxAndPool(pdiskPerNode, groupCount);
        WaitForDynamicVSlotsReady();
        // Uncomment to see cluster rebalancing logs
        // Env.Runtime->SetLogPriority(NKikimrServices::BS_CLUSTER_BALANCING, NActors::NLog::PRI_DEBUG);
    }

    bool IsDynamicGroup(ui32 groupId) {
        return groupId & 0x80000000;
    }

    std::unordered_map<TPDiskId, ui32> BuildPDiskUsageMap(const NKikimrBlobStorage::TBaseConfig& config) {
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

    std::unordered_map<TPDiskId, ui32> BuildPDiskUsageMap() {
        return BuildPDiskUsageMap(Env.FetchBaseConfig());
    }

    bool DynamicVSlotsAreReady() {
        const auto config = Env.FetchBaseConfig();
        bool hasDynamicVSlot = false;
        for (const auto& vslot : config.GetVSlot()) {
            if (!IsDynamicGroup(vslot.GetGroupId())) {
                continue;
            }

            hasDynamicVSlot = true;
            if (!vslot.GetReady()) {
                return false;
            }
        }

        return hasDynamicVSlot;
    }

    void WaitForDynamicVSlotsReady() {
        const bool success = WaitFor([&] {
            return DynamicVSlotsAreReady();
        }, 60);

        UNIT_ASSERT_C(success, "Expected dynamic VSlots to become ready");
    }

    static TVDiskPositionKey MakeVDiskPositionKey(const NKikimrBlobStorage::TBaseConfig_TVSlot& vslot) {
        return {
            vslot.GetGroupId(),
            vslot.GetFailRealmIdx(),
            vslot.GetFailDomainIdx(),
            vslot.GetVDiskIdx(),
        };
    }

    static TVDiskPositionKey MakeVDiskPositionKey(const NKikimrBlobStorage::TReassignGroupDisk& reassign) {
        return {
            reassign.GetGroupId(),
            reassign.GetFailRealmIdx(),
            reassign.GetFailDomainIdx(),
            reassign.GetVDiskIdx(),
        };
    }

    std::map<TVDiskPositionKey, ui32> BuildVDiskSourceUsageMap(const NKikimrBlobStorage::TBaseConfig& config) {
        const auto pdiskUsageMap = BuildPDiskUsageMap(config);
        std::map<TVDiskPositionKey, ui32> usageByVDisk;

        for (const auto& vslot : config.vslot()) {
            if (!IsDynamicGroup(vslot.GetGroupId())) {
                continue;
            }

            TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
            auto it = pdiskUsageMap.find(pdiskId);
            if (it == pdiskUsageMap.end()) {
                continue;
            }

            usageByVDisk[MakeVDiskPositionKey(vslot)] = it->second;
        }

        return usageByVDisk;
    }

    void DefineBoxWithDrives(ui64 itemConfigGeneration, const std::vector<NKikimrBlobStorage::EPDiskType>& driveTypes) {
        NKikimrBlobStorage::TConfigRequest request;

        auto *hostConfig = request.AddCommand()->MutableDefineHostConfig();
        hostConfig->SetHostConfigId(1);
        hostConfig->SetItemConfigGeneration(itemConfigGeneration);

        for (ui32 index = 0; index < driveTypes.size(); ++index) {
            auto *drive = hostConfig->AddDrive();
            drive->SetPath(TStringBuilder() << "SectorMap:" << index << ":1000");
            drive->SetType(driveTypes[index]);
        }

        auto *emptyHostConfig = request.AddCommand()->MutableDefineHostConfig();
        emptyHostConfig->SetHostConfigId(2);
        emptyHostConfig->SetItemConfigGeneration(itemConfigGeneration);

        auto *box = request.AddCommand()->MutableDefineBox();
        box->SetBoxId(1);
        box->SetItemConfigGeneration(itemConfigGeneration);

        for (ui32 nodeId : Env.Runtime->GetNodes()) {
            auto *host = box->AddHost();
            host->MutableKey()->SetNodeId(nodeId);
            host->SetHostConfigId(1);
        }

        const auto response = Env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        for (const auto& status : response.GetStatus()) {
            UNIT_ASSERT_C(status.GetSuccess(), status.GetErrorDescription());
        }
    }

    void DefineStoragePool(
        ui64 storagePoolId,
        const TString& poolName,
        NKikimrBlobStorage::EPDiskType pdiskType,
        ui32 numGroups
    ) {
        NKikimrBlobStorage::TConfigRequest request;

        auto *pool = request.AddCommand()->MutableDefineStoragePool();
        pool->SetBoxId(1);
        pool->SetStoragePoolId(storagePoolId);
        pool->SetName(poolName);
        pool->SetKind(poolName);
        pool->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(Env.Settings.Erasure.GetErasure()));
        pool->SetVDiskKind("Default");
        pool->SetNumGroups(numGroups);
        pool->AddPDiskFilter()->AddProperty()->SetType(pdiskType);

        const auto response = Env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        UNIT_ASSERT_C(response.GetStatus(0).GetSuccess(), response.GetStatus(0).GetErrorDescription());
    }

    void SendReassignNotViable(const IEventHandle& request) {
        auto response = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
        auto* record = response->Record.MutableResponse();
        record->SetSuccess(true);

        auto* status = record->AddStatus();
        status->SetSuccess(false);
        status->SetFailReason(NKikimrBlobStorage::TConfigResponse::TStatus::kReassignNotViable);

        Env.Runtime->Send(new IEventHandle(request.Sender, request.Recipient, response.release()), request.Sender.NodeId());
    }

    std::unordered_map<ui32, ui64> BuildGroupStoragePoolMap(const NKikimrBlobStorage::TBaseConfig& config) {
        std::unordered_map<ui32, ui64> storagePoolByGroup;

        for (const auto& group : config.GetGroup()) {
            if (!IsDynamicGroup(group.GetGroupId())) {
                continue;
            }

            storagePoolByGroup[group.GetGroupId()] = group.GetStoragePoolId();
        }

        return storagePoolByGroup;
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
            Env.Sim(TDuration::Seconds(1));
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
        TTestEnv env(8, TBlobStorageGroupType::Erasure4Plus2Block, 1, 2, 10, TDuration::MilliSeconds(100));

        UNIT_ASSERT(env.EachPDiskHasNVDisks(2));

        env->AlterBox(1, 2);

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
        }, 60);

        UNIT_ASSERT(seenParameters);
        UNIT_ASSERT(success);
    }

    Y_UNIT_TEST(ClusterBalancingEvenDistributionNotPossible) {
        TTestEnv env(8, TBlobStorageGroupType::Erasure4Plus2Block, 1, 3, 10, TDuration::MilliSeconds(100));

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

        bool success = env.WaitFor(check, 60);

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

        env.Env.Sim(TDuration::MilliSeconds(200));

        // Check that the cluster balancing doesn't do anything now.
        // Optimal distribution is already achieved, any reassignment will only move data around for no reason.
        UNIT_ASSERT(check());

        auto usageMap2 = env.BuildPDiskUsageMap();

        UNIT_ASSERT(usageMap1 == usageMap2);
    }

    Y_UNIT_TEST(ClusterBalancingMaxReassignAttemptsArePerSourceBucket) {
        // 3 Erasure4Plus2Block groups make 24 dynamic VSlots. Spread over 9 source
        // PDisks, this gives at least two source usage buckets before new PDisks
        // are added as balancing targets.
        TTestEnv env(9, TBlobStorageGroupType::Erasure4Plus2Block, 1, 3, 1);

        std::map<TTestEnv::TVDiskPositionKey, ui32> sourceUsageByVDisk;
        ui32 maxSourceUsage = 0;
        ui32 blockedTopBucketAttempts = 0;
        bool seenLowerBucket = false;
        bool captureReassigns = false;

        env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                return true;
            }

            const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
            for (const auto& command : request.GetCommand()) {
                if (command.GetCommandCase() != NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                    continue;
                }

                if (!captureReassigns) {
                    env.SendReassignNotViable(*ev);
                    return false;
                }

                const auto& reassign = command.GetReassignGroupDisk();
                const auto key = TTestEnv::MakeVDiskPositionKey(reassign);
                const auto it = sourceUsageByVDisk.find(key);
                UNIT_ASSERT_C(it != sourceUsageByVDisk.end(), "Unexpected ReassignGroupDisk command: " << reassign.ShortDebugString());

                if (it->second == maxSourceUsage) {
                    ++blockedTopBucketAttempts;
                    env.SendReassignNotViable(*ev);
                    return false;
                }

                seenLowerBucket = true;
            }

            return true;
        };

        env->AlterBox(1, 2);

        const auto config = env->FetchBaseConfig();
        sourceUsageByVDisk = env.BuildVDiskSourceUsageMap(config);

        std::set<ui32, std::greater<ui32>> sourceUsages;
        for (const auto& [_, usage] : sourceUsageByVDisk) {
            sourceUsages.insert(usage);
        }
        UNIT_ASSERT_C(sourceUsages.size() >= 2, "Expected at least two source usage buckets");

        maxSourceUsage = *sourceUsages.begin();
        captureReassigns = true;

        const bool success = env.WaitFor([&] {
            return seenLowerBucket;
        }, 3);

        UNIT_ASSERT_C(blockedTopBucketAttempts > 0, "Expected to block at least one reassign from the top source bucket");
        UNIT_ASSERT_C(success,
            "Expected cluster balancing to continue with a lower source usage bucket after reaching per-bucket reassign cap;"
            << " blockedTopBucketAttempts# " << blockedTopBucketAttempts);
    }

    Y_UNIT_TEST(ClusterBalancingSkipsNonImprovableStoragePool) {
        constexpr ui64 SsdPoolId = 2;

        TTestEnv env(8, TBlobStorageGroupType::Erasure4Plus2Block, 1, 2, 100);

        env.DefineBoxWithDrives(1, {
            NKikimrBlobStorage::EPDiskType::ROT,
            NKikimrBlobStorage::EPDiskType::SSD,
        });
        env.DefineStoragePool(SsdPoolId, "ssd", NKikimrBlobStorage::EPDiskType::SSD, 1);
        env.WaitForDynamicVSlotsReady();

        std::unordered_map<ui32, ui64> storagePoolByGroup;
        ui32 rotPoolReassigns = 0;
        ui32 ssdPoolReassigns = 0;
        bool captureReassigns = false;

        env->Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                return true;
            }

            const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
            for (const auto& command : request.GetCommand()) {
                if (command.GetCommandCase() != NKikimrBlobStorage::TConfigRequest::TCommand::kReassignGroupDisk) {
                    continue;
                }

                if (!captureReassigns) {
                    env.SendReassignNotViable(*ev);
                    return false;
                }

                const auto& reassign = command.GetReassignGroupDisk();
                const auto it = storagePoolByGroup.find(reassign.GetGroupId());
                UNIT_ASSERT_C(it != storagePoolByGroup.end(), "Unexpected ReassignGroupDisk command: " << reassign.ShortDebugString());

                if (it->second == SsdPoolId) {
                    ++ssdPoolReassigns;
                } else {
                    ++rotPoolReassigns;
                }

                env.SendReassignNotViable(*ev);
                return false;
            }

            return true;
        };

        env.DefineBoxWithDrives(2, {
            NKikimrBlobStorage::EPDiskType::ROT,
            NKikimrBlobStorage::EPDiskType::SSD,
            NKikimrBlobStorage::EPDiskType::ROT,
        });

        const auto config = env->FetchBaseConfig();
        storagePoolByGroup = env.BuildGroupStoragePoolMap(config);
        UNIT_ASSERT_C(
            std::any_of(storagePoolByGroup.begin(), storagePoolByGroup.end(), [](const auto& item) {
                return item.second == SsdPoolId;
            }),
            "Expected test setup to create groups in SSD pool");

        captureReassigns = true;
        const bool success = env.WaitFor([&] {
            return rotPoolReassigns > 0;
        }, 30);

        UNIT_ASSERT_C(success, "Expected cluster balancing to try the improvable ROT pool");
        UNIT_ASSERT_C(ssdPoolReassigns == 0,
            "Expected cluster balancing to skip the SSD pool because no move can improve its usage;"
            << " ssdPoolReassigns# " << ssdPoolReassigns
            << " rotPoolReassigns# " << rotPoolReassigns);
    }
}
