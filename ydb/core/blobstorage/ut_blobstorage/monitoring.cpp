#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <util/string/printf.h>

constexpr bool VERBOSE = false;

void SetupEnv(const TBlobStorageGroupInfo::TTopology& topology, std::unique_ptr<TEnvironmentSetup>& env,
        ui32& groupSize, TBlobStorageGroupType& groupType, ui32& groupId, std::vector<ui32>& pdiskLayout,
        ui32 burstThresholdNs = 0, float diskTimeAvailableScale = 1) {
    groupSize = topology.TotalVDisks;
    groupType = topology.GType;
    env.reset(new TEnvironmentSetup({
        .NodeCount = groupSize,
        .Erasure = groupType,
        .DiskType = NPDisk::EDeviceType::DEVICE_TYPE_ROT,
        .BurstThresholdNs = burstThresholdNs,
        .DiskTimeAvailableScale =  diskTimeAvailableScale,
    }));

    env->CreateBoxAndPool(1, 1);
    env->Sim(TDuration::Seconds(30));

    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    auto response = env->Invoke(request);

    const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
    UNIT_ASSERT_VALUES_EQUAL(baseConfig.GroupSize(), 1);
    groupId = baseConfig.GetGroup(0).GetGroupId();
    pdiskLayout = MakePDiskLayout(baseConfig, topology, groupId);
}

auto GetHostedVDiskCounters(TEnvironmentSetup& env, const TBlobStorageGroupInfo::TTopology& topology, ui32 groupId) {
    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    const auto response = env.Invoke(request);
    std::vector<TIntrusivePtr<NMonitoring::TDynamicCounters>> result;
    for (const auto& vslot : response.GetStatus(0).GetBaseConfig().GetVSlot()) {
        if (vslot.GetGroupId() != groupId) {
            continue;
        }
        const auto& id = vslot.GetVSlotId();
        const ui32 order = topology.GetOrderNumber(TVDiskIdShort(vslot.GetFailRealmIdx(),
                vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        auto counters = GetServiceCounters(env.Runtime->GetNode(id.GetNodeId())->AppData->Counters, "vdisks");
        for (const auto& [label, value] : std::initializer_list<std::pair<TString, TString>>{
                {"storagePool", env.StoragePoolName}, {"group", ToString(groupId)},
                {"orderNumber", Sprintf("%02u", order)}, {"pdisk", Sprintf("%09u", id.GetPDiskId())},
                {"media", "rot"}}) {
            counters = counters->FindSubgroup(label, value);
            UNIT_ASSERT_C(counters, "Missing " << label << "=" << value << " for VSlot " << id.ShortDebugString());
        }
        result.push_back(counters);
    }
    UNIT_ASSERT_VALUES_EQUAL(result.size(), topology.TotalVDisks);
    return result;
}

template <typename TInflightActor>
void TestVDiskAdvancedCost(const TBlobStorageGroupInfo::TTopology& topology, TInflightActor* actor) {
    std::unique_ptr<TEnvironmentSetup> env;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout);

    const auto vdiskCounters = GetHostedVDiskCounters(*env, topology, groupId);
    auto getCost = [&]() {
        ui64 cost = 0;
        for (const auto& counters : vdiskCounters) {
            UNIT_ASSERT(!counters->FindSubgroup("subsystem", "cost"));
            auto advancedCost = counters->FindSubgroup("subsystem", "advancedCost");
            UNIT_ASSERT(advancedCost);
            auto available = advancedCost->FindCounter("DiskTimeAvailableCtr");
            UNIT_ASSERT(available);
            UNIT_ASSERT_GT(available->Val(), 0);
            for (const TString& operation : {"read", "write"}) {
                auto group = advancedCost->FindSubgroup("operation", operation);
                UNIT_ASSERT(group);
                auto counter = group->FindCounter("UserDiskCost");
                UNIT_ASSERT(counter);
                cost += counter->Val();
            }
        }
        return cost;
    };
    auto getDSProxyCost = [&]() {
        ui64 cost = 0;
        for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
            auto* appData = env->Runtime->GetNode(nodeId)->AppData.get();
            cost += GetServiceCounters(appData->Counters, "dsproxynode")->
                    GetSubgroup("subsystem", "request")->
                    GetSubgroup("storagePool", env->StoragePoolName)->
                    GetCounter("DSProxyDiskCostNs", true)->Val();
        }
        return cost;
    };

    const ui64 costBefore = getCost();
    const ui64 dsproxyCostBefore = getDSProxyCost();
    actor->SetGroupId(TGroupId::FromValue(groupId));
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(10));

    UNIT_ASSERT_GT(getCost(), costBefore);
    // DSProxy uses the QoS cost model, so its cost need not equal advancedCost.
    UNIT_ASSERT_GT(getDSProxyCost(), dsproxyCostBefore);
    UNIT_ASSERT_VALUES_EQUAL(actor->ResponsesByStatus[NKikimrProto::ERROR], 0);
    UNIT_ASSERT_GT(actor->ResponsesByStatus[NKikimrProto::OK], 0);
}

#define MAKE_TEST_W_DATASIZE(erasure, requestType, requests, inflight, dataSize)                        \
Y_UNIT_TEST(Test##requestType##erasure##Requests##requests##Inflight##inflight##BlobSize##dataSize) {   \
    auto groupType = TBlobStorageGroupType::Erasure##erasure;                                           \
    ui32 realms = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 1;                       \
    ui32 domains = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 8;                      \
    TBlobStorageGroupInfo::TTopology topology(groupType, realms, domains, 1, true);                     \
    auto actor = new TInflightActor##requestType({requests, inflight}, dataSize);                       \
    TestVDiskAdvancedCost(topology, actor);                                                            \
}

Y_UNIT_TEST_SUITE(CostMetricsPutMirror3dc) {
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10000, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10000, 1000, 1000);
}

Y_UNIT_TEST_SUITE(CostMetricsPutBlock4Plus2) {
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 10000, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Put, 10000, 1000, 1000);
}

Y_UNIT_TEST_SUITE(CostMetricsPutHugeMirror3dc) {
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 1, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 100, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 2, 2, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 10, 10, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Put, 100, 10, 2000000);
}

Y_UNIT_TEST_SUITE(CostMetricsGetMirror3dc) {
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10000, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10000, 1000, 1000);
}

Y_UNIT_TEST_SUITE(CostMetricsGetBlock4Plus2) {
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 10000, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Get, 10000, 1000, 1000);
}

Y_UNIT_TEST_SUITE(CostMetricsGetHugeMirror3dc) {
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 1, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 100, 1, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 2, 2, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10, 10, 2000000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 100, 10, 2000000);
}

Y_UNIT_TEST_SUITE(CostMetricsPatchMirror3dc) {
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 100, 1, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(Mirror3dc, Patch, 10000, 100, 1000);
}

Y_UNIT_TEST_SUITE(CostMetricsPatchBlock4Plus2) {
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 1, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 10, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 100, 1, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 2, 2, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 10, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 100, 10, 1000);
    MAKE_TEST_W_DATASIZE(4Plus2Block, Patch, 10000, 100, 1000);
}

enum class ELoadDistribution : ui8 {
    DistributionBurst = 0,
    DistributionEvenly,
};

template <typename TInflightActor>
void TestBurst(ui32 requests, ui32 inflight, TDuration delay, ELoadDistribution loadDistribution,
        ui32 burstThresholdNs = 0, float diskTimeAvailableScale = 1) {
    TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
    auto* actor = new TInflightActor({requests, inflight, delay}, 8_MB);
    std::unique_ptr<TEnvironmentSetup> env;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout, burstThresholdNs,
            diskTimeAvailableScale);

    actor->SetGroupId(TGroupId::FromValue(groupId));
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(10));

    ui64 redMs = env->AggregateVDiskCounters(env->StoragePoolName, groupSize, groupSize, groupId, pdiskLayout,
            "advancedCost", "BurstDetector_redMs");

    if (loadDistribution == ELoadDistribution::DistributionBurst) {
        UNIT_ASSERT_VALUES_UNEQUAL(redMs, 0);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(redMs, 0);
    }
}

Y_UNIT_TEST_SUITE(BurstDetection) {
    Y_UNIT_TEST(TestPutEvenly) {
        TestBurst<TInflightActorPut>(10, 1, TDuration::Seconds(1), ELoadDistribution::DistributionEvenly);
    }

    Y_UNIT_TEST(TestPutBurst) {
        TestBurst<TInflightActorPut>(10, 10, TDuration::MilliSeconds(1), ELoadDistribution::DistributionBurst);
    }

    Y_UNIT_TEST(TestOverlySensitive) {
        TestBurst<TInflightActorPut>(10, 1, TDuration::Seconds(1), ELoadDistribution::DistributionBurst, 1);
    }
}

void TestDiskTimeAvailableScaling() {
    TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
    std::unique_ptr<TEnvironmentSetup> env;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout, 0, 1);

    const auto vdiskCounters = GetHostedVDiskCounters(*env, topology, groupId);
    auto getAvailable = [&]() {
        ui64 value = 0;
        for (const auto& counters : vdiskCounters) {
            auto advancedCost = counters->FindSubgroup("subsystem", "advancedCost");
            UNIT_ASSERT(advancedCost);
            auto available = advancedCost->FindCounter("DiskTimeAvailableCtr");
            UNIT_ASSERT(available);
            value += available->Val();
        }
        return value;
    };
    const i64 test1 = getAvailable();
    UNIT_ASSERT_GT(test1, 0);

    // PDisk mock reports TrueMediaType=NVME even when configured as ROT.
    env->SetIcbControl(0, "VDiskControls.DiskTimeAvailableScaleNVME", 2'000);
    env->Sim(TDuration::Minutes(5));

    const i64 test2 = getAvailable();

    i64 delta = test1 * 2 - test2;

    UNIT_ASSERT_LE_C(std::abs(delta), 10, "Total time available: with scale=1 time=" << test1 <<
            ", with scale=2 time=" << test2);
}

Y_UNIT_TEST_SUITE(DiskTimeAvailable) {
    Y_UNIT_TEST(Scaling) {
        TestDiskTimeAvailableScaling();
    }
}

template <typename TInflightActor>
void TestDSProxyAndVDiskEqualByteCounters(TInflightActor* actor) {
    std::unique_ptr<TEnvironmentSetup> env;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
    SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout);
    actor->SetGroupId(TGroupId::FromValue(groupId));
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(10));
    ui64 dsproxyCounter = 0;
    ui64 vdiskCounter = 0;

    for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
        auto* appData = env->Runtime->GetNode(nodeId)->AppData.get();
        for(auto sizeClass : {"256", "4096", "262144", "1048576", "16777216", "4194304"})
            dsproxyCounter += GetServiceCounters(appData->Counters, "dsproxynode")->
                    GetSubgroup("subsystem", "request")->
                    GetSubgroup("storagePool", env->StoragePoolName)->
                    GetSubgroup("handleClass", "PutTabletLog")->
                    GetSubgroup("sizeClass", sizeClass)->
                    GetCounter("generatedSubrequestBytes")->Val();
    }
    vdiskCounter = env->AggregateVDiskCountersWithHandleClass(env->StoragePoolName, groupSize, groupSize, groupId, pdiskLayout,
            "PutTabletLog", "requestBytes");
    if constexpr(VERBOSE) {
        for (ui32 i = 1; i <= groupSize; ++i) {
            Cerr << " ##################### Node " << i << " ##################### " << Endl;
            env->Runtime->GetNode(i)->AppData->Counters->OutputPlainText(Cerr);
        }
    }
    UNIT_ASSERT(dsproxyCounter != 0);
    UNIT_ASSERT_VALUES_EQUAL(dsproxyCounter, vdiskCounter);
}


Y_UNIT_TEST_SUITE(TestDSProxyAndVDiskEqualByteCounters) {
    Y_UNIT_TEST(MultiPut) {
        auto actor = new TInflightActorPut({10, 10}, 1000, 10);
        TestDSProxyAndVDiskEqualByteCounters(actor);
    }

    Y_UNIT_TEST(SinglePut) {
        auto actor = new TInflightActorPut({1, 1}, 1000);
        TestDSProxyAndVDiskEqualByteCounters(actor);
    }
}

#undef MAKE_BURST_TEST
