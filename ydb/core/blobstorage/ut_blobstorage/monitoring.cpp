#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include "ut_helpers.h"

constexpr bool VERBOSE = false;

TString MakeData(ui32 dataSize) {
    TString data(dataSize, '\0');
    for (ui32 i = 0; i < dataSize; ++i) {
        data[i] = 'A' + (i % 26);
    }
    return data;
}

ui64 AggregateVDiskCounters(std::unique_ptr<TEnvironmentSetup>& env, const NKikimrBlobStorage::TBaseConfig& baseConfig,
        TString storagePool, ui32 groupSize, ui32 groupId, const std::vector<ui32>& pdiskLayout, TString subsystem,
        TString counter, bool derivative = false) {
    ui64 ctr = 0;

    for (const auto& vslot : baseConfig.GetVSlot()) {
        auto* appData = env->Runtime->GetNode(vslot.GetVSlotId().GetNodeId())->AppData.get();
        for (ui32 i = 0; i < groupSize; ++i) {
            ctr += GetServiceCounters(appData->Counters, "vdisks")->
                    GetSubgroup("storagePool", storagePool)->
                    GetSubgroup("group", std::to_string(groupId))->
                    GetSubgroup("orderNumber", "0" + std::to_string(i))->
                    GetSubgroup("pdisk", "00000" + std::to_string(pdiskLayout[i]))->
                    GetSubgroup("media", "rot")->
                    GetSubgroup("subsystem", subsystem)->
                    GetCounter(counter, derivative)->Val();
        }
    }
    return ctr;
};

void SetupEnv(const TBlobStorageGroupInfo::TTopology& topology, std::unique_ptr<TEnvironmentSetup>& env,
        NKikimrBlobStorage::TBaseConfig& baseConfig, ui32& groupSize, TBlobStorageGroupType& groupType,
        ui32& groupId, std::vector<ui32>& pdiskLayout) {
    groupSize = topology.TotalVDisks;
    groupType = topology.GType;
    env.reset(new TEnvironmentSetup({
        .NodeCount = groupSize,
        .Erasure = groupType,
        .DiskType = NPDisk::EDeviceType::DEVICE_TYPE_ROT,
    }));

    env->CreateBoxAndPool(1, 1);
    env->Sim(TDuration::Seconds(30));

    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    auto response = env->Invoke(request);

    baseConfig = response.GetStatus(0).GetBaseConfig();
    UNIT_ASSERT_VALUES_EQUAL(baseConfig.GroupSize(), 1);
    groupId = baseConfig.GetGroup(0).GetGroupId();
    pdiskLayout.resize(groupSize);
    for (const auto& vslot : baseConfig.GetVSlot()) {
        const auto& vslotId = vslot.GetVSlotId();
        ui32 orderNumber = topology.GetOrderNumber(TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        if (vslot.GetGroupId() == groupId) {
            pdiskLayout[orderNumber] = vslotId.GetPDiskId();
        }
    }
}

template <typename TInflightActor>
void TestDSProxyAndVDiskEqualCost(const TBlobStorageGroupInfo::TTopology& topology, TInflightActor* actor) {
    std::unique_ptr<TEnvironmentSetup> env;
    NKikimrBlobStorage::TBaseConfig baseConfig;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, baseConfig, groupSize, groupType, groupId, pdiskLayout);

    ui64 dsproxyCost = 0;
    ui64 vdiskCost = 0;

    auto updateCounters = [&]() {
        dsproxyCost = 0;

        for (const auto& vslot : baseConfig.GetVSlot()) {
            auto* appData = env->Runtime->GetNode(vslot.GetVSlotId().GetNodeId())->AppData.get();
            dsproxyCost += GetServiceCounters(appData->Counters, "dsproxynode")->
                    GetSubgroup("subsystem", "request")->
                    GetSubgroup("storagePool", env->StoragePoolName)->
                    GetCounter("DSProxyDiskCostNs")->Val();
        }
        vdiskCost = AggregateVDiskCounters(env, baseConfig, env->StoragePoolName, groupSize, groupId,
                pdiskLayout, "cost", "SkeletonFrontUserCostNs");
    };

    updateCounters();
    UNIT_ASSERT_VALUES_EQUAL(dsproxyCost, vdiskCost);

    actor->SetGroupId(groupId);
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(15));

    updateCounters();

    TStringStream str;
    double proportion = 1. * dsproxyCost / vdiskCost;
    i64 diff = (i64)dsproxyCost - vdiskCost;
    str << "OKs# " << actor->ResponsesByStatus[NKikimrProto::OK] << ", Errors# " << actor->ResponsesByStatus[NKikimrProto::ERROR]
            << ", Cost on dsproxy# " << dsproxyCost << ", Cost on vdisks# " << vdiskCost << ", proportion# " << proportion
            << " diff# " << diff;

    if constexpr(VERBOSE) {
        Cerr << str.Str() << Endl;
        // env->Runtime->GetAppData()->Counters->OutputPlainText(Cerr);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(dsproxyCost, vdiskCost, str.Str());
}

#define MAKE_TEST(erasure, requestType, requests, inflight)                         \
Y_UNIT_TEST(Test##requestType##erasure##Requests##requests##Inflight##inflight) {   \
    auto groupType = TBlobStorageGroupType::Erasure##erasure;                       \
    ui32 realms = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 1;   \
    ui32 domains = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 8;  \
    TBlobStorageGroupInfo::TTopology topology(groupType, realms, domains, 1, true); \
    auto actor = new TInflightActor##requestType({requests, inflight});             \
    TestDSProxyAndVDiskEqualCost(topology, actor);                                  \
}

#define MAKE_TEST_W_DATASIZE(erasure, requestType, requests, inflight, dataSize)                        \
Y_UNIT_TEST(Test##requestType##erasure##Requests##requests##Inflight##inflight##BlobSize##dataSize) {   \
    auto groupType = TBlobStorageGroupType::Erasure##erasure;                                           \
    ui32 realms = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 1;                       \
    ui32 domains = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 8;                      \
    TBlobStorageGroupInfo::TTopology topology(groupType, realms, domains, 1, true);                     \
    auto actor = new TInflightActor##requestType({requests, inflight}, dataSize);                       \
    TestDSProxyAndVDiskEqualCost(topology, actor);                                                      \
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
    MAKE_TEST_W_DATASIZE(Mirror3dc, Get, 10000, 100, 2000000);
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
void TestBurst(const TBlobStorageGroupInfo::TTopology& topology, TInflightActor* actor, ELoadDistribution loadDistribution) {
    std::unique_ptr<TEnvironmentSetup> env;
    NKikimrBlobStorage::TBaseConfig baseConfig;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, baseConfig, groupSize, groupType, groupId, pdiskLayout);

    actor->SetGroupId(groupId);
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(10));

    ui64 redMs = AggregateVDiskCounters(env, baseConfig, env->StoragePoolName, groupSize, groupId,
            pdiskLayout, "advancedCost", "BurstDetector_redMs");
    
    if (loadDistribution == ELoadDistribution::DistributionBurst) {
        UNIT_ASSERT_VALUES_UNEQUAL(redMs, 0);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(redMs, 0);
    }
}

#define MAKE_BURST_TEST(requestType, requests, inflight, delay, distribution)                       \
Y_UNIT_TEST(Test##requestType##distribution) {                                                      \
    TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);   \
    auto* actor = new TInflightActor##requestType({requests, inflight, delay}, 8_MB);               \
    TestBurst(topology, actor, ELoadDistribution::Distribution##distribution);                      \
}

Y_UNIT_TEST_SUITE(BurstDetection) {
    MAKE_BURST_TEST(Put, 10, 1, TDuration::Seconds(1), Evenly);
    MAKE_BURST_TEST(Put, 10, 100, TDuration::Zero(), Burst);
}

#undef MAKE_BURST_TEST
