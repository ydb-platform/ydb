#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include "ut_helpers.h"

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

template <typename TInflightActor>
void TestDSProxyAndVDiskEqualCost(const TBlobStorageGroupInfo::TTopology& topology, TInflightActor* actor) {
    std::unique_ptr<TEnvironmentSetup> env;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout);

    ui64 dsproxyCost = 0;
    ui64 vdiskCost = 0;
    ui64 queuePut = 0;
    ui64 queueSent = 0;

    std::vector<TString> priorities = {
        "GetAsyncRead", "GetDiscover", "GetFastRead", "GetLowRead",
        "PutAsyncBlob", "PutTabletLog", "PutUserData"
    };

    auto updateCounters = [&]() {
        dsproxyCost = 0;
        queuePut = 0;
        queueSent = 0;

        for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
            auto* appData = env->Runtime->GetNode(nodeId)->AppData.get();
            dsproxyCost += GetServiceCounters(appData->Counters, "dsproxynode")->
                    GetSubgroup("subsystem", "request")->
                    GetSubgroup("storagePool", env->StoragePoolName)->
                    GetCounter("DSProxyDiskCostNs")->Val();

            for (TString priority : priorities) {
                queuePut += GetServiceCounters(appData->Counters, "dsproxy_queue")->
                    GetSubgroup("queue", priority)->
                    GetCounter("QueueItemsPut")->Val();
                queueSent += GetServiceCounters(appData->Counters, "dsproxy_queue")->
                    GetSubgroup("queue", priority)->
                    GetCounter("QueueItemsSent")->Val();
            }
        }
        vdiskCost = env->AggregateVDiskCounters(env->StoragePoolName, groupSize, groupSize, groupId, pdiskLayout,
                "cost", "SkeletonFrontUserCostNs");
    };

    updateCounters();
    UNIT_ASSERT_VALUES_EQUAL(dsproxyCost, vdiskCost);

    actor->SetGroupId(TGroupId::FromValue(groupId));
    env->Runtime->Register(actor, 1);
    env->Sim(TDuration::Minutes(5));

    updateCounters();
    env->Sim(TDuration::Minutes(5));
    updateCounters();

    TStringStream str;
    double proportion = 1. * dsproxyCost / vdiskCost;
    i64 diff = (i64)dsproxyCost - vdiskCost;
    ui32 oks = actor->ResponsesByStatus[NKikimrProto::OK];
    ui32 errors = actor->ResponsesByStatus[NKikimrProto::ERROR];
    str << "OKs# " << oks << ", Errors# " << errors << ", QueueItemsPut# " << queuePut
            << ", QueueItemsSent# " << queueSent << ", Cost on dsproxy# " << dsproxyCost
            << ", Cost on vdisks# " << vdiskCost << ", proportion# " << proportion << " diff# " << diff;

    if constexpr(VERBOSE) {
        Cerr << str.Str() << Endl;
        for (ui32 i = 1; i <= groupSize; ++i) {
            Cerr << " ##################### Node " << i << " ##################### " << Endl;
            env->Runtime->GetNode(i)->AppData->Counters->OutputPlainText(Cerr);
        }
    }

    if (dsproxyCost == vdiskCost) {
        return;
    }
    UNIT_ASSERT_C(oks != actor->RequestsSent || queuePut != queueSent, str.Str());
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

    i64 test1 = env->AggregateVDiskCounters(env->StoragePoolName, groupSize, groupSize, groupId, pdiskLayout,
            "advancedCost", "DiskTimeAvailable");

    env->SetIcbControl(0, "VDiskControls.DiskTimeAvailableScaleNVME", 2'000);
    env->Sim(TDuration::Minutes(5));

    i64 test2 = env->AggregateVDiskCounters(env->StoragePoolName, groupSize, groupSize, groupId, pdiskLayout,
            "advancedCost", "DiskTimeAvailable");

    i64 delta = test1 * 2 - test2;

    UNIT_ASSERT_LE_C(std::abs(delta), 10, "Total time available: with scale=1 time=" << test1 <<
            ", with scale=2 time=" << test2);
}

Y_UNIT_TEST_SUITE(DiskTimeAvailable) {
    Y_UNIT_TEST(Scaling) {
        TestDiskTimeAvailableScaling();
    }
}

#undef MAKE_BURST_TEST
