#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

constexpr bool VERBOSE = false;

TString MakeData(ui32 dataSize) {
    TString data(dataSize, '\0');
    for (ui32 i = 0; i < dataSize; ++i) {
        data[i] = 'A' + (i % 26);
    }
    return data;
}

template <typename TDerived>
class TInflightActor : public TActorBootstrapped<TDerived> {
public:
    struct TSettings {
        ui32 Requests;
        ui32 MaxInFlight;
        TDuration Delay = TDuration::Zero();
    };

public:
    TInflightActor(TSettings settings)
        : RequestsToSend(settings.Requests)
        , RequestInFlight(settings.MaxInFlight)
        , Settings(settings)
    {}

    virtual ~TInflightActor() = default;

    void SetGroupId(ui32 groupId) {
        GroupId = groupId;
    }
    void Bootstrap(const TActorContext &ctx) {
        BootstrapImpl(ctx);
    }

protected:
    void ScheduleRequests() {
        while (RequestInFlight > 0 && RequestsToSend > 0) {
            TMonotonic now = TMonotonic::Now();
            TDuration timePassed = now - LastTs;
            if (timePassed >= Settings.Delay) {
                LastTs = now;
                RequestInFlight--;
                RequestsToSend--;
                SendRequest();
            } else {
                TActorBootstrapped<TDerived>::Schedule(Settings.Delay - timePassed, new TEvents::TEvWakeup);
            }
        }
    }

    void HandleReply(NKikimrProto::EReplyStatus status) {
        if (status == NKikimrProto::OK) {
            OKs++;
        } else {
            Fails++;
        }
        ++RequestInFlight;
        ScheduleRequests();
    }

    virtual void BootstrapImpl(const TActorContext &ctx) = 0;
    virtual void SendRequest() = 0;

protected:
    ui32 RequestsToSend;
    ui32 RequestInFlight;
    ui32 GroupId;
    TMonotonic LastTs;
    TSettings Settings;

public:
    ui32 OKs = 0;
    ui32 Fails = 0;
};

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
        ui32& groupId, std::vector<ui32>& pdiskLayout, ui32 burstThresholdNs = 0, float diskTimeAvailableScale = 1) {
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
    str << "OKs# " << actor->OKs << ", Fails# " << actor->Fails << ", Cost on dsproxy# "
            << dsproxyCost << ", Cost on vdisks# " << vdiskCost << ", proportion# " << proportion
            << " diff# " << diff;

    if constexpr(VERBOSE) {
        Cerr << str.Str() << Endl;
        // env->Runtime->GetAppData()->Counters->OutputPlainText(Cerr);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(dsproxyCost, vdiskCost, str.Str());
}

class TInflightActorPut : public TInflightActor<TInflightActorPut> {
public:
    TInflightActorPut(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvStatusResult::EventType, ScheduleRequests);
        cFunc(TEvents::TEvWakeup::EventType, ScheduleRequests);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        // dummy request to establish the session
        auto ev = new TEvBlobStorage::TEvStatus(TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorPut::StateWork);
    }

protected:
    virtual void SendRequest() override {
        TString data = MakeData(DataSize);
        auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 10, DataSize, RequestsToSend + 1),
                data, TInstant::Max(), NKikimrBlobStorage::UserData);
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

private:
    std::string Data;
    ui32 DataSize;
};

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

class TInflightActorGet : public TInflightActor<TInflightActorGet> {
public:
    TInflightActorGet(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvPutResult::EventType, ScheduleRequests);
        cFunc(TEvents::TEvWakeup::EventType, ScheduleRequests);
        hFunc(TEvBlobStorage::TEvGetResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        TString data = MakeData(DataSize);
        BlobId = TLogoBlobID(1, 1, 1, 10, DataSize, 1);
        auto ev = new TEvBlobStorage::TEvPut(BlobId, data, TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorGet::StateWork);
    }

protected:
    virtual void SendRequest() override {
        auto ev = new TEvBlobStorage::TEvGet(BlobId, 0, 10, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

private:
    TLogoBlobID BlobId;
    std::string Data;
    ui32 DataSize;
};

class TInflightActorPatch : public TInflightActor<TInflightActorPatch> {
public:
    TInflightActorPatch(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        hFunc(TEvBlobStorage::TEvPatchResult, Handle);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        TString data = MakeData(DataSize);
        for (ui32 i = 0; i < RequestInFlight; ++i) {
            TLogoBlobID blobId(1, 1, 1, 10, DataSize, 1 + i);
            auto ev = new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max());
            SendToBSProxy(SelfId(), GroupId, ev, 0);
        }
        Become(&TInflightActorPatch::StateWork);
    }

protected:
    virtual void SendRequest() override {
        TLogoBlobID oldId = Blobs.front();
        Blobs.pop_front();
        TLogoBlobID newId(1, 1, oldId.Step() + 1, 10, DataSize, oldId.Cookie());
        Y_ABORT_UNLESS(TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(oldId, &newId, BlobIdMask, GroupId, GroupId));
        TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[1]);
        char c = 'a' + RequestsToSend % 26;
        diffs[0].Set(TString(DataSize, c), 0);
        auto ev = new TEvBlobStorage::TEvPatch(GroupId, oldId, newId, BlobIdMask, std::move(diffs), 1, TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }


    void Handle(TEvBlobStorage::TEvPatchResult::TPtr res) {
        Blobs.push_back(res->Get()->Id);
        HandleReply(res->Get()->Status);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        Blobs.push_back(res->Get()->Id);
        if (++BlobsWritten == RequestInFlight) {
            ScheduleRequests();
        }
    }

protected:
    std::deque<TLogoBlobID> Blobs;
    ui32 BlobIdMask = TLogoBlobID::MaxCookie & 0xfffff000;
    ui32 BlobsWritten = 0;
    std::string Data;
    ui32 DataSize;
};

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
void TestBurst(ui32 requests, ui32 inflight, TDuration delay, ELoadDistribution loadDistribution,
        ui32 burstThresholdNs = 0, float diskTimeAvailableScale = 1) {
    TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
    auto* actor = new TInflightActor({requests, inflight, delay}, 8_MB);
    std::unique_ptr<TEnvironmentSetup> env;
    NKikimrBlobStorage::TBaseConfig baseConfig;
    ui32 groupSize;
    TBlobStorageGroupType groupType;
    ui32 groupId;
    std::vector<ui32> pdiskLayout;
    SetupEnv(topology, env, baseConfig, groupSize, groupType, groupId, pdiskLayout, burstThresholdNs, diskTimeAvailableScale);

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
    auto measure = [](float scale) {
        TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
        std::unique_ptr<TEnvironmentSetup> env;
        ui32 groupSize;
        TBlobStorageGroupType groupType;
        ui32 groupId;
        std::vector<ui32> pdiskLayout;
        SetupEnv(topology, env, groupSize, groupType, groupId, pdiskLayout, 0, scale);

        return AggregateVDiskCounters(env, env->StoragePoolName, groupSize, groupId, pdiskLayout,
                "advancedCost", "DiskTimeAvailable");
    };

    i64 test1 = measure(1);
    i64 test2 = measure(2);

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
