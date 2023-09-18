#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

constexpr bool VERBOSE = true;

TString MakeData(ui32 dataSize) {
    TString data(dataSize, '\0');
    for (ui32 i = 0; i < dataSize; ++i) {
        data[i] = 'A' + (i % 26);
    }
    return data;
}

class TInflightActor : public TActorBootstrapped<TInflightActor> {
public:
    TInflightActor(ui32 requests, ui32 inflight)
        : RequestCount(requests)
        , RequestInflight(inflight)
    {}

    virtual ~TInflightActor() = default;

    void SetGroupId(ui32 groupId) {
        GroupId = groupId;
    }
    void Bootstrap(const TActorContext &ctx) {
        BootstrapImpl(ctx);
    }

protected:
    void SendRequests() {
        while (RequestInflight > 0 && RequestCount > 0) {
            RequestInflight--;
            RequestCount--;
            SendRequest();
        }
    }

    void HandleReply(NKikimrProto::EReplyStatus status) {
        if (status == NKikimrProto::OK) {
            OKs++;
        } else {
            Fails++;
        }
        ++RequestInflight;
        SendRequests();
    }

    virtual void BootstrapImpl(const TActorContext &ctx) = 0;
    virtual void SendRequest() = 0;

protected:
    ui32 RequestCount;
    ui32 RequestInflight;
    ui32 GroupId;

public:
    ui32 OKs = 0;
    ui32 Fails = 0;
};

void Test(const TBlobStorageGroupInfo::TTopology& topology, TInflightActor* actor) {
    const ui32 groupSize = topology.TotalVDisks;
    const auto& groupErasure = topology.GType;
    TEnvironmentSetup env{{
        .NodeCount = groupSize,
        .Erasure = groupErasure,
    }};

    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));

    NKikimrBlobStorage::TConfigRequest request;
    request.AddCommand()->MutableQueryBaseConfig();
    auto response = env.Invoke(request);

    const auto& baseConfig = response.GetStatus(0).GetBaseConfig();
    UNIT_ASSERT_VALUES_EQUAL(baseConfig.GroupSize(), 1);
    ui32 groupId = baseConfig.GetGroup(0).GetGroupId();
    std::vector<ui32> pdiskIds(groupSize);
    for (const auto& vslot : baseConfig.GetVSlot()) {
        const auto& vslotId = vslot.GetVSlotId();
        ui32 orderNumber = topology.GetOrderNumber(TVDiskIdShort(vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()));
        if (vslot.GetGroupId() == groupId) {
            pdiskIds[orderNumber] = vslotId.GetPDiskId();
        }
    }

    ui64 dsproxyCost = 0;
    ui64 vdiskCost = 0;
    ui64 hugePuts = 0;
    ui64 logPuts = 0;

    auto* appData = env.Runtime->GetAppData();
    Y_VERIFY(appData);

    auto vdisksTotal = [&](TString subsystem, TString counter, bool derivative = false) {
        ui64 ctr = 0;
        for (ui32 i = 0; i < groupSize; ++i) {
            ctr += GetServiceCounters(appData->Counters, "vdisks")->
                    GetSubgroup("storagePool", env.StoragePoolName)->
                    GetSubgroup("group", std::to_string(groupId))->
                    GetSubgroup("orderNumber", "0" + std::to_string(i))->
                    GetSubgroup("pdisk", "00000" + std::to_string(pdiskIds[i]))->
                    GetSubgroup("media", "rot")->
                    GetSubgroup("subsystem", subsystem)->
                    GetCounter(counter, derivative)->Val();
        }
        return ctr;
    };

    auto updateCounters = [&]() {
        dsproxyCost = GetServiceCounters(appData->Counters, "dsproxynode")->
                GetSubgroup("subsystem", "request")->
                GetSubgroup("storagePool", env.StoragePoolName)->
                GetCounter("DSProxyDiskCostNs")->Val();
        vdiskCost = vdisksTotal("outofspace", "EstimatedDiskTimeConsumptionNs");
        logPuts = vdisksTotal("skeletonfront", "SkeletonFront/LogPuts/CostProcessed");
        hugePuts = vdisksTotal("skeletonfront", "SkeletonFront/HugePutsForeground/CostProcessed");
    };

    updateCounters();
    UNIT_ASSERT_VALUES_EQUAL(dsproxyCost, vdiskCost);
    
    actor->SetGroupId(groupId);
    env.Runtime->Register(actor, 1);
    env.Sim(TDuration::Minutes(15));

    updateCounters();

    TStringStream str;
    double proportion = 1. * dsproxyCost / vdiskCost;
    i64 diff = (i64)dsproxyCost - vdiskCost;
    str << "OKs# " << actor->OKs << ", Fails# " << actor->Fails << ", Cost on dsproxy# "
            << dsproxyCost << ", Cost on vdisks# " << vdiskCost << ", proportion# " << proportion
            << " diff# " << diff << " hugePuts# " << hugePuts << " logPuts# " << logPuts;

    if constexpr(VERBOSE) {
        Cerr << str.Str() << Endl;
        // env.Runtime->GetAppData()->Counters->OutputPlainText(Cerr);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(dsproxyCost, vdiskCost, str.Str());
}

class TInflightActorPut : public TInflightActor {
public:
    TInflightActorPut(ui32 requests, ui32 inflight, ui32 dataSize = 1024)
        : TInflightActor(requests, inflight)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvStatusResult::EventType, SendRequests);
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
        auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 10, DataSize, RequestCount + 1),
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
    auto actor = new TInflightActor##requestType(requests, inflight);               \
    Test(topology, actor);                                                          \
}

#define MAKE_TEST_W_DATASIZE(erasure, requestType, requests, inflight, dataSize)                        \
Y_UNIT_TEST(Test##requestType##erasure##Requests##requests##Inflight##inflight##BlobSize##dataSize) {   \
    auto groupType = TBlobStorageGroupType::Erasure##erasure;                                           \
    ui32 realms = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 1;                       \
    ui32 domains = (groupType == TBlobStorageGroupType::ErasureMirror3dc) ? 3 : 8;                      \
    TBlobStorageGroupInfo::TTopology topology(groupType, realms, domains, 1, true);                     \
    auto actor = new TInflightActor##requestType(requests, inflight, dataSize);                         \
    Test(topology, actor);                                                                              \
}

class TInflightActorGet : public TInflightActor {
public:
    TInflightActorGet(ui32 requests, ui32 inflight, ui32 dataSize = 1024)
        : TInflightActor(requests, inflight)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvPutResult::EventType, SendRequests);
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
