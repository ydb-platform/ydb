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

template <typename TInflightActor>
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

    auto vdisksTotal = [&](TString subsystem, TString counter, bool derivative = false) {
        ui64 ctr = 0;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            auto* appData = env.Runtime->GetNode(vslot.GetVSlotId().GetNodeId())->AppData.get();
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
        }
        return ctr;
    };

    auto updateCounters = [&]() {
        for (const auto& vslot : baseConfig.GetVSlot()) {
            auto* appData = env.Runtime->GetNode(vslot.GetVSlotId().GetNodeId())->AppData.get();
            dsproxyCost += GetServiceCounters(appData->Counters, "dsproxynode")->
                    GetSubgroup("subsystem", "request")->
                    GetSubgroup("storagePool", env.StoragePoolName)->
                    GetCounter("DSProxyDiskCostNs")->Val();
        }
        vdiskCost = vdisksTotal("cost", "SkeletonFrontUserCostNs");
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
            << " diff# " << diff;

    if constexpr(VERBOSE) {
        Cerr << str.Str() << Endl;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(dsproxyCost, vdiskCost, str.Str());
}

class TInflightActorPut : public TInflightActor<TInflightActorPut> {
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

class TInflightActorGet : public TInflightActor<TInflightActorGet> {
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

class TInflightActorPatch : public TInflightActor<TInflightActorPatch> {
public:
    TInflightActorPatch(ui32 requests, ui32 inflight, ui32 dataSize = 1024)
        : TInflightActor(requests, inflight)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        hFunc(TEvBlobStorage::TEvPatchResult, Handle);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        TString data = MakeData(DataSize);
        for (ui32 i = 0; i < RequestInflight; ++i) {
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
        char c = 'a' + RequestCount % 26;
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
        if (++BlobsWritten == RequestInflight) {
            SendRequests();
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
