#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>

Y_UNIT_TEST_SUITE(BlobCorruption) {

enum EBlobSize : ui32 {
    Val_SmallBlob = 100,
    Val_HugeBlob = 4_MB,
};

enum ECorruptionMask : ui32 {
    Val_OneCorrupted                        = 0b000001,
    Val_TwoCorrruptedMain                   = 0b001001,
    Val_OneCorruptedMainOneCorruptedHandoff = 0b010001,
    Val_TwoCorruptedHandoff                 = 0b110000,
    Val_TwoCorrruptedInSameDc               = 0b000011,
};

struct TTestCtx : public TTestCtxBase {
    TTestCtx(TBlobStorageGroupType erasure, EBlobSize blobSize, ECorruptionMask partCorruptionMask)
        : TTestCtxBase(TEnvironmentSetup::TSettings{
            .NodeCount = erasure.BlobSubgroupSize() + 1,
            .Erasure = erasure,
            .PDiskSize = 10_TB,
            .EnableDeepScrubbing = true,
        })
        , BlobSize(blobSize)
        , PartCorruptionMask(partCorruptionMask)
    {}

    struct TAggregateScrubMetrics {
        TAggregateScrubMetrics(TString counterName, bool isHuge, TErasureType::EErasureSpecies erasure)
            : CounterName(counterName)
            , IsHuge(isHuge)
            , Erasure(erasure)
        {}

        ui64 operator()(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) const {
            return counters->GetSubgroup("subsystem", "deepScrubbing")
                    ->GetSubgroup("blobSize", IsHuge ? "huge" : "small")
                    ->GetSubgroup("erasure", TErasureType::ErasureSpeciesName(Erasure))
                    ->GetCounter(CounterName, false)->Val();
        }

        TString CounterName;
        bool IsHuge;
        TErasureType::EErasureSpecies Erasure;
    };

    TString MakePrefix() const {
        return TStringBuilder() << "CorruptedParts# " << Bin(PartCorruptionMask) << " NodesWithCorruptedParts# "
                << Bin(NodesWithCorruptedPartsMask) << " DisabledNodes# " << Bin(DisabledNodesMask) << " : ";
    }

    void Initialize() override {
        CreateOneGroup();
        AllocateEdgeActor(true);
        GetGroupStatus(GroupId);

        ui64 tabletId = 5000;
        ui32 channel = 1;
        ui32 generation = 1;
        ui32 step = 1;
        ui32 blobSize = (ui32)BlobSize;
        ui32 cookie = 1;
        TString data = MakeData(blobSize, 1);
        GroupInfo = Env->GetGroupInfo(GroupId);

        NodesWithCorruptedPartsMask = 0;
        DisabledNodesMask = 0;

        TLogoBlobID blobId(tabletId, generation, step, channel, blobSize, cookie);

        Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPut::EventType) {
                if (ev->Sender.NodeId() == ev->Recipient.NodeId()) {
                    return true;
                }
                auto* vput = ev->Get<TEvBlobStorage::TEvVPut>();
                TLogoBlobID partId = LogoBlobIDFromLogoBlobID(vput->Record.GetBlobID());
                if (PartCorruptionMask & (1 << (partId.PartId() - 1))) {
                    vput->Record.SetBuffer(MakeData(vput->GetBuffer().size(), 2));
                    NodesWithCorruptedPartsMask |= (1 << (ev->Recipient.NodeId() - 1));
                }
            }
            return true;
        };

        Env->Runtime->WrapInActorContext(Edge, [&] {
            TString data = MakeData(blobSize, 1);
            SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
        });
        auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(Edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_UNEQUAL(NodesWithCorruptedPartsMask, 0);

        Env->Runtime->FilterFunction = {};

        WriteCompressedData({
            .GroupId = GroupId,
            .TotalBlobs = 100,
            .BlobSize = blobSize,
        });

        Env->Runtime->FilterFunction = {};
    }

    void TestDeepScrubbing() {
        Initialize();
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto* setPeriod = request.AddCommand()->MutableSetScrubPeriodicity();
            setPeriod->SetScrubPeriodicity(1);
            auto response = Env->Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
        };

        // wait for full scrub cycle to finish
        for (ui32 orderNumber = 0; orderNumber < Erasure.BlobSubgroupSize(); ++orderNumber) {
            TActorId vdiskActorId = GroupInfo->GetActorId(orderNumber);
            const ui32 nodeId = vdiskActorId.NodeId();
            TActorId edge = Env->Runtime->AllocateEdgeActor(nodeId);
            if (((1 << (nodeId - 1)) & NodesWithCorruptedPartsMask) == 0) {
                continue;
            }
            while (true) {
                const ui64 cookie = RandomNumber<ui64>();
                Env->Runtime->Send(new IEventHandle(TEvBlobStorage::EvScrubAwait, 0, vdiskActorId, edge, nullptr, cookie), nodeId);
                auto ev = Env->WaitForEdgeActorEvent<TEvScrubNotify>(edge);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
                if (ev->Get()->Success) {
                    break;
                }
            }
        }

        std::vector<ui32> pdiskLayout = MakePDiskLayout(BaseConfig, GroupInfo->GetTopology(), GroupId);

        {
            ui64 blobsScrubbed =
                    Env->AggregateVDiskCountersWithCallback(Env->StoragePoolName, NodeCount, Erasure.BlobSubgroupSize(),
                            GroupId, pdiskLayout, TAggregateScrubMetrics("BlobsChecked", false, Erasure.GetErasure())) +
                    Env->AggregateVDiskCountersWithCallback(Env->StoragePoolName, NodeCount, Erasure.BlobSubgroupSize(),
                            GroupId, pdiskLayout, TAggregateScrubMetrics("BlobsChecked", true, Erasure.GetErasure()));
            ui64 dataIssues =
                    Env->AggregateVDiskCountersWithCallback(Env->StoragePoolName, NodeCount, Erasure.BlobSubgroupSize(),
                            GroupId, pdiskLayout, TAggregateScrubMetrics("DataIssues", false, Erasure.GetErasure())) +
                    Env->AggregateVDiskCountersWithCallback(Env->StoragePoolName, NodeCount, Erasure.BlobSubgroupSize(),
                            GroupId, pdiskLayout, TAggregateScrubMetrics("DataIssues", true, Erasure.GetErasure()));
    
            UNIT_ASSERT_VALUES_UNEQUAL_C(blobsScrubbed, 0, MakePrefix());
            UNIT_ASSERT_VALUES_UNEQUAL_C(dataIssues, 0, MakePrefix());
        }
    }

    void TestBlobChecker() {
        Initialize();
        Env->UpdateSettings({
            .BlobCheckerPeriodicity = TDuration::Seconds(10),
        });

        Env->Sim(TDuration::Seconds(100));
        
        {
            TAppData* appData = Env->Runtime->GetNode(Env->Settings.ControllerNodeId)->AppData.get();
            ui64 groupsChecked = GetServiceCounters(appData->Counters, "storage_pool_stat")
                    ->GetSubgroup("subsystem", "blob_checker")
                    ->GetCounter("ChecksCompleted", false)->Val();

            ui64 dataIssues = GetServiceCounters(appData->Counters, "storage_pool_stat")
                    ->GetSubgroup("subsystem", "blob_checker")
                    ->GetCounter("DataIssues", false)->Val();

            UNIT_ASSERT_VALUES_UNEQUAL_C(groupsChecked, 0, MakePrefix());
            UNIT_ASSERT_VALUES_UNEQUAL_C(dataIssues, 0, MakePrefix());
        }
    }

private:
    ui32 BlobSize;
    ui32 PartCorruptionMask;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    ui32 NodesWithCorruptedPartsMask = 0;
    ui32 DisabledNodesMask = 0;
};

#define DATA_CORRUPTION_TEST(detector, erasure, blobSize, corruptionMask)       \
Y_UNIT_TEST(Test##detector##erasure##blobSize##corruptionMask) {                \
    TTestCtx ctx(TBlobStorageGroupType::Erasure##erasure,                       \
            EBlobSize::Val_##blobSize, ECorruptionMask::Val_##corruptionMask);  \
    ctx.Test##detector();                                                       \
}

// TODO: fix and uncomment the tests below
// DATA_CORRUPTION_TEST(4Plus2Block, SmallBlob, OneCorrupted);
// DATA_CORRUPTION_TEST(4Plus2Block, HugeBlob, OneCorrupted);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, SmallBlob, TwoCorrruptedMain);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, HugeBlob, TwoCorrruptedMain);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, SmallBlob, OneCorruptedMainOneCorruptedHandoff);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, HugeBlob, OneCorruptedMainOneCorruptedHandoff);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, SmallBlob, TwoCorruptedHandoff);
DATA_CORRUPTION_TEST(DeepScrubbing, 4Plus2Block, HugeBlob, TwoCorruptedHandoff);
// DATA_CORRUPTION_TEST(Mirror3dc, SmallBlob, OneCorrupted);
// DATA_CORRUPTION_TEST(Mirror3dc, HugeBlob, OneCorrupted);
// DATA_CORRUPTION_TEST(Mirror3dc, SmallBlob, TwoCorrruptedInSameDc);
// DATA_CORRUPTION_TEST(Mirror3dc, HugeBlob, TwoCorrruptedInSameDc);

DATA_CORRUPTION_TEST(BlobChecker, 4Plus2Block, SmallBlob, TwoCorruptedHandoff);
DATA_CORRUPTION_TEST(BlobChecker, 4Plus2Block, SmallBlob, OneCorruptedMainOneCorruptedHandoff);
DATA_CORRUPTION_TEST(BlobChecker, 4Plus2Block, SmallBlob, OneCorrupted);
DATA_CORRUPTION_TEST(BlobChecker, 4Plus2Block, SmallBlob, TwoCorrruptedInSameDc);

#undef DATA_CORRUPTION_TEST
}
