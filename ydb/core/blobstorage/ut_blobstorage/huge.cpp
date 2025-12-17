#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/erasure/erasure.h>

namespace {

    class THugeBlobTest {
        TEnvironmentSetup Env;
        TTestActorSystem& Runtime;
        ui32 DataSize = 32_KB;
        TString Data = FastGenDataForLZ4(DataSize, 1 /*seed*/);
        TLogoBlobID BlobId{1000, 1, 1, 0, DataSize, 0};
        ui32 GroupId;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TBlobStorageGroupType GType;
        TBlobStorageGroupInfo::TVDiskIds VDiskIds;
        TBlobStorageGroupInfo::TServiceIds ServiceIds;
        std::vector<TActorId> PutQueueIds;
        std::vector<TActorId> GetQueueIds;
        std::vector<TRope> Parts;
        ui8 TestSubgroupNodeId = 6;
        TActorId DSProxyId;

    public:

        static THugeBlobTest CreateHugeBlobTest() {
            TFeatureFlags ff;
            return THugeBlobTest(std::move(ff));
        }

        THugeBlobTest(TFeatureFlags&& featureFlags)
            : Env{{
                    .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                    .FeatureFlags = std::move(featureFlags),
                    .UseFakeConfigDispatcher = true,
                }}
            , Runtime(*Env.Runtime)
        {
            Env.CreateBoxAndPool(1, 1);

            std::vector<ui32> groups = Env.GetGroups();
            UNIT_ASSERT(!groups.empty());
            GroupId = groups.front();

            Info = Env.GetGroupInfo(GroupId);
            GType = Info->Type;

            Info->PickSubgroup(BlobId.Hash(), &VDiskIds, &ServiceIds);

            for (const TVDiskID& vdiskId : VDiskIds) {
                PutQueueIds.push_back(Env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0));
                GetQueueIds.push_back(Env.CreateQueueActor(vdiskId, NKikimrBlobStorage::GetFastRead, 0));
            }

            Parts.resize(GType.TotalPartCount());
            const bool success = ErasureSplit(TErasureType::CrcModeNone, GType, TRope(Data), Parts, nullptr);
            UNIT_ASSERT(success);
        }

        void MultiplePutViaDsProxy(ui32 dataSize1, ui32 dataSize2) {
            TString data1 = FastGenDataForLZ4(dataSize1, 1 /*seed*/);
            TLogoBlobID blobId1{1000, 1, 1, 0, dataSize1, 0};

            auto* put1 = new TEvBlobStorage::TEvPut(blobId1, data1, TInstant::Max(),
                NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault);

            TLogoBlobID blobId2{1000, 2, 1, 0, dataSize2, 0};
            TString data2 = FastGenDataForLZ4(dataSize2, 1 /*seed*/);
            auto* put2 = new TEvBlobStorage::TEvPut(blobId2, data2, TInstant::Max(),
                NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault);

            auto edge = Runtime.AllocateEdgeActor(DSProxyId.NodeId(), __FILE__, __LINE__);
            Runtime.Send(new IEventHandle(DSProxyId, edge, put1), DSProxyId.NodeId());
            Runtime.Send(new IEventHandle(DSProxyId, edge, put2), DSProxyId.NodeId());

            for (size_t i = 0; i < 2; ++i) {
                auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
                auto status = res->Get()->Status;
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
            }
            Env.Runtime->DestroyActor(edge);
        }

        void CreateDSProxy() {
            DSProxyId = Env.CreateRealDSProxy(GroupId, TestSubgroupNodeId);
        }

        void SetHugeBlobSizeOnAllVDisks(ui32 hugeBlobSize) {
            Cerr << "new MinHugeBlobSize# " << hugeBlobSize << Endl;

            for (auto& serviceId : ServiceIds) {
                auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
                auto& record = ev->Record;
                auto& config = *record.MutableConfig();
                auto& bsConfig = *config.MutableBlobStorageConfig();
                auto& perf = *bsConfig.MutableVDiskPerformanceSettings();
                auto& type = *perf.AddVDiskTypes();
                type.SetPDiskType(NKikimrBlobStorage::EPDiskType::ROT);
                type.SetMinHugeBlobSizeInBytes(hugeBlobSize);
                const ui32 serviceNodeId = serviceId.NodeId();
                TActorId edge = Runtime.AllocateEdgeActor(serviceNodeId, __FILE__, __LINE__);
                Runtime.Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(serviceNodeId), edge, ev.release()),
                    serviceNodeId);
                Env.WaitForEdgeActorEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
            }
        }
    };

}

Y_UNIT_TEST_SUITE(HugeBlobOnlineSizeChange) {

    Y_UNIT_TEST(SendHugeViaDSProxy) {
        // https://github.com/ydb-platform/ydb/issues/30753
        THugeBlobTest test = THugeBlobTest::CreateHugeBlobTest();
        // On VDisk over MockPDisk HugeBlobSize=32513 will be 28673 because of different chunk and append block size
        // this makes target part size 28672 (since with header it is 28680 which is > 28673 on VDisk, but DSProxy didn't account for the header)
        test.SetHugeBlobSizeOnAllVDisks(32513);
        test.CreateDSProxy();
        // Before the fix both puts would go in a one MultiPut which is illegal
        // since VDisk will treat first blob as huge and huge blobs are prohibited in MultiPuts
        // DSProxy should receive MinHugeBlobSizeInBytes with header size accounted for
        test.MultiplePutViaDsProxy(114561 /* PartSize=28672 */, 1024);
        // In a fixed version 28672 + max header size (8) = 28680, so DSProxy knows that blob is huge
        // and will not try to batch it in MultiPut
    }

}
