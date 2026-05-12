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
            ff.SetForceDistconfDisable(true);
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
            const bool success = ErasureSplit(TErasureType::CrcModeNone, GType, TRope(Data), Parts,
                nullptr, GetDefaultRcBufAllocator());
            UNIT_ASSERT(success);
        }

        void Put(ui8 subgroupNodeId, ui8 partIdx) {
            Cerr << "writing partIdx# " << (int)partIdx << " to " << (int)subgroupNodeId << Endl;
            const TActorId queueActorId = PutQueueIds[subgroupNodeId];
            auto edge = Runtime.AllocateEdgeActor(queueActorId.NodeId(), __FILE__, __LINE__);
            const TLogoBlobID putId(BlobId, partIdx + 1);
            Runtime.Send(new IEventHandle(queueActorId, edge, new TEvBlobStorage::TEvVPut(putId, Parts[partIdx],
                VDiskIds[subgroupNodeId], false, nullptr, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false)), queueActorId.NodeId());
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge);
            auto& record = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
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

        void SwitchHugeBlobSize(bool small) {
            Cerr << "small blob# " << small << Endl;

            auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            auto& record = ev->Record;
            auto& config = *record.MutableConfig();
            auto& bsConfig = *config.MutableBlobStorageConfig();
            auto& perf = *bsConfig.MutableVDiskPerformanceSettings();
            auto& type = *perf.AddVDiskTypes();
            type.SetPDiskType(NKikimrBlobStorage::EPDiskType::ROT);
            type.SetMinHugeBlobSizeInBytes(small ? 4096 : 524288);

            const ui32 serviceNodeId = ServiceIds[TestSubgroupNodeId].NodeId();
            TActorId edge = Runtime.AllocateEdgeActor(serviceNodeId, __FILE__, __LINE__);
            Runtime.Send(new IEventHandle(NConsole::MakeConfigsDispatcherID(serviceNodeId), edge, ev.release()),
                serviceNodeId);
            Env.WaitForEdgeActorEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
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

        void CompactFresh() {
            Cerr << "compacting fresh" << Endl;

            const TActorId serviceId = ServiceIds[TestSubgroupNodeId];
            const ui32 serviceNodeId = serviceId.NodeId();
            TActorId edge = Runtime.AllocateEdgeActor(serviceNodeId, __FILE__, __LINE__);
            Runtime.Send(new IEventHandle(serviceId, edge, TEvCompactVDisk::Create(EHullDbType::LogoBlobs,
                TEvCompactVDisk::EMode::FRESH_ONLY)), serviceNodeId);
            Env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge);
        }

        void CompactLevels() {
            Cerr << "compacting levels" << Endl;

            const TActorId serviceId = ServiceIds[TestSubgroupNodeId];
            const ui32 serviceNodeId = serviceId.NodeId();
            TActorId edge = Runtime.AllocateEdgeActor(serviceNodeId, __FILE__, __LINE__);
            Runtime.Send(new IEventHandle(serviceId, edge, TEvCompactVDisk::Create(EHullDbType::LogoBlobs,
                TEvCompactVDisk::EMode::FULL)), serviceNodeId);
            Env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge);
       }

        void PutPartsInMask(ui32 mask) {
            for (ui32 i = 0; i < GType.TotalPartCount(); ++i) {
                if (mask & (1 << i)) {
                    Put(TestSubgroupNodeId, i);
                }
            }
        }

        void CheckPartsInPlace(ui32 mask, ui32 numDistinctSST) {
            Cerr << "checking parts in place mask# " << mask << Endl;

            std::set<ui64> sstIds;
            const TActorId serviceId = ServiceIds[TestSubgroupNodeId];
            auto captureRes = Env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult,
                TEvBlobStorage::TEvCaptureVDiskLayout>(serviceId);
            for (const auto& item : captureRes->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs) {
                    Cerr << item.ToString() << Endl;
                    if (item.RecordType == T::ERecordType::IndexRecord && item.SstId) {
                        sstIds.insert(item.SstId);
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(sstIds.size(), numDistinctSST);

            auto getQuery = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(VDiskIds[TestSubgroupNodeId],
                TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead, {}, {}, {{BlobId}});
            const TActorId queueId = GetQueueIds[TestSubgroupNodeId];
            const ui32 queueNodeId = queueId.NodeId();
            auto edge = Runtime.AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Runtime.Send(new IEventHandle(queueId, edge, getQuery.release()), queueNodeId);
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
            auto& record = res->Get()->Record;

            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            if (!mask) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                auto& result = record.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(result.GetBlobID()), BlobId);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::NODATA);
            } else {
                for (const auto& result : record.GetResult()) {
                    const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(result.GetBlobID());
                    UNIT_ASSERT_VALUES_EQUAL(id.FullID(), BlobId);
                    UNIT_ASSERT(id.PartId());
                    const ui8 partIdx = id.PartId() - 1;

                    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_EQUAL(res->Get()->GetBlobData(result), Parts[partIdx]);
                    UNIT_ASSERT(mask & (1 << partIdx));

                    mask &= ~(1 << partIdx);
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(mask, 0);
        }

        void RunTest(ui32 fresh1, ui32 fresh2, ui32 huge1, ui32 huge2, bool targetHuge, ui32 fresh3, ui32 huge3,
                bool targetHuge2, bool targetHuge3) {
            if (fresh1 || fresh2) {
                SwitchHugeBlobSize(false);
                PutPartsInMask(fresh1);
                PutPartsInMask(fresh2);
                CheckPartsInPlace(fresh1 | fresh2, 0);
            }
            if (huge1 || huge2) {
                SwitchHugeBlobSize(true);
                PutPartsInMask(huge1);
                PutPartsInMask(huge2);
                CheckPartsInPlace(huge1 | huge2 | fresh1 | fresh2, 0);
            }
            const ui32 baseMask = fresh1 | fresh2 | huge1 | huge2;
            if (baseMask) {
                SwitchHugeBlobSize(targetHuge);
                CompactFresh();
                CheckPartsInPlace(baseMask, 1);
            }
            if (const ui32 extraMask = fresh3 | huge3) {
                if (fresh3) {
                    SwitchHugeBlobSize(false);
                    PutPartsInMask(fresh3);
                }
                if (huge3) {
                    SwitchHugeBlobSize(true);
                    PutPartsInMask(huge3);
                }
                SwitchHugeBlobSize(targetHuge2);
                CompactFresh();
                CheckPartsInPlace(baseMask | extraMask, !!baseMask + !!extraMask);

                SwitchHugeBlobSize(targetHuge3);
                CompactLevels();
                CheckPartsInPlace(baseMask | extraMask, 1);
            }
        }

        static void CompactionTest() {
            for (ui32 fresh1 = 0; fresh1 < 8; ++fresh1) {
            for (ui32 fresh2 = 0; fresh2 < 2; ++fresh2) {
            for (ui32 huge1 = 0; huge1 < 4; ++huge1) {
            for (ui32 huge2 = 0; huge2 < 2; ++huge2) {
            for (bool targetHuge : {true, false}) {
            for (ui32 fresh3 = 0; fresh3 < 4; ++fresh3) {
            for (ui32 huge3 = 0; huge3 < 2; ++huge3) {
            for (bool targetHuge2 : {true, false}) {
            for (bool targetHuge3 : {true, false}) {
                Cerr << "fresh1# " << fresh1 << " fresh2# " << fresh2 << " huge1# " << huge1
                    << " huge2# " << huge2 << " targetHuge# " << targetHuge
                    << " fresh3# " << fresh3
                    << " huge3# " << huge3
                    << " targetHuge2# " << targetHuge2
                    << " targetHuge3# " << targetHuge3
                    << Endl;
                THugeBlobTest test = THugeBlobTest::CreateHugeBlobTest();
                test.RunTest(fresh1, fresh2, huge1, huge2, targetHuge, fresh3, huge3, targetHuge2, targetHuge3);
            }}}}}}}}}
        }
    };

}

Y_UNIT_TEST_SUITE(HugeBlobOnlineSizeChange) {

    Y_UNIT_TEST(Compaction) {
        THugeBlobTest::CompactionTest();
    }

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
