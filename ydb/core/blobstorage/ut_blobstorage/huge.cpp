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
    public:
        THugeBlobTest(double defragThresholdToRunCompaction)
            : Env{{
                    .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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
            const bool success = ErasureSplit(TErasureType::CrcModeNone, GType, TRope(Data), Parts);
            UNIT_ASSERT(success);

            for (ui32 i = 1; i <= Env.Settings.NodeCount; ++i) {
                Env.SetIcbControl(i, "VDiskControls.DefragThresholdToRunCompactionPerMille", defragThresholdToRunCompaction * 1000);
            }

//            for (ui32 i = 0; i < 6; ++i) { // put main parts
//                Put(i, i);
//            }
        }

        void Put(ui8 subgroupNodeId, ui8 partIdx) {
            Cerr << "writing partIdx# " << (int)partIdx << " to " << (int)subgroupNodeId << Endl;
            const TActorId queueActorId = PutQueueIds[subgroupNodeId];
            auto edge = Runtime.AllocateEdgeActor(queueActorId.NodeId(), __FILE__, __LINE__);
            const TLogoBlobID putId(BlobId, partIdx + 1);
            Runtime.Send(new IEventHandle(queueActorId, edge, new TEvBlobStorage::TEvVPut(putId, Parts[partIdx],
                VDiskIds[subgroupNodeId], false, nullptr, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog)),
                queueActorId.NodeId());
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge);
            auto& record = res->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
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

        static void CompactionTest(double defragThresholdToRunCompaction) {
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
                THugeBlobTest test(defragThresholdToRunCompaction);
                test.RunTest(fresh1, fresh2, huge1, huge2, targetHuge, fresh3, huge3, targetHuge2, targetHuge3);
            }}}}}}}}}
        }
    };

}

Y_UNIT_TEST_SUITE(HugeBlobOnlineSizeChange) {

    Y_UNIT_TEST(Compaction) {
        THugeBlobTest::CompactionTest(0);
    }
    Y_UNIT_TEST(CompactionIndependenceWithDefrag) {
        THugeBlobTest::CompactionTest(0.001);
    }

}
