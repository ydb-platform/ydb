#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(SnapshotTesting) {
    Y_UNIT_TEST(Compaction) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1, 0, NKikimrBlobStorage::EPDiskType::SSD);
        env.Sim(TDuration::Seconds(1));
        auto groups = env.GetGroups();
        auto groupInfo = env.GetGroupInfo(groups.front());

        const TVDiskID vdiskId = groupInfo->GetVDiskId(0);
        const TActorId vdiskActorId = groupInfo->GetActorId(0);
        const TActorId queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, 0);
        const TActorId edge = runtime->AllocateEdgeActor(queue.NodeId(), __FILE__, __LINE__);

        // put some data
        const ui64 tabletId = 1000;
        const ui8 channel = 0;
        const ui32 generation = 1;
        ui32 step = 1;
        THashMap<TLogoBlobID, TString> blobs;
        std::unordered_set<ui32> chunksWithData;

        auto putDataChunk = [&](bool putToBlobs) {
            Cerr << "putDataChunk" << Endl;
            const TInstant begin = runtime->GetClock();

            for (ui32 i = 0; i < 100; ++i) {
                const ui32 len = 1 + RandomNumber(131072u);
                TString data = env.GenerateRandomString(len);
                const TLogoBlobID id(tabletId, generation, step, channel, len, 0, 1);
                auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(id, TRope(data), vdiskId, false, nullptr, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog);
                runtime->Send(new IEventHandle(queue, edge, ev.release()), queue.NodeId());
                if (putToBlobs) {
                    blobs.emplace(id, std::move(data));
                }
                ++step;

                { // check result
                    auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, false);
                    const auto& record = ev->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(record.GetBlobID()), id);
                }
            }

            Cerr << "putDataChunk done duration# " << runtime->GetClock() - begin << Endl;
        };

        auto doCompact = [&] {
            Cerr << "doCompact" << Endl;
            const TInstant begin = runtime->GetClock();

            auto ev = std::make_unique<IEventHandle>(vdiskActorId, edge, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
            ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, vdiskActorId);
            runtime->Send(ev.release(), edge.NodeId());
            env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge, false);

            Cerr << "doCompact done duration# " << runtime->GetClock() - begin << Endl;
        };

        for (;;) {
            putDataChunk(true);
            doCompact();

            // check if it was compacted
            bool done = false;
            auto layout = env.SyncQuery<TEvBlobStorage::TEvCaptureVDiskLayoutResult, TEvBlobStorage::TEvCaptureVDiskLayout>(vdiskActorId);
            for (const auto& item : layout->Layout) {
                using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
                if (item.Database == T::EDatabase::LogoBlobs && item.RecordType == T::ERecordType::InplaceBlob) {
                    done = true;
                    break;
                }
            }
            if (done) {
                break;
            }
        }

        auto checkReadable = [&](std::optional<TString> snapshotId, bool shouldBeReadable) {
            ui32 numUnreadable = 0;

            Cerr << "checkReadable" << Endl;
            const TInstant begin = runtime->GetClock();

            for (const auto& [id, data] : blobs) {
                auto ev = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead);
                ev->AddExtremeQuery(id, 0, 0);
                if (snapshotId) {
                    ev->Record.SetSnapshotId(*snapshotId);
                }
                runtime->Send(new IEventHandle(queue, edge, ev.release()), queue.NodeId());
                { // check result
                    auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, false);
                    const auto& record = ev->Get()->Record;
                    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(record.ResultSize(), 1);
                    const auto& result = record.GetResult(0);
                    if (shouldBeReadable) {
                        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetBlobData(result).ConvertToString(), data);
                    } else if (result.GetStatus() == NKikimrProto::NODATA) {
                        ++numUnreadable;
                    }
                }
            }

            Cerr << "checkReadable done duration# " << runtime->GetClock() - begin << Endl;

            return numUnreadable;
        };

        checkReadable(std::nullopt, true);

        TString snapshotId = "snap";
        {
            auto res = env.SyncQuery<TEvBlobStorage::TEvVTakeSnapshotResult, TEvBlobStorage::TEvVTakeSnapshot>(vdiskActorId,
                vdiskId, snapshotId, 100 * 86400);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimrProto::OK);
        };

        auto setBarrier = [&] {
            Cerr << "setBarrier" << Endl;
            const TInstant begin = runtime->GetClock();

            auto ev = std::make_unique<TEvBlobStorage::TEvVCollectGarbage>(tabletId, generation, step, channel, true,
                generation, step, false, nullptr, nullptr, vdiskId, TInstant::Max());
            runtime->Send(new IEventHandle(queue, edge, ev.release()), queue.NodeId());
            { // check result
                auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVCollectGarbageResult>(edge, false);
                const auto& record = ev->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            }
            ++step;

            Cerr << "setBarrier done duration# " << runtime->GetClock() - begin << Endl;
        };

        for (ui32 i = 0; i < 250 || checkReadable(std::nullopt, false) == 0; ++i) {
            Cerr << "iteration# " << i << " Clock# " << runtime->GetClock() << Endl;
            putDataChunk(false);
            doCompact();
            if (i % 10 == 9) {
                setBarrier();
                doCompact();
            }
        }

        checkReadable(snapshotId, true);
    }
}
