#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(BlobStorageBlockRace) {
    void RunTest(TBlobStorageGroupType erasure) {
        TEnvironmentSetup env{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        auto sendBlock = [&](ui32 orderNum, ui64 tabletId, ui64 guid) {
            const TVDiskID vdiskId = info->GetVDiskId(orderNum);

            NKikimrProto::EReplyStatus status;

            env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
                const TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVBlock(tabletId, 1, vdiskId,
                    TInstant::Max(), TWriteSource::Unknown, guid)), edge.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVBlockResult>(edge);
                status = res->Get()->Record.GetStatus();
            });

            return status;
        };

        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 1, 0), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 1, 0), NKikimrProto::ALREADY);
        env.Sim(TDuration::Seconds(10));
        for (ui32 orderNum = 1; orderNum < info->GetTotalVDisksNum(); ++orderNum) {
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 1, 0), NKikimrProto::ALREADY);
        }

        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 2, 1), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(sendBlock(0, 2, 1), NKikimrProto::OK);
        env.Sim(TDuration::Seconds(10));
        for (ui32 orderNum = 1; orderNum < info->GetTotalVDisksNum(); ++orderNum) {
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 0), NKikimrProto::ALREADY);
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 1), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(sendBlock(orderNum, 2, 2), NKikimrProto::ALREADY);
        }
    }

    Y_UNIT_TEST(Test) { RunTest(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(TestBlock82) { RunTest(TBlobStorageGroupType::Erasure8Plus2Block); }

    void RunBlocksRacingViaSyncLog(TBlobStorageGroupType erasure) {
        TEnvironmentSetup env{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        }};

        ui32 numVDiskSyncEvents = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVSyncResult:
                case TEvBlobStorage::EvVSyncFullResult:
                    ++numVDiskSyncEvents;
                    break;
            }
            return true;
        };

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        env.Sim(TDuration::Seconds(30));

        const TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        const ui64 tabletId = 1;
        const ui32 generation = 1;
        const ui64 guid1 = 1;
        const ui64 guid2 = 2;
        ui32 responsesPending = 0;

        std::vector<TActorId> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            queues.push_back(env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
                1000));
        }

        auto issue = [&](ui32 begin, ui32 end, ui64 guid) {
            for (ui32 i = begin; i < end; ++i) {
                env.Runtime->Send(new IEventHandle(queues[i], edge, new TEvBlobStorage::TEvVBlock(tabletId, generation,
                    info->GetVDiskId(i), TInstant::Max(), TWriteSource::Unknown, guid)), edge.NodeId());
                ++responsesPending;
            }
        };

        auto checkStatus = [&](NKikimrProto::EReplyStatus expected) {
            for (; responsesPending; --responsesPending) {
                auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVBlockResult>(edge, false);
                const auto& record = ev->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), expected);
            }
        };

        numVDiskSyncEvents = 0;
        issue(0, info->Type.Handoff(), guid1);
        issue(info->Type.Handoff(), info->GetTotalVDisksNum(), guid2);
        checkStatus(NKikimrProto::OK);
        issue(info->Type.Handoff(), info->GetTotalVDisksNum(), guid1);
        checkStatus(NKikimrProto::ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(numVDiskSyncEvents, 0);

        // wait for sync to complete
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(numVDiskSyncEvents);

        issue(info->Type.Handoff(), info->GetTotalVDisksNum(), guid1);
        checkStatus(NKikimrProto::ALREADY);
        issue(0, info->Type.Handoff(), guid1);
        checkStatus(NKikimrProto::OK);

        issue(info->Type.Handoff(), info->GetTotalVDisksNum(), guid2);
        checkStatus(NKikimrProto::OK);
        issue(0, info->Type.Handoff(), guid2);
        checkStatus(NKikimrProto::ALREADY);
    }

    Y_UNIT_TEST(BlocksRacingViaSyncLog) { RunBlocksRacingViaSyncLog(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(BlocksRacingViaSyncLogBlock82) { RunBlocksRacingViaSyncLog(TBlobStorageGroupType::Erasure8Plus2Block); }

    void RunBlocksRacingViaSyncLog2(TBlobStorageGroupType erasure) {
        TEnvironmentSetup env{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .Erasure = erasure,
        }};

        ui32 numVDiskSyncEvents = 0;
        std::deque<std::pair<ui32, std::unique_ptr<IEventHandle>>> postponedEventQ;
        bool postponeMainNodes = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVSyncResult:
                case TEvBlobStorage::EvVSyncFullResult:
                    ++numVDiskSyncEvents;
                    break;
            }
            if (postponeMainNodes && ev->GetTypeRewrite() == TEvBlobStorage::EvLog && nodeId >= 1 && nodeId <= erasure.TotalPartCount()) {
                postponedEventQ.emplace_back(nodeId, std::exchange(ev, nullptr));
                return false;
            }
            return true;
        };
        auto sendPostponed = [&] {
            for (auto& [nodeId, ev] : std::exchange(postponedEventQ, {})) {
                env.Runtime->Send(ev.release(), nodeId);
            }
        };

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        env.Sim(TDuration::Seconds(30));

        const TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        const ui64 tabletId = 1;
        const ui32 generation = 1;
        const ui64 guid1 = 1;
        const ui64 guid2 = 2;

        std::vector<TActorId> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            queues.push_back(env.CreateQueueActor(info->GetVDiskId(i), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
                1000));
            UNIT_ASSERT_VALUES_EQUAL(info->GetActorId(i).NodeId(), i + 1);
        }

        auto issue = [&](ui32 begin, ui32 end, ui64 guid) {
            for (ui32 i = begin; i < end; ++i) {
                env.Runtime->Send(new IEventHandle(queues[i], edge, new TEvBlobStorage::TEvVBlock(tabletId, generation,
                    info->GetVDiskId(i), TInstant::Max(), TWriteSource::Unknown, guid)), edge.NodeId());
            }
        };

        auto checkStatus = [&](ui32 num, NKikimrProto::EReplyStatus expected) {
            for (; num; --num) {
                auto ev = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVBlockResult>(edge, false);
                const auto& record = ev->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), expected);
            }
        };

        numVDiskSyncEvents = 0;
        postponeMainNodes = true;
        issue(0, erasure.TotalPartCount(), guid1);
        issue(erasure.TotalPartCount(), erasure.BlobSubgroupSize(), guid2);
        checkStatus(2, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(numVDiskSyncEvents, 0);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(numVDiskSyncEvents);
        postponeMainNodes = false;
        sendPostponed();
        checkStatus(erasure.TotalPartCount(), NKikimrProto::OK);
        env.Sim(TDuration::Seconds(10));
        issue(0, erasure.TotalPartCount(), guid2);
        checkStatus(erasure.TotalPartCount(), NKikimrProto::ALREADY);
        issue(0, erasure.TotalPartCount(), guid1);
        checkStatus(erasure.TotalPartCount(), NKikimrProto::OK);
    }

    Y_UNIT_TEST(BlocksRacingViaSyncLog2) { RunBlocksRacingViaSyncLog2(TBlobStorageGroupType::Erasure4Plus2Block); }
    Y_UNIT_TEST(BlocksRacingViaSyncLog2Block82) { RunBlocksRacingViaSyncLog2(TBlobStorageGroupType::Erasure8Plus2Block); }
}
