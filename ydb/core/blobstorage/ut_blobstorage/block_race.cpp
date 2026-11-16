#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

Y_UNIT_TEST_SUITE(BlobStorageBlockRace) {
    Y_UNIT_TEST(Test) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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

    Y_UNIT_TEST(BlocksRacingViaSyncLog) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
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

    Y_UNIT_TEST(BlocksRacingViaSyncLog2) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};

        ui32 numVDiskSyncEvents = 0;
        std::deque<std::pair<ui32, std::unique_ptr<IEventHandle>>> postponedEventQ;
        bool postponeNodes1to6 = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVSyncResult:
                case TEvBlobStorage::EvVSyncFullResult:
                    ++numVDiskSyncEvents;
                    break;
            }
            if (postponeNodes1to6 && ev->GetTypeRewrite() == TEvBlobStorage::EvLog && nodeId >= 1 && nodeId <= 6) {
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
        postponeNodes1to6 = true;
        issue(0, 6, guid1);
        issue(6, 8, guid2);
        checkStatus(2, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(numVDiskSyncEvents, 0);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(numVDiskSyncEvents);
        postponeNodes1to6 = false;
        sendPostponed();
        checkStatus(6, NKikimrProto::OK);
        env.Sim(TDuration::Seconds(10));
        issue(0, 6, guid2);
        checkStatus(6, NKikimrProto::ALREADY);
        issue(0, 6, guid1);
        checkStatus(6, NKikimrProto::OK);
    }

    Y_UNIT_TEST(AlreadyWaitsForItsOwnLogCommit) {
        constexpr ui64 TabletId = 100500;
        constexpr ui32 Generation = 10;
        TEnvironmentSetup env{{.NodeCount = 1, .Erasure = TBlobStorageGroupType::ErasureNone}};
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Seconds(30));
        auto& runtime = env.Runtime;
        const auto info = env.GetGroupInfo(env.GetGroups().front());
        const auto vdisk = info->GetVDiskId(0);
        const auto queue = env.CreateQueueActor(vdisk, NKikimrBlobStorage::PutTabletLog, 1000);
        const auto writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const auto retry = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        bool holding = true;
        bool already = false;
        ui32 completedWrites = 0;
        std::vector<std::unique_ptr<IEventHandle>> logs;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (holding && ev->GetTypeRewrite() == NPDisk::TEvLogResult::EventType) {
                auto *actor = runtime->GetActor(ev->GetRecipientRewrite());
                if (actor && TypeName(*actor).Contains("TSkeleton")) {
                    logs.push_back(std::move(ev));
                    return false;
                }
            }
            if (ev->GetRecipientRewrite() == retry && ev->GetTypeRewrite() == TEvBlobStorage::EvVBlockResult) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvBlobStorage::TEvVBlockResult>()->Record.GetStatus(),
                    NKikimrProto::ALREADY);
                already = true;
                return false;
            }
            if (ev->GetRecipientRewrite() == writer && ev->GetTypeRewrite() == TEvBlobStorage::EvVBlockResult) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvBlobStorage::TEvVBlockResult>()->Record.GetStatus(),
                    NKikimrProto::OK);
                ++completedWrites;
                return false;
            }
            return true;
        };
        auto send = [&](ui64 tabletId, const TActorId& sender) {
            runtime->Send(new IEventHandle(queue, sender, new TEvBlobStorage::TEvVBlock(tabletId, Generation,
                vdisk, TInstant::Max())), 1);
            env.Sim(TDuration::MilliSeconds(100));
        };
        send(TabletId - 1, writer);
        UNIT_ASSERT_VALUES_EQUAL(logs.size(), 1);
        send(TabletId, writer);
        UNIT_ASSERT_VALUES_EQUAL(logs.size(), 2);
        send(TabletId, retry);
        UNIT_ASSERT(!already);

        holding = false;
        runtime->Send(logs.front().release(), 1);
        env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!already, "ALREADY was released by an earlier, unrelated LSN");
        UNIT_ASSERT_VALUES_EQUAL(completedWrites, 1);
        runtime->Send(logs.back().release(), 1);
        env.Sim(TDuration::MilliSeconds(100));
        UNIT_ASSERT(already);
        UNIT_ASSERT_VALUES_EQUAL(completedWrites, 2);
        runtime->FilterFunction = {};
    }
}
