#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <ydb/core/blobstorage/dsproxy/group_sessions.h>

#include <util/stream/null.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(CountingEvents) {

    struct TTestInfo {
        std::unique_ptr<TTestActorSystem> &Runtime;
        TActorId Edge;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
    };

    void SendPut(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPut>(blobId, data, TInstant::Max());

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});

        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvPutResult);
        TEvBlobStorage::TEvPutResult *putResult = handle->Get<TEvBlobStorage::TEvPutResult>();
        UNIT_ASSERT_EQUAL(putResult->Status, status);
    };

    void SendGet(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status)
    {
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvGet>(blobId, 0, 0, TInstant::Max(), NKikimrBlobStorage::AsyncRead);
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Buffer.ConvertToString(), data);
    };

    void SendCollect(const TTestInfo &test, const TLogoBlobID &blobId,
            NKikimrProto::EReplyStatus status)
    {
        std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(blobId.TabletID(), blobId.Generation(),
            blobId.Step(), blobId.Channel(), true, blobId.Generation(), blobId.Step(), nullptr, nullptr, TInstant::Max(), false);
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvCollectGarbageResult);
        TEvBlobStorage::TEvCollectGarbageResult *collectResult = handle->Get<TEvBlobStorage::TEvCollectGarbageResult>();
        UNIT_ASSERT_EQUAL(collectResult->Status, status);
    }

    TIntrusivePtr<TGroupQueues> ReceiveGroupQueues(const TTestInfo &test) {
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, new TEvRequestProxySessionsState);
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL_C(handle->Type, TEvBlobStorage::EvProxySessionsState, "expected# " << (ui64)TEvBlobStorage::EvProxySessionsState
                << " given# " << handle->Type);
        TEvProxySessionsState *state = handle->Get<TEvProxySessionsState>();
        return state->GroupQueues;
    }

    void NormalizePredictedDelays(const TIntrusivePtr<TGroupQueues> &queues) {
        for (TGroupQueues::TFailDomain &domain : queues->FailDomains) {
            for (TGroupQueues::TVDisk &vDisk : domain.VDisks) {
                ui32 begin = NKikimrBlobStorage::EVDiskQueueId::Begin;
                ui32 end = NKikimrBlobStorage::EVDiskQueueId::End;
                for (ui32 id = begin; id < end; ++id) {
                    NKikimrBlobStorage::EVDiskQueueId vDiskQueueId = static_cast<NKikimrBlobStorage::EVDiskQueueId>(id);
                    auto flowRecord = vDisk.Queues.FlowRecordForQueueId(vDiskQueueId);
                    if (flowRecord) {
                        flowRecord->SetPredictedDelayNs(1'000'000);
                    }
                }
            }
        }
    }


    void CountingEventsTest(TString typeOperation, ui32 eventsCount, TBlobStorageGroupType groupType)
    {
        TEnvironmentSetup env({
            .VDiskReplPausedAtStart = true,
            .Erasure = groupType,
            .UseActorSystemTimeInBSQueue = false,
        });
        auto& runtime = env.Runtime;

        bool printEvents = false;
        ui32 eventCtr = 0;
        
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (printEvents) {
                Ctest << "Counter# " << eventCtr++
                        << " Type# " << ev->GetTypeRewrite()
                        << " Name# " << ev->GetTypeName()
                        << " ToString()# " << ev->ToString() << Endl;
            }
            return true;
        };

        env.CreateBoxAndPool();
        env.CommenceReplication();

        auto groups = env.GetGroups();
        auto info = env.GetGroupInfo(groups[0]);

        const TActorId& edge = runtime->AllocateEdgeActor(1);
        TTestInfo test{runtime, edge, info};

        // set predicted a response time
        TIntrusivePtr<TGroupQueues> queues = ReceiveGroupQueues(test);

        constexpr ui32 size = 100;
        TString data(size, 'a');
        ui64 tabletId = 1;

        ui64 startEventsCount = 0;
        ui64 finishEventsCount = 0;

        if (typeOperation == "put") {
            TLogoBlobID originalBlobId(tabletId, 1, 0, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendPut(test, originalBlobId, data, NKikimrProto::OK);

            printEvents = true;
            startEventsCount = test.Runtime->GetEventsProcessed();
            TLogoBlobID originalBlobId2(tabletId, 1, 1, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendPut(test, originalBlobId2, data, NKikimrProto::OK);
            finishEventsCount = test.Runtime->GetEventsProcessed();
            printEvents = false;

            UNIT_ASSERT_VALUES_EQUAL(finishEventsCount - startEventsCount, eventsCount);

            startEventsCount = test.Runtime->GetEventsProcessed();
            TLogoBlobID originalBlobId3(tabletId, 1, 2, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendPut(test, originalBlobId3, data, NKikimrProto::OK);
            finishEventsCount = test.Runtime->GetEventsProcessed();

            UNIT_ASSERT_VALUES_EQUAL(finishEventsCount - startEventsCount, eventsCount);
        } else if (typeOperation == "get") {
            TLogoBlobID originalBlobId(tabletId, 1, 0, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendPut(test, originalBlobId, data, NKikimrProto::OK);
            NormalizePredictedDelays(queues);
            SendGet(test, originalBlobId, data, NKikimrProto::OK);

            printEvents = true;
            startEventsCount = test.Runtime->GetEventsProcessed();
            NormalizePredictedDelays(queues);
            SendGet(test, originalBlobId, data, NKikimrProto::OK);
            finishEventsCount = test.Runtime->GetEventsProcessed();
            printEvents = false;

            UNIT_ASSERT_VALUES_EQUAL(finishEventsCount - startEventsCount, eventsCount);

            startEventsCount = test.Runtime->GetEventsProcessed();
            NormalizePredictedDelays(queues);
            SendGet(test, originalBlobId, data, NKikimrProto::OK);
            finishEventsCount = test.Runtime->GetEventsProcessed();

            UNIT_ASSERT_VALUES_EQUAL(finishEventsCount - startEventsCount, eventsCount);
        } else if (typeOperation == "collect") {
            TLogoBlobID originalBlobId(tabletId, 1, 0, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendPut(test, originalBlobId, data, NKikimrProto::OK);

            printEvents = true;
            startEventsCount = test.Runtime->GetEventsProcessed();
            TLogoBlobID originalBlobId2(tabletId, 1, 1, 0, size, 0);
            NormalizePredictedDelays(queues);
            SendCollect(test, originalBlobId2, NKikimrProto::OK);
            finishEventsCount = test.Runtime->GetEventsProcessed();
            printEvents = false;

            UNIT_ASSERT_VALUES_EQUAL(finishEventsCount - startEventsCount, eventsCount);
        }
    }

    Y_UNIT_TEST(Put_Mirror3of4) {
        CountingEventsTest("put", 116, TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(Put_Mirror3dc) {
        CountingEventsTest("put", 49, TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(Put_Block42) {
        CountingEventsTest("put", 89, TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(Put_None) {
        CountingEventsTest("put", 19, TBlobStorageGroupType::ErasureNone);
    }

    Y_UNIT_TEST(Get_Mirror3of4) {
        return; // Flaky after adding random into strategy; TODO(kruall): FIX IT
        CountingEventsTest("get", 36, TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(Get_Mirror3dc) {
        CountingEventsTest("get", 14, TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(Get_Block42) {
        CountingEventsTest("get", 69, TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(Get_None) {
        CountingEventsTest("get", 14, TBlobStorageGroupType::ErasureNone);
    }

    Y_UNIT_TEST(Collect_Mirror3of4) {
        CountingEventsTest("collect", 112, TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(Collect_Mirror3dc) {
        CountingEventsTest("collect", 124, TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(Collect_Block42) {
        CountingEventsTest("collect", 112, TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(Collect_None) {
        CountingEventsTest("collect", 16, TBlobStorageGroupType::ErasureNone);
    }
}

