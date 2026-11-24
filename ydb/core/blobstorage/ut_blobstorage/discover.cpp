#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(Discover) {

    void SendGet(const TTestInfo &test, const TLogoBlobID &blobId, const TString &data,
            NKikimrProto::EReplyStatus status, std::optional<ui64> readerTabletId = {}, std::optional<ui32> readerTabletGeneration = {})
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> getQueries{new TEvBlobStorage::TEvGet::TQuery[1]};
        getQueries[0].Id = blobId;
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(getQueries, 1, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead);
        if (readerTabletId && readerTabletGeneration) {
            ev->ReaderTabletData = {*readerTabletId, *readerTabletGeneration};
        }
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        if (status == NKikimrProto::EReplyStatus::OK) {
            UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Buffer.ConvertToString(), data);
        }
    }

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
    }

    Y_UNIT_TEST(TestForceBlockedGenerationTEvDiscoverRequest) {
        TEnvironmentSetup env(true);
        TTestInfo test = InitTest(env);

        ui64 tabletId = 1;
        ui32 tabletGeneration = 1;

        constexpr ui32 size = 100;
        TString data(size, 'a');
        TLogoBlobID originalBlobId(tabletId, tabletGeneration, 0, 0, size, 0);

        // check that TEvGet returns OK without reader params
        SendPut(test, originalBlobId, data, NKikimrProto::OK);
        // check that TEvGet returns OK with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK, tabletId, tabletGeneration);

        // block tablet generation
        ui32 blockedTabletGeneration = tabletGeneration + 1;
        auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(
            tabletId,
            blockedTabletGeneration,
            false,
            true,
            TInstant::Max(),
            blockedTabletGeneration,
            true
        );

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });

        auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvDiscoverResult);
        auto discoverResult = handle->Get<TEvBlobStorage::TEvDiscoverResult>();
        UNIT_ASSERT(discoverResult);

        // check that TEvGet still returns OK for blocked generation without reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK);
        // check that now TEvGet returns BLOCKED for blocked generation with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::BLOCKED, tabletId, tabletGeneration);
    }

    void PutBlobs(TEnvironmentSetup& env, TActorId edge, ui32 groupId, const std::vector<TLogoBlobID>& ids,
            const TString& data) {
        for (const TLogoBlobID& id : ids) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvDiscoverResult>> RunDiscover(TEnvironmentSetup& env, TActorId edge,
            ui32 groupId, ui64 tabletId, bool discoverBlockedGeneration) {
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvDiscover(tabletId, 0, true, discoverBlockedGeneration,
                TInstant::Max(), 0, true));
        });
        return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(edge, false);
    }

    void BlockedGenerationTest(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env{{.Erasure = erasure}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);

        const ui64 tabletId = 1;
        const ui32 blockedGeneration = 5;
        const TString data = "hello, world";
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
        PutBlobs(env, edge, groupId, {id}, data);

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvBlock(tabletId, blockedGeneration, TInstant::Max()));
        });
        const auto block = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(block->Get()->Status, NKikimrProto::OK);

        const auto res = RunDiscover(env, edge, groupId, tabletId, true);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Id, id);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->BlockedGeneration, blockedGeneration);
    }

    Y_UNIT_TEST(BlockedGeneration_block42) {
        BlockedGenerationTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(BlockedGeneration_mirror3dc) {
        BlockedGenerationTest(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(BlockedGeneration_mirror3of4) {
        BlockedGenerationTest(TBlobStorageGroupType::ErasureMirror3of4);
    }

    void UnreadableNewestBlobTest(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env{{.Erasure = erasure}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);

        const ui64 tabletId = 1;
        const TString data = "hello, world";
        const TLogoBlobID older(tabletId, 1, 1, 0, data.size(), 0);
        const TLogoBlobID newest(tabletId, 1, 2, 0, data.size(), 0);
        PutBlobs(env, edge, groupId, {older, newest}, data);
        env.Sim(TDuration::Seconds(10));

        // The newest blob is written in full, but reading it gives NODATA, as it would after its data was lost.
        // Discover must report the loss instead of returning the older blob as the newest one.
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->Recipient != edge) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->ResponseSz == 1 && msg->Responses[0].Id == newest) {
                    msg->Responses[0].Status = NKikimrProto::NODATA;
                    msg->Responses[0].Buffer = {};
                }
            }
            return true;
        };

        const auto res = RunDiscover(env, edge, groupId, tabletId, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::ERROR, "Id# " << res->Get()->Id);
    }

    Y_UNIT_TEST(UnreadableNewestBlob_block42) {
        UnreadableNewestBlobTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(UnreadableNewestBlob_mirror3dc) {
        UnreadableNewestBlobTest(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(UnreadableNewestBlob_mirror3of4) {
        UnreadableNewestBlobTest(TBlobStorageGroupType::ErasureMirror3of4);
    }
}
