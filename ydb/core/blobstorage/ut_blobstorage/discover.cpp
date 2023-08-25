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
}
