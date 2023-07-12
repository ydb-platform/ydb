#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(Get) {

    void SendGet(
        const TTestInfo &test,
        const TLogoBlobID &blobId,
        const TString &data,
        NKikimrProto::EReplyStatus status,
        std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> readerTabletData = {},
        std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> forceBlockTabletData = {},
        bool mustRestoreFirst = false,
        bool isIndexOnly = false,
        bool checkResultData = true)
    {
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> getQueries{new TEvBlobStorage::TEvGet::TQuery[1]};
        getQueries[0].Id = blobId;
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(getQueries, 1, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead, mustRestoreFirst, isIndexOnly, forceBlockTabletData);
        ev->ReaderTabletData = readerTabletData;
        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });
        std::unique_ptr<IEventHandle> handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvGetResult);
        TEvBlobStorage::TEvGetResult *getResult = handle->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_VALUES_EQUAL(getResult->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(getResult->Responses[0].Status, status);
        if (checkResultData && status == NKikimrProto::EReplyStatus::OK) {
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

    Y_UNIT_TEST(TestBlockedEvGetRequest) {
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
        SendGet(test, originalBlobId, data, NKikimrProto::OK, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));

        // block tablet generation
        ui32 blockedTabletGeneration = tabletGeneration + 1;
        auto ev = std::make_unique<TEvBlobStorage::TEvBlock>(tabletId, blockedTabletGeneration, TInstant::Max());

        test.Runtime->WrapInActorContext(test.Edge, [&] {
            SendToBSProxy(test.Edge, test.Info->GroupID, ev.release());
        });

        auto handle = test.Runtime->WaitForEdgeActorEvent({test.Edge});
        UNIT_ASSERT_EQUAL(handle->Type, TEvBlobStorage::EvBlockResult);
        auto blockResult = handle->Get<TEvBlobStorage::TEvBlockResult>();
        UNIT_ASSERT(blockResult);

        // check that TEvGet still returns OK for blocked generation without reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK);
        // check that now TEvGet returns BLOCKED for blocked generation with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::BLOCKED, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));
    }

    Y_UNIT_TEST(TestForceBlockTabletDataWithIndexRestoreGetRequest) {
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
        SendGet(test, originalBlobId, data, NKikimrProto::OK, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));

        // block tablet generation
        ui32 blockedTabletGeneration = tabletGeneration + 1;
        SendGet(test, originalBlobId, data, NKikimrProto::OK, std::nullopt, TEvBlobStorage::TEvGet::TForceBlockTabletData(tabletId, blockedTabletGeneration), false, true, false);

        // check that TEvGet still returns OK for blocked generation without reader params
        SendGet(test, originalBlobId, data, NKikimrProto::OK);
        // check that now TEvGet returns BLOCKED for blocked generation with reader params
        SendGet(test, originalBlobId, data, NKikimrProto::BLOCKED, TEvBlobStorage::TEvGet::TReaderTabletData(tabletId, tabletGeneration));
    }
}
