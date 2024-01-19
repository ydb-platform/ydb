#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(MultiGet) {

    Y_UNIT_TEST(SequentialGet) {
        TEnvironmentSetup env(false, TBlobStorageGroupType::Erasure4Plus2Block);
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool();
        const ui32 groupId = env.GetGroups().front();

        const TActorId& edge = runtime->AllocateEdgeActor(1);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        ui32 numInFlight = 0;

        constexpr ui32 blobsToSend = 10'000;

        for (ui32 i = 1; i <= blobsToSend; ++i) {
            const TString buffer = "A SMALL BLOB 16b";
            const TLogoBlobID id(1, 1, i, 0, buffer.size(), 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            ++numInFlight;
        }

        for (; numInFlight > 0; --numInFlight) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        auto rusage = TRusage::Get();
        const ui64 rssOnBegin = rusage.MaxRss;
        Cerr << "rssOnBegin# " << rssOnBegin << Endl;

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(1, TLogoBlobID(1, 0, 0, 0, 0, 0),
                TLogoBlobID(1, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie), false, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
            UNIT_ASSERT_EQUAL(res->Get()->Responses.size(), blobsToSend);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        rusage = TRusage::Get();
        const ui64 rssOnEnd = rusage.MaxRss;

        Cerr << rssOnBegin << " -> " << rssOnEnd << Endl;
    }

}
