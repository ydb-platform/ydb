#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(ExtraBlockChecks) {
    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        });

        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const auto& edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvBlock(1, 10, TInstant::Max()));
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvBlock(2, 10, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        const TString data = "data";

        runtime->WrapInActorContext(edge, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(TLogoBlobID(1, 11, 1, 0, data.size(), 1), data, TInstant::Max());
            ev->ExtraBlockChecks.emplace_back(2, 10);
            SendToBSProxy(edge, info->GroupID, ev.release());
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::BLOCKED);
        }

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(TLogoBlobID(1, 11, 1, 0, data.size(), 2), data, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        runtime->WrapInActorContext(edge, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(TLogoBlobID(1, 11, 1, 0, data.size(), 3), data, TInstant::Max());
            ev->ExtraBlockChecks.emplace_back(2, 10);
            SendToBSProxy(edge, info->GroupID, ev.release());
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(TLogoBlobID(1, 11, 1, 0, data.size(), 4), data, TInstant::Max()));
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::BLOCKED);
            res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }
}
