#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(DiscoverBlockedGeneration) {

    // The discover asks every VDisk for the blocked generation in parallel with its index scan. Here the get-block
    // replies are held until the scan has found the blob, so the answer is prepared before they arrive.
    void LateGetBlockRepliesTest(TBlobStorageGroupType::EErasureSpecies erasure) {
        TEnvironmentSetup env{{.Erasure = erasure}};
        env.CreateBoxAndPool();
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);

        const ui64 tabletId = 1;
        const ui32 blockedGeneration = 5;
        const TString data = "hello, world";
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvBlock(tabletId, blockedGeneration, TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);

        bool hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        std::unique_ptr<IEventHandle> result;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (hold && ev->GetTypeRewrite() == TEvBlobStorage::EvVGetBlockResult) {
                held.emplace_back(nodeId, std::move(ev));
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvDiscoverResult && ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvDiscover(tabletId, 0, true, true, TInstant::Max(), 0,
                true));
        });
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!held.empty());
        UNIT_ASSERT_C(!result, "the discover answered before the get-block replies");

        hold = false;
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        held.clear();
        env.Sim(TDuration::Seconds(1));
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(result);
        const TEvBlobStorage::TEvDiscoverResult *msg = result->Get<TEvBlobStorage::TEvDiscoverResult>();
        UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(msg->Id, id);
        UNIT_ASSERT_VALUES_EQUAL(msg->BlockedGeneration, blockedGeneration);
    }

    Y_UNIT_TEST(LateGetBlockReplies_block42) {
        LateGetBlockRepliesTest(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(LateGetBlockReplies_mirror3dc) {
        LateGetBlockRepliesTest(TBlobStorageGroupType::ErasureMirror3dc);
    }
}
