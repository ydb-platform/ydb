#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // the event goes to NodeWarden, as the reply to the put it made for an interpile request
    bool IsToNodeWarden(TEnvironmentSetup& env, const IEventHandle& ev) {
        IActor *actor = env.Runtime->GetActor(ev.GetRecipientRewrite());
        return actor && TypeName(*actor).Contains("TNodeWarden");
    }

    TEnvironmentSetup::TSettings MakeSettings(bool interpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(interpileTrafficOptimization);
        return {
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        };
    }

    std::unique_ptr<TEvBlobStorage::TEvPutResult> Put(TEnvironmentSetup& env, ui32 groupId, const TLogoBlobID& id,
            const TString& data) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        return std::unique_ptr<TEvBlobStorage::TEvPutResult>(res->Release().Release());
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgePutResult) {

    // The put checks the block of another tablet as well, as BlobDepot does for the tablets it writes for. That
    // tablet is blocked, so the put made the interpile way in a remote pile must be refused too, not only the one
    // made in the pile of the writer.
    Y_UNIT_TEST(InterpilePutChecksExtraBlocks) {
        TEnvironmentSetup env(MakeSettings(true));
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();

        const TString data = "blob of a blocked tablet";
        Put(env, groupId, TLogoBlobID(100500, 1, 1, 0, data.size(), 0), data);
        env.Sim(TDuration::Seconds(5));

        const ui64 blockedTabletId = 100600;
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvBlock(blockedTabletId, 5, TInstant::Max()));
        });
        auto block = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(block->Get()->Status, NKikimrProto::OK, block->Get()->ToString());

        const TLogoBlobID id(100500, 1, 2, 0, data.size(), 0);
        std::vector<NKikimrProto::EReplyStatus> remoteStatuses; // of the puts made for interpile requests
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->HasEvent() && IsToNodeWarden(env, *ev)) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>(); msg->Id == id) {
                    remoteStatuses.push_back(msg->Status);
                }
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            auto *put = new TEvBlobStorage::TEvPut(id, data, TInstant::Max());
            put->ExtraBlockChecks.emplace_back(blockedTabletId, 5);
            SendToBSProxy(edge, groupId, put);
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        // the refusal of the local pile may end the put before the remote piles answer
        for (ui32 i = 0; remoteStatuses.empty() && i < 300; ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_UNEQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_C(!remoteStatuses.empty(), "no write reached a remote pile the interpile way");
        for (const auto status : remoteStatuses) {
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::BLOCKED);
        }
    }

}
