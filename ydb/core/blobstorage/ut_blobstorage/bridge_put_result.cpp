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

    // Two puts go to a remote pile the interpile way in one batch. The VDisks there never answer the second one; the
    // first one has a short deadline and must end by it, not wait for the whole batch.
    Y_UNIT_TEST(InterpileBatchKeepsDeadlineOfEachPut) {
        TEnvironmentSetup env(MakeSettings(true));
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        const TString data = "blob put in a batch";
        Put(env, groupId, TLogoBlobID(100500, 1, 1, 0, data.size(), 0), data);
        env.Sim(TDuration::Seconds(5));

        const TLogoBlobID shortId(100500, 1, 2, 0, data.size(), 0);
        const TLogoBlobID stuckId(100500, 1, 3, 0, data.size(), 0);
        ui32 maxInterpileBatch = 0;
        ui32 numDropped = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                maxInterpileBatch = Max<ui32>(maxInterpileBatch, ev->Get<TEvInterpilePut>()->Record.ItemsSize());
            }
            auto isStuck = [&](const auto& blobId, const auto& vdiskId) {
                return LogoBlobIDFromLogoBlobID(blobId).FullID() == stuckId &&
                    VDiskIDFromVDiskID(vdiskId).GroupID.GetRawId() != pileGroups[0];
            };
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut && ev->HasEvent()) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                if (isStuck(record.GetBlobID(), record.GetVDiskID())) {
                    ++numDropped;
                    return false;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVMultiPut && ev->HasEvent()) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVMultiPut>()->Record;
                for (const auto& item : record.GetItems()) {
                    if (isStuck(item.GetBlobID(), record.GetVDiskID())) {
                        ++numDropped;
                        return false;
                    }
                }
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(10);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(shortId, data, deadline), 1);
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(stuckId, data, TInstant::Max()), 2);
        });
        std::unique_ptr<TEvBlobStorage::TEvPutResult> shortResult;
        // the stuck put ends by the maximum put timeout of the proxy, a minute; the put with the short deadline must
        // not wait for that
        const TInstant waitDeadline = deadline + TDuration::Seconds(5);
        while (!shortResult) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, waitDeadline);
            UNIT_ASSERT_C(res, "no reply to the put with the short deadline soon after its deadline");
            if (res->Get()->Id == shortId) {
                shortResult.reset(res->Release().Release());
            }
        }
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(maxInterpileBatch >= 2, "the puts didn't go the interpile way in one batch");
        UNIT_ASSERT(numDropped);
    }

}
