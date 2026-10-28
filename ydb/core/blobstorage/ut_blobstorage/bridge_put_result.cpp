#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

using namespace NBridgeTest;

namespace {

    // VDisks validating barriers report the blob written beyond the garbage collection barrier; the VDisks of the
    // piles chosen by markPile do so here for the blob of the test
    template<typename TMarkPile>
    void MarkWrittenBeyondBarrier(std::unique_ptr<IEventHandle>& ev, const TLogoBlobID& id,
            const std::vector<ui32>& pileGroups, TMarkPile&& markPile, ui32& numMarked) {
        auto isMarkedGroup = [&](const NKikimrBlobStorage::TVDiskID& vdiskId) {
            const ui32 groupId = VDiskIDFromVDiskID(vdiskId).GroupID.GetRawId();
            for (ui32 pile = 0; pile < pileGroups.size(); ++pile) {
                if (pileGroups[pile] == groupId) {
                    return markPile(pile);
                }
            }
            return false;
        };
        if (!ev->HasEvent()) {
            return;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPutResult) {
            auto& record = ev->Get<TEvBlobStorage::TEvVPutResult>()->Record;
            if (LogoBlobIDFromLogoBlobID(record.GetBlobID()).FullID() == id && isMarkedGroup(record.GetVDiskID())) {
                record.SetWrittenBeyondBarrier(true);
                ++numMarked;
            }
        } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvVMultiPutResult) {
            auto& record = ev->Get<TEvBlobStorage::TEvVMultiPutResult>()->Record;
            if (isMarkedGroup(record.GetVDiskID())) {
                for (auto& item : *record.MutableItems()) {
                    if (LogoBlobIDFromLogoBlobID(item.GetBlobID()).FullID() == id) {
                        item.SetWrittenBeyondBarrier(true);
                        ++numMarked;
                    }
                }
            }
        }
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

    // The writer on node 1 (pile 0) puts the blob, the VDisks of the piles chosen by markPile report it written
    // beyond the barrier; the writer must see that
    template<typename TMarkPile>
    void CheckWrittenBeyondBarrierReported(bool interpileTrafficOptimization, TMarkPile&& markPile) {
        TEnvironmentSetup env(MakeSettings(interpileTrafficOptimization));
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        // the interpile way is taken once the queues towards the remote piles have connected
        const TString data = "blob written beyond the barrier";
        Put(env, groupId, TLogoBlobID(100500, 1, 1, 0, data.size(), 0), data);
        env.Sim(TDuration::Seconds(5));

        const TLogoBlobID id(100500, 1, 2, 0, data.size(), 0);
        ui32 numMarked = 0;
        ui32 numInterpilePuts = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            numInterpilePuts += ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut;
            MarkWrittenBeyondBarrier(ev, id, pileGroups, markPile, numMarked);
            return true;
        };
        const auto res = Put(env, groupId, id, data);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(numMarked);
        UNIT_ASSERT(!interpileTrafficOptimization || numInterpilePuts);
        UNIT_ASSERT_C(res->WrittenBeyondBarrier, res->ToString());
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgePutResult) {

    // the VDisks of every pile report the write beyond the barrier
    Y_UNIT_TEST(WrittenBeyondBarrierReported) {
        CheckWrittenBeyondBarrierReported(false, [](ui32 /*pile*/) { return true; });
    }

    // only the VDisks of a remote pile do, and the write reaches them the interpile way
    Y_UNIT_TEST(WrittenBeyondBarrierReportedInterpile) {
        CheckWrittenBeyondBarrierReported(true, [](ui32 pile) { return pile != 0; });
    }

}
