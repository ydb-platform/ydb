#include "flat_boot_cookie.h"
#include "flat_boot_oven.h"
#include "flat_executor_gclogic.h"
#include "flat_sausage_grind.h"
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {

constexpr ui32 HistoryCutterUtBlobSize = 42;

TLogoBlobID HistoryCutterUtBlob(ui64 tabletId, ui32 generation, ui32 channel) {
    return TLogoBlobID(tabletId, generation, 0, channel, HistoryCutterUtBlobSize, 0);
}

// The allocator is only exercised on the WriteToLog/SendCollectGarbage paths;
// ApplyLogEntry touches only ChannelInfo and HistoryCutter.
TAutoPtr<NPageCollection::TSteppedCookieAllocator> MakeGCCookies(
        const TTabletStorageInfo& info, ui32 generation = 1) {
    // Every channel needs a slot or the allocator asserts; the group is a placeholder.
    TVector<NPageCollection::TSlot> slots;
    for (ui32 ch = 0; ch < (ui32)info.Channels.size(); ++ch) {
        const ui32 group = info.Channels[ch].History.empty()
            ? 1u : info.Channels[ch].History.front().GroupID;
        slots.emplace_back(static_cast<ui8>(ch), group);
    }
    return new NPageCollection::TSteppedCookieAllocator(
        info.TabletID,
        ui64(generation) << 32,
        NPageCollection::TCookieRange{0, 999},
        TArrayRef<const NPageCollection::TSlot>(slots)
    );
}

// Drives one SendCollectGarbage pass from inside the actor system, since the call
// needs a real TActorContext to dispatch TEvCollectGarbage.
class TCollectGarbageDriver : public NActors::TActorBootstrapped<TCollectGarbageDriver> {
public:
    TCollectGarbageDriver(TExecutorGCLogic* logic, NActors::TActorId done)
        : Logic(logic), Done(done) {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Logic->SendCollectGarbage(ctx);
        ctx.Send(Done, new NActors::TEvents::TEvWakeup());
        Die(ctx);
    }

private:
    TExecutorGCLogic* const Logic;
    const NActors::TActorId Done;
};

} // namespace

Y_UNIT_TEST_SUITE(TFlatTableExecutorGC) {
    bool TestDeduplication(TVector<TLogoBlobID> keep, TVector<TLogoBlobID> dontkeep, ui32 gen, ui32 step, TVector<TLogoBlobID> expectKeep, TVector<TLogoBlobID> expectnot) {
        DeduplicateGCKeepVectors(&keep, &dontkeep, gen, step);
        return (keep == expectKeep) && (dontkeep == expectnot);
    }

    Y_UNIT_TEST(TestGCVectorDeduplicaton) {
        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            }
        ));


        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 3,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            }
        ));
    }
}


Y_UNIT_TEST_SUITE(THistoryCutter) {
    Y_UNIT_TEST(TestHistoryCutter) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(1, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        ui32 group = 0;
        for (ui32 gen : {1, 2, 5, 6, 7, 9, 10}) {
            info->Channels[0].History.emplace_back(gen, ++group);
        }
        THistoryCutter cutter(info);
        for (ui32 gen : {3, 4, 8, 9}) {
            cutter.SeenBlob(TLogoBlobID(1, gen, 1, 0, 42, 0));
        }
        std::vector<const TTabletChannelInfo::THistoryEntry*> toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
        UNIT_ASSERT_VALUES_EQUAL(toCut[2]->FromGeneration, 6);
    }



    Y_UNIT_TEST(NoCutsWhenHistoryHasLessThanTwoEntries) {
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            THistoryCutter cutter(info);
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
        {
            TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(7, TTabletTypes::Dummy);
            info->Channels.emplace_back();
            info->Channels[0].History.emplace_back(1, 100);
            THistoryCutter cutter(info);
            cutter.SeenBlob(HistoryCutterUtBlob(7, 999, 0));
            UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
        }
    }

    Y_UNIT_TEST(BecomeUncertainDisablesCutsForThatChannel) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(2, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 10);
        info->Channels[0].History.emplace_back(100, 20);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(BecomeUncertainDoesNotAffectOtherChannels) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(3, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.BecomeUncertain(0);
        // Channel 1: no blobs seen in [1, 10) => first history entry is cuttable.
        auto toCut = cutter.GetHistoryToCut(1);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(ForeignTabletBlobIsIgnored) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(4, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(99999, 5, 0)); // wrong tablet
        // No valid seen generations => entire first segment looks empty.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
    }

    Y_UNIT_TEST(SeenGenerationInsideRangeBlocksCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(5, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(5, 50, 0)); // 50 in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT(toCut.empty());
    }

    Y_UNIT_TEST(SeenGenerationAtNextBoundaryAllowsCutOfPreviousSegment) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(6, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10, 1);
        info->Channels[0].History.emplace_back(100, 2);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(6, 100, 0)); // first seen at next boundary, none in [10, 100)
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 10);
    }

    Y_UNIT_TEST(LastHistoryEntryIsNeverCut) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(8, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(5, 2);
        info->Channels[0].History.emplace_back(9, 3);
        THistoryCutter cutter(info);
        // No blobs seen — both leading segments cuttable; latest entry (9) must not appear.
        auto toCut = cutter.GetHistoryToCut(0);
        UNIT_ASSERT_VALUES_EQUAL(toCut.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(toCut[0]->FromGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(toCut[1]->FromGeneration, 5);
    }

    Y_UNIT_TEST(ChannelIsolation) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(9, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        info->Channels.emplace_back();
        info->Channels[1].History.emplace_back(1, 3);
        info->Channels[1].History.emplace_back(10, 4);
        THistoryCutter cutter(info);
        cutter.SeenBlob(HistoryCutterUtBlob(9, 5, 0)); // only channel 0
        UNIT_ASSERT(cutter.GetHistoryToCut(1).size() == 1);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    Y_UNIT_TEST(DuplicateSeenBlobIsIdempotent) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(10, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(1, 1);
        info->Channels[0].History.emplace_back(10, 2);
        THistoryCutter cutter(info);
        const auto b = HistoryCutterUtBlob(10, 5, 0);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        cutter.SeenBlob(b);
        UNIT_ASSERT(cutter.GetHistoryToCut(0).empty());
    }

    // A pending DoNotKeep mark must pin the history entry that resolves its
    // blob's generation to a group, or the entry is cut before GC delivers the
    // flag and the group becomes irresolvable. Ablation: drop the SeenBlob call
    // in ApplyDelta's delta.Deleted loop and GetHistoryToCut returns [10, 100).
    Y_UNIT_TEST(DeletedBlobInApplyDeltaPinsHistoryEntry) {
        const ui64 tabletId = 30;

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        // Two history entries: [10, 100) -> group 1, [100, inf) -> group 2.
        info->Channels[0].History.emplace_back(10u, 1u);
        info->Channels[0].History.emplace_back(100u, 2u);

        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info));

        // Put a DoNotKeep blob at generation 50 (inside [10, 100)) into Deleted.
        TGCBlobDelta delta;
        delta.Deleted.push_back(HistoryCutterUtBlob(tabletId, 50, 0));

        // ApplyLogEntry is the public entry point; it calls ApplyDelta internally.
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // The history entry covering [10, 100) must be blocked: generation 50
        // was seen there, so the entry must not appear in the cut list.
        auto toCut = gcLogic.HistoryCutter.GetHistoryToCut(0);
        UNIT_ASSERT_C(toCut.empty(),
            "history entry [10, 100) must not be cuttable while gen-50 blob has a pending DoNotKeep mark");
    }

    // The precondition the sentinel guard relies on: generations below the first
    // surviving history entry resolve to Max<ui32>().
    Y_UNIT_TEST(GroupForGenerationReturnsSentinelBelowFirstEntry) {
        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(31, TTabletTypes::Dummy);
        info->Channels.emplace_back();
        info->Channels[0].History.emplace_back(10u, 77u); // first entry starts at gen 10

        // A generation strictly below the first entry must resolve to Max<ui32>().
        ui32 group = info->Channels[0].GroupForGeneration(5);
        UNIT_ASSERT_VALUES_EQUAL_C(group, Max<ui32>(),
            "GroupForGeneration must return Max<ui32>() for generations below the first history entry");
    }

    // SendCollectGarbage used to dereference &affectedGroups[GroupForGeneration(gen)]
    // unchecked; below the first surviving entry that is Max<ui32>(), and the collect
    // sent there ended in a BS error -> TEvPoison -> boot loop. Setup mirrors the state
    // after a cut: one history entry from generation 10, GC marks left for 5 and 6.
    // Ablation: drop the `if (vec)` guards and the sentinel proxy receives a collect.
    Y_UNIT_TEST(SendCollectGarbageSkipsSentinelGroup) {
        const ui64 tabletId = 32;
        const ui32 channel = 2;
        const ui32 survivingGroup = 77;
        const ui32 survivingFromGen = 10;

        TTestBasicRuntime runtime(1);
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        runtime.Initialize(app->Unwrap());

        // Stand in for the BS proxies. Without these the collect requests resolve to no
        // mailbox and are dropped before anything can observe them.
        const auto survivingEdge = runtime.AllocateEdgeActor();
        const auto sentinelEdge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageProxyID(survivingGroup), survivingEdge);
        runtime.RegisterService(MakeBlobStorageProxyID(Max<ui32>()), sentinelEdge);

        TIntrusivePtr<TTabletStorageInfo> info = new TTabletStorageInfo(tabletId, TTabletTypes::Dummy);
        info->Channels.resize(channel + 1);
        for (ui32 ch = 0; ch <= channel; ++ch) {
            info->Channels[ch].Channel = ch;
            // Single entry: everything below survivingFromGen resolves to the sentinel.
            info->Channels[ch].History.emplace_back(survivingFromGen, survivingGroup);
        }

        // Tablet generation must exceed the blob generations below so they are collectable.
        TExecutorGCLogic gcLogic(info, MakeGCCookies(*info, 20));
        gcLogic.FollowersSyncComplete(true);

        TGCBlobDelta delta;
        // Generations 5 and 6 sit below the surviving entry -> sentinel group.
        delta.Created.push_back(HistoryCutterUtBlob(tabletId, 5, channel));
        delta.Deleted.push_back(HistoryCutterUtBlob(tabletId, 6, channel));
        // Generation 12 resolves normally and must still be collected.
        delta.Created.push_back(HistoryCutterUtBlob(tabletId, 12, channel));
        TGCLogEntry entry(TGCTime(1, 1), delta);
        gcLogic.ApplyLogEntry(entry);

        // Count collect requests per proxy. GrabEdgeEvent cannot express "nothing
        // arrived" -- it throws once the queue drains -- so observe and count instead.
        THashMap<TActorId, ui32> collectsByProxy;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                ++collectsByProxy[ev->Recipient];
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto done = runtime.AllocateEdgeActor();
        runtime.Register(new TCollectGarbageDriver(&gcLogic, done));
        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(done);

        // The collect is dispatched before the driver's wakeup, so by now it has been observed.
        UNIT_ASSERT_C(collectsByProxy[MakeBlobStorageProxyID(survivingGroup)] > 0,
            "the surviving history entry must still receive its collect request");
        UNIT_ASSERT_VALUES_EQUAL_C(collectsByProxy.Value(MakeBlobStorageProxyID(Max<ui32>()), 0u), 0u,
            "a collect request must never be addressed to the Max<ui32>() sentinel group");
        UNIT_ASSERT_VALUES_EQUAL_C(collectsByProxy.size(), 1u,
            "collects must go to the surviving group only");
        // The guard's second observable: monitoring reports both below-sentinel marks
        // (the gen-5 keep and the gen-6 delete) as dropped.
        UNIT_ASSERT_VALUES_EQUAL(gcLogic.TakeSentinelDroppedMarks(), 2u);
    }

    Y_UNIT_TEST(HistoryCuttingUnsoundForExternalBlobWriters) {
        // The seen-generations criterion only covers executor-written blobs; a tablet
        // whose channels also hold externally-written blobs (ColumnShard portions via
        // TBlobManager) must not be cut by the executor at all. Observed live: entries
        // cut under external blobs leave GroupFor() resolving to Max<ui32> and GC
        // retrying an invalid group until the tablet is unusable.
        auto makeInfo = [](TTabletTypes::EType type) {
            auto info = MakeIntrusive<TTabletStorageInfo>();
            info->TabletID = 1;
            info->TabletType = type;
            return info;
        };
        using NTabletFlatExecutor::TExecutorGCLogic;
        const auto columnShard = makeInfo(TTabletTypes::ColumnShard);
        // Channels 0/1 carry only executor blobs even on ColumnShard, so they stay with this cutter.
        UNIT_ASSERT(TExecutorGCLogic::IsHistoryCuttingSound(*columnShard, 0));
        UNIT_ASSERT(TExecutorGCLogic::IsHistoryCuttingSound(*columnShard, 1));
        UNIT_ASSERT(!TExecutorGCLogic::IsHistoryCuttingSound(*columnShard, 2));
        UNIT_ASSERT(!TExecutorGCLogic::IsHistoryCuttingSound(*columnShard, 65));
        UNIT_ASSERT(TExecutorGCLogic::IsHistoryCuttingSound(*makeInfo(TTabletTypes::DataShard), 2));
        UNIT_ASSERT(TExecutorGCLogic::IsHistoryCuttingSound(*makeInfo(TTabletTypes::KeyValue), 2));
    }

    // A blob that is deleted but not yet collected keeps its DoNotKeep mark in the GC
    // deltas; cutting its entry makes the mark undeliverable, so the delta must feed
    // the cutter. Observed live: GC to the sentinel group retried forever on channel 1.
    Y_UNIT_TEST(PendingDeleteDeltaPinsHistoryEntry) {
        static constexpr ui64 TabletId = 42;
        static constexpr ui32 Channel = 1;
        auto info = MakeIntrusive<TTabletStorageInfo>(TabletId, TTabletTypes::Dummy);
        info->Channels.resize(Channel + 1);
        info->Channels[Channel].Channel = Channel;
        info->Channels[Channel].History.emplace_back(0, 100);
        info->Channels[Channel].History.emplace_back(10, 200);

        NBoot::TSteppedCookieAllocatorFactory cookies(*info, /*gen=*/10);
        TExecutorGCLogic gcLogic(info, cookies.Sys(NBoot::TCookie::EIdx::GCExt));
        UNIT_ASSERT_VALUES_EQUAL(gcLogic.HistoryCutter.GetHistoryToCut(Channel).size(), 1);

        TGCLogEntry entry(TGCTime(10, 1));
        entry.Delta.Deleted.push_back(TLogoBlobID(TabletId, /*gen=*/3, /*step=*/1, Channel, HistoryCutterUtBlobSize, 0));
        gcLogic.ApplyLogEntry(entry);
        UNIT_ASSERT_C(gcLogic.HistoryCutter.GetHistoryToCut(Channel).empty(),
            "an entry with a pending DoNotKeep mark must not be cuttable");
    }

    Y_UNIT_TEST(CreatedDeltaPinsHistoryEntry) {
        static constexpr ui64 TabletId = 43;
        static constexpr ui32 Channel = 1;
        auto info = MakeIntrusive<TTabletStorageInfo>(TabletId, TTabletTypes::Dummy);
        info->Channels.resize(Channel + 1);
        info->Channels[Channel].Channel = Channel;
        info->Channels[Channel].History.emplace_back(0, 100);
        info->Channels[Channel].History.emplace_back(10, 200);

        NBoot::TSteppedCookieAllocatorFactory cookies(*info, /*gen=*/10);
        TExecutorGCLogic gcLogic(info, cookies.Sys(NBoot::TCookie::EIdx::GCExt));

        TGCLogEntry entry(TGCTime(10, 1));
        entry.Delta.Created.push_back(TLogoBlobID(TabletId, /*gen=*/3, /*step=*/1, Channel, HistoryCutterUtBlobSize, 0));
        gcLogic.ApplyLogEntry(entry);
        UNIT_ASSERT_C(gcLogic.HistoryCutter.GetHistoryToCut(Channel).empty(),
            "an entry with a live blob must not be cuttable");
    }
}

}
}
