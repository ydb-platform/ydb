#include "barriers_tree.h"
#include "barriers_essence.h"
#include <util/random/fast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

//#define STR Cerr
#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageBarriersTreeTest) {

        static const TString VDiskLogPrefix = "PREFIX";

        class TWriter {
        public:
            TWriter()
                : Info(TBlobStorageGroupType::Erasure4Plus2Block, 1, 4)
            {
                TVDiskID vdisk0(0, 1, 0, 0 /*domain*/, 0 /*vdisk*/);
                TVDiskID vdisk1(0, 1, 0, 1 /*domain*/, 0 /*vdisk*/);
                TVDiskID vdisk2(0, 1, 0, 2 /*domain*/, 0 /*vdisk*/);
                TVDiskID vdisk3(0, 1, 0, 3 /*domain*/, 0 /*vdisk*/);
                Cache0 = TIngressCache::Create(Info.PickTopology(), vdisk0);
                Cache1 = TIngressCache::Create(Info.PickTopology(), vdisk1);
                Cache2 = TIngressCache::Create(Info.PickTopology(), vdisk2);
                Cache3 = TIngressCache::Create(Info.PickTopology(), vdisk3);
            }

            void Write(NBarriers::TTree &tree, const TKeyBarrier &key, ui32 collectGen, ui32 collectStep) {
                for (const auto &x : {Cache0, Cache1, Cache2, Cache3}) {
                    TMemRecBarrier memRec(collectGen, collectStep, TBarrierIngress(x.Get()));
                    tree.Update(true, key, memRec);
                }
            }

            void Write(NBarriers::TMemView &memView, const TKeyBarrier &key, ui32 collectGen, ui32 collectStep) {
                for (const auto &x : {Cache0, Cache1, Cache2, Cache3}) {
                    TMemRecBarrier memRec(collectGen, collectStep, TBarrierIngress(x.Get()));
                    memView.Update(key, memRec);
                }
            }

            TIngressCachePtr GetCache0() const {
                return Cache0;
            }

            TBlobStorageGroupType GetGType() const {
                return Info.Type;
            }

        private:
            TBlobStorageGroupInfo Info;
            TIngressCachePtr Cache0;
            TIngressCachePtr Cache1;
            TIngressCachePtr Cache2;
            TIngressCachePtr Cache3;
        };

        Y_UNIT_TEST(Tree) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::Erasure4Plus2Block, 1, 4);
            TVDiskID vdisk0(0, 1, 0, 0 /*domain*/, 0 /*vdisk*/);
            TIngressCachePtr cache0 = TIngressCache::Create(info.PickTopology(), vdisk0);
            NBarriers::TTree tree(cache0, VDiskLogPrefix);
            TWriter writer;
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;

            const ui64 tabletId = 893475;
            const ui32 channel = 4;

            writer.Write(tree, TKeyBarrier(tabletId, channel, 15, 1, false), 14, 100);
            writer.Write(tree, TKeyBarrier(tabletId, channel, 15, 2, false), 14, 200);
            tree.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft && *soft == NBarriers::TCurrentBarrier(15, 2, 14, 200));

            writer.Write(tree, TKeyBarrier(tabletId, channel, 15, 3, true), Max<ui32>(), Max<ui32>());
            tree.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft && soft->IsDead() && hard && hard->IsDead());
        }

        Y_UNIT_TEST(MemViewSnapshots) {
            TWriter writer;
            NBarriers::TMemView memView(writer.GetCache0(), VDiskLogPrefix, true);
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;

            const ui64 tabletId = 893475;
            const ui32 channel = 4;

            writer.Write(memView, TKeyBarrier(tabletId, channel, 15, 1, false), 14, 100);
            TMaybe<NBarriers::TMemViewSnap> snap1 = memView.GetSnapshot();
            writer.Write(memView, TKeyBarrier(tabletId, channel, 15, 2, false), 14, 200);
            NBarriers::TMemViewSnap snap2 = memView.GetSnapshot();
            snap1->GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft && *soft == NBarriers::TCurrentBarrier(15, 1, 14, 100));
            snap2.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft && *soft == NBarriers::TCurrentBarrier(15, 2, 14, 200));

            writer.Write(memView, TKeyBarrier(tabletId, channel, 15, 3, true), Max<ui32>(), Max<ui32>());

            // Drop the oldest snapshot after the write
            snap1 = { };

            // Take a new snapshot before any new writes
            NBarriers::TMemViewSnap snap3 = memView.GetSnapshot();

            // New snapshot must see the latest write
            snap3.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft && soft->IsDead() && hard && hard->IsDead());
        }

        Y_UNIT_TEST(CompleteTabletDeletionDropsBarriersKeepsBlobsUnneeded) {
            TWriter writer;
            NBarriers::TMemView memView(writer.GetCache0(), VDiskLogPrefix, true);
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;

            const ui64 tabletId = 893475;
            const ui32 channel = 4;
            const TKeyBarrier currentSoft(tabletId, channel, 15, 2, false);
            const TKeyBarrier oldSoft(tabletId, channel, 15, 1, false);
            const TKeyLogoBlob blobBelow(TLogoBlobID(tabletId, 14, 150, channel, 100, 0));
            const TKeyLogoBlob blobAbove(TLogoBlobID(tabletId, 14, 250, channel, 100, 0));
            const TKeyLogoBlob blobOtherChannel(TLogoBlobID(tabletId, 1, 1, 0, 100, 0));
            const TMemRecLogoBlob blobMemRec;
            const TMemRecBarrier barrierMemRec;

            writer.Write(memView, oldSoft, 14, 100);
            writer.Write(memView, currentSoft, 14, 200);

            {
                NGcOpt::TBarriersEssence essence(memView.GetSnapshot(), writer.GetGType());
                UNIT_ASSERT(essence.Keep(currentSoft, barrierMemRec, {}, false, true).KeepIndex);
                UNIT_ASSERT(!essence.Keep(oldSoft, barrierMemRec, {}, false, true).KeepIndex);
                UNIT_ASSERT(!essence.Keep(blobBelow, blobMemRec, {}, false, true).KeepIndex);
                UNIT_ASSERT(essence.Keep(blobAbove, blobMemRec, {}, false, true).KeepIndex);
                UNIT_ASSERT(essence.Keep(blobOtherChannel, blobMemRec, {}, false, true).KeepIndex);
                UNIT_ASSERT(essence.Keep(TKeyBlock(tabletId), TMemRecBlock(Max<ui32>()), {}, false, true).KeepIndex);
            }

            memView.MarkTabletDeleted(tabletId);

            NBarriers::TMemViewSnap snap = memView.GetSnapshot();
            UNIT_ASSERT(snap.IsTabletDeleted(tabletId));
            // the barrier records are gone, and no barrier is made up in their place
            snap.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft.Empty() && hard.Empty());
            snap.GetBarrier(tabletId, 0, soft, hard);
            UNIT_ASSERT(soft.Empty() && hard.Empty());

            // ...yet nothing of this tablet is kept, on any channel, barriers included
            NGcOpt::TBarriersEssence essence(snap, writer.GetGType());
            UNIT_ASSERT(!essence.Keep(currentSoft, barrierMemRec, {}, false, true).KeepIndex);
            UNIT_ASSERT(!essence.Keep(oldSoft, barrierMemRec, {}, false, true).KeepIndex);
            UNIT_ASSERT(!essence.Keep(blobBelow, blobMemRec, {}, false, true).KeepIndex);
            UNIT_ASSERT(!essence.Keep(blobAbove, blobMemRec, {}, false, true).KeepIndex);
            UNIT_ASSERT(!essence.Keep(blobOtherChannel, blobMemRec, {}, false, true).KeepIndex);
            UNIT_ASSERT(essence.Keep(TKeyBlock(tabletId), TMemRecBlock(Max<ui32>()), {}, false, true).KeepIndex);

            // blobs are still protected while garbage collection is not permitted yet
            UNIT_ASSERT(essence.Keep(blobBelow, blobMemRec, {}, false, false).KeepIndex);
            UNIT_ASSERT(essence.Keep(blobAbove, blobMemRec, {}, false, false).KeepIndex);
        }

        Y_UNIT_TEST(MarkTabletsDeletedBulk) {
            TWriter writer;
            NBarriers::TMemView memView(writer.GetCache0(), VDiskLogPrefix, true);
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;

            const ui64 deletedTablet1 = 100;
            const ui64 deletedTablet2 = 200;
            const ui64 aliveTablet = 300;
            const ui32 channel = 4;

            for (ui64 tabletId : {deletedTablet1, deletedTablet2, aliveTablet}) {
                writer.Write(memView, TKeyBarrier(tabletId, channel, 15, 1, false), 14, 100);
            }
            // make one of the channels dead the old way, it must be purged as well
            writer.Write(memView, TKeyBarrier(deletedTablet1, channel, 15, 2, true), Max<ui32>(), Max<ui32>());

            memView.MarkTabletsDeleted({deletedTablet1, deletedTablet2});
            // marking the same tablets again must be a no-op
            memView.MarkTabletsDeleted({deletedTablet1, deletedTablet2});

            NBarriers::TMemViewSnap snap = memView.GetSnapshot();
            UNIT_ASSERT(snap.IsTabletDeleted(deletedTablet1));
            UNIT_ASSERT(snap.IsTabletDeleted(deletedTablet2));
            UNIT_ASSERT(!snap.IsTabletDeleted(aliveTablet));

            const TMemRecLogoBlob blobMemRec;
            NGcOpt::TBarriersEssence essence(snap, writer.GetGType());
            for (ui64 tabletId : {deletedTablet1, deletedTablet2}) {
                // both the soft and the hard entry have been purged from the index
                snap.GetBarrier(tabletId, channel, soft, hard);
                UNIT_ASSERT(soft.Empty() && hard.Empty());
                const TKeyLogoBlob blob(TLogoBlobID(tabletId, 20, 1, channel, 100, 0));
                UNIT_ASSERT(!essence.Keep(blob, blobMemRec, {}, false, true).KeepIndex);
            }

            snap.GetBarrier(aliveTablet, channel, soft, hard);
            UNIT_ASSERT(soft && *soft == NBarriers::TCurrentBarrier(15, 1, 14, 100));
            UNIT_ASSERT(hard.Empty());
            const TKeyLogoBlob aliveBlob(TLogoBlobID(aliveTablet, 20, 1, channel, 100, 0));
            UNIT_ASSERT(essence.Keep(aliveBlob, blobMemRec, {}, false, true).KeepIndex);
        }

        Y_UNIT_TEST(MarkTabletDeletedIgnoresLaterBarriers) {
            TWriter writer;
            NBarriers::TTree tree(writer.GetCache0(), VDiskLogPrefix);
            TMaybe<NBarriers::TCurrentBarrier> soft;
            TMaybe<NBarriers::TCurrentBarrier> hard;

            const ui64 tabletId = 42;
            const ui32 channel = 1;

            tree.MarkTabletDeleted(tabletId);
            UNIT_ASSERT(tree.IsTabletDeleted(tabletId));
            writer.Write(tree, TKeyBarrier(tabletId, channel, 1, 1, false), 1, 1);
            // the barrier is dropped on arrival rather than indexed, so the tree stays empty for it
            tree.GetBarrier(tabletId, channel, soft, hard);
            UNIT_ASSERT(soft.Empty() && hard.Empty());
            UNIT_ASSERT(tree.IsTabletDeleted(tabletId));
        }
    }

} // NKikimr
