#include "barriers_tree.h"
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
                : Info(TBlobStorageGroupType::ErasureMirror3, 1, 4)
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

        private:
            TBlobStorageGroupInfo Info;
            TIngressCachePtr Cache0;
            TIngressCachePtr Cache1;
            TIngressCachePtr Cache2;
            TIngressCachePtr Cache3;
        };

        Y_UNIT_TEST(Tree) {
            TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3, 1, 4);
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
    }

} // NKikimr
