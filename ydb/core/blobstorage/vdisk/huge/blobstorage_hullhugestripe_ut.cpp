#include "blobstorage_hullhugestripe.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeStripeHeap) {

        static constexpr ui32 ChunkSize = 128 * 4064; // multiple of append block
        static constexpr ui32 Append = 4064;

        TStripeHeap MakeHeap() {
            return TStripeHeap("vdisk", ChunkSize, Append);
        }

        Y_UNIT_TEST(AlignSize) {
            TStripeHeap h = MakeHeap();
            UNIT_ASSERT_VALUES_EQUAL(h.AlignSize(1), Append);
            UNIT_ASSERT_VALUES_EQUAL(h.AlignSize(Append), Append);
            UNIT_ASSERT_VALUES_EQUAL(h.AlignSize(Append + 1), 2 * Append);
        }

        Y_UNIT_TEST(AllocateNeedsChunk) {
            TStripeHeap h = MakeHeap();
            THugeSlot slot;
            UNIT_ASSERT(!h.Allocate(Append, &slot));
        }

        Y_UNIT_TEST(AllocateAndFreeCoalesce) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b, c;
            h.Allocate(Append, &a, 7);
            UNIT_ASSERT_VALUES_EQUAL(a.GetChunkId(), 7u);
            UNIT_ASSERT_VALUES_EQUAL(a.GetOffset(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(a.GetSize(), Append);

            UNIT_ASSERT(h.Allocate(2 * Append, &b));
            UNIT_ASSERT_VALUES_EQUAL(b.GetChunkId(), 7u);
            UNIT_ASSERT_VALUES_EQUAL(b.GetOffset(), Append);
            UNIT_ASSERT_VALUES_EQUAL(b.GetSize(), 2 * Append);

            UNIT_ASSERT(h.Allocate(Append, &c));
            UNIT_ASSERT_VALUES_EQUAL(c.GetOffset(), 3 * Append);

            // free middle, then first: they must coalesce into one hole
            TFreeRes r1 = h.Free(b.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r1.ChunkId, 0u);
            TFreeRes r2 = h.Free(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r2.ChunkId, 0u);

            THugeSlot d;
            UNIT_ASSERT(h.Allocate(3 * Append, &d));
            UNIT_ASSERT_VALUES_EQUAL(d.GetOffset(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(d.GetSize(), 3 * Append);

            h.Free(d.GetDiskPart());
            TFreeRes r3 = h.Free(c.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r3.ChunkId, 7u);
            UNIT_ASSERT(!h.ContainsChunk(7));
        }

        Y_UNIT_TEST(BestFitPrefersSmallerHole) {
            TStripeHeap h = MakeHeap();
            THugeSlot big, two;
            h.AddChunk(1);
            h.AddChunk(2);
            UNIT_ASSERT(h.Allocate(ChunkSize - 3 * Append, &big));
            UNIT_ASSERT_VALUES_EQUAL(big.GetChunkId(), 1u);
            // chunk 1 has a 3-block hole, chunk 2 is empty (full-chunk hole)
            UNIT_ASSERT(h.Allocate(2 * Append, &two));
            UNIT_ASSERT_VALUES_EQUAL(two.GetChunkId(), 1u);
        }

        Y_UNIT_TEST(LockPreventsAllocation) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 3);
            UNIT_ASSERT(h.LockChunk(3));
            UNIT_ASSERT(!h.Allocate(Append, &b));
            h.AddChunk(4);
            UNIT_ASSERT(h.Allocate(Append, &b));
            UNIT_ASSERT_VALUES_EQUAL(b.GetChunkId(), 4u);
        }

        Y_UNIT_TEST(DoesNotOpenNewChunkIfHoleFits) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 9);
            h.AddChunk(10);
            UNIT_ASSERT(h.Allocate(Append, &b));
            UNIT_ASSERT_VALUES_EQUAL(b.GetChunkId(), 9u);
        }

        Y_UNIT_TEST(ProtoRoundTripCarriesOnlyChunkIds) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 5);
            UNIT_ASSERT(h.Allocate(3 * Append, &b));
            h.LockChunk(5);

            NKikimrVDiskData::THugeKeeperStripeHeap proto;
            h.SaveToProto(proto);

            // the entry point records which chunks the heap owns and nothing about what is inside them
            UNIT_ASSERT_VALUES_EQUAL(proto.ChunkIdsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(proto.GetChunkIds(0), 5u);

            TStripeHeap loaded("vdisk", proto);
            UNIT_ASSERT(loaded.ContainsChunk(5));

            // the chunk comes back empty, and the extents are put back by replaying the references the hull holds
            loaded.RecoveryOccupyDerived(a.GetDiskPart());
            loaded.RecoveryOccupyDerived(b.GetDiskPart());
            UNIT_ASSERT(loaded.DropUnreferencedChunks().empty());

            THugeSlot conv = loaded.ConvertDiskPart(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(conv.GetSize(), Append);

            TFreeRes r = loaded.Free(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r.ChunkId, 0u);
            r = loaded.Free(b.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r.ChunkId, 5u);
        }

        Y_UNIT_TEST(UnreferencedChunkGoesBackToSlotHeap) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 5);
            h.Allocate(Append, &b, 6);

            NKikimrVDiskData::THugeKeeperStripeHeap proto;
            h.SaveToProto(proto);
            UNIT_ASSERT_VALUES_EQUAL(proto.ChunkIdsSize(), 2u);

            // the entry point is allowed to be stale in the safe direction: chunk 6 is named but, by the time the
            // database is recovered, nothing points into it any more
            TStripeHeap loaded("vdisk", proto);
            loaded.RecoveryOccupyDerived(a.GetDiskPart());

            const std::vector<ui32> reclaimed = loaded.DropUnreferencedChunks();
            UNIT_ASSERT_VALUES_EQUAL(reclaimed.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(reclaimed[0], 6u);
            UNIT_ASSERT(loaded.ContainsChunk(5));
            UNIT_ASSERT(!loaded.ContainsChunk(6));
        }

        Y_UNIT_TEST(RecoveryModeAllocate) {
            TStripeHeap h = MakeHeap();
            h.AddChunk(8);
            TDiskPart p(8, 2 * Append, Append + 10); // payload not aligned
            h.RecoveryModeAllocate(p);
            THugeSlot slot = h.ConvertDiskPart(p);
            UNIT_ASSERT_VALUES_EQUAL(slot.GetOffset(), 2 * Append);
            UNIT_ASSERT_VALUES_EQUAL(slot.GetSize(), 2 * Append);

            THugeSlot extra;
            UNIT_ASSERT(h.Allocate(Append, &extra));
            UNIT_ASSERT(extra.GetOffset() == 0u || extra.GetOffset() == 4 * Append);
        }

        Y_UNIT_TEST(ReleaseOccupyInFlight) {
            TStripeHeap h = MakeHeap();
            THugeSlot a;
            h.Allocate(Append, &a, 11);
            TFreeRes rel = h.ReleaseStripe(a);
            UNIT_ASSERT(!rel.InLockedChunks);
            UNIT_ASSERT_VALUES_EQUAL(rel.ChunkId, 11u);
            UNIT_ASSERT(!h.ContainsChunk(11));
            h.OccupyStripe(a, false);
            UNIT_ASSERT(h.ContainsChunk(11));
            TFreeRes r = h.Free(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r.ChunkId, 11u);
        }

        Y_UNIT_TEST(ShrinkStripeReleasesTail) {
            TStripeHeap h = MakeHeap();
            THugeSlot a;
            h.Allocate(8 * Append, &a, 13);
            UNIT_ASSERT_VALUES_EQUAL(a.GetSize(), 8 * Append);

            h.ShrinkStripe(a, 3 * Append);

            // the tail went back to the free list, so the next allocation lands right behind the shrunk stripe
            THugeSlot b;
            UNIT_ASSERT(h.Allocate(5 * Append, &b));
            UNIT_ASSERT_VALUES_EQUAL(b.GetChunkId(), 13u);
            UNIT_ASSERT_VALUES_EQUAL(b.GetOffset(), 3 * Append);

            UNIT_ASSERT_VALUES_EQUAL(h.Free(b.GetDiskPart()).ChunkId, 0u);
            UNIT_ASSERT_VALUES_EQUAL(h.Free(TDiskPart(13, 0, 3 * Append)).ChunkId, 13u);
            UNIT_ASSERT(!h.ContainsChunk(13));
        }

        Y_UNIT_TEST(ShredForceFree) {
            TStripeHeap h = MakeHeap();
            THugeSlot a;
            h.Allocate(Append, &a, 12);
            h.ShredNotify({12});
            TFreeRes r = h.Free(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r.ChunkId, 0u);
            UNIT_ASSERT_VALUES_EQUAL(h.RemoveChunk(), 12u);
            UNIT_ASSERT_VALUES_EQUAL(h.RemoveChunk(), 0u);
        }

        Y_UNIT_TEST(StatCanBeFreed) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 1);
            h.Allocate(Append, &b, 2);
            THeapStat st = h.GetStat();
            UNIT_ASSERT_VALUES_EQUAL(st.CurrentlyUsedChunks, 2u);
            UNIT_ASSERT_VALUES_EQUAL(st.CanBeFreedChunks, 1u);
        }

        Y_UNIT_TEST(GetOwnedChunks) {
            TStripeHeap h = MakeHeap();
            THugeSlot a;
            h.Allocate(Append, &a, 15);
            TSet<TChunkIdx> chunks;
            h.GetOwnedChunks(chunks);
            UNIT_ASSERT(chunks.contains(15));
        }

        Y_UNIT_TEST(SerializeParseRoundTrip) {
            TStripeHeap h = MakeHeap();
            THugeSlot a, b;
            h.Allocate(Append, &a, 21);
            UNIT_ASSERT(h.Allocate(2 * Append, &b));
            h.LockChunk(21);

            NKikimrVDiskData::THugeKeeperStripeHeap proto;
            h.SaveToProto(proto);
            TStripeHeap restored("vdisk", proto);
            UNIT_ASSERT(restored.ContainsChunk(21));
            restored.RecoveryOccupyDerived(a.GetDiskPart());
            restored.RecoveryOccupyDerived(b.GetDiskPart());
            UNIT_ASSERT(restored.DropUnreferencedChunks().empty());

            // the lock is volatile: the defrag that took it does not survive a restart, so the chunk is
            // allocatable again after parsing, and the new stripe lands past the two derived ones
            THugeSlot c;
            UNIT_ASSERT(restored.Allocate(Append, &c));
            UNIT_ASSERT_VALUES_EQUAL(c.GetChunkId(), 21u);
            UNIT_ASSERT_VALUES_EQUAL(c.GetOffset(), 3 * Append);
            UNIT_ASSERT_VALUES_EQUAL(restored.Free(c.GetDiskPart()).ChunkId, 0u);

            TFreeRes r = restored.Free(a.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r.ChunkId, 0u);
            TFreeRes r2 = restored.Free(b.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(r2.ChunkId, 21u);
        }
    }

} // NKikimr
