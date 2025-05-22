#include "blobstorage_hullhugeheap.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>


// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeChain) {

        /////////////////////////////////////////////////////////////////////////////////////////////////////////
        void AllocateScenaryOneChunk(TChain &chain, TVector<NPrivate::TChunkSlot> &arr, ui32 slotsInChunk) {
            NPrivate::TChunkSlot id;
            bool res = false;
            res = chain.Allocate(&id);
            UNIT_ASSERT_EQUAL(res, false);
            chain.Allocate(&id, 6);
            arr.push_back(id);
            STR << id.ToString() << "\n";

            for (ui32 i = 0; i < slotsInChunk - 1; i++) {
                res = chain.Allocate(&id);
                UNIT_ASSERT_EQUAL(res, true);
                STR << id.ToString() << "\n";
                arr.push_back(id);
            }

            res = chain.Allocate(&id);
            UNIT_ASSERT_EQUAL(res, false);
            UNIT_ASSERT_EQUAL(chain.GetStat(), THeapStat(1, 0, {}));
            STR << "All allocated\n";
        }

        void FreeScenaryOneChunk(TChain &chain, TVector<NPrivate::TChunkSlot> &arr, ui32 slotsInChunk) {
            ui32 count = 0;
            for (const auto &x : arr) {
                STR << "Free: " << x.ToString() << "\n";
                ui32 chunkId = chain.Free(x).ChunkId;
                count++;
                if (count == slotsInChunk) {
                    UNIT_ASSERT_EQUAL(chunkId, 6);
                } else {
                    UNIT_ASSERT_EQUAL(chunkId, 0);
                }
            }
            STR << "All freed\n";
        }

        void AllocFreeOneChunk(ui32 slotsInChunk) {
            TChain chain("vdisk", slotsInChunk, 1);
            TVector<NPrivate::TChunkSlot> arr;
            AllocateScenaryOneChunk(chain, arr, slotsInChunk);
            FreeScenaryOneChunk(chain, arr, slotsInChunk);
        }

        Y_UNIT_TEST(HeapAllocSmall) {
            AllocFreeOneChunk(8);
        }

        Y_UNIT_TEST(HeapAllocLargeStandard) {
            AllocFreeOneChunk(128);
        }

        Y_UNIT_TEST(HeapAllocLargeNonStandard) {
            AllocFreeOneChunk(143);
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////////

        void PreliminaryAllocate(ui32 num, TChain &chain, TVector<NPrivate::TChunkSlot> &arr) {
            TVector<ui32> chunks = {27, 9, 6, 3};
            for (ui32 i = 0; i < num; i++) {
                NPrivate::TChunkSlot id;
                bool res = chain.Allocate(&id);
                if (!res) {
                    ui32 chunkId = chunks.back();
                    chunks.pop_back();
                    chain.Allocate(&id, chunkId);
                }
                arr.push_back(id);
            }
        }

        void FreeChunksScenary(TChain &chain, TVector<NPrivate::TChunkSlot> &arr, TVector<ui32> &chunks) {
            for (const auto &x : arr) {
                STR << "Free " << x.ToString() << "\n";
                ui32 chunkId = chain.Free(x).ChunkId;
                if (chunkId)
                    chunks.push_back(chunkId);
            }
            STR << "Freeing done\n";
        }

        void PrintOutVec(const TVector<NPrivate::TChunkSlot> &arr) {
            for (const auto &x : arr)
                STR << " " << x.ToString();
        }

        void AllocateChunksScenary(TChain &chain, TVector<NPrivate::TChunkSlot> &arr, TVector<ui32> &chunks) {
            TVector<NPrivate::TChunkSlot> tmp;
            NPrivate::TChunkSlot id;
            bool res = false;
            for (ui32 i = 0; i < arr.size(); i++) {
                res = chain.Allocate(&id);
                if (!res) {
                    ui32 chunkId = chunks.back();
                    chunks.pop_back();
                    chain.Allocate(&id, chunkId);
                }
                tmp.push_back(id);
            }

            Sort(tmp.begin(), tmp.end());
            TVector<NPrivate::TChunkSlot> arr2(arr);
            Sort(arr2.begin(), arr2.end());
            STR << "tmp: ";
            PrintOutVec(tmp);
            STR << "\n";
            STR << "arr2: ";
            PrintOutVec(arr2);
            STR << "\n";

            UNIT_ASSERT_EQUAL(tmp, arr2);
        }

        void AllocFreeAlloc(ui32 slotsInChunk) {
            TChain chain("vdisk", slotsInChunk, 1);
            TVector<NPrivate::TChunkSlot> arr;
            TVector<ui32> chunks;

            PreliminaryAllocate(24, chain, arr);
            FreeChunksScenary(chain, arr, chunks);
            AllocateChunksScenary(chain, arr, chunks);
        }

        void AllocFreeRestartAlloc(ui32 slotsInChunk) {
            TVector<NPrivate::TChunkSlot> arr;
            TVector<ui32> chunks;

            TStringStream serialized;

            TChain chain("vdisk", slotsInChunk, 1);
            PreliminaryAllocate(24, chain, arr);
            FreeChunksScenary(chain, arr, chunks);
            chain.Save(&serialized);

            TStringInput str(serialized.Str());
            TChain chain2 = TChain::Load(&str, "vdisk", 1 /*appendBlockSize*/, slotsInChunk);
        }

        Y_UNIT_TEST(AllocFreeAllocTest) {
            AllocFreeAlloc(8);
        }

        Y_UNIT_TEST(AllocFreeRestartAllocTest) {
            AllocFreeRestartAlloc(8);
        }
    }

    Y_UNIT_TEST_SUITE(TChainLayoutBuilder) {

        Y_UNIT_TEST(TestProdConf) {
            // just build a layout we want in prod, check it manually
            const ui32 overhead = 8;
            const ui32 milestone = 512 << 10;
            const ui32 right = 10 << 20;
            const ui32 AppendBlockSize = 4064;
            const ui32 left = AppendBlockSize;

            const ui32 leftBlocks = left / AppendBlockSize;
            const ui32 milestoneBlocks = milestone / AppendBlockSize;
            const ui32 rightBlocks = right / AppendBlockSize + !!(right % AppendBlockSize);
            STR << "leftBlocks# " << leftBlocks
                << " milestoneBlocks# " << milestoneBlocks
                << " rightBlocks# " << rightBlocks << "\n";
            NPrivate::TChainLayoutBuilder builder("", leftBlocks, milestoneBlocks, rightBlocks, overhead);
            TString tableStr = builder.ToString(AppendBlockSize);
            STR << tableStr << "\n";
            TVector<NPrivate::TChainLayoutBuilder::TSeg> canonical = {
                {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 10}, {10, 12}, {12, 14}, {14, 16},
                {16, 18}, {18, 21}, {21, 24}, {24, 28}, {28, 32}, {32, 37}, {37, 42}, {42, 48}, {48, 55},
                {55, 62}, {62, 70}, {70, 79}, {79, 89}, {89, 101}, {101, 114}, {114, 129}, {129, 145},
                {145, 163}, {163, 183}, {183, 205}, {205, 230}, {230, 258}, {258, 290}, {290, 326}, {326, 366},
                {366, 411}, {411, 462}, {462, 519}, {519, 583}, {583, 655}, {655, 736}, {736, 828}, {828, 931},
                {931, 1047}, {1047, 1177}, {1177, 1324}, {1324, 1489}, {1489, 1675}, {1675, 1884}, {1884, 2119},
                {2119, 2383}, {2383, 2680}
            };
            UNIT_ASSERT_EQUAL(builder.GetLayout(), canonical);
        }

        Y_UNIT_TEST(TestMilestoneId) {
            // just build a layout we want in prod, check check that new layout milestone id points
            // to beginning of the old layout
            const ui32 overhead = 8;
            const ui32 oldLeft = 512 << 10;
            const ui32 newLeft = 64 << 10;
            const ui32 milestone = 512 << 10;
            const ui32 right = 10 << 20;
            const ui32 AppendBlockSize = 4064;

            const ui32 oldLeftBlocks = oldLeft / AppendBlockSize;
            const ui32 newLeftBlocks = newLeft / AppendBlockSize;
            const ui32 milestoneBlocks = milestone / AppendBlockSize;
            const ui32 rightBlocks = right / AppendBlockSize + !!(right % AppendBlockSize);
            STR << "oldLeftBlocks# " << oldLeftBlocks
                << " newLeftBlocks# " << newLeftBlocks
                << " milestoneBlocks# " << milestoneBlocks
                << " rightBlocks# " << rightBlocks << "\n";
            NPrivate::TChainLayoutBuilder oldBuilder("", oldLeftBlocks, milestoneBlocks, rightBlocks, overhead);
            NPrivate::TChainLayoutBuilder newBuilder("", newLeftBlocks, milestoneBlocks, rightBlocks, overhead);
            UNIT_ASSERT_EQUAL(oldBuilder.GetLayout()[0], newBuilder.GetMilestoneSegment());
        }
    }

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeHeap) {

        Y_UNIT_TEST(AllocateAllFromOneChunk) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = 58 << 10u;
            ui32 mileStoneBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            heap.FinishRecovery();
            ui32 hugeBlobSize = 6u << 20u;

            heap.AddChunk(5);
            heap.AddChunk(3);

            THugeSlot slot;
            ui32 slotSize;
            bool res = false;
            for (ui32 i = 0; i < heap.SlotNumberOfThisSize(hugeBlobSize); i++) {
                res = heap.Allocate(hugeBlobSize, &slot, &slotSize);

                UNIT_ASSERT_EQUAL(res, true);
                STR << "Allocated: " << slot.ToString() << "\n";
                UNIT_ASSERT(slot.GetChunkId() == 3);
            }
            res = heap.Allocate(hugeBlobSize, &slot, &slotSize);
            UNIT_ASSERT_EQUAL(res, true);
            UNIT_ASSERT(slot.GetChunkId() == 5);
            STR << "Allocated: " << slot.ToString() << "\n";

            // just serialize/deserialize
            TString serialized = heap.Serialize();
            THeap newHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            newHeap.ParseFromString(serialized);
            newHeap.FinishRecovery();
        }

        void AllocateScenary(THeap &heap, ui32 hugeBlobSize, TVector<THugeSlot> &arr) {
            heap.AddChunk(5);
            heap.AddChunk(3);

            THugeSlot slot;
            ui32 slotSize;
            bool res = false;
            for (ui32 i = 0; i < heap.SlotNumberOfThisSize(hugeBlobSize) * 2; i++) {
                res = heap.Allocate(hugeBlobSize, &slot, &slotSize);
                UNIT_ASSERT_EQUAL(res, true);
                arr.push_back(slot);
                STR << "Allocated: " << slot.ToString() << "\n";
            }
            STR << "Allocated all\n";
            UNIT_ASSERT_EQUAL(heap.Allocate(hugeBlobSize, &slot, &slotSize), false);
        }

        void FreeScenary(THeap &heap, TVector<THugeSlot> &arr) {
            for (const auto &x : arr) {
                STR << "Free: " << x.ToString() << "\n";
                heap.Free(x.GetDiskPart());
            }
            ui32 c1 = heap.RemoveChunk();
            STR << "Remove chunk: " << c1 << "\n";
            UNIT_ASSERT(c1 != 0);
            ui32 c2 = heap.RemoveChunk();
            STR << "Remove chunk: " << c2 << "\n";
            UNIT_ASSERT(c2 != 0);
            UNIT_ASSERT_EQUAL(c1 + c2, 8);
        }

        Y_UNIT_TEST(AllocateAllReleaseAll) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = 56u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            heap.FinishRecovery();
            TVector<THugeSlot> arr;

            AllocateScenary(heap, 6u << 20u, arr);
            FreeScenary(heap, arr);
        }

        Y_UNIT_TEST(AllocateAllSerializeDeserializeReleaseAll) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = 56u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            heap.FinishRecovery();
            TVector<THugeSlot> arr;

            AllocateScenary(heap, 6u << 20u, arr);
            TString heap1 = heap.ToString();
            TString serialized = heap.Serialize();
            UNIT_ASSERT(THeap::CheckEntryPoint(serialized));
            THeap newHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            newHeap.ParseFromString(serialized);
            newHeap.FinishRecovery();
            TString heap2 = newHeap.ToString();
            UNIT_ASSERT_VALUES_EQUAL(heap1, heap2);
            FreeScenary(newHeap, arr);
        }

        Y_UNIT_TEST(RecoveryMode) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = 56u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            heap.FinishRecovery();

            heap.RecoveryModeAddChunk(2);
            heap.RecoveryModeAddChunk(34);
            TVector<ui32> rmChunks;
            rmChunks.push_back(2);
            rmChunks.push_back(34);
            heap.RecoveryModeRemoveChunks(rmChunks);
            heap.RecoveryModeAddChunk(20);
            heap.RecoveryModeAddChunk(34);
            heap.RecoveryModeAddChunk(5);
            heap.RecoveryModeAddChunk(2);

            const ui32 hugeBlobSize = 6u << 20u;
            const ui32 slotSize = heap.SlotSizeOfThisSize(hugeBlobSize);

            heap.RecoveryModeAllocate(TDiskPart(2, slotSize * 2, 6u << 20u));
            heap.RecoveryModeAllocate(TDiskPart(2, slotSize * 5, 6u << 20u));
            heap.RecoveryModeAllocate(TDiskPart(34, 0, 6u << 20u));
        }

        Y_UNIT_TEST(BorderValues) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = appendBlockSize + 1;
            ui32 maxBlobInBytes = MaxVDiskBlobSize;
            ui32 overhead = 8u;
            ui32 freeChunksReservation = 1;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            heap.FinishRecovery();

            THugeSlot hugeSlot;
            ui32 slotSize;
            bool res = false;
            res = heap.Allocate(minHugeBlobInBytes, &hugeSlot, &slotSize);
            UNIT_ASSERT_EQUAL(res, false); // no chunks
            res = heap.Allocate(maxBlobInBytes, &hugeSlot, &slotSize);
            UNIT_ASSERT_EQUAL(res, false); // no chunks
        }

        enum class EWrite_SaveEntryPoint_Restart {
            MigrateFrom_Old_To_New,
            RollbackFrom_New_To_Old,
        };

        Y_UNIT_TEST(WriteRestore) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 4064u;
            ui32 minHugeBlobInBytes = appendBlockSize;
            ui32 mileStoneBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;

            THeap oldHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            oldHeap.FinishRecovery();

            THeap fromHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            fromHeap.ParseFromString(oldHeap.Serialize());
            fromHeap.FinishRecovery();
            TVector<THugeSlot> arr;

            AllocateScenary(fromHeap, 6u << 20u, arr);
            TString serialized = fromHeap.Serialize();
            UNIT_ASSERT(THeap::CheckEntryPoint(serialized));
            THeap toHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            toHeap.ParseFromString(serialized);
            toHeap.FinishRecovery();
            FreeScenary(toHeap, arr);
        }
    }

} // NKikimr
