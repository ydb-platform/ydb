#include "blobstorage_hullhugeheap.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>


// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeDefs) {

        Y_UNIT_TEST(FreeRes1) {
            TMask mask;
            mask.Set(0, 8);
            mask.Reset(1);
            TFreeRes res = {15, mask, 8};

            STR << "TFreeRes# " << res.ToString() << "\n";
            UNIT_ASSERT_EQUAL(res.ToString(), "{ChunkIdx: 15 Mask# 10111111}");

            TMask constMask = TChain::BuildConstMask("", 8);
            TFreeRes constRes = {0, constMask, 8};
            STR << "constMask# " << constRes.ToString() << "\n";
            UNIT_ASSERT_EQUAL(constRes.ToString(), "{ChunkIdx: 0 Mask# 11111111}");

            res.Mask.Reset(0);
            STR << "first non zero bit: " << res.Mask.FirstNonZeroBit() << "\n";
            UNIT_ASSERT_EQUAL(res.Mask.FirstNonZeroBit(), 2);

            res.Mask.Set(1);
            res.Mask.Set(0);
            UNIT_ASSERT_EQUAL(res.Mask, constRes.Mask);
        }
    }


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
            TChain chain("vdisk", slotsInChunk);
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
            TChain chain("vdisk", slotsInChunk);
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

            {
                TChain chain("vdisk", slotsInChunk);
                PreliminaryAllocate(24, chain, arr);
                FreeChunksScenary(chain, arr, chunks);
                chain.Save(&serialized);
            }
            {
                TChain chain("vdisk", slotsInChunk);
                TStringInput str(serialized.Str());
                chain.Load(&str);
            }
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
            NPrivate::TChainLayoutBuilder builder(leftBlocks, milestoneBlocks, rightBlocks, overhead);
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
            NPrivate::TChainLayoutBuilder oldBuilder(oldLeftBlocks, milestoneBlocks, rightBlocks, overhead);
            NPrivate::TChainLayoutBuilder newBuilder(newLeftBlocks, milestoneBlocks, rightBlocks, overhead);
            UNIT_ASSERT_EQUAL(oldBuilder.GetLayout()[0], newBuilder.GetMilestoneSegment());
        }
    }

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeLayout) {

        Y_UNIT_TEST(TestOldAppendBlockSize) {
            TAllChains all("vdisk", 134274560, 56896, 512 << 10, 512 << 10, 512 << 10, 10 << 20, 8);
            all.PrintOutChains(STR);
            all.PrintOutSearchTable(STR);
            std::pair<ui32, ui32> p = all.GetTablesSize();
            TVector<NPrivate::TChainLayoutBuilder::TSeg> canonical = {
                {9, 10}, {10, 11}, {11, 12}, {12, 13}, {13, 14}, {14, 15}, {15, 16}, {16, 18}, {18, 20},
                {20, 22}, {22, 24}, {24, 27}, {27, 30}, {30, 33}, {33, 37}, {37, 41}, {41, 46}, {46, 51},
                {51, 57}, {57, 64}, {64, 72}, {72, 81}, {81, 91}, {91, 102}, {102, 114}, {114, 128},
                {128, 144}, {144, 162}, {162, 182}, {182, 204}
            };
            UNIT_ASSERT_EQUAL(all.GetLayout(), canonical);
            UNIT_ASSERT_EQUAL(p, (std::pair<ui32, ui32>(30, 186)));
        }

        Y_UNIT_TEST(TestNewAppendBlockSize) {
            TAllChains all("vdisk", 134274560, 4064, 512 << 10, 512 << 10, 512 << 10, 10 << 20, 8);
            all.PrintOutChains(STR);
            all.PrintOutSearchTable(STR);
            TVector<NPrivate::TChainLayoutBuilder::TSeg> canonical = {
                {129, 145}, {145, 163}, {163, 183}, {183, 205}, {205, 230}, {230, 258}, {258, 290},
                {290, 326}, {326, 366}, {366, 411}, {411, 462}, {462, 519}, {519, 583}, {583, 655},
                {655, 736}, {736, 828}, {828, 931}, {931, 1047}, {1047, 1177}, {1177, 1324},
                {1324, 1489}, {1489, 1675}, {1675, 1884}, {1884, 2119}, {2119, 2383}, {2383, 2680}
            };
            UNIT_ASSERT_EQUAL(all.GetLayout(), canonical);
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
            THeap heap("vdisk", chunkSize, appendBlockSize, appendBlockSize, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
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
            THeap newHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            newHeap.ParseFromString(serialized);
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
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
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
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            TVector<THugeSlot> arr;

            AllocateScenary(heap, 6u << 20u, arr);
            TString serialized = heap.Serialize();
            UNIT_ASSERT(THeap::CheckEntryPoint(serialized));
            THeap newHeap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            newHeap.ParseFromString(serialized);
            FreeScenary(newHeap, arr);
        }

        Y_UNIT_TEST(RecoveryMode) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 minHugeBlobInBytes = 56u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);

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
            ui32 minHugeBlobInBytes = appendBlockSize;
            ui32 minREALHugeBlobInBytes = minHugeBlobInBytes / appendBlockSize * appendBlockSize + 1;
            ui32 maxBlobInBytes = MaxVDiskBlobSize;
            ui32 overhead = 8u;
            ui32 freeChunksReservation = 1;
            THeap heap("vdisk", chunkSize, appendBlockSize, minHugeBlobInBytes, minHugeBlobInBytes, minHugeBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);

            THugeSlot hugeSlot;
            ui32 slotSize;
            bool res = false;
            res = heap.Allocate(minREALHugeBlobInBytes, &hugeSlot, &slotSize);
            UNIT_ASSERT_EQUAL(res, false); // no chunks
            res = heap.Allocate(maxBlobInBytes, &hugeSlot, &slotSize);
            UNIT_ASSERT_EQUAL(res, false); // no chunks
        }

        enum class EWrite_SaveEntryPoint_Restart {
            MigrateFrom_Old_To_New,
            RollbackFrom_New_To_Old,
        };

        void Write_SaveEntryPoint_Restart(EWrite_SaveEntryPoint_Restart mode) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 4064u;
            ui32 minHugeBlobInBytes = appendBlockSize;
            ui32 oldMinHugeBlobInBytes = 64u << 10u;
            ui32 mileStoneBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;

            ui32 fromMin = 0;
            ui32 toMin = 0;
            switch (mode) {
                case EWrite_SaveEntryPoint_Restart::MigrateFrom_Old_To_New:
                        fromMin = oldMinHugeBlobInBytes;
                        toMin = minHugeBlobInBytes;
                        break;
                case EWrite_SaveEntryPoint_Restart::RollbackFrom_New_To_Old:
                        fromMin = minHugeBlobInBytes;
                        toMin = oldMinHugeBlobInBytes;
                        break;
            }
            THeap oldHeap("vdisk", chunkSize, appendBlockSize, oldMinHugeBlobInBytes, oldMinHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);

            THeap fromHeap("vdisk", chunkSize, appendBlockSize, fromMin, oldMinHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            fromHeap.ParseFromString(oldHeap.Serialize());
            TVector<THugeSlot> arr;

            AllocateScenary(fromHeap, 6u << 20u, arr);
            TString serialized = fromHeap.Serialize();
            UNIT_ASSERT(THeap::CheckEntryPoint(serialized));
            THeap toHeap("vdisk", chunkSize, appendBlockSize, toMin, oldMinHugeBlobInBytes, mileStoneBlobInBytes,
                    maxBlobInBytes, overhead, freeChunksReservation);
            toHeap.ParseFromString(serialized);
            FreeScenary(toHeap, arr);
        }

        Y_UNIT_TEST(MigrateFrom_Old_To_New) {
            Write_SaveEntryPoint_Restart(EWrite_SaveEntryPoint_Restart::MigrateFrom_Old_To_New);
        }

        Y_UNIT_TEST(RollbackFrom_New_To_Old) {
            Write_SaveEntryPoint_Restart(EWrite_SaveEntryPoint_Restart::RollbackFrom_New_To_Old);
        }
    }

} // NKikimr
