#include "aggr.h"
#include "aggr_rh_ht.h"
#include "spilling_mem.h"

#include <util/stream/format.h>

#include <vector>

void aggr_external_rh_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui16 partBits, ui32 partBufferSize1, ui16 hashBits2, ui32 partBufferSize2) {

    Cout << "Robin Hood HT, aggregation in External Memory with partitioning and HT combine" << Endl;

    if (hashBits == 0) {
        hashBits = round_to_nearest_power_of_two(cardinality);
    }
    if (hashBits2 == 0) {
        hashBits2 = hashBits - partBits + 1;
    }

    ui32 slotSize = 8;
    TSpilling sp(8 * 1024 * 1024);
    ui32 parts = 1ull << partBits;
    std::vector<std::vector<TSpillingBlock>> spills;
    spills.resize(parts);

    ui64 swaps = 0;
    ui64 probes = 0;
    ui64 total1 = 0;
    ui64 total2 = 0;

  {
    // 1. intermediate aggr

    auto slotCount = 1ull << hashBits;
    assert(fillRatio <= 100);
    auto fillCount = slotCount * fillRatio / 100;

    ui64 * buffer = new ui64[slotCount * slotSize];
    TRHHashTable ht(buffer, hashBits, keyCount, slotSize);

    ui64 n = 1024;

    ui64 * readBuffer = new ui64[8 * n];

    ui64 * partCounts = new ui64[parts];
    ui64 * partBuffers = new ui64[parts * partBufferSize1 * slotSize];

    Cout << "HT1 Size: " << ht.SlotCount << ", Rows Width: " << slotSize * 8 << Endl;
    Cout << "HT1 Mem: " << slotCount * slotSize * 8 << Endl;
    Cout << "Buffers: " << parts * partBufferSize1 * slotSize * 8 << Endl;

    total1 = slotCount * slotSize * 8 + parts * partBufferSize1 * slotSize * 8;

    Cout << "Total MEM 1: " << total1 << Endl;

    while (rowCount) {
        ui64 d = rowCount > n ? n : rowCount;
        rowCount -= d;
        fi.Load(readBuffer, d * 8 * 8);
        auto buffer = readBuffer;
        for (ui64 i = 0; i < d; i++) {
            auto hash = hash_keys(buffer, keyCount);
            auto result = ht.Insert(hash, buffer);
            assert(result);
            Y_UNUSED(result);
            buffer += 8;
            if (ht.Count >= fillCount || ((rowCount == 0) && (i == d - 1))) {
                swaps += ht.CollisionSwapCount;
                probes += ht.CollisionProbes;
                {
                    for (ui32 i = 0; i < parts; i++) {
                        partCounts[i] = 0;
                    }
                    for (ui64 i = 0; i < ht.SlotCount; i++) {
                        auto slot = ht.GetSlot(i);
                        if (slot[ht.CountIndex]) {
                            ui32 partIndex = slot[0] & ((1ull << partBits) - 1);
                            auto bufferIndex = partCounts[partIndex]++;
                            std::copy(slot, slot + slotSize, partBuffers + (partIndex * partBufferSize1 + bufferIndex) * slotSize);
                            if (partCounts[partIndex] == partBufferSize1) {
                                spills[partIndex].push_back(sp.Save(partBuffers + partIndex * partBufferSize1 * slotSize, partBufferSize1 * slotSize * 8, 0));
                                partCounts[partIndex] = 0;
                            }
                        }
                    }
                    for (ui32 i = 0; i < parts; i++) {
                        if (partCounts[i]) {
                            spills[i].push_back(sp.Save(partBuffers + i * partBufferSize1 * slotSize, partCounts[i] * slotSize * 8, 0));
                        }
                    }
                }
                ht.Reset();
            }
        }
    }

    delete[] readBuffer;
    delete[] partCounts;
    delete[] partBuffers;
    delete[] buffer;
  }

    ui64 sum = 0;
    ui64 nw = 0;
ui64 sum1 = 0;

  {
    // 2. final aggr

    ui64 n = 1024;
    ui64 * writeBuffer = new ui64[(keyCount + 2) * n];
    ui64 * partBuffer = new ui64[partBufferSize2 * slotSize];

    auto slotCount = 1ull << hashBits2;
    ui64 * buffer = new ui64[slotCount * slotSize];
    TRHHashTable ht(buffer, hashBits2, keyCount, slotSize);

    Cout << "HT2 Size: " << ht.SlotCount << ", Rows Width: " << slotSize * 8 << Endl;
    Cout << "HT2 Mem: " << slotCount * slotSize * 8 << Endl;
    Cout << "Buffer: " << partBufferSize2 * slotSize * 8 << Endl;

    total2 = slotCount * slotSize * 8 + partBufferSize2 * slotSize * 8;

    Cout << "Total MEM 2: " << total2 << Endl;

    ui64 * wb = writeBuffer;
    ui64 nn = 0;
    for (ui32 i = 0; i < parts; i++) {
        // 2.1. combine
        for (auto block : spills[i]) {
            assert(block.BlockSize <= partBufferSize2 * slotSize * 8);
            ui32 count = block.BlockSize / (slotSize * 8);
            assert(count * (slotSize * 8) == block.BlockSize);
            sp.Load(block, 0, partBuffer, block.BlockSize);
            sp.Delete(block);
            auto record = partBuffer;
            for (ui32 j = 0; j < count; j++) {
                sum1 += record[slotSize - 1];
                auto result = ht.Insert(record[0], record + 1, record[slotSize - 1]);
                assert(result);
                Y_UNUSED(result);
                record += slotSize;
            }
        }

        // 2.2 write

        for (ui64 i = 0; i < ht.SlotCount; i++) {
            auto slot = ht.GetSlot(i);
            if (slot[ht.CountIndex]) {
                std::copy(slot, slot + 1 + keyCount, wb);
                wb[keyCount + 1] = slot[ht.CountIndex];
                sum += slot[ht.CountIndex];
                if (++nn == n) {
                    fo.Write(writeBuffer, nn * (keyCount + 2) * 8);
                    nw += nn;
                    nn = 0;
                    wb = writeBuffer;
                } else {
                    wb += keyCount + 2;
                }
            }
        }

        ht.Reset();
    }

    if (nn) {
        nw += nn;
        fo.Write(writeBuffer, nn * (keyCount + 2) * 8);
    }

    delete[] buffer;
    delete[] partBuffer;
    delete[] writeBuffer;
  }

    Cout << "Total record processed: " << sum << " (" << sum1 << ")" << Endl;
    Cout << "Unique keys sets: " << nw << Endl;
    Cout << "Collision Swaps: " << swaps << Endl;
    Cout << "Extra probes: " << probes << Endl;
    if (sum) {
        Cout << "Probe length: " << (sum + probes) / double(sum) << Endl;
    }
    Cout << "Spilling WriteChunkCount: " << sp.WriteChunkCount << Endl;
    Cout << "Spilling ReadChunkCount: " << sp.ReadChunkCount << Endl;
    Cout << "Spilling MaxChunkCount: " << sp.MaxChunkCount << Endl;
    Cout << "Grand total MEM: " << std::max(total1, total2) << Endl;
}