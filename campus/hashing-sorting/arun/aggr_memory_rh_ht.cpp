#include "aggr.h"
#include "aggr_rh_ht.h"

void aggr_memory_rh_ht(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui32 keyCount, ui64 cardinality, ui16 hashBits) {

    if (hashBits == 0) {
        hashBits = round_to_nearest_power_of_two(cardinality);
    }
    auto slotCount = 1ull << hashBits;

    assert(cardinality <= slotCount);

    ui32 slotSize = 8;
    ui64 * buffer = new ui64[slotCount * slotSize];

    TRHHashTable ht(buffer, hashBits, keyCount, slotSize);

    Cout << "Robin Hood HT, aggregation in Main Memory" << Endl;
    Cout << "HT Size: " << ht.SlotCount << ", Rows Width: " << slotSize * 8 << Endl;
    Cout << "HT Mem: " << slotCount * slotSize * 8 << Endl;

    ui64 n = 1024;

    ui64 * readBuffer = new ui64[8 * n];

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
        }
    }

    delete[] readBuffer;

    ui64 * writeBuffer = new ui64[(keyCount + 2) * n];

    ui64 * wb = writeBuffer;
    ui64 nw = 0;
    ui64 nn = 0;
    ui64 sum = 0;
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
    if (nn) {
        fo.Write(writeBuffer, nn * (keyCount + 2) * 8);
    }
    Cout << "Total record processed: " << sum << Endl;
    Cout << "Unique keys sets: " << nw + nn << Endl;
    Cout << "Collision Swaps: " << ht.CollisionSwapCount << Endl;
    Cout << "Extra probes: " << ht.CollisionProbes << Endl;
    if (sum) {
        Cout << "Probe length: " << (sum + ht.CollisionProbes) / double(sum) << Endl;
    }
}