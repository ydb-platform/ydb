#pragma once

#include <util/system/types.h>

#include <algorithm>
#include <cassert>

// Simple in-memory HT designed especially for COUNT(*) aggregations
//
// 1. Just array of 2^N records
// 2. Record is array of ui64
// 3. First item is hash, last item is count with keys in between
// 4. Use count == 0 as mark of free slot
// 5. Most significant N bits of hash match record index if no collisions
// 6. Collisions are dealed with linear probing
// 7. HT is insert/update only, no delete

class TLPHashTable {

public:
    TLPHashTable(ui64 * buffer, ui32 prefixSize, ui32 keyCount, ui32 rowSize)
        : Buffer(buffer), KeyCount(keyCount), RowSize(rowSize) {

        assert(prefixSize >= 2);
        assert(keyCount + 2 <= rowSize);
        SlotCount = 1ull << prefixSize;
        SlotCount4 = SlotCount >> 2;
        Count = 0;
        CollisionCount = 0;
        CountIndex = RowSize - 1;
        ShiftCount = 64 - prefixSize;
        CollisionProbes = 0;
    }

    bool Insert(ui64 hash, ui64* keys) {
        ui32 firstProbeIndex = hash >> ShiftCount;
        ui64 * record = Buffer + firstProbeIndex * RowSize;
        auto i = firstProbeIndex;
        bool collision = false;
        while (true) {
            // free slot?
            if (record[CountIndex] == 0) {
                record[0] = hash;
                std::copy(keys, keys + KeyCount, record + 1);
                record[CountIndex] = 1;
                Count++;
                if (collision) {
                    CollisionCount++;
                }
                return true;
            }
            // match?
            if (hash == record[0] && std::equal(keys, keys + KeyCount, record + 1)) {
                record[CountIndex]++;
                return true;
            }
            collision = true;
            // liner probing with ring
            CollisionProbes++;
            if (++i == SlotCount) {
                i = 0;
                record = Buffer;
            } else {
                record += RowSize;
            }
            // HT is full
            if (i == firstProbeIndex) {
                return false;
            }
        }
    }

    ui64* GetSlot(ui32 index) {
        return Buffer + index * RowSize;
    }

public:
    ui64 *Buffer;
    ui32 KeyCount;
    ui32 RowSize;
    ui32 SlotCount;
    ui32 SlotCount4;
    ui32 Count;
    ui32 ShiftCount;
    ui32 CollisionCount;
    ui32 CountIndex;
    ui64 CollisionProbes;
};