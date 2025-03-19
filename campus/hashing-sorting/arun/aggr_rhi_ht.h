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
// 5. Collisions are dealed with Robin Hood Hashing technique
// 6. Non empty slots are sorted by (hash, keys)
// 7. HT is insert/update only, no delete

constexpr ui64 EMPTY = 0xFFFFFFFFFFFFull;

class TRHIndirectHashTable {

public:
    TRHIndirectHashTable(ui64 * buffer, ui32 prefixSize, ui32 keyCount, ui32 rowSize)
        : Buffer(buffer), KeyCount(keyCount), RowSize(rowSize) {

        assert(prefixSize >= 2);
        assert(keyCount + 2 <= rowSize);
        SlotCount = 1ull << prefixSize;
        SlotCount4 = SlotCount >> 2;
        Count = 0;
        CollisionSwapCount = 0;
        CountIndex = RowSize - 1;
        ShiftCount = 64 - prefixSize;
        CollisionProbes = 0;
        Record = new ui64[RowSize];
        EmptyRecord = new ui64[RowSize];
        EmptyRecord[CountIndex] = 0;
        Indices = new ui64[SlotCount];
        std::fill(Indices, Indices + SlotCount, EMPTY);
    }

    ~TRHIndirectHashTable() {
        delete[] Indices;
        delete[] EmptyRecord;
        delete[] Record;
    }

    bool Insert(ui64 hash, ui64* keys) {
        // prepare item
        ui64 *item = Count == SlotCount ? Record : Buffer + Count * RowSize;
        item[0] = hash;
        std::copy(keys, keys + KeyCount, item + 1);
        item[CountIndex] = 1;

        ui32 firstProbeIndex = hash >> ShiftCount;
        auto index = firstProbeIndex;
        auto nextIndex = Count;
        while (true) {
            // free slot?
            if (Indices[index] == EMPTY) {
                Indices[index] = nextIndex;
                Count++;
                return true;
            }

            auto record = Buffer + Indices[index] * RowSize;

            auto equals = true;
            for (ui32 i = 0; i < KeyCount + 1; i++) {
                if (item[i] < record[i]) {
                    if (Count == SlotCount) {
                        return false;
                    }
                    // swap
                    auto saveIndex = Indices[index];
                    Indices[index] = nextIndex;
                    nextIndex = saveIndex;

                    CollisionSwapCount++;
                    equals = false;
                    break;
                } else if (item[i] > record[i]) {
                    equals = false;
                    break;
                }
            }

            // match
            if (equals) {
                record[CountIndex] += item[CountIndex];
                return true;
            }

            // liner probing with ring
            CollisionProbes++;
            if (++index == SlotCount) {
                index = 0;
            }
            // HT is full
            if (index == firstProbeIndex) {
                return false;
            }
        }
    }


    ui64* GetSlot(ui32 index) {
        auto slotIndex = Indices[index];
        return slotIndex == EMPTY ? EmptyRecord : Buffer + slotIndex * RowSize;
    }

public:
    ui64 *Buffer;
    ui64 *Record;
    ui64 *EmptyRecord;
    ui64 *Indices;
    ui32 KeyCount;
    ui32 RowSize;
    ui32 SlotCount;
    ui32 SlotCount4;
    ui32 Count;
    ui32 ShiftCount;
    ui32 CollisionSwapCount;
    ui32 CountIndex;
    ui64 CollisionProbes;
};