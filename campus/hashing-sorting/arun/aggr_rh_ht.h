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

class TRHHashTable {

public:
    TRHHashTable(ui64 * buffer, ui32 prefixSize, ui32 keyCount, ui32 rowSize)
        : Buffer(buffer), KeyCount(keyCount), RowSize(rowSize) {

        assert(prefixSize >= 2);
        assert(keyCount + 2 <= rowSize);
        SlotCount = 1ull << prefixSize;
        SlotCount4 = SlotCount >> 2;
        CountIndex = RowSize - 1;
        ShiftCount = 64 - prefixSize;
        Record = new ui64[RowSize];

        Reset();
    }

    ~TRHHashTable() {
        delete[] Record;
    }

    void Reset() {
        Count = 0;
        CollisionSwapCount = 0;
        CollisionProbes = 0;
        MinHashIndex = 0;
        ui64 * record = Buffer;
        for (ui32 i = 0; i < SlotCount; i++) {
            record[CountIndex] = 0;
            record += RowSize;
        }
    }

    bool Insert(ui64 hash, ui64* keys, ui64 count = 1) {
        // prepare record
        Record[0] = hash;
        std::copy(keys, keys + KeyCount, Record + 1);
        Record[CountIndex] = count;

        ui32 firstProbeIndex = std::max<ui64>(hash >> ShiftCount, MinHashIndex);
        ui64 * record = Buffer + firstProbeIndex * RowSize;
        auto index = firstProbeIndex;
        auto overflow = false;
        while (true) {
            // free slot?
            if (record[CountIndex] == 0) {
                if (overflow) {
                    assert(index == MinHashIndex);
                    MinHashIndex++;
                }
                std::copy(Record, Record + RowSize, record);
                Count++;
                return true;
            }

            auto equals = true;

            if (overflow && index == MinHashIndex) {
                assert(Record[0] >= record[0]); // eq is possible if *ALL* hashes are the same and we compare keys
                if (Count == SlotCount) {
                    return false;
                }

                CollisionSwapCount++;
                MinHashIndex++;

                auto record2 = record + RowSize;
                for (auto i = index + 1; i < SlotCount; i++) {
                    if (record2[CountIndex] == 0) {
                        std::copy_backward(record, record + (i - index) * RowSize, record2 + RowSize);
                        std::copy(Record, Record + RowSize, record);
                        Count++;
                        return true;
                    }
                    record2 += RowSize;
                }
                return false;
            } else {
                for (ui32 i = 0; i < KeyCount + 1; i++) {
                    if (Record[i] < record[i]) {
                        if (Count == SlotCount) {
                            return false;
                        }

                        CollisionSwapCount++;

                        auto record2 = record + RowSize;
                        for (ui32 i = index + 1; i < SlotCount; i++) {
                            if (record2[CountIndex] == 0) {
                                // shift w/o overflow
                                std::copy_backward(record, record + (i - index) * RowSize, record2 + RowSize);
                                std::copy(Record, Record + RowSize, record);
                                if (overflow) {
                                    MinHashIndex++;
                                }
                                Count++;
                                return true;
                            }
                            record2 += RowSize;
                        }

                        assert(!overflow);

                        record2 = Buffer;
                        for (ui32 i = 0; i < index; i++) {
                            if (record2[CountIndex] == 0) {
                                // shift with overflow
                                std::copy_backward(Buffer, record2, record2 + RowSize);
                                std::copy(Buffer + (SlotCount - 1) * RowSize, Buffer + SlotCount * RowSize, Buffer);
                                std::copy_backward(record, Buffer + (SlotCount - 1) * RowSize, Buffer + SlotCount * RowSize);
                                std::copy(Record, Record + RowSize, record);
                                MinHashIndex++;
                                Count++;
                                return true;
                            }
                            record2 += RowSize;
                        }

                        return false;
                    } else if (Record[i] > record[i]) {
                        equals = false;
                        break;
                    }
                }
            }

            // match
            if (equals) {
                record[CountIndex] += Record[CountIndex];
                return true;
            }

            // liner probing with ring
            CollisionProbes++;
            if (++index == SlotCount) {
                index = 0;
                record = Buffer;
                overflow = true;
            } else {
                record += RowSize;
            }
            // HT is full
            if (index == firstProbeIndex) {
                return false;
            }
        }
    }


    ui64* GetSlot(ui32 index) {
        return Buffer + index * RowSize;
    }

public:
    ui64 *Buffer;
    ui64 *Record;
    ui32 KeyCount;
    ui32 RowSize;
    ui64 SlotCount;
    ui64 SlotCount4;
    ui64 Count;
    ui64 ShiftCount;
    ui32 CollisionSwapCount;
    ui32 CountIndex;
    ui64 CollisionProbes;
    ui32 MinHashIndex;
};