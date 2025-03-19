#include "aggr.h"
#include "aggr_rh_ht.h"
#include "spilling_mem.h"

#include <util/stream/format.h>

#include <vector>
#include <deque>

constexpr ui32 slotSize = 8;

template <ui32 keyCount>
struct TInputData {

    TInputData() {

    }

    TInputData(TSpillingBlock block, ui64 *buffer, ui64 bufferSize) {
        Init(block, buffer, bufferSize);
    }

    void Init(TSpillingBlock block, ui64 *buffer, ui64 bufferSize) {
        Block = block;
        Offset = 0;
        Count = 0;
        Size = Block.BlockSize / (slotSize * 8);
        Buffer = buffer;
        BufferSize = bufferSize;
        Record = Buffer;
        EndOfBuffer = Buffer;
    }

    bool Next(TSpilling& sp) {
        while (true) {
            while (Record < EndOfBuffer && Record[slotSize - 1] == 0) {
                Record += slotSize;
            }

            if (Record < EndOfBuffer) {
                return true;
            }

            if (Offset == Size) {
                return false;
            }

            Count = std::min(BufferSize, Size - Offset);
            sp.Load(Block, Offset * slotSize * 8, Buffer, Count * slotSize * 8);
            Offset += Count;
            Record = Buffer;
            EndOfBuffer = Buffer + Count * slotSize;
        }
    }

    inline ui32 Compare(ui64 *& record, ui64& count, ui32 mask, ui32 ownMask) {
        if (Record >= EndOfBuffer) {
            return mask;
        }
        if (mask == 0) {
            count = Record[slotSize - 1];
            record = Record;
            return ownMask;
        } else {
            if (record[0] < Record[0]) {
                return mask;
            } else if (record[0] > Record[0]) {
                count = Record[slotSize - 1];
                record = Record;
                return ownMask;
            }

            // record[0] == Record[0]

            if constexpr (keyCount == 1) {
                if (record[1] == Record[1]) {
                    count += Record[slotSize - 1];
                    return (mask | ownMask);
                }
            } else if constexpr (keyCount == 2) {
                if (record[1] == Record[1] && record[2] == Record[2]) {
                    count += Record[slotSize - 1];
                    return (mask | ownMask);
                }
            } else if constexpr (keyCount == 3) {
                if (record[1] == Record[1] && record[2] == Record[2] && record[3] == Record[3]) {
                    count += Record[slotSize - 1];
                    return (mask | ownMask);
                }
            } else if constexpr (keyCount == 4) {
                if (record[1] == Record[1] && record[2] == Record[2] && record[3] == Record[3] && record[4] == Record[4]) {
                    count += Record[slotSize - 1];
                    return (mask | ownMask);
                }
            }

            for (ui32 i = 1; i < keyCount + 1; i++) {
                if (record[i] < Record[i]) {
                    return mask;
                } else if (record[i] > Record[i]) {
                    count = Record[slotSize - 1];
                    record = Record;
                    return ownMask;
                }
            }

            count += Record[slotSize - 1];
            return (mask | ownMask);
        }
    }

    inline void IncIfUse(ui32 mask, ui32 ownMask) {
        Record += ((mask & ownMask) != 0) * slotSize;
    }

    TSpillingBlock Block;
    ui64 *Buffer;
    ui64 *Record;
    ui64 *EndOfBuffer;
    ui64 BufferSize;
    ui64 Offset;
    ui64 Count;
    ui64 Size;
};

template <bool finalize, ui32 keyCount, ui32 p>
ui32 merge2pway(ui64 * partBuffer, ui32 partBufferSize, TFileOutput& fo, TSpilling& sp, std::deque<TSpillingBlock>& spills) {

    constexpr ui32 n = 1ul << p;
    auto inputBufferSize = partBufferSize >> (p + 1);
    auto mergeBufferSize = partBufferSize >> 1;

    TInputData<keyCount> data[n];
    auto buffer = partBuffer;
    for (ui32 i = 0; i < n; i++) {
        data[i].Init(spills.front(), buffer, inputBufferSize);
        spills.pop_front();
        buffer += inputBufferSize * slotSize;
    }

    auto mergeBuffer = buffer;
    auto merged = sp.Empty(0);
    ui32 indexm = 0;
    ui32 result = 0;

    while (true) {

        auto notEmpty = false;
        for (ui32 i = 0; i < n; i++) {
            notEmpty |= data[i].Next(sp);
        }

        if (!notEmpty) {
            break;
        }

        ui64 count = 0;
        ui64 * record = nullptr;

        ui32 use = 0;
        for (ui32 i = 0; i < n; i++) {
            use = data[i].Compare(record, count, use, 1 << i);
        }

        if constexpr (finalize) {
            auto recordm = mergeBuffer + indexm * (keyCount + 2);
            std::copy(record, record + 1 + keyCount, recordm);
            recordm[keyCount + 1] = count;
        } else {
            auto recordm = mergeBuffer + indexm * slotSize;
            std::copy(record, record + 1 + keyCount, recordm);
            recordm[slotSize - 1] = count;
        }

        for (ui32 i = 0; i < n; i++) {
            data[i].IncIfUse(use, 1 << i);
        }

        if (Y_UNLIKELY(++indexm == mergeBufferSize)) {
            if constexpr (finalize) {
                fo.Write(mergeBuffer, indexm * (keyCount + 2) * 8);
            } else {
                merged = sp.Append(merged, mergeBuffer, indexm * slotSize * 8);
            }
            result += indexm;
            indexm = 0;
        }
    }

    for (ui32 i = 0; i < n; i++) {
        sp.Delete(data[i].Block);
    }

    if constexpr (finalize) {
        fo.Write(mergeBuffer, indexm * (keyCount + 2) * 8);
    } else {
        spills.push_back(sp.Append(merged, mergeBuffer, indexm * slotSize * 8));
    }
    result += indexm;

    return result;
}


template <ui32 keyCount>
void aggr_external_merge(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize) {

    Cout << "Robin Hood HT, aggregation in External Memory with Merge combine" << Endl;

    if (hashBits == 0) {
        hashBits = round_to_nearest_power_of_two(cardinality);
    }

    TSpilling sp(8 * 1024 * 1024);
    std::deque<TSpillingBlock> spills;

    ui64 swaps = 0;
    ui64 probes = 0;
    ui64 sum = 0;
    ui64 total1 = 0;
    ui64 total2 = 0;
    ui64 overlaps = 0;

  {
    // 1. intermediate aggr

    auto slotCount = 1ull << hashBits;
    assert(fillRatio <= 100);
    auto fillCount = slotCount * fillRatio / 100;

    ui64 * buffer = new ui64[slotCount * slotSize];
    TRHHashTable ht(buffer, hashBits, keyCount, slotSize);

    Cout << "HT1 Size: " << ht.SlotCount << ", Rows Width: " << slotSize * 8 << Endl;
    Cout << "HT1 Mem: " << slotCount * slotSize * 8 << Endl;

    total1 = slotCount * slotSize * 8;

    Cout << "Total MEM 1: " << total1 << Endl;

    ui64 n = 1024;

    ui64 * readBuffer = new ui64[8 * n];

    while (rowCount) {
        ui64 d = rowCount > n ? n : rowCount;
        rowCount -= d;
        fi.Load(readBuffer, d * 8 * 8);
        auto record = readBuffer;
        for (ui64 i = 0; i < d; i++) {
            auto hash = hash_keys(record, keyCount);
            auto result = ht.Insert(hash, record);
            assert(result);
            Y_UNUSED(result);
            record += 8;

            if (ht.Count >= fillCount || (rowCount == 0 && i == d - 1)) {
                swaps += ht.CollisionSwapCount;
                probes += ht.CollisionProbes;

                overlaps += (ht.MinHashIndex > 0);
                auto b = sp.Save(buffer + ht.MinHashIndex * slotSize, (slotCount - ht.MinHashIndex) * slotSize * 8, 0);
                spills.push_back(sp.Append(b, buffer, ht.MinHashIndex * slotSize * 8));

                ht.Reset();
            }
        }
        sum += d;
    }

    delete[] readBuffer;
    delete[] buffer;
  }

    Cout << "Total record processed: " << sum << Endl;
    Cout << "Overlaps: " << overlaps << Endl;
    Cout << "Collision Swaps: " << swaps << Endl;
    Cout << "Extra probes: " << probes << Endl;
    if (sum) {
        Cout << "Probe length: " << (sum + probes) / double(sum) << Endl;
    }

    ui64 nw = 0;

  {
    // 2. final aggr

    ui64 * partBuffer = new ui64[partBufferSize * slotSize];
    assert(partBufferSize >= 4);
    total2 = partBufferSize * slotSize * 8;

    Cout << "Buffers 2: " << total2 << Endl;
    Cout << "Total MEM 2: " << total2 << Endl;

    while(!spills.empty()) {

        switch (std::min<ui32>(spills.size(), partBufferSize >> 1)) {
        case 0:
            break;
        case 1:
        {
            auto inputBufferSize = partBufferSize >> 1;
            auto mergeBufferSize = partBufferSize >> 1;

            auto mergeBuffer = partBuffer + inputBufferSize * slotSize;

            auto block = spills.front();
            spills.pop_front();

            ui32 offset = 0;
            auto size = block.BlockSize / (slotSize * 8);
            ui32 indexm = 0;
            ui64 * recordm = mergeBuffer;

            while (offset < size) {
                auto count = std::min<ui32>(inputBufferSize, size - offset);
                sp.Load(block, offset * slotSize * 8, partBuffer, count * slotSize * 8);
                offset += count;

                ui64 * record = partBuffer;

                for (ui32 i = 0; i < count; i++) {
                    if (record[slotSize - 1]) {
                        std::copy(record, record + 1 + keyCount, recordm);
                        recordm[keyCount + 1] = record[slotSize - 1];
                        recordm += (keyCount + 2);
                        indexm++;

                        if (indexm == mergeBufferSize) {
                            fo.Write(mergeBuffer, indexm * (keyCount + 2) * 8);
                            nw += indexm;
                            recordm = mergeBuffer;
                            indexm = 0;
                        }
                    }
                    record += slotSize;
                }

            }

            if (indexm) {
                fo.Write(mergeBuffer, indexm * (keyCount + 2) * 8);
                nw += indexm;
            }

            sp.Delete(block);

            break;
        }
        case 2: case 3:
        {
            bool finalize = spills.size() == 2;
            if (finalize) {
                nw += merge2pway<true, keyCount, 1>(partBuffer, partBufferSize, fo, sp, spills);
            } else {
                merge2pway<false, keyCount, 1>(partBuffer, partBufferSize, fo, sp, spills);
            }
            break;
        }
        case 4: case 5: case 6: case 7:
        {
            bool finalize = spills.size() == 4;
            if (finalize) {
                nw += merge2pway<true, keyCount, 2>(partBuffer, partBufferSize, fo, sp, spills);
            } else {
                merge2pway<false, keyCount, 2>(partBuffer, partBufferSize, fo, sp, spills);
            }
            break;
        }
        case 8: case 9: case 10: case 11:
        case 12: case 13: case 14: case 15:
        {
            bool finalize = spills.size() == 8;
            if (finalize) {
                nw += merge2pway<true, keyCount, 3>(partBuffer, partBufferSize, fo, sp, spills);
            } else {
                merge2pway<false, keyCount, 3>(partBuffer, partBufferSize, fo, sp, spills);
            }
            break;
        }
        default:
        {
            bool finalize = spills.size() == 16;
            if (finalize) {
                nw += merge2pway<true, keyCount, 4>(partBuffer, partBufferSize, fo, sp, spills);
            } else {
                merge2pway<false, keyCount, 4>(partBuffer, partBufferSize, fo, sp, spills);
            }
            break;
        }
        }
    }

    delete[] partBuffer;
  }

    Cout << "Unique keys sets: " << nw << Endl;
    Cout << "Spilling WriteChunkCount: " << sp.WriteChunkCount << Endl;
    Cout << "Spilling ReadChunkCount: " << sp.ReadChunkCount << Endl;
    Cout << "Spilling MaxChunkCount: " << sp.MaxChunkCount << Endl;
    Cout << "Grand total MEM: " << std::max(total1, total2) << Endl;
}

template
void aggr_external_merge<1>(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize);

template
void aggr_external_merge<2>(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize);

template
void aggr_external_merge<3>(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize);

template
void aggr_external_merge<4>(TFileInput& fi, TFileOutput& fo, ui64 rowCount, ui64 cardinality, ui16 hashBits, ui16 fillRatio, ui32 partBufferSize);
