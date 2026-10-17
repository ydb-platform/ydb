#include "mkql_grace_join_imp.h"

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/log/log.h>

#include <contrib/libs/xxhash/xxhash.h>
#include <string_view>
#include <utility>

namespace NKikimr::NMiniKQL::NGraceJoin {

TTable::EAddTupleResult TTable::AddTuple(ui64* intColumns, char** stringColumns, ui32* stringsSizes, NYql::NUdf::TUnboxedValue* iColumns, const TTable& other) {
    if ((intColumns[0] & 1)) {
        return EAddTupleResult::Unmatched;
    }

    TotalPacked_++;

    TempTuple_.clear();
    TempTuple_.insert(TempTuple_.end(), intColumns, intColumns + NullsBitmapSize_ + NumberOfKeyIntColumns_);

    if (NumberOfKeyIColumns_ > 0) {
        for (ui32 i = 0; i < NumberOfKeyIColumns_; i++) {
            TempTuple_.push_back((ColInterfaces_ + i)->HashI->Hash(*(iColumns + i)));
        }
    }

    ui64 totalBytesForStrings = 0;
    ui64 totalIntsForStrings = 0;

    // Processing variable length string columns
    if (NumberOfKeyStringColumns_ != 0 || NumberOfKeyIColumns_ != 0) {
        totalBytesForStrings += sizeof(ui32) * NumberOfKeyStringColumns_;
        totalBytesForStrings += sizeof(ui32) * NumberOfKeyIColumns_;

        for (ui64 i = 0; i < NumberOfKeyStringColumns_; i++) {
            totalBytesForStrings += stringsSizes[i];
        }

        for (ui64 i = 0; i < NumberOfKeyIColumns_; i++) {
            TStringBuf val = (ColInterfaces_ + i)->Packer->Pack(*(iColumns + i));
            IColumnsVals_[i].clear();
            IColumnsVals_[i].insert(IColumnsVals_[i].begin(), val.cbegin(), val.end());
            totalBytesForStrings += val.size();
        }

        totalIntsForStrings = (totalBytesForStrings + sizeof(ui64) - 1) / sizeof(ui64);

        TempTuple_.push_back(totalIntsForStrings);
        TempTuple_.resize(TempTuple_.size() + totalIntsForStrings);

        TempTuple_.back() = 0;

        ui64* startPtr = (TempTuple_.data() + TempTuple_.size() - totalIntsForStrings);
        char* currStrPtr = reinterpret_cast<char*>(startPtr);

        for (ui64 i = 0; i < NumberOfKeyStringColumns_; i++) {
            WriteUnaligned<ui32>(currStrPtr, stringsSizes[i]);
            currStrPtr += sizeof(ui32);
            std::memcpy(currStrPtr, stringColumns[i], stringsSizes[i]);
            currStrPtr += stringsSizes[i];
        }

        for (ui64 i = 0; i < NumberOfKeyIColumns_; i++) {
            WriteUnaligned<ui32>(currStrPtr, IColumnsVals_[i].size());
            currStrPtr += sizeof(ui32);
            std::memcpy(currStrPtr, IColumnsVals_[i].data(), IColumnsVals_[i].size());
            currStrPtr += IColumnsVals_[i].size();
        }
    }

    XXH64_hash_t hash = XXH64(TempTuple_.data() + NullsBitmapSize_, (TempTuple_.size() - NullsBitmapSize_) * sizeof(ui64), 0);

    if (!hash) {
        hash = 1;
    }

    ui64 bucket = hash & BucketsMask;

    if (!IsAny_ && other.TableBucketsStats_[bucket].BloomFilter.IsFinalized()) {
        auto bucket2 = &other.TableBucketsStats_[bucket];
        auto& bloomFilter = bucket2->BloomFilter;
        ++BloomLookups;
        if (bloomFilter.IsMissing(hash)) {
            ++BloomHits;
            return EAddTupleResult::Unmatched;
        }
    }

    std::vector<ui64, TMKQLAllocator<ui64>>& keyIntVals = TableBuckets_[bucket].KeyIntVals;
    std::vector<ui32, TMKQLAllocator<ui32>>& stringsOffsets = TableBuckets_[bucket].StringsOffsets;
    std::vector<ui64, TMKQLAllocator<ui64>>& dataIntVals = TableBuckets_[bucket].DataIntVals;
    std::vector<char, TMKQLAllocator<char>>& stringVals = TableBuckets_[bucket].StringsValues;
    TKeysHashTable& kh = TableBucketsStats_[bucket].AnyHashTable;

    ui32 offset = keyIntVals.size(); // Offset of tuple inside the keyIntVals vector

    keyIntVals.push_back(hash);
    keyIntVals.insert(keyIntVals.end(), TempTuple_.begin(), TempTuple_.end());

    if (IsAny_) {
        if (!AddKeysToHashTable(kh, keyIntVals.begin() + offset, iColumns)) {
            keyIntVals.resize(offset);
            ++AnyFiltered;
            return EAddTupleResult::AnyMatch;
        }

        if (other.TableBucketsStats_[bucket].BloomFilter.IsFinalized()) {
            auto bucket2 = &other.TableBucketsStats_[bucket];
            auto& bloomFilter = bucket2->BloomFilter;
            ++BloomLookups;
            if (bloomFilter.IsMissing(hash)) {
                keyIntVals.resize(offset);
                ++BloomHits;
                return EAddTupleResult::Unmatched;
            }
        }
    }

    TableBucketsStats_[bucket].TuplesNum++;

    if (NumberOfStringColumns_ || NumberOfIColumns_) {
        stringsOffsets.push_back(TableBucketsStats_[bucket].KeyIntValsTotalSize);   // Adding offset to tuple in keyIntVals vector
        stringsOffsets.push_back(TableBucketsStats_[bucket].StringValuesTotalSize); // Adding offset to string values

        // Adding strings sizes for keys and data
        if (NumberOfStringColumns_) {
            stringsOffsets.insert(stringsOffsets.end(), stringsSizes, stringsSizes + NumberOfStringColumns_);
        }

        if (NumberOfIColumns_) {
            for (ui64 i = NumberOfKeyIColumns_; i < NumberOfIColumns_; i++) {
                TStringBuf val = (ColInterfaces_ + i)->Packer->Pack(*(iColumns + i));
                IColumnsVals_[i].clear();
                IColumnsVals_[i].insert(IColumnsVals_[i].begin(), val.cbegin(), val.end());
            }
            for (ui64 i = 0; i < NumberOfIColumns_; i++) {
                stringsOffsets.push_back(IColumnsVals_[i].size());
            }
        }
    }

    // Adding data values
    ui64* dataColumns = intColumns + NullsBitmapSize_ + NumberOfKeyIntColumns_;
    dataIntVals.insert(dataIntVals.end(), dataColumns, dataColumns + NumberOfDataIntColumns_);

    // Adding strings values for data columns
    char** dataStringsColumns = stringColumns + NumberOfKeyStringColumns_;
    ui32* dataStringsSizes = stringsSizes + NumberOfKeyStringColumns_;

    ui64 initialStringsSize = stringVals.size();
    for (ui64 i = 0; i < NumberOfDataStringColumns_; i++) {
        ui32 currStringSize = *(dataStringsSizes + i);
        stringVals.insert(stringVals.end(), *(dataStringsColumns + i), *(dataStringsColumns + i) + currStringSize);
    }

    for (ui64 i = 0; i < NumberOfDataIColumns_; i++) {
        stringVals.insert(stringVals.end(), IColumnsVals_[NumberOfKeyIColumns_ + i].begin(), IColumnsVals_[NumberOfKeyIColumns_ + i].end());
    }

    TableBucketsStats_[bucket].KeyIntValsTotalSize += keyIntVals.size() - offset;
    TableBucketsStats_[bucket].StringValuesTotalSize += stringVals.size() - initialStringsSize;
    return EAddTupleResult::Added;
}

void TTable::ResetIterator() {
    CurrIterIndex_ = 0;
    CurrIterBucket_ = 0;
    if (IsTableJoined_) {
        JoinTable1_->ResetIterator();
        JoinTable2_->ResetIterator();
    }
    TotalUnpacked_ = 0;
}

// Checks if there are more tuples and sets bucketId and tupleId to next valid.
inline bool HasMoreTuples(std::vector<TTableBucketStats>& tableBucketsStats, ui64& bucketId, ui64& tupleId, ui64 bucketLimit) {
    if (bucketId >= bucketLimit) {
        return false;
    }

    if (tupleId >= tableBucketsStats[bucketId].TuplesNum) {
        tupleId = 0;
        bucketId++;

        if (bucketId == bucketLimit) {
            return false;
        }

        while (tableBucketsStats[bucketId].TuplesNum == 0) {
            bucketId++;
            if (bucketId == bucketLimit) {
                return false;
            }
        }
    }

    return true;
}

// Returns value of next tuple. Returs true if there are more tuples
bool TTable::NextTuple(TupleData& td) {
    if (HasMoreTuples(TableBucketsStats_, CurrIterBucket_, CurrIterIndex_, TableBucketsStats_.size())) {
        GetTupleData(CurrIterBucket_, CurrIterIndex_, td);
        CurrIterIndex_++;
        return true;
    } else {
        td.AllNulls = true;
        return false;
    }
}

inline bool CompareIColumns(const ui32* stringSizes1, const char* vals1,
                            const ui32* stringSizes2, const char* vals2,
                            TColTypeInterface* colInterfaces, ui64 nStringColumns, ui64 nIColumns) {
    ui32 currOffset1 = 0;
    ui32 currOffset2 = 0;
    ui32 currSize1 = 0;
    ui32 currSize2 = 0;
    NYql::NUdf::TUnboxedValue val1;
    NYql::NUdf::TUnboxedValue val2;
    TStringBuf str1;
    TStringBuf str2;

    for (ui32 i = 0; i < nStringColumns; i++) {
        currSize1 = *(stringSizes1 + i);
        currSize2 = *(stringSizes2 + i);
        if (currSize1 != currSize2) {
            return false;
        }
        currOffset1 += currSize1 + sizeof(ui32);
        currOffset2 += currSize2 + sizeof(ui32);
    }

    if (0 != std::memcmp(vals1, vals2, currOffset1)) {
        return false;
    }

    for (ui32 i = 0; i < nIColumns; i++) {
        currSize1 = *(stringSizes1 + nStringColumns + i);
        currSize2 = *(stringSizes2 + nStringColumns + i);
        currOffset1 += sizeof(ui32);
        currOffset2 += sizeof(ui32);
        str1 = TStringBuf(vals1 + currOffset1, currSize1);
        val1 = (colInterfaces + i)->Packer->Unpack(str1, colInterfaces->HolderFactory);
        str2 = TStringBuf(vals2 + currOffset2, currSize2);
        val2 = (colInterfaces + i)->Packer->Unpack(str2, colInterfaces->HolderFactory);
        if (!((colInterfaces + i)->EquateI->Equals(val1, val2))) {
            return false;
        }

        currOffset1 += currSize1;
        currOffset2 += currSize2;
    }
    return true;
}

inline bool CompareIColumns(const char* vals1,
                            const char* vals2,
                            NYql::NUdf::TUnboxedValue* iColumns,
                            TColTypeInterface* colInterfaces,
                            ui64 nStringColumns, ui64 nIColumns) {
    ui32 currOffset1 = 0;
    NYql::NUdf::TUnboxedValue val1;
    TStringBuf str1;

    for (ui32 i = 0; i < nStringColumns; i++) {
        auto currSize1 = ReadUnaligned<ui32>(vals1 + currOffset1);
        auto currSize2 = ReadUnaligned<ui32>(vals2 + currOffset1);
        if (currSize1 != currSize2) {
            return false;
        }
        currOffset1 += currSize1 + sizeof(ui32);
    }

    if (0 != std::memcmp(vals1, vals2, currOffset1)) {
        return false;
    }

    for (ui32 i = 0; i < nIColumns; i++) {
        auto currSize1 = ReadUnaligned<ui32>(vals1 + currOffset1);
        currOffset1 += sizeof(ui32);
        str1 = TStringBuf(vals1 + currOffset1, currSize1);
        val1 = (colInterfaces + i)->Packer->Unpack(str1, colInterfaces->HolderFactory);
        auto& val2 = iColumns[i];
        if (!((colInterfaces + i)->EquateI->Equals(val1, val2))) {
            return false;
        }

        currOffset1 += currSize1;
    }
    return true;
}

// Resizes KeysHashTable to new slots, keeps old content.
void ResizeHashTable(TKeysHashTable& t, ui64 newSlots) {
    std::vector<ui64, TMKQLAllocator<ui64>> newTable(newSlots * t.SlotSize, 0);
    for (auto it = t.Table.begin(); it != t.Table.end(); it += t.SlotSize) {
        if (*it == 0) {
            continue;
        }
        ui64 hash = *it;
        ui64 newSlotNum = hash % (newSlots);
        auto newIt = newTable.begin() + t.SlotSize * newSlotNum;
        while (*newIt != 0) {
            newIt += t.SlotSize;
            if (newIt == newTable.end()) {
                newIt = newTable.begin();
            }
        }
        std::copy_n(it, t.SlotSize, newIt);
    }
    t.NSlots = newSlots;
    t.Table = std::move(newTable);
}

bool IsTablesSwapRequired(ui64 tuplesNum1, ui64 tuplesNum2, bool table1Batch, bool table2Batch) {
    return tuplesNum2 > tuplesNum1 && !table1Batch || table2Batch;
}

ui64 ComputeJoinSlotsSizeForBucket(const TTableBucket& bucket, const TTableBucketStats& bucketStats, ui64 headerSize, bool tableHasKeyStringColumns, bool tableHasKeyIColumns) {
    ui64 tuplesNum = bucketStats.TuplesNum;

    ui64 avgStringsSize = (3 * (bucket.KeyIntVals.size() - tuplesNum * headerSize)) / (2 * tuplesNum + 1) + 1;
    ui64 slotSize = headerSize + 1; // Header [Short Strings] SlotIdx
    if (tableHasKeyStringColumns || tableHasKeyIColumns) {
        slotSize = slotSize + avgStringsSize;
    }

    return slotSize;
}

ui64 ComputeNumberOfSlots(ui64 tuplesNum) {
    return (3 * tuplesNum + 1) | 1;
}

bool TTable::TryToPreallocateMemoryForJoin(TTable& t1, TTable& t2, EJoinKind /* joinKind */, bool hasMoreLeftTuples, bool hasMoreRightTuples) {
    // If the batch is final or the only one, then the buckets are processed sequentially, the memory for the hash tables is freed immediately after processing.
    // So, no preallocation is required.
    if (!hasMoreLeftTuples && !hasMoreRightTuples) {
        return true;
    }

    for (ui64 bucket = 0; bucket < NGraceJoin::NumberOfBuckets; bucket++) {
        ui64 tuplesNum1 = t1.TableBucketsStats_[bucket].TuplesNum;
        ui64 tuplesNum2 = t2.TableBucketsStats_[bucket].TuplesNum;

        TTable& tableForPreallocation = IsTablesSwapRequired(tuplesNum1, tuplesNum2, hasMoreLeftTuples || LeftTableBatch_, hasMoreRightTuples || RightTableBatch_) ? t1 : t2;
        if (!tableForPreallocation.TableBucketsStats_[bucket].TuplesNum || tableForPreallocation.TableBuckets_[bucket].NSlots) {
            continue;
        }

        TTableBucket& bucketForPreallocation = tableForPreallocation.TableBuckets_[bucket];
        TTableBucketStats& bucketForPreallocationStats = tableForPreallocation.TableBucketsStats_[bucket];

        const auto nSlots = ComputeJoinSlotsSizeForBucket(bucketForPreallocation, bucketForPreallocationStats, tableForPreallocation.HeaderSize_,
                                                          tableForPreallocation.NumberOfKeyStringColumns_ != 0, tableForPreallocation.NumberOfKeyIColumns_ != 0);
        const auto slotSize = ComputeNumberOfSlots(tableForPreallocation.TableBucketsStats_[bucket].TuplesNum);

        bool wasException = false;
        try {
            bucketForPreallocation.JoinSlots.reserve(nSlots * slotSize);
            bucketForPreallocationStats.BloomFilter.Reserve(bucketForPreallocationStats.TuplesNum);
        } catch (const TMemoryLimitExceededException&) {
            wasException = true;
        }

        if (wasException || TlsAllocState->IsMemoryYellowZoneEnabled()) {
            UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Debug, TStringBuilder() << "Preallocation failed. WasException: " << wasException);
            for (ui64 i = 0; i < bucket; ++i) {
                auto& b1 = t1.TableBuckets_[i];
                b1.JoinSlots.resize(0);
                b1.JoinSlots.shrink_to_fit();
                auto& s1 = t1.TableBucketsStats_[i];
                s1.BloomFilter.Shrink();

                auto& b2 = t2.TableBuckets_[i];
                b2.JoinSlots.resize(0);
                b2.JoinSlots.shrink_to_fit();
                auto& s2 = t2.TableBucketsStats_[i];
                s2.BloomFilter.Shrink();
            }
            return false;
        }
    }

    return true;
}

// Joins two tables and returns join result in joined table. Tuples of joined table could be received by
// joined table iterator
void TTable::Join(TTable& t1, TTable& t2, EJoinKind joinKind, bool hasMoreLeftTuples, bool hasMoreRightTuples, ui32 fromBucket, ui32 toBucket) {
    if (hasMoreLeftTuples) {
        LeftTableBatch_ = true;
    }

    if (hasMoreRightTuples) {
        RightTableBatch_ = true;
    }

    auto table1Batch = LeftTableBatch_;
    auto table2Batch = RightTableBatch_;

    JoinTable1_ = &t1;
    JoinTable2_ = &t2;

    JoinKind_ = joinKind;

    IsTableJoined_ = true;

    MKQL_ENSURE(joinKind != EJoinKind::Cross, "Cross Join is not allowed in Grace Join");

    const bool needCrossIds = JoinKind_ == EJoinKind::Inner || JoinKind_ == EJoinKind::Full || JoinKind_ == EJoinKind::Left || JoinKind_ == EJoinKind::Right;

    ui64 tuplesFound = 0;

    for (ui64 bucket = fromBucket; bucket < toBucket; bucket++) {
        auto& joinResults = TableBuckets_[bucket].JoinIds;
        joinResults.clear();
        TTableBucket* bucket1 = &JoinTable1_->TableBuckets_[bucket];
        TTableBucket* bucket2 = &JoinTable2_->TableBuckets_[bucket];
        TTableBucketStats* bucketStats1 = &JoinTable1_->TableBucketsStats_[bucket];
        TTableBucketStats* bucketStats2 = &JoinTable2_->TableBucketsStats_[bucket];

        ui64 tuplesNum1 = JoinTable1_->TableBucketsStats_[bucket].TuplesNum;
        ui64 tuplesNum2 = JoinTable2_->TableBucketsStats_[bucket].TuplesNum;

        ui64 headerSize1 = JoinTable1_->HeaderSize_;
        ui64 headerSize2 = JoinTable2_->HeaderSize_;
        ui64 nullsSize1 = JoinTable1_->NullsBitmapSize_;
        ui64 nullsSize2 = JoinTable2_->NullsBitmapSize_;
        ui64 keyIntOffset1 = HashSize + nullsSize1;
        ui64 keyIntOffset2 = HashSize + nullsSize2;
        bool table1HasKeyStringColumns = (JoinTable1_->NumberOfKeyStringColumns_ != 0);
        bool table2HasKeyStringColumns = (JoinTable2_->NumberOfKeyStringColumns_ != 0);
        bool table1HasKeyIColumns = (JoinTable1_->NumberOfKeyIColumns_ != 0);
        bool table2HasKeyIColumns = (JoinTable2_->NumberOfKeyIColumns_ != 0);
        bool swapTables = IsTablesSwapRequired(tuplesNum1, tuplesNum2, table1Batch, table2Batch);

        if (swapTables) {
            std::swap(bucket1, bucket2);
            std::swap(bucketStats1, bucketStats2);
            std::swap(headerSize1, headerSize2);
            std::swap(nullsSize1, nullsSize2);
            std::swap(keyIntOffset1, keyIntOffset2);
            std::swap(table1HasKeyStringColumns, table2HasKeyStringColumns);
            std::swap(table1HasKeyIColumns, table2HasKeyIColumns);
            std::swap(tuplesNum1, tuplesNum2);
        }

        auto& leftIds = bucket1->LeftIds;
        leftIds.clear();

        const bool selfJoinSameKeys = (JoinTable1_ == JoinTable2_);
        const bool needLeftIds = ((swapTables ? (JoinKind_ == EJoinKind::Right || JoinKind_ == EJoinKind::RightOnly) : (JoinKind_ == EJoinKind::Left || JoinKind_ == EJoinKind::LeftOnly)) || JoinKind_ == EJoinKind::Full || JoinKind_ == EJoinKind::Exclusion) && !selfJoinSameKeys;
        const bool isLeftSemi = swapTables ? JoinKind_ == EJoinKind::RightSemi : JoinKind_ == EJoinKind::LeftSemi;
        // const bool isRightSemi = swapTables ? JoinKind == EJoinKind::LeftSemi : JoinKind == EJoinKind::RightSemi;
        bucketStats2->HashtableMatches = ((swapTables ? (JoinKind_ == EJoinKind::Left || JoinKind_ == EJoinKind::LeftOnly || JoinKind_ == EJoinKind::LeftSemi) : (JoinKind_ == EJoinKind::Right || JoinKind_ == EJoinKind::RightOnly || JoinKind_ == EJoinKind::RightSemi)) || JoinKind_ == EJoinKind::Full || JoinKind_ == EJoinKind::Exclusion) && !selfJoinSameKeys;
        // In this case, all keys except for NULLs have matched key on other side, and NULLs are handled by AddTuple

        if (tuplesNum2 == 0) {
            if (needLeftIds) {
                for (ui32 leftId = 0; leftId != tuplesNum1; ++leftId) {
                    leftIds.push_back(leftId);
                }
            }
            continue;
        }
        if (tuplesNum1 == 0 && (hasMoreRightTuples || hasMoreLeftTuples || !bucketStats2->HashtableMatches)) {
            continue;
        }

        ui64 slotSize = ComputeJoinSlotsSizeForBucket(*bucket2, *bucketStats2, headerSize2, table2HasKeyStringColumns, table2HasKeyIColumns);

        ui64& nSlots = bucket2->NSlots;
        auto& joinSlots = bucket2->JoinSlots;
        auto& bloomFilter = bucketStats2->BloomFilter;
        bool initHashTable = false;

        Y_DEBUG_ABORT_UNLESS(bucketStats2->SlotSize == 0 || bucketStats2->SlotSize == slotSize);
        if (!nSlots) {
            nSlots = ComputeNumberOfSlots(tuplesNum2);
            joinSlots.resize(nSlots * slotSize, 0);
            bloomFilter.Resize(tuplesNum2);
            initHashTable = true;
            bucketStats2->SlotSize = slotSize;
            ++InitHashTableCount;
        }

        auto firstSlot = [begin = joinSlots.begin(), slotSize, nSlots](auto hash) {
            ui64 slotNum = hash % nSlots;
            return begin + slotNum * slotSize;
        };

        auto nextSlot = [begin = joinSlots.begin(), end = joinSlots.end(), slotSize](auto it) {
            it += slotSize;
            if (it == end) {
                it = begin;
            }
            return it;
        };

        if (initHashTable) {
            ui32 tuple2Idx = 0;
            auto it2 = bucket2->KeyIntVals.begin();
            for (ui64 keysValSize = headerSize2; it2 != bucket2->KeyIntVals.end(); it2 += keysValSize, ++tuple2Idx) {
                if (table2HasKeyStringColumns || table2HasKeyIColumns) {
                    keysValSize = headerSize2 + *(it2 + headerSize2 - 1);
                }

                ui64 hash = *it2;
                // Note: if hashtable is re-created after being spilled
                // (*(it2 + HashSize) & 1) may be true (even though key does NOT contain NULL)

                bloomFilter.Add(hash);

                auto slotIt = firstSlot(hash);

                ++HashLookups;
                for (; *slotIt != 0; slotIt = nextSlot(slotIt))
                {
                    ++HashO1Iterations;
                }
                ++HashSlotIterations;

                if (keysValSize <= slotSize - 1)
                {
                    std::copy_n(it2, keysValSize, slotIt);
                } else {
                    std::copy_n(it2, headerSize2, slotIt);

                    *(slotIt + headerSize2) = it2 + headerSize2 - bucket2->KeyIntVals.begin();
                }
                slotIt[slotSize - 1] = tuple2Idx;
            }
            bloomFilter.Finalize();
            if (swapTables) {
                JoinTable1Total += tuplesNum2;
            } else {
                JoinTable2Total += tuplesNum2;
            }
        }

        if (swapTables) {
            JoinTable2Total += tuplesNum1;
        } else {
            JoinTable1Total += tuplesNum1;
        }

        ui32 tuple1Idx = 0;
        auto it1 = bucket1->KeyIntVals.begin();
        //  /-------headerSize---------------------------\
        //  hash nulls-bitmap keyInt[] KeyIHash[] strSize| [strPos | strs] slotIdx
        // \---------------------------------------slotSize ---------------------/
        // bit0 of nulls bitmap denotes key-with-nulls
        // strSize only present if HasKeyStrCol || HasKeyICol
        // strPos is only present if (HasKeyStrCol || HasKeyICol) && strSize + headerSize >= slotSize
        // slotSize, slotIdx and strPos is only for hashtable (table2)
        ui64 bloomHits = 0;
        ui64 bloomLookups = 0;

        for (ui64 keysValSize = headerSize1; it1 != bucket1->KeyIntVals.end(); it1 += keysValSize, ++tuple1Idx) {
            if (table1HasKeyStringColumns || table1HasKeyIColumns) {
                keysValSize = headerSize1 + *(it1 + headerSize1 - 1);
            }

            ui64 hash = *it1;

            Y_DEBUG_ABORT_UNLESS((*(it1 + HashSize) & 1) == 0); // Keys with NULL never reaches Join

            if (initHashTable) {
                bloomLookups++;
                if (bloomFilter.IsMissing(hash)) {
                    if (needLeftIds) {
                        leftIds.push_back(tuple1Idx);
                    }
                    bloomHits++;
                    continue;
                }
            }

            ++HashLookups;

            auto saveTuplesFound = tuplesFound;
            auto slotIt = firstSlot(hash);
            for (; *slotIt != 0; slotIt = nextSlot(slotIt))
            {
                ++HashO1Iterations;
                if (*slotIt != hash) {
                    continue;
                }

                auto tuple2Idx = slotIt[slotSize - 1];

                ++HashSlotIterations;
                if (table1HasKeyIColumns || !(keysValSize - nullsSize1 <= slotSize - 1 - nullsSize2)) {
                    // 2nd condition cannot be true unless HasKeyStringColumns or HasKeyIColumns, hence size at the end of header is present

                    if (!std::equal(it1 + keyIntOffset1, it1 + headerSize1 - 1, slotIt + keyIntOffset2)) {
                        continue;
                    }

                    auto slotStringsStart = slotIt + headerSize2;
                    ui64 slotStringsSize = *(slotIt + headerSize2 - 1);

                    if (headerSize2 + slotStringsSize + 1 > slotSize)
                    {
                        ui64 stringsPos = *(slotIt + headerSize2);
                        slotStringsStart = bucket2->KeyIntVals.begin() + stringsPos;
                    }

                    if (table1HasKeyIColumns)
                    {
                        ui64 stringsOffsetsIdx1 = tuple1Idx * (JoinTable1_->NumberOfStringColumns_ + JoinTable1_->NumberOfIColumns_ + 2);
                        ui64 stringsOffsetsIdx2 = tuple2Idx * (JoinTable2_->NumberOfStringColumns_ + JoinTable2_->NumberOfIColumns_ + 2);
                        ui32* stringsSizesPtr1 = bucket1->StringsOffsets.data() + stringsOffsetsIdx1 + 2;
                        ui32* stringsSizesPtr2 = bucket2->StringsOffsets.data() + stringsOffsetsIdx2 + 2;

                        if (!CompareIColumns(stringsSizesPtr1,
                                             (char*)(it1 + headerSize1),
                                             stringsSizesPtr2,
                                             (char*)(slotStringsStart),
                                             JoinTable1_->ColInterfaces_, JoinTable1_->NumberOfStringColumns_, JoinTable1_->NumberOfKeyIColumns_)) {
                            continue;
                        }
                    } else {
                        ui64 stringsSize = *(it1 + headerSize1 - 1);
                        if (stringsSize != slotStringsSize || !std::equal(it1 + headerSize1, it1 + headerSize1 + stringsSize, slotStringsStart)) {
                            continue;
                        }
                    }

                } else {
                    if (!std::equal(it1 + keyIntOffset1, it1 + keysValSize, slotIt + keyIntOffset2)) {
                        continue;
                    }
                }

                *(slotIt + HashSize) |= 1; // mark right slot as matched
                tuplesFound++;
                if (needCrossIds) {
                    TJoinTuplesIds joinIds;
                    joinIds.Id1 = swapTables ? tuple2Idx : tuple1Idx;
                    joinIds.Id2 = swapTables ? tuple1Idx : tuple2Idx;
                    joinResults.emplace_back(joinIds);
                }
            }
            if (saveTuplesFound == tuplesFound) {
                ++BloomFalsePositives;
                if (needLeftIds) {
                    leftIds.push_back(tuple1Idx);
                }
            } else if (isLeftSemi) {
                leftIds.push_back(tuple1Idx);
            }
        }

        if (!hasMoreLeftTuples && !hasMoreRightTuples) {
            bloomFilter.Shrink();

            if (bucketStats2->HashtableMatches) {
                auto slotIt = joinSlots.cbegin();
                auto end = joinSlots.cend();
                auto isSemi = JoinKind_ == EJoinKind::LeftSemi || JoinKind_ == EJoinKind::RightSemi;
                auto& leftIds2 = bucket2->LeftIds;

                for (; slotIt != end; slotIt += slotSize) {
                    if ((*(slotIt + HashSize) & 1) == isSemi && *slotIt != 0) {
                        auto id2 = *(slotIt + slotSize - 1);

                        Y_DEBUG_ABORT_UNLESS(id2 < bucketStats2->TuplesNum);
                        leftIds2.push_back(id2);
                    }
                }
                std::sort(leftIds2.begin(), leftIds2.end());
            }
            joinSlots.clear();
            joinSlots.shrink_to_fit();
            nSlots = 0;
        }

        if (bloomHits < bloomLookups / 8) {
            // Bloomfilter was inefficient, drop it
            bloomFilter.Shrink();
        }
        BloomHits += bloomHits;
        BloomLookups += bloomLookups;

        UDF_LOG(Logger_, LogComponent_, GRACEJOIN_TRACE, TStringBuilder() << (const void*)this << '#' << bucket << " Table1 " << JoinTable1_->TableBucketsStats_[bucket].TuplesNum << " Table2 " << JoinTable2_->TableBucketsStats_[bucket].TuplesNum << " LeftTableBatch " << LeftTableBatch_ << " RightTableBatch " << RightTableBatch_ << " leftIds " << leftIds.size() << " joinIds " << joinResults.size() << " joinKind " << (int)JoinKind_ << " swapTables " << swapTables << " initHashTable " << initHashTable);
    }

    HasMoreLeftTuples_ = hasMoreLeftTuples;
    HasMoreRightTuples_ = hasMoreRightTuples;

    TuplesFound_ += tuplesFound;
}

inline void TTable::GetTupleData(ui32 bucketNum, ui32 tupleId, TupleData& td) {
    ui64 keyIntsOffset = 0;
    ui64 dataIntsOffset = 0;
    ui64 keyStringsOffset = 0;
    ui64 dataStringsOffset = 0;

    td.AllNulls = false;

    TotalUnpacked_++;

    TTableBucket& tb = TableBuckets_[bucketNum];
    ui64 stringsOffsetsIdx = tupleId * (NumberOfStringColumns_ + NumberOfIColumns_ + 2);

    if (NumberOfKeyStringColumns_ != 0 || NumberOfKeyIColumns_ != 0) {
        keyIntsOffset = tb.StringsOffsets[stringsOffsetsIdx];
    } else {
        keyIntsOffset = HeaderSize_ * tupleId;
    }

    for (ui64 i = 0; i < NumberOfKeyIntColumns_ + NullsBitmapSize_; ++i) {
        td.IntColumns[i] = tb.KeyIntVals[keyIntsOffset + HashSize + i];
    }

    dataIntsOffset = NumberOfDataIntColumns_ * tupleId;

    for (ui64 i = 0; i < NumberOfDataIntColumns_; ++i) {
        td.IntColumns[NumberOfKeyIntColumns_ + NullsBitmapSize_ + i] = tb.DataIntVals[dataIntsOffset + i];
    }

    char* strPtr = nullptr;
    if (NumberOfKeyStringColumns_ != 0 || NumberOfKeyIColumns_ != 0) {
        keyStringsOffset = tb.StringsOffsets[stringsOffsetsIdx] + HeaderSize_;

        strPtr = reinterpret_cast<char*>(tb.KeyIntVals.data() + keyStringsOffset);

        for (ui64 i = 0; i < NumberOfKeyStringColumns_; ++i)
        {
            td.StrSizes[i] = tb.StringsOffsets[stringsOffsetsIdx + 2 + i];
            Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui32>(strPtr) == td.StrSizes[i]);
            strPtr += sizeof(ui32);
            td.StrColumns[i] = strPtr;
            strPtr += td.StrSizes[i];
        }

        for (ui64 i = 0; i < NumberOfKeyIColumns_; i++) {
            ui32 currSize = tb.StringsOffsets[stringsOffsetsIdx + 2 + NumberOfKeyStringColumns_ + i];
            Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui32>(strPtr) == currSize);
            strPtr += sizeof(ui32);
            *(td.IColumns + i) = (ColInterfaces_ + i)->Packer->Unpack(TStringBuf(strPtr, currSize), ColInterfaces_->HolderFactory);
            strPtr += currSize;
        }
    }

    if (NumberOfDataStringColumns_ || NumberOfDataIColumns_ != 0) {
        dataStringsOffset = tb.StringsOffsets[stringsOffsetsIdx + 1];
    }

    strPtr = (tb.StringsValues.data() + dataStringsOffset);

    for (ui64 i = 0; i < NumberOfDataStringColumns_; ++i) {
        ui32 currIdx = NumberOfKeyStringColumns_ + i;
        td.StrColumns[currIdx] = strPtr;
        td.StrSizes[currIdx] = tb.StringsOffsets[stringsOffsetsIdx + 2 + currIdx];
        strPtr += td.StrSizes[currIdx];
    }

    for (ui64 i = 0; i < NumberOfDataIColumns_; i++) {
        ui32 currIdx = NumberOfStringColumns_ + NumberOfKeyIColumns_ + i;
        ui32 currSize = tb.StringsOffsets[stringsOffsetsIdx + 2 + currIdx];

        *(td.IColumns + NumberOfKeyIColumns_ + i) = (ColInterfaces_ + NumberOfKeyIColumns_ + i)->Packer->Unpack(TStringBuf(strPtr, currSize), ColInterfaces_->HolderFactory);

        strPtr += currSize;
    }
}

inline bool TTable::AddKeysToHashTable(TKeysHashTable& t, ui64* keys, NYql::NUdf::TUnboxedValue* iColumns) {
    if (t.NSlots == 0) {
        t.SlotSize = HeaderSize_ + NumberOfKeyStringColumns_ * 2;
        t.Table.resize(DefaultTuplesNum * t.SlotSize, 0);
        t.NSlots = DefaultTuplesNum;
    }

    if (t.FillCount > t.NSlots / 2) {
        ResizeHashTable(t, 2 * t.NSlots + 1);
    }

    if ((*(keys + HashSize) & 1)) { // Keys with null value
        return true;
    }

    ui64 hash = *keys;
    ui64 slot = hash % t.NSlots;
    auto it = t.Table.begin() + slot * t.SlotSize;

    ui64 keyIntOffset = HashSize + NullsBitmapSize_;
    ui64 keysSize = HeaderSize_;
    ui64 keyStringsSize = 0;
    if (NumberOfKeyStringColumns_ > 0 || NumberOfKeyIColumns_ > 0) {
        keyStringsSize = *(keys + HeaderSize_ - 1);
        keysSize = HeaderSize_ + keyStringsSize;
    }

    auto nextSlot = [begin = t.Table.begin(), end = t.Table.end(), slotSize = t.SlotSize](auto it) {
        it += slotSize;
        if (it == end) {
            it = begin;
        }
        return it;
    };

    for (auto itValSize = HeaderSize_; *it != 0; it = nextSlot(it)) {
        if (*it != hash) {
            continue;
        }

        if (NumberOfKeyIColumns_ == 0 && (itValSize <= t.SlotSize)) {
            if (!std::equal(it + keyIntOffset, it + itValSize, keys + keyIntOffset)) {
                continue;
            }
            return false;
        }

        Y_DEBUG_ABORT_UNLESS(NumberOfKeyStringColumns_ > 0 || NumberOfKeyIColumns_ > 0);

        itValSize = HeaderSize_ + *(it + HeaderSize_ - 1);
        auto slotStringsStart = it + HeaderSize_;

        if (!std::equal(it + keyIntOffset, it + HeaderSize_ - 1, keys + keyIntOffset)) {
            continue;
        }

        if (NumberOfKeyIColumns_ > 0) {
            if (!CompareIColumns(
                    (char*)(slotStringsStart),
                    (char*)(keys + HeaderSize_),
                    iColumns,
                    JoinTable1_->ColInterfaces_, JoinTable1_->NumberOfStringColumns_, JoinTable1_->NumberOfKeyIColumns_)) {
                continue;
            }
            return false;
        }

        Y_DEBUG_ABORT_UNLESS(!(itValSize <= t.SlotSize));

        ui64 stringsPos = *(it + HeaderSize_);
        slotStringsStart = t.SpillData.begin() + stringsPos;

        if (keysSize != itValSize || !std::equal(slotStringsStart, slotStringsStart + itValSize, keys + HeaderSize_)) {
            continue;
        }
        return false;
    }

    if (keysSize > t.SlotSize) {
        ui64 spillDataOffset = t.SpillData.size();
        t.SpillData.insert(t.SpillData.end(), keys + HeaderSize_, keys + keysSize);
        std::copy_n(keys, HeaderSize_, it);
        *(it + HeaderSize_) = spillDataOffset;
    } else {
        std::copy_n(keys, keysSize, it);
    }

    t.FillCount++;
    return true;
}

bool TTable::NextJoinedData(TupleData& td1, TupleData& td2, ui64 bucketLimit) {
    while (CurrIterBucket_ < bucketLimit) {
        if (auto& joinIds = TableBuckets_[CurrIterBucket_].JoinIds; CurrIterIndex_ != joinIds.size()) {
            Y_DEBUG_ABORT_UNLESS(JoinKind_ == EJoinKind::Inner || JoinKind_ == EJoinKind::Left || JoinKind_ == EJoinKind::Right || JoinKind_ == EJoinKind::Full);
            auto ids = joinIds[CurrIterIndex_++];

            JoinTable1_->GetTupleData(CurrIterBucket_, ids.Id1, td1);
            JoinTable2_->GetTupleData(CurrIterBucket_, ids.Id2, td2);

            return true;
        }

        auto leftSide = [this](auto sideTable, auto& tdL, auto& tdR) {
            const auto& bucket = sideTable->TableBuckets_[CurrIterBucket_];
            auto& currIterIndex = sideTable->CurrIterIndex_;
            const auto& leftIds = bucket.LeftIds;

            if (currIterIndex != leftIds.size()) {
                auto id = leftIds[currIterIndex++];

                sideTable->GetTupleData(CurrIterBucket_, id, tdL);
                tdR.AllNulls = true;

                return true;
            }

            return false;
        };

        if (leftSide(JoinTable1_, td1, td2)) {
            return true;
        }
        if (leftSide(JoinTable2_, td2, td1)) {
            return true;
        }

        ++CurrIterBucket_;
        CurrIterIndex_ = 0;
        JoinTable1_->CurrIterIndex_ = 0;
        JoinTable2_->CurrIterIndex_ = 0;
    }

    return false;
}

void TTable::Clear() {
    for (ui64 bucket = 0; bucket < NumberOfBuckets; bucket++) {
        ClearBucket(bucket);
    }
}

void TTable::ClearBucket(ui64 bucket) {
    TTableBucket& tb = TableBuckets_[bucket];
    tb.KeyIntVals.clear();
    tb.DataIntVals.clear();
    tb.StringsOffsets.clear();
    tb.StringsValues.clear();
    tb.InterfaceValues.clear();
    tb.InterfaceOffsets.clear();
    tb.JoinIds.clear();
    tb.LeftIds.clear();
    tb.JoinSlots.clear();
    tb.NSlots = 0;

    TTableBucketStats& tbs = TableBucketsStats_[bucket];
    tbs.TuplesNum = 0;
    tbs.KeyIntValsTotalSize = 0;
    tbs.StringValuesTotalSize = 0;
}

void TTable::ShrinkBucket(ui64 bucket) {
    TTableBucket& tb = TableBuckets_[bucket];
    tb.KeyIntVals.shrink_to_fit();
    tb.DataIntVals.shrink_to_fit();
    tb.StringsOffsets.shrink_to_fit();
    tb.StringsValues.shrink_to_fit();
    tb.InterfaceValues.shrink_to_fit();
    tb.InterfaceOffsets.shrink_to_fit();
    tb.JoinIds.shrink_to_fit();
    tb.LeftIds.shrink_to_fit();
    tb.JoinSlots.shrink_to_fit();
}

void TTable::InitializeBucketSpillers(ISpiller::TPtr spiller) {
    for (size_t i = 0; i < NumberOfBuckets; ++i) {
        TableBucketsSpillers_.emplace_back(spiller, 5_MB);
    }
}

ui64 TTable::GetSizeOfBucket(ui64 bucket) const {
    return TableBuckets_[bucket].KeyIntVals.size() * sizeof(ui64) + TableBuckets_[bucket].JoinSlots.size() * sizeof(ui64) + TableBuckets_[bucket].DataIntVals.size() * sizeof(ui64) + TableBuckets_[bucket].StringsValues.size() + TableBuckets_[bucket].StringsOffsets.size() * sizeof(ui32) + TableBuckets_[bucket].InterfaceValues.size() + TableBuckets_[bucket].InterfaceOffsets.size() * sizeof(ui32);
}

bool TTable::TryToReduceMemoryAndWait(ui64 bucket) {
    if (GetSizeOfBucket(bucket) < SpillingSizeLimit / NumberOfBuckets) {
        return false;
    }
    if (const auto& tbs = TableBucketsStats_[bucket]; tbs.HashtableMatches) {
        auto& tb = TableBuckets_[bucket];

        if (!tb.JoinSlots.empty()) {
            const auto slotSize = tbs.SlotSize;
            Y_DEBUG_ABORT_UNLESS(slotSize);
            auto it = tb.JoinSlots.cbegin();
            const auto end = tb.JoinSlots.cend();

            for (; it != end; it += slotSize) {
                // Note: we need not check if *it is 0
                if ((*(it + HashSize) & 1)) {
                    ui64 keyIntsOffset;
                    auto tupleId = *(it + slotSize - 1);
                    Y_DEBUG_ABORT_UNLESS(tupleId < tbs.TuplesNum);

                    if (NumberOfKeyStringColumns_ != 0 || NumberOfKeyIColumns_ != 0) {
                        ui64 stringsOffsetsIdx = tupleId * (NumberOfStringColumns_ + NumberOfIColumns_ + 2);
                        keyIntsOffset = tb.StringsOffsets[stringsOffsetsIdx];
                    } else {
                        keyIntsOffset = HeaderSize_ * tupleId;
                    }
                    tb.KeyIntVals[keyIntsOffset + HashSize] |= 1;
                }
            }
            tb.JoinSlots.clear();
            tb.JoinSlots.shrink_to_fit();
        }
    }
    TableBucketsSpillers_[bucket].SpillBucket(std::move(TableBuckets_[bucket]));
    TableBuckets_[bucket] = TTableBucket{};

    return TableBucketsSpillers_[bucket].IsProcessingSpilling();
}

void TTable::UpdateSpilling() {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        TableBucketsSpillers_[i].Update();
    }
}

bool TTable::IsSpillingFinished() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers_[i].IsProcessingSpilling()) {
            return false;
        }
    }
    return true;
}

bool TTable::IsSpillingAcceptingDataRequests() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers_[i].IsInMemory()) {
            continue;
        }

        if (!TableBucketsSpillers_[i].IsAcceptingDataRequests()) {
            return false;
        }
    }
    return true;
}

bool TTable::IsRestoringSpilledBuckets() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers_[i].IsRestoring()) {
            return true;
        }
    }
    return false;
}

void TTable::FinalizeSpilling() {
    for (ui32 bucket = 0; bucket < NumberOfBuckets; ++bucket) {
        if (!TableBucketsSpillers_[bucket].IsInMemory()) {
            TableBucketsSpillers_[bucket].Finalize();
            TableBucketsSpillers_[bucket].SpillBucket(std::move(TableBuckets_[bucket]));
            TableBuckets_[bucket] = TTableBucket{};
        }
    }
}

bool TTable::IsBucketInMemory(ui32 bucket) const {
    return TableBucketsSpillers_[bucket].IsInMemory();
}

bool TTable::IsSpilledBucketWaitingForExtraction(ui32 bucket) const {
    return TableBucketsSpillers_[bucket].IsExtractionRequired();
}

void TTable::StartLoadingBucket(ui32 bucket) {
    MKQL_ENSURE(!TableBucketsSpillers_[bucket].IsInMemory(), "Internal logic error");

    TableBucketsSpillers_[bucket].StartBucketRestoration();
}

void TTable::PrepareBucket(ui64 bucket) {
    if (!TableBucketsSpillers_[bucket].IsExtractionRequired()) {
        return;
    }
    TableBuckets_[bucket] = std::move(TableBucketsSpillers_[bucket].ExtractBucket());
}

// Creates new table with key columns and data columns
TTable::TTable(NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent,
               ui64 numberOfKeyIntColumns, ui64 numberOfKeyStringColumns,
               ui64 numberOfDataIntColumns, ui64 numberOfDataStringColumns,
               ui64 numberOfKeyIColumns, ui64 numberOfDataIColumns,
               ui64 nullsBitmapSize, TColTypeInterface* colInterfaces,
               bool isAny)
    :

    NumberOfKeyIntColumns_(numberOfKeyIntColumns)
    ,
    NumberOfKeyStringColumns_(numberOfKeyStringColumns)
    ,
    NumberOfKeyIColumns_(numberOfKeyIColumns)
    ,
    NumberOfDataIntColumns_(numberOfDataIntColumns)
    ,
    NumberOfDataStringColumns_(numberOfDataStringColumns)
    ,
    NumberOfDataIColumns_(numberOfDataIColumns)
    ,
    ColInterfaces_(colInterfaces)
    ,
    NullsBitmapSize_(nullsBitmapSize)
    ,
    IsAny_(isAny)
    ,
    Logger_(std::move(logger))
    ,
    LogComponent_(logComponent)
{
    NumberOfKeyColumns_ = NumberOfKeyIntColumns_ + NumberOfKeyStringColumns_ + NumberOfKeyIColumns_;
    NumberOfDataColumns_ = NumberOfDataIntColumns_ + NumberOfDataStringColumns_ + NumberOfDataIColumns_;
    NumberOfColumns_ = NumberOfKeyColumns_ + NumberOfDataColumns_;
    NumberOfStringColumns_ = NumberOfKeyStringColumns_ + NumberOfDataStringColumns_;
    NumberOfIColumns_ = NumberOfKeyIColumns_ + NumberOfDataIColumns_;

    BytesInKeyIntColumns_ = NumberOfKeyIntColumns_ * sizeof(ui64);

    TotalStringsSize_ = (numberOfKeyStringColumns > 0 || NumberOfKeyIColumns_ > 0) ? 1 : 0;

    HeaderSize_ = HashSize + NullsBitmapSize_ + NumberOfKeyIntColumns_ + NumberOfKeyIColumns_ + TotalStringsSize_;

    TableBuckets_.resize(NumberOfBuckets);
    TableBucketsStats_.resize(NumberOfBuckets);

    const ui64 reservedSizePerTuple = (2 * DefaultTupleBytes) / sizeof(ui64);

    TempTuple_.reserve(reservedSizePerTuple);
    IColumnsHashes_.resize(NumberOfKeyIColumns_);
    IColumnsVals_.resize(NumberOfIColumns_);

    const ui64 totalForTuples = DefaultTuplesNum * reservedSizePerTuple;

    for (auto& b : TableBuckets_) {
        b.KeyIntVals.reserve((totalForTuples * NumberOfKeyColumns_) / (NumberOfColumns_ + 1));
        b.StringsOffsets.reserve((totalForTuples * NumberOfStringColumns_) / (NumberOfColumns_ + 1));
        b.DataIntVals.reserve((totalForTuples * NumberOfDataIntColumns_) / (NumberOfColumns_ + 1));
        b.StringsValues.reserve((totalForTuples * NumberOfStringColumns_) / (NumberOfColumns_ + 1));
        b.InterfaceOffsets.reserve((totalForTuples * NumberOfIColumns_) / (NumberOfColumns_ + 1));
        b.InterfaceValues.reserve((totalForTuples * NumberOfIColumns_) / (NumberOfColumns_ + 1));
    }
}

TTable::~TTable() {
    UDF_LOG_IF(InitHashTableCount, Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)this << '#' << "InitHashTableCount " << InitHashTableCount << " BloomLookups " << BloomLookups << " BloomHits " << BloomHits << " BloomFalsePositives " << BloomFalsePositives << " HashLookups " << HashLookups << " HashChainTraversal " << HashO1Iterations / (double)HashLookups << " HashSlotOperations " << HashSlotIterations / (double)HashLookups << " Table1 " << JoinTable1Total << " Table2 " << JoinTable2Total << " TuplesFound " << TuplesFound_);

    UDF_LOG_IF(JoinTable1_ && JoinTable1_->AnyFiltered, Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)this << '#' << "L AnyFiltered " << JoinTable1_->AnyFiltered);
    UDF_LOG_IF(JoinTable1_ && JoinTable1_->BloomLookups, Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)this << '#' << "L BloomLookups " << JoinTable1_->BloomLookups << " BloomHits " << JoinTable1_->BloomHits);
    UDF_LOG_IF(JoinTable2_ && JoinTable2_->AnyFiltered, Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)this << '#' << "R AnyFiltered " << JoinTable2_->AnyFiltered);
    UDF_LOG_IF(JoinTable2_ && JoinTable2_->BloomLookups, Logger_, LogComponent_, GRACEJOIN_DEBUG, TStringBuilder() << (const void*)this << '#' << "R BloomLookups " << JoinTable2_->BloomLookups << " BloomHits " << JoinTable2_->BloomHits);
};

TTableBucketSpiller::TTableBucketSpiller(ISpiller::TPtr spiller, size_t sizeLimit)
    : StateUi64Adapter_(spiller, sizeLimit)
    , StateUi32Adapter_(spiller, sizeLimit)
    , StateCharAdapter_(spiller, sizeLimit)
{
}

void TTableBucketSpiller::Update() {
    StateUi64Adapter_.Update();
    StateUi32Adapter_.Update();
    StateCharAdapter_.Update();

    if (State_ == EState::Spilling) {
        ProcessBucketSpilling();
    } else if (State_ == EState::Finalizing) {
        ProcessFinalizing();
    } else if (State_ == EState::Restoring) {
        ProcessBucketRestoration();
    }
}

void TTableBucketSpiller::Finalize() {
    IsFinalizingRequested_ = true;
}

void TTableBucketSpiller::SpillBucket(TTableBucket&& bucket) {
    MKQL_ENSURE(NextVectorToProcess_ == ENextVectorToProcess::None, "Internal logic error");
    State_ = EState::Spilling;

    CurrentBucket_ = std::move(bucket);
    NextVectorToProcess_ = ENextVectorToProcess::KeyAndVals;

    ProcessBucketSpilling();
}

TTableBucket&& TTableBucketSpiller::ExtractBucket() {
    MKQL_ENSURE(State_ == EState::WaitingForExtraction, "Internal logic error");
    MKQL_ENSURE(SpilledBucketsCount_ == 0, "Internal logic error");
    State_ = EState::InMemory;
    return std::move(CurrentBucket_);
}

bool TTableBucketSpiller::IsInMemory() const {
    return State_ == EState::InMemory;
}

bool TTableBucketSpiller::IsExtractionRequired() const {
    return State_ == EState::WaitingForExtraction;
}

bool TTableBucketSpiller::IsProcessingSpilling() const {
    return State_ == EState::Spilling;
}

bool TTableBucketSpiller::IsAcceptingDataRequests() const {
    return State_ == EState::AcceptingDataRequests;
}

bool TTableBucketSpiller::IsRestoring() const {
    return State_ == EState::Restoring;
}

void TTableBucketSpiller::StartBucketRestoration() {
    MKQL_ENSURE(State_ == EState::AcceptingDataRequests, "Internal logic error");
    MKQL_ENSURE(NextVectorToProcess_ == ENextVectorToProcess::None, "Internal logic error");

    NextVectorToProcess_ = ENextVectorToProcess::KeyAndVals;
    State_ = EState::Restoring;
    ProcessBucketRestoration();
}

void TTableBucketSpiller::ProcessBucketSpilling() {
    while (NextVectorToProcess_ != ENextVectorToProcess::None) {
        switch (NextVectorToProcess_) {
            case ENextVectorToProcess::KeyAndVals:
                if (!StateUi64Adapter_.IsAcceptingData()) {
                    return;
                }

                StateUi64Adapter_.AddData(std::move(CurrentBucket_.KeyIntVals));
                NextVectorToProcess_ = ENextVectorToProcess::DataIntVals;
                break;
            case ENextVectorToProcess::DataIntVals:
                if (!StateUi64Adapter_.IsAcceptingData()) {
                    return;
                }

                StateUi64Adapter_.AddData(std::move(CurrentBucket_.DataIntVals));
                NextVectorToProcess_ = ENextVectorToProcess::StringsValues;
                break;
            case ENextVectorToProcess::StringsValues:
                if (!StateCharAdapter_.IsAcceptingData()) {
                    return;
                }

                StateCharAdapter_.AddData(std::move(CurrentBucket_.StringsValues));
                NextVectorToProcess_ = ENextVectorToProcess::StringsOffsets;
                break;
            case ENextVectorToProcess::StringsOffsets:
                if (!StateUi32Adapter_.IsAcceptingData()) {
                    return;
                }

                StateUi32Adapter_.AddData(std::move(CurrentBucket_.StringsOffsets));
                NextVectorToProcess_ = ENextVectorToProcess::InterfaceValues;
                break;
            case ENextVectorToProcess::InterfaceValues:
                if (!StateCharAdapter_.IsAcceptingData()) {
                    return;
                }

                StateCharAdapter_.AddData(std::move(CurrentBucket_.InterfaceValues));
                NextVectorToProcess_ = ENextVectorToProcess::InterfaceOffsets;
                break;
            case ENextVectorToProcess::InterfaceOffsets:
                if (!StateUi32Adapter_.IsAcceptingData()) {
                    return;
                }

                StateUi32Adapter_.AddData(std::move(CurrentBucket_.InterfaceOffsets));
                NextVectorToProcess_ = ENextVectorToProcess::None;
                SpilledBucketsCount_++;

                break;
            default:
                return;
        }
    }

    if (IsFinalizingRequested_) {
        if (!StateCharAdapter_.IsAcceptingData() || !StateUi32Adapter_.IsAcceptingData() || !StateUi64Adapter_.IsAcceptingData()) {
            return;
        }
        State_ = EState::Finalizing;
        StateUi64Adapter_.Finalize();
        StateUi32Adapter_.Finalize();
        StateCharAdapter_.Finalize();

        ProcessFinalizing();
        return;
    }
    State_ = EState::AcceptingData;
}

void TTableBucketSpiller::ProcessFinalizing() {
    if (StateCharAdapter_.IsAcceptingDataRequests() && StateUi32Adapter_.IsAcceptingDataRequests() && StateUi64Adapter_.IsAcceptingDataRequests()) {
        State_ = EState::AcceptingDataRequests;
    }
}

template <class T>
void TTableBucketSpiller::AppendVector(std::vector<T, TMKQLAllocator<T>>& first, std::vector<T, TMKQLAllocator<T>>&& second) const {
    if (first.empty()) {
        first = std::move(second);
        return;
    }
    first.insert(first.end(), second.begin(), second.end());
    second.clear();
}

void TTableBucketSpiller::ProcessBucketRestoration() {
    while (NextVectorToProcess_ != ENextVectorToProcess::None) {
        switch (NextVectorToProcess_) {
            case ENextVectorToProcess::KeyAndVals:
                if (StateUi64Adapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.KeyIntVals, StateUi64Adapter_.ExtractVector());
                    NextVectorToProcess_ = ENextVectorToProcess::DataIntVals;
                    break;
                }

                if (StateUi64Adapter_.IsAcceptingDataRequests()) {
                    StateUi64Adapter_.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::DataIntVals:
                if (StateUi64Adapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.DataIntVals, StateUi64Adapter_.ExtractVector());
                    NextVectorToProcess_ = ENextVectorToProcess::StringsValues;
                    break;
                }

                if (StateUi64Adapter_.IsAcceptingDataRequests()) {
                    StateUi64Adapter_.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::StringsValues:
                if (StateCharAdapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.StringsValues, StateCharAdapter_.ExtractVector());
                    NextVectorToProcess_ = ENextVectorToProcess::StringsOffsets;
                    break;
                }

                if (StateCharAdapter_.IsAcceptingDataRequests()) {
                    StateCharAdapter_.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::StringsOffsets:
                if (StateUi32Adapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.StringsOffsets, StateUi32Adapter_.ExtractVector());
                    NextVectorToProcess_ = ENextVectorToProcess::InterfaceValues;
                    break;
                }

                if (StateUi32Adapter_.IsAcceptingDataRequests()) {
                    StateUi32Adapter_.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::InterfaceValues:
                if (StateCharAdapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.InterfaceValues, StateCharAdapter_.ExtractVector());
                    NextVectorToProcess_ = ENextVectorToProcess::InterfaceOffsets;
                    break;
                }

                if (StateCharAdapter_.IsAcceptingDataRequests()) {
                    StateCharAdapter_.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::InterfaceOffsets:
                if (StateUi32Adapter_.IsDataReady()) {
                    AppendVector(CurrentBucket_.InterfaceOffsets, StateUi32Adapter_.ExtractVector());

                    SpilledBucketsCount_--;
                    if (SpilledBucketsCount_ == 0) {
                        NextVectorToProcess_ = ENextVectorToProcess::None;
                        State_ = EState::WaitingForExtraction;
                    } else {
                        NextVectorToProcess_ = ENextVectorToProcess::KeyAndVals;
                    }

                    break;
                }

                if (StateUi32Adapter_.IsAcceptingDataRequests()) {
                    StateUi32Adapter_.RequestNextVector();
                    break;
                }
                return;
            default:
                return;
        }
    }
}

} // namespace NKikimr::NMiniKQL::NGraceJoin
