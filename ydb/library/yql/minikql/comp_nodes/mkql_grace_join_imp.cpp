#include "mkql_grace_join_imp.h"

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/log/log.h>

#include <contrib/libs/xxhash/xxhash.h>
#include <string_view>


namespace NKikimr {
namespace NMiniKQL {

namespace GraceJoin {


TTable::EAddTupleResult TTable::AddTuple(  ui64 * intColumns, char ** stringColumns, ui32 * stringsSizes, NYql::NUdf::TUnboxedValue * iColumns, const TTable &other) {

    if ((intColumns[0] & 1))
        return EAddTupleResult::Unmatched;

    TotalPacked++;

    TempTuple.clear();
    TempTuple.insert(TempTuple.end(), intColumns, intColumns + NullsBitmapSize_ + NumberOfKeyIntColumns);

    if ( NumberOfKeyIColumns > 0 ) {
        for (ui32 i = 0; i < NumberOfKeyIColumns; i++) {
            TempTuple.push_back((ColInterfaces + i)->HashI->Hash(*(iColumns+i)));
        }
    }


    ui64 totalBytesForStrings = 0;
    ui64 totalIntsForStrings = 0;

    // Processing variable length string columns
    if ( NumberOfKeyStringColumns != 0 || NumberOfKeyIColumns != 0) {

        totalBytesForStrings += sizeof(ui32)*NumberOfKeyStringColumns;
        totalBytesForStrings += sizeof(ui32)*NumberOfKeyIColumns;

        for( ui64 i = 0; i < NumberOfKeyStringColumns; i++ ) {
            totalBytesForStrings += stringsSizes[i];
        }

        for ( ui64 i = 0; i < NumberOfKeyIColumns; i++) {
            TStringBuf val = (ColInterfaces + i)->Packer->Pack(*(iColumns+i));
            IColumnsVals[i].clear();
            IColumnsVals[i].insert(IColumnsVals[i].begin(), val.cbegin(), val.end());
            totalBytesForStrings += val.size();
        }

        totalIntsForStrings = (totalBytesForStrings + sizeof(ui64) - 1) / sizeof(ui64);

        TempTuple.push_back(totalIntsForStrings);
        TempTuple.resize(TempTuple.size() + totalIntsForStrings);

        TempTuple.back() = 0;

        ui64 * startPtr = (TempTuple.data() + TempTuple.size() - totalIntsForStrings );
        char * currStrPtr = reinterpret_cast< char* > (startPtr);

        for( ui64 i = 0; i < NumberOfKeyStringColumns; i++) {
            WriteUnaligned<ui32>(currStrPtr, stringsSizes[i] );
            currStrPtr+=sizeof(ui32);
            std::memcpy(currStrPtr, stringColumns[i], stringsSizes[i] );
            currStrPtr+=stringsSizes[i];
        }

        for( ui64 i = 0; i < NumberOfKeyIColumns; i++) {
            WriteUnaligned<ui32>(currStrPtr, IColumnsVals[i].size() );
            currStrPtr+=sizeof(ui32);
            std::memcpy(currStrPtr, IColumnsVals[i].data(), IColumnsVals[i].size() );
            currStrPtr+=IColumnsVals[i].size();
        }
    }

    XXH64_hash_t hash = XXH64(TempTuple.data() + NullsBitmapSize_, (TempTuple.size() - NullsBitmapSize_) * sizeof(ui64), 0);

    if (!hash) hash = 1;

    ui64 bucket = hash & BucketsMask;

    if (!IsAny_ && other.TableBucketsStats[bucket].BloomFilter.IsFinalized())  {
        auto bucket2 = &other.TableBucketsStats[bucket];
        auto &bloomFilter = bucket2->BloomFilter;
        ++BloomLookups_;
        if (bloomFilter.IsMissing(hash)) {
            ++BloomHits_;
            return EAddTupleResult::Unmatched;
        }
    }

    std::vector<ui64, TMKQLAllocator<ui64>> & keyIntVals = TableBuckets[bucket].KeyIntVals;
    std::vector<ui32, TMKQLAllocator<ui32>> & stringsOffsets = TableBuckets[bucket].StringsOffsets;
    std::vector<ui64, TMKQLAllocator<ui64>> & dataIntVals = TableBuckets[bucket].DataIntVals;
    std::vector<char, TMKQLAllocator<char>> & stringVals = TableBuckets[bucket].StringsValues;
    KeysHashTable & kh = TableBucketsStats[bucket].AnyHashTable;

    ui32 offset = keyIntVals.size();  // Offset of tuple inside the keyIntVals vector

    keyIntVals.push_back(hash);
    keyIntVals.insert(keyIntVals.end(), TempTuple.begin(), TempTuple.end());

    if (IsAny_) {
        if ( !AddKeysToHashTable(kh, keyIntVals.begin() + offset, iColumns) ) {
            keyIntVals.resize(offset);
            ++AnyFiltered_;
            return EAddTupleResult::AnyMatch;
        }

        if (other.TableBucketsStats[bucket].BloomFilter.IsFinalized())  {
            auto bucket2 = &other.TableBucketsStats[bucket];
            auto &bloomFilter = bucket2->BloomFilter;
            ++BloomLookups_;
            if (bloomFilter.IsMissing(hash)) {
                keyIntVals.resize(offset);
                ++BloomHits_;
                return EAddTupleResult::Unmatched;
            }
        }
    }

    TableBucketsStats[bucket].TuplesNum++;

    if (NumberOfStringColumns || NumberOfIColumns ) {
        stringsOffsets.push_back(TableBucketsStats[bucket].KeyIntValsTotalSize); // Adding offset to tuple in keyIntVals vector
        stringsOffsets.push_back(TableBucketsStats[bucket].StringValuesTotalSize);  // Adding offset to string values


        // Adding strings sizes for keys and data
        if ( NumberOfStringColumns ) {
            stringsOffsets.insert( stringsOffsets.end(), stringsSizes, stringsSizes+NumberOfStringColumns );
        }

        if ( NumberOfIColumns ) {
            for ( ui64 i = NumberOfKeyIColumns; i < NumberOfIColumns; i++) {
                TStringBuf val = (ColInterfaces + i)->Packer->Pack(*(iColumns+i));
                IColumnsVals[i].clear();
                IColumnsVals[i].insert(IColumnsVals[i].begin(), val.cbegin(), val.end());
            }
            for (ui64 i = 0; i < NumberOfIColumns; i++ ) {
                stringsOffsets.push_back(IColumnsVals[i].size());
            }
        }
    }

    // Adding data values
    ui64 * dataColumns = intColumns + NullsBitmapSize_ + NumberOfKeyIntColumns;
    dataIntVals.insert(dataIntVals.end(), dataColumns, dataColumns + NumberOfDataIntColumns);

    // Adding strings values for data columns
    char ** dataStringsColumns = stringColumns + NumberOfKeyStringColumns;
    ui32 * dataStringsSizes = stringsSizes + NumberOfKeyStringColumns;

    ui64 initialStringsSize = stringVals.size();
    for( ui64 i = 0; i < NumberOfDataStringColumns; i++) {
        ui32 currStringSize = *(dataStringsSizes + i);
        stringVals.insert(stringVals.end(), *(dataStringsColumns + i), *(dataStringsColumns + i) + currStringSize);
    }

    for ( ui64 i = 0; i < NumberOfDataIColumns; i++) {
        stringVals.insert( stringVals.end(), IColumnsVals[NumberOfKeyIColumns + i].begin(), IColumnsVals[NumberOfKeyIColumns + i].end());
    }

    TableBucketsStats[bucket].KeyIntValsTotalSize += keyIntVals.size() - offset;
    TableBucketsStats[bucket].StringValuesTotalSize += stringVals.size() - initialStringsSize;
    return EAddTupleResult::Added;
}

void TTable::ResetIterator() {
    CurrIterIndex = 0;
    CurrIterBucket = 0;
    CurrJoinIdsIterIndex = 0;
    Table2Initialized_ = false;
    if (IsTableJoined) {
        JoinTable1->ResetIterator();
        JoinTable2->ResetIterator();
    }
    TotalUnpacked = 0;
}

// Checks if there are more tuples and sets bucketId and tupleId to next valid.
inline bool HasMoreTuples(std::vector<TTableBucketStats> & tableBucketsStats, ui64 & bucketId, ui64 & tupleId, ui64 bucketLimit ) {

    if (bucketId >= bucketLimit) return false;

    if ( tupleId >= tableBucketsStats[bucketId].TuplesNum ) {
        tupleId = 0;
        bucketId ++;

        if (bucketId == bucketLimit) {
            return false;
        }

        while( tableBucketsStats[bucketId].TuplesNum == 0 ) {
           bucketId ++;
            if (bucketId == bucketLimit) {
                return false;
            }
        }
    }

    return true;

}


// Returns value of next tuple. Returs true if there are more tuples
bool TTable::NextTuple(TupleData & td){
    if (HasMoreTuples(TableBucketsStats, CurrIterBucket, CurrIterIndex, TableBucketsStats.size())) {
        GetTupleData(CurrIterBucket, CurrIterIndex, td);
        CurrIterIndex++;
        return true;
    } else {
        td.AllNulls = true;
        return false;
    }
}


inline bool HasBitSet( ui64 * buf, ui64 Nbits ) {
    while(Nbits > sizeof(ui64)*8) {
        if (*buf++) return true;
        Nbits -= sizeof(ui64)*8;
    }
    return ((*buf) << (sizeof(ui64) * 8 - Nbits));
}


inline bool CompareIColumns(    const ui32* stringSizes1, const char * vals1,
                                const ui32* stringSizes2, const char * vals2,
                                TColTypeInterface * colInterfaces, ui64 nStringColumns, ui64 nIColumns) {
    ui32 currOffset1 = 0;
    ui32 currOffset2 = 0;
    ui32 currSize1 = 0;
    ui32 currSize2 = 0;
    NYql::NUdf::TUnboxedValue val1, val2;
    TStringBuf str1, str2;

    for (ui32 i = 0; i < nStringColumns; i ++) {
        currSize1 = *(stringSizes1 + i);
        currSize2 = *(stringSizes2 + i);
        if (currSize1 != currSize2)
            return false;
        currOffset1 += currSize1 + sizeof(ui32);
        currOffset2 += currSize2 + sizeof(ui32);
    }

    if (0 != std::memcmp(vals1, vals2, currOffset1))
        return false;

    for (ui32 i = 0; i < nIColumns; i ++) {

        currSize1 = *(stringSizes1 + nStringColumns + i );
        currSize2 = *(stringSizes2 + nStringColumns + i );
        currOffset1 += sizeof(ui32);
        currOffset2 += sizeof(ui32);
        str1 = TStringBuf(vals1 + currOffset1, currSize1);
        val1 = (colInterfaces + i)->Packer->Unpack(str1, colInterfaces->HolderFactory);
        str2 = TStringBuf(vals2 + currOffset2, currSize2 );
        val2 = (colInterfaces + i)->Packer->Unpack(str2, colInterfaces->HolderFactory);
        if ( ! ((colInterfaces + i)->EquateI->Equals(val1,val2)) ) {
            return false;
        }

        currOffset1 += currSize1;
        currOffset2 += currSize2;

    }
    return true;
}

inline bool CompareIColumns(    const char * vals1,
                                const char * vals2,
                                NYql::NUdf::TUnboxedValue * iColumns,
                                TColTypeInterface * colInterfaces,
                                ui64 nStringColumns, ui64 nIColumns) {
    ui32 currOffset1 = 0;
    NYql::NUdf::TUnboxedValue val1;
    TStringBuf str1;

    for (ui32 i = 0; i < nStringColumns; i ++) {
        auto currSize1 = ReadUnaligned<ui32>(vals1 + currOffset1);
        auto currSize2 = ReadUnaligned<ui32>(vals2 + currOffset1);
        if (currSize1 != currSize2)
            return false;
        currOffset1 += currSize1 + sizeof(ui32);
    }

    if (0 != std::memcmp(vals1, vals2, currOffset1))
        return false;

    for (ui32 i = 0; i < nIColumns; i ++) {

        auto currSize1 = ReadUnaligned<ui32>(vals1 + currOffset1);
        currOffset1 += sizeof(ui32);
        str1 = TStringBuf(vals1 + currOffset1, currSize1);
        val1 = (colInterfaces + i)->Packer->Unpack(str1, colInterfaces->HolderFactory);
        auto &val2 = iColumns[i];
        if ( ! ((colInterfaces + i)->EquateI->Equals(val1,val2)) ) {
            return false;
        }

        currOffset1 += currSize1;

    }
    return true;
}

// Resizes KeysHashTable to new slots, keeps old content.
void ResizeHashTable(KeysHashTable &t, ui64 newSlots){

    std::vector<ui64, TMKQLAllocator<ui64>> newTable(newSlots * t.SlotSize , 0);
    for ( auto it = t.Table.begin(); it != t.Table.end(); it += t.SlotSize ) {
        if ( *it == 0)
            continue;
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


// Joins two tables and returns join result in joined table. Tuples of joined table could be received by
// joined table iterator
void TTable::Join( TTable & t1, TTable & t2, EJoinKind joinKind, bool hasMoreLeftTuples, bool hasMoreRightTuples, ui32 fromBucket, ui32 toBucket ) {
    if ( hasMoreLeftTuples )
        LeftTableBatch_ = true;

    if( hasMoreRightTuples )
        RightTableBatch_ = true;

    auto table1Batch = LeftTableBatch_;
    auto table2Batch = RightTableBatch_;

    JoinTable1 = &t1;
    JoinTable2 = &t2;

    JoinKind = joinKind;


    IsTableJoined = true;

    MKQL_ENSURE(joinKind != EJoinKind::Cross, "Cross Join is not allowed in Grace Join");

    if ( JoinKind == EJoinKind::Right || JoinKind == EJoinKind::RightOnly || JoinKind == EJoinKind::RightSemi ) {
        std::swap(JoinTable1, JoinTable2);
        std::swap(table1Batch, table2Batch);
    }

    ui64 tuplesFound = 0;

    std::vector<JoinTuplesIds, TMKQLAllocator<JoinTuplesIds, EMemorySubPool::Temporary>> joinResults;


    for (ui64 bucket = fromBucket; bucket < toBucket; bucket++) {
        joinResults.clear();
        TTableBucket * bucket1 = &JoinTable1->TableBuckets[bucket];
        TTableBucket * bucket2 = &JoinTable2->TableBuckets[bucket];
        TTableBucketStats * bucketStats1 = &JoinTable1->TableBucketsStats[bucket];
        TTableBucketStats * bucketStats2 = &JoinTable2->TableBucketsStats[bucket];

        ui64 tuplesNum1 = JoinTable1->TableBucketsStats[bucket].TuplesNum;
        ui64 tuplesNum2 = JoinTable2->TableBucketsStats[bucket].TuplesNum;

        ui64 headerSize1 = JoinTable1->HeaderSize;
        ui64 headerSize2 = JoinTable2->HeaderSize;
        ui64 nullsSize1 = JoinTable1->NullsBitmapSize_;
        ui64 nullsSize2 = JoinTable2->NullsBitmapSize_;
        ui64 keyIntOffset1 = HashSize + nullsSize1;
        ui64 keyIntOffset2 = HashSize + nullsSize2;
        bool table1HasKeyStringColumns = (JoinTable1->NumberOfKeyStringColumns != 0);
        bool table2HasKeyStringColumns = (JoinTable2->NumberOfKeyStringColumns != 0);
        bool table1HasKeyIColumns = (JoinTable1->NumberOfKeyIColumns != 0);
        bool table2HasKeyIColumns = (JoinTable2->NumberOfKeyIColumns != 0);
        bool swapTables = tuplesNum2 > tuplesNum1 && !table1Batch || table2Batch;


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

        if (tuplesNum2 == 0 || tuplesNum1 == 0)
            continue;

        ui64 slotSize = headerSize2 + 1;

        ui64 avgStringsSize = ( 3 * (bucket2->KeyIntVals.size() - tuplesNum2 * headerSize2) ) / ( 2 * tuplesNum2 + 1)  + 1;

        if (table2HasKeyStringColumns || table2HasKeyIColumns ) {
            slotSize = slotSize + avgStringsSize;
        }

        ui64 &nSlots = bucket2->NSlots;
        auto &joinSlots = bucket2->JoinSlots;
        auto &bloomFilter = bucketStats2->BloomFilter;
        bool initHashTable = false;

        if (!nSlots) {
            nSlots = (3 * tuplesNum2 + 1) | 1;
            joinSlots.resize(nSlots*slotSize, 0);
            bloomFilter.Resize(tuplesNum2);
            initHashTable = true;
            ++InitHashTableCount_;
        }

        auto firstSlot = [begin = joinSlots.begin(), slotSize, nSlots](auto hash) {
                ui64 slotNum = hash % nSlots;
                return begin + slotNum * slotSize;
        };

        auto nextSlot = [begin = joinSlots.begin(), end = joinSlots.end(), slotSize](auto it) {
            it += slotSize;
            if (it == end)
                it = begin;
            return it;
        };

        if (initHashTable) {
            ui32 tuple2Idx = 0;
            auto it2 = bucket2->KeyIntVals.begin();
            for (ui64 keysValSize = headerSize2; it2 != bucket2->KeyIntVals.end(); it2 += keysValSize, ++tuple2Idx) {
                if ( table2HasKeyStringColumns || table2HasKeyIColumns) {
                    keysValSize = headerSize2 + *(it2 + headerSize2 - 1) ;
                }

                ui64 hash = *it2;
                ui64 * nullsPtr = it2+1;
                if (HasBitSet(nullsPtr, 1))
                    continue;

                bloomFilter.Add(hash);

                auto slotIt = firstSlot(hash);

                ++HashLookups_;
                for (; *slotIt != 0; slotIt = nextSlot(slotIt))
                {
                    ++HashO1Iterations_;
                }
                ++HashSlotIterations_;

                if (keysValSize <= slotSize - 1)
                {
                    std::copy_n(it2, keysValSize, slotIt);
                }
                else
                {
                    std::copy_n(it2, headerSize2, slotIt);

                    *(slotIt + headerSize2) = it2 + headerSize2 - bucket2->KeyIntVals.begin();
                }
                slotIt[slotSize - 1] = tuple2Idx;
            }
            bloomFilter.Finalize();
            if (swapTables) JoinTable1Total_ += tuplesNum2; else JoinTable2Total_ += tuplesNum2;
        }

        if (swapTables) JoinTable2Total_ += tuplesNum1; else JoinTable1Total_ += tuplesNum1;

        ui32 tuple1Idx = 0;
        auto it1 = bucket1->KeyIntVals.begin();
        //  /-------headerSize---------------------------\
        //  hash nulls-bitmap keyInt[] KeyIHash[] strSize| [strPos | strs] slotIdx
        //  \---------------------------------------slotSize ---------------------/
        // bit0 of nulls bitmap denotes key-with-nulls
        // strSize only present if HasKeyStrCol || HasKeyICol
        // strPos is only present if (HasKeyStrCol || HasKeyICol) && strSize + headerSize >= slotSize
        // slotSize, slotIdx and strPos is only for hashtable (table2)
        ui64 bloomHits = 0;
        ui64 bloomLookups = 0;
        
        for (ui64 keysValSize = headerSize1; it1 != bucket1->KeyIntVals.end(); it1 += keysValSize, ++tuple1Idx ) {

            if ( table1HasKeyStringColumns || table1HasKeyIColumns ) {
                keysValSize = headerSize1 + *(it1 + headerSize1 - 1) ;
            }

            ui64 hash = *it1;
            ui64 * nullsPtr = it1+1;
            if (HasBitSet(nullsPtr, 1))
            {
                continue;
            }

            if (initHashTable) {
                bloomLookups++;
                if (bloomFilter.IsMissing(hash)) {
                    bloomHits++;
                    continue;
                }
            }

            ++HashLookups_;

            auto saveTuplesFound = tuplesFound;
            auto slotIt = firstSlot(hash);
            for (; *slotIt != 0; slotIt = nextSlot(slotIt) )
            {
                ++HashO1Iterations_;
                if (*slotIt != hash)
                    continue;

                auto tuple2Idx = slotIt[slotSize - 1];

                ++HashSlotIterations_;
                if (table1HasKeyIColumns || !(keysValSize - nullsSize1 <= slotSize - 1 - nullsSize2)) {
                    // 2nd condition cannot be true unless HasKeyStringColumns or HasKeyIColumns, hence size at the end of header is present

                    if (!std::equal(it1 + keyIntOffset1, it1 + headerSize1 - 1, slotIt + keyIntOffset2))
                        continue;

                    auto slotStringsStart = slotIt + headerSize2;
                    ui64 slotStringsSize = *(slotIt + headerSize2 - 1);

                    if (headerSize2 + slotStringsSize + 1 > slotSize)
                    {
                        ui64 stringsPos = *(slotIt + headerSize2);
                        slotStringsStart = bucket2->KeyIntVals.begin() + stringsPos;
                    }

                    if (table1HasKeyIColumns)
                    {
                        ui64 stringsOffsetsIdx1 = tuple1Idx * (JoinTable1->NumberOfStringColumns + JoinTable1->NumberOfIColumns + 2);
                        ui64 stringsOffsetsIdx2 = tuple2Idx * (JoinTable2->NumberOfStringColumns + JoinTable2->NumberOfIColumns + 2);
                        ui32 * stringsSizesPtr1 = bucket1->StringsOffsets.data() + stringsOffsetsIdx1 + 2;
                        ui32 * stringsSizesPtr2 = bucket2->StringsOffsets.data() + stringsOffsetsIdx2 + 2;

                        if (!CompareIColumns( stringsSizesPtr1 ,
                                    (char *) (it1 + headerSize1 ),
                                    stringsSizesPtr2,
                                    (char *) (slotStringsStart),
                                    JoinTable1 -> ColInterfaces, JoinTable1->NumberOfStringColumns, JoinTable1 -> NumberOfKeyIColumns ))
                            continue;
                    } else {
                        ui64 stringsSize = *(it1 + headerSize1 - 1);
                        if (stringsSize != slotStringsSize || !std::equal(it1 + headerSize1, it1 + headerSize1 + stringsSize, slotStringsStart))
                            continue;
                    }

                } else {
                    if (!std::equal(it1 + keyIntOffset1, it1 + keysValSize, slotIt + keyIntOffset2))
                        continue;
                }

                tuplesFound++;
                JoinTuplesIds joinIds;
                joinIds.id1 = swapTables ? tuple2Idx : tuple1Idx;
                joinIds.id2 = swapTables ? tuple1Idx : tuple2Idx;
                joinResults.emplace_back(joinIds);
            }
            BloomFalsePositives_ += saveTuplesFound == tuplesFound;
        }

        if (!hasMoreLeftTuples && !hasMoreRightTuples) {
            joinSlots.clear();
            joinSlots.shrink_to_fit();
            nSlots = 0;
            bloomFilter.Shrink();
        }

        if (bloomHits < bloomLookups/8) {
            // Bloomfilter was inefficient, drop it
            bloomFilter.Shrink();
        }
        BloomHits_ += bloomHits;
        BloomLookups_ += bloomLookups;

        std::sort(joinResults.begin(), joinResults.end(), [](JoinTuplesIds a, JoinTuplesIds b)
        {
            if (a.id1 < b.id1) return true;
            if (a.id1 == b.id1 && (a.id2 < b.id2)) return true;
            return false;
        });


        TableBuckets[bucket].JoinIds.assign(joinResults.begin(), joinResults.end());

        std::vector<ui32, TMKQLAllocator<ui32>> & rightIds = TableBuckets[bucket].RightIds;
        std::vector<JoinTuplesIds, TMKQLAllocator<JoinTuplesIds>> & joinIds = TableBuckets[bucket].JoinIds;
        std::set<ui32> & leftMatchedIds = TableBuckets[bucket].AllLeftMatchedIds;
        std::set<ui32> & rightMatchedIds = TableBuckets[bucket].AllRightMatchedIds;

        if ( JoinKind == EJoinKind::Full || JoinKind == EJoinKind::Exclusion ) {
            rightIds.clear();
            rightIds.reserve(joinIds.size());
            for (const auto & id: joinIds) {
                rightIds.emplace_back(id.id2);
                if (LeftTableBatch_ || RightTableBatch_) {
                    leftMatchedIds.insert(id.id1);
                    rightMatchedIds.insert(id.id2);
                }
            }
            std::sort(rightIds.begin(), rightIds.end());
        }

        if (JoinKind == EJoinKind::Left || JoinKind == EJoinKind::LeftOnly || JoinKind == EJoinKind::LeftSemi ) {
            if (RightTableBatch_ ) {
                for (auto & jid: joinIds ) {
                    leftMatchedIds.insert(jid.id1);
                }
            }

        }

        if (JoinKind == EJoinKind::Right || JoinKind == EJoinKind::RightOnly || JoinKind == EJoinKind::RightSemi )  {
            if (LeftTableBatch_) {
                for (auto & jid: joinIds ) {
                    leftMatchedIds.insert(jid.id1);
                }
            }

        }
        YQL_LOG(GRACEJOIN_TRACE)
            << (const void *)this << '#'
            << bucket
            << " Table1 " << JoinTable1->TableBucketsStats[bucket].TuplesNum
            << " Table2 " << JoinTable2->TableBucketsStats[bucket].TuplesNum
            << " LeftTableBatch " << LeftTableBatch_
            << " leftMatchedIds " << leftMatchedIds.size()
            << " RightTableBatch " << RightTableBatch_
            << " rightMatchedIds " << rightMatchedIds.size()
            << " rightIds " << rightIds.size()
            << " joinIds " << joinIds.size()
            << " joinKind " << (int)JoinKind
            << " swapTables " << swapTables
            << " initHashTable " << initHashTable
            ;
    }

    HasMoreLeftTuples_ = hasMoreLeftTuples;
    HasMoreRightTuples_ = hasMoreRightTuples;

    TuplesFound_ += tuplesFound;
    
}

inline void TTable::GetTupleData(ui32 bucketNum, ui32 tupleId, TupleData & td) {

    ui64 keyIntsOffset = 0;
    ui64 dataIntsOffset = 0;
    ui64 keyStringsOffset = 0;
    ui64 dataStringsOffset = 0;

    td.AllNulls = false;

    TotalUnpacked++;

    TTableBucket & tb = TableBuckets[bucketNum];
    ui64 stringsOffsetsIdx = tupleId * (NumberOfStringColumns + NumberOfIColumns + 2);

    if(NumberOfKeyStringColumns != 0 || NumberOfKeyIColumns !=0 ) {
        keyIntsOffset = tb.StringsOffsets[stringsOffsetsIdx];
    } else {
        keyIntsOffset = HeaderSize * tupleId;
    }


    for ( ui64 i = 0; i < NumberOfKeyIntColumns + NullsBitmapSize_; ++i) {
        td.IntColumns[i] = tb.KeyIntVals[keyIntsOffset + HashSize + i];
    }

    dataIntsOffset = NumberOfDataIntColumns * tupleId;

    for ( ui64 i = 0; i < NumberOfDataIntColumns; ++i) {
        td.IntColumns[NumberOfKeyIntColumns + NullsBitmapSize_ + i] = tb.DataIntVals[dataIntsOffset + i];
    }

    char *strPtr = nullptr;
    if(NumberOfKeyStringColumns != 0 || NumberOfKeyIColumns != 0) {
        keyStringsOffset = tb.StringsOffsets[stringsOffsetsIdx] + HeaderSize;

        strPtr = reinterpret_cast<char *>(tb.KeyIntVals.data() + keyStringsOffset);

        for (ui64 i = 0; i < NumberOfKeyStringColumns; ++i)
        {
            td.StrSizes[i] = tb.StringsOffsets[stringsOffsetsIdx + 2 + i];
            Y_ENSURE(ReadUnaligned<ui32>(strPtr) == td.StrSizes[i]);
            strPtr += sizeof(ui32);
            td.StrColumns[i] = strPtr;
            strPtr += td.StrSizes[i];
        }

        for ( ui64 i = 0; i < NumberOfKeyIColumns; i++) {
            ui32 currSize = tb.StringsOffsets[stringsOffsetsIdx + 2 + NumberOfKeyStringColumns + i];
            Y_ENSURE(ReadUnaligned<ui32>(strPtr) == currSize);
            strPtr += sizeof(ui32);
            *(td.IColumns + i) = (ColInterfaces + i)->Packer->Unpack(TStringBuf(strPtr, currSize), ColInterfaces->HolderFactory);
            strPtr += currSize;
        }


    }


    if(NumberOfDataStringColumns || NumberOfDataIColumns != 0) {
         dataStringsOffset = tb.StringsOffsets[stringsOffsetsIdx + 1];
    }

    strPtr = (tb.StringsValues.data() + dataStringsOffset);



    for ( ui64 i = 0; i < NumberOfDataStringColumns; ++i ) {
        ui32 currIdx = NumberOfKeyStringColumns + i;
        td.StrColumns[currIdx] = strPtr;
        td.StrSizes[currIdx] = tb.StringsOffsets[stringsOffsetsIdx + 2 + currIdx];
        strPtr += td.StrSizes[currIdx];
    }

    for (ui64 i = 0; i < NumberOfDataIColumns; i++ ) {
        ui32 currIdx = NumberOfStringColumns + NumberOfKeyIColumns + i;
        ui32 currSize = tb.StringsOffsets[stringsOffsetsIdx + 2 + currIdx];

         *(td.IColumns + NumberOfKeyIColumns + i) = (ColInterfaces + NumberOfKeyIColumns + i)->Packer->Unpack(TStringBuf(strPtr, currSize), ColInterfaces->HolderFactory);

         strPtr += currSize;
    }


}

inline bool TTable::HasJoinedTupleId(TTable *joinedTable, ui32 &tupleId2) {

    if (joinedTable->CurrIterBucket != CurrIterBucket)
    {
        CurrIterBucket = joinedTable->CurrIterBucket;
        CurrJoinIdsIterIndex = 0;
    }
    auto& jids = TableBuckets[CurrIterBucket].JoinIds;

    if (CurrJoinIdsIterIndex < jids.size() && joinedTable->CurrIterIndex == jids[CurrJoinIdsIterIndex].id1)
    {
        tupleId2 = jids[CurrJoinIdsIterIndex].id2;
        return true;
    }
    else
    {
       return false;
    }
}



inline bool TTable::AddKeysToHashTable(KeysHashTable& t, ui64* keys, NYql::NUdf::TUnboxedValue * iColumns) {

    if (t.NSlots == 0) {
        t.SlotSize = HeaderSize + NumberOfKeyStringColumns * 2;
        t.Table.resize(DefaultTuplesNum * t.SlotSize, 0);
        t.NSlots = DefaultTuplesNum;
    }

    if ( t.FillCount > t.NSlots/2 ) {
        ResizeHashTable(t, 2 * t.NSlots + 1);
    }

    if ( HasBitSet(keys + HashSize, 1)) // Keys with null value
        return true;

    ui64 hash = *keys;
    ui64 slot = hash % t.NSlots;
    auto it = t.Table.begin() + slot * t.SlotSize;

    ui64 keyIntOffset = HashSize + NullsBitmapSize_;
    ui64 keysSize = HeaderSize;
    ui64 keyStringsSize = 0;
    if ( NumberOfKeyStringColumns > 0 || NumberOfKeyIColumns > 0) {
        keyStringsSize = *(keys + HeaderSize - 1);
        keysSize = HeaderSize + keyStringsSize;
    }

    auto nextSlot = [begin = t.Table.begin(), end = t.Table.end(), slotSize = t.SlotSize](auto it) {
        it += slotSize;
        if (it == end)
            it = begin;
        return it;
    };

    for (auto itValSize = HeaderSize; *it != 0; it = nextSlot(it)) {

        if (*it != hash)
            continue;

        if ( NumberOfKeyIColumns == 0 && (itValSize <= t.SlotSize)) {
            if (!std::equal(it + keyIntOffset, it + itValSize, keys + keyIntOffset))
                continue;
            return false;
        }

        Y_DEBUG_ABORT_UNLESS( NumberOfKeyStringColumns > 0 || NumberOfKeyIColumns > 0);

        itValSize = HeaderSize + *(it + HeaderSize - 1);
        auto slotStringsStart = it + HeaderSize;

        if (!std::equal(it + keyIntOffset, it + HeaderSize - 1, keys + keyIntOffset))
            continue;

        if (NumberOfKeyIColumns > 0) {
            if (!CompareIColumns( 
                        (char *) (slotStringsStart),
                        (char *) (keys + HeaderSize ),
                        iColumns,
                        JoinTable1 -> ColInterfaces, JoinTable1->NumberOfStringColumns, JoinTable1 -> NumberOfKeyIColumns ))
                continue;
            return false;
        }

        Y_DEBUG_ABORT_UNLESS(!(itValSize <= t.SlotSize));

        ui64 stringsPos = *(it + HeaderSize);
        slotStringsStart = t.SpillData.begin() + stringsPos;

        if (keysSize != itValSize || !std::equal(slotStringsStart, slotStringsStart + itValSize, keys + HeaderSize))
            continue;
        return false;
    }

    if (keysSize > t.SlotSize) {
        ui64 spillDataOffset = t.SpillData.size();
        t.SpillData.insert(t.SpillData.end(), keys + HeaderSize, keys + keysSize);
        std::copy_n(keys, HeaderSize, it);
        *(it + HeaderSize) = spillDataOffset;
    } else {
        std::copy_n(keys, keysSize, it);
    }

    t.FillCount++;
    return true;

}

inline bool HasRightIdMatch(ui64 currId, ui64 & rightIdIter, const std::vector<ui32, TMKQLAllocator<ui32>> & rightIds) {

    if (rightIdIter >= rightIds.size()) return false;

    while ( rightIdIter < rightIds.size() && currId > rightIds[rightIdIter])  rightIdIter++;

    if (rightIdIter >= rightIds.size()) return false;

    return currId == rightIds[rightIdIter];
}


bool TTable::NextJoinedData( TupleData & td1, TupleData & td2, ui64 bucketLimit) {
    if ( JoinKind == EJoinKind::Inner ) {
        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {

                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td2);
                CurrJoinIdsIterIndex++;
                return true;
            }
            JoinTable1->CurrIterIndex++;
        }
        return false;
    }

    if ( JoinKind == EJoinKind::Left ) {
        while (HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td2);
                CurrJoinIdsIterIndex++;
                auto& jids = TableBuckets[CurrIterBucket].JoinIds;
                if ( (CurrJoinIdsIterIndex == jids.size()) || ( JoinTable1->CurrIterIndex != jids[CurrJoinIdsIterIndex].id1) ) JoinTable1->CurrIterIndex++;

                return true;
            } else {
                if (RightTableBatch_ && HasMoreRightTuples_ ) {
                    JoinTable1->CurrIterIndex++;
                    continue;
                }

                std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
                if ( leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex)) {
                    JoinTable1->CurrIterIndex++;
                    continue;
                }

                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
            }
            JoinTable1->CurrIterIndex++;
            return true;
        }
        td1.AllNulls = true;
        td2.AllNulls = true;
        return false;
    }

    if (  JoinKind == EJoinKind::Right ) {
        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td1);
                CurrJoinIdsIterIndex++;
                auto& jids = TableBuckets[CurrIterBucket].JoinIds;
                if ( (CurrJoinIdsIterIndex == jids.size()) || ( JoinTable1->CurrIterIndex != jids[CurrJoinIdsIterIndex].id1) ) JoinTable1->CurrIterIndex++;
                return true;
            } else {
                if (LeftTableBatch_ && HasMoreLeftTuples_ ) {
                    JoinTable1->CurrIterIndex++;
                    continue;
                }

                std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
                if ( leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex)) {
                    JoinTable1->CurrIterIndex++;
                    continue;
                }


                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                td1.AllNulls = true;
            }
            JoinTable1->CurrIterIndex++;
            return true;
        }
        td1.AllNulls = true;
        td2.AllNulls = true;
        return false;
    }



    if (JoinKind == EJoinKind::LeftOnly ) {

        if ( RightTableBatch_ && HasMoreRightTuples_ )
            return false;

        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;

            bool globalMatchedId = false;
            if ( RightTableBatch_  ) {
                std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
                globalMatchedId = leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex);
            }

            if (!HasJoinedTupleId(JoinTable1, tupleId2) && !globalMatchedId )
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
                JoinTable1->CurrIterIndex++;
                return true;
            } else {
                while (HasJoinedTupleId(JoinTable1, tupleId2)) {
                    CurrJoinIdsIterIndex++;
                }
            }
            JoinTable1->CurrIterIndex++;
        }
        return false;

    }

    if (JoinKind == EJoinKind::RightOnly ) {

        if (LeftTableBatch_ && HasMoreLeftTuples_ )
            return false;

        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;

            bool globalMatchedId = false;
            if ( LeftTableBatch_ ) {
                std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
                globalMatchedId = leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex);
            }

            if (!HasJoinedTupleId(JoinTable1, tupleId2) && !globalMatchedId )
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                td1.AllNulls = true;
                JoinTable1->CurrIterIndex++;
                return true;
            } else {
                while (HasJoinedTupleId(JoinTable1, tupleId2)) {
                    CurrJoinIdsIterIndex++;
                }
            }
            JoinTable1->CurrIterIndex++;
        }
        return false;

    }


    if ( JoinKind == EJoinKind::LeftSemi) {

        if (RightTableBatch_ && HasMoreRightTuples_ )
            return false;

        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;

            if ( !RightTableBatch_  && HasJoinedTupleId(JoinTable1, tupleId2))
            {

                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
                while( HasJoinedTupleId(JoinTable1, tupleId2) ) {
                    CurrJoinIdsIterIndex++;
                }
                JoinTable1->CurrIterIndex++;
                return true;
            }


            std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
            if ( RightTableBatch_ && leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex) ) {
                JoinTable1->GetTupleData(JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
                JoinTable1->CurrIterIndex++;
                return true;
            }

            JoinTable1->CurrIterIndex++;
        }
        return false;
    }

    if ( JoinKind == EJoinKind::RightSemi ) {

        if (LeftTableBatch_ && HasMoreLeftTuples_ )
            return false;

        while(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if ( !LeftTableBatch_ && HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                td1.AllNulls = true;
                while( HasJoinedTupleId(JoinTable1, tupleId2) ) {
                    CurrJoinIdsIterIndex++;
                }
                JoinTable1->CurrIterIndex++;
                return true;
            }

            std::set<ui32> & leftMatchedIds = TableBuckets[JoinTable1->CurrIterBucket].AllLeftMatchedIds;
            if ( LeftTableBatch_ && leftMatchedIds.contains( (ui32) JoinTable1->CurrIterIndex) ) {
                JoinTable1->GetTupleData(JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                td2.AllNulls = true;
                JoinTable1->CurrIterIndex++;
                return true;
            }

            JoinTable1->CurrIterIndex++;
        }
        return false;
    }

    if ( JoinKind == EJoinKind::Full ) {
        if(HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td2);
                CurrJoinIdsIterIndex++;
                auto& jids = TableBuckets[CurrIterBucket].JoinIds;
                if ( (CurrJoinIdsIterIndex == jids.size()) || ( JoinTable1->CurrIterIndex != jids[CurrJoinIdsIterIndex].id1) ) JoinTable1->CurrIterIndex++;

                return true;
            } else {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
            }
            JoinTable1->CurrIterIndex++;
            return true;
        }
        td1.AllNulls = true;

        if (!Table2Initialized_) {
            CurrIterBucket = 0;
            CurrJoinIdsIterIndex = 0;
            Table2Initialized_ = true;
        }

        while (HasMoreTuples(JoinTable2->TableBucketsStats, JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex, bucketLimit)) {

            if (CurrIterBucket != JoinTable2->CurrIterBucket) {
                CurrIterBucket = JoinTable2->CurrIterBucket;
                CurrJoinIdsIterIndex = 0;
            }
            auto& rightIds = TableBuckets[CurrIterBucket].RightIds;
             if (HasRightIdMatch(JoinTable2->CurrIterIndex, CurrJoinIdsIterIndex, rightIds)) {
                JoinTable2->CurrIterIndex++;
                continue;
            }
            JoinTable2->GetTupleData(JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex, td2);
            JoinTable2->CurrIterIndex++;
            return true;

        }

        td1.AllNulls = true;
        td2.AllNulls = true;
        return false;

    }

    if ( JoinKind == EJoinKind::Exclusion ) {
        while (HasMoreTuples(JoinTable1->TableBucketsStats, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, bucketLimit)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                CurrJoinIdsIterIndex++;
                auto& jids = TableBuckets[CurrIterBucket].JoinIds;
                if ( (CurrJoinIdsIterIndex == jids.size()) || ( JoinTable1->CurrIterIndex != jids[CurrJoinIdsIterIndex].id1) ) JoinTable1->CurrIterIndex++;
                continue;
            } else {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
            }
            JoinTable1->CurrIterIndex++;
            return true;
        }

        td1.AllNulls = true;

        if (!Table2Initialized_) {
            CurrIterBucket = 0;
            CurrJoinIdsIterIndex = 0;
            Table2Initialized_ = true;
        }

        while (HasMoreTuples(JoinTable2->TableBucketsStats, JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex, bucketLimit)) {

            if (CurrIterBucket != JoinTable2->CurrIterBucket) {
                CurrIterBucket = JoinTable2->CurrIterBucket;
                CurrJoinIdsIterIndex = 0;
            }
            auto& rightIds = TableBuckets[CurrIterBucket].RightIds;
             if (HasRightIdMatch(JoinTable2->CurrIterIndex, CurrJoinIdsIterIndex, rightIds)) {
                JoinTable2->CurrIterIndex++;
                continue;
            }
            JoinTable2->GetTupleData(JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex, td2);
            JoinTable2->CurrIterIndex++;
            return true;

        }

        td1.AllNulls = true;
        td2.AllNulls = true;
        return false;

    }

    return false;

 }

void TTable::Clear() {
    for (ui64 bucket = 0; bucket < NumberOfBuckets; bucket++) {
        ClearBucket(bucket);
    }
}

void TTable::ClearBucket(ui64 bucket) {
    TTableBucket & tb = TableBuckets[bucket];
    tb.KeyIntVals.clear();
    tb.DataIntVals.clear();
    tb.StringsOffsets.clear();
    tb.StringsValues.clear();
    tb.InterfaceValues.clear();
    tb.InterfaceOffsets.clear();
    tb.JoinIds.clear();
    tb.RightIds.clear();
    tb.JoinSlots.clear();
    tb.NSlots = 0;

    TTableBucketStats & tbs = TableBucketsStats[bucket];
    tbs.TuplesNum = 0;
    tbs.KeyIntValsTotalSize = 0;
    tbs.StringValuesTotalSize = 0;
}

void TTable::ShrinkBucket(ui64 bucket) {
    TTableBucket & tb = TableBuckets[bucket];
    tb.KeyIntVals.shrink_to_fit();
    tb.DataIntVals.shrink_to_fit();
    tb.StringsOffsets.shrink_to_fit();
    tb.StringsValues.shrink_to_fit();
    tb.InterfaceValues.shrink_to_fit();
    tb.InterfaceOffsets.shrink_to_fit();
    tb.JoinIds.shrink_to_fit();
    tb.RightIds.shrink_to_fit();
    tb.JoinSlots.shrink_to_fit();
}

void TTable::InitializeBucketSpillers(ISpiller::TPtr spiller) {
    for (size_t i = 0; i < NumberOfBuckets; ++i) {
        TableBucketsSpillers.emplace_back(spiller, 5_MB);
    }
}

ui64 TTable::GetSizeOfBucket(ui64 bucket) const {
    return TableBuckets[bucket].KeyIntVals.size() * sizeof(ui64)
    + TableBuckets[bucket].JoinSlots.size() * sizeof(ui64)
    + TableBuckets[bucket].DataIntVals.size() * sizeof(ui64)
    + TableBuckets[bucket].StringsValues.size()
    + TableBuckets[bucket].StringsOffsets.size() * sizeof(ui32)
    + TableBuckets[bucket].InterfaceValues.size()
    + TableBuckets[bucket].InterfaceOffsets.size() * sizeof(ui32);
}

bool TTable::TryToReduceMemoryAndWait() {
    i32 largestBucketIndex = 0;
    ui64 largestBucketSize = 0;
    for (ui32 bucket = 0; bucket < NumberOfBuckets; ++bucket) {
        if (TableBucketsSpillers[bucket].IsProcessingSpilling()) return true;

        ui64 bucketSize = GetSizeOfBucket(bucket);
        if (bucketSize > largestBucketSize) {
            largestBucketSize = bucketSize;
            largestBucketIndex = bucket;
        }
    }

    if (largestBucketSize < SpillingSizeLimit/NumberOfBuckets) return false;
    TableBucketsSpillers[largestBucketIndex].SpillBucket(std::move(TableBuckets[largestBucketIndex]));
    TableBuckets[largestBucketIndex] = TTableBucket{};

    return TableBucketsSpillers[largestBucketIndex].IsProcessingSpilling();
}

void TTable::UpdateSpilling() {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        TableBucketsSpillers[i].Update();
    }
}

bool TTable::IsSpillingFinished() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers[i].IsProcessingSpilling()) return false;
    }
    return true;
}

bool TTable::IsSpillingAcceptingDataRequests() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers[i].IsInMemory()) continue;

        if (!TableBucketsSpillers[i].IsAcceptingDataRequests()) return false;
    }
    return true;
}

bool TTable::IsRestoringSpilledBuckets() const {
    for (ui64 i = 0; i < NumberOfBuckets; ++i) {
        if (TableBucketsSpillers[i].IsRestoring()) return true;
    }
    return false;
}

void TTable::FinalizeSpilling() {
    for (ui32 bucket = 0; bucket < NumberOfBuckets; ++bucket) {
        if (!TableBucketsSpillers[bucket].IsInMemory()) {
            TableBucketsSpillers[bucket].Finalize();
            TableBucketsSpillers[bucket].SpillBucket(std::move(TableBuckets[bucket]));
            TableBuckets[bucket] = TTableBucket{};
            
        }
    }
}

bool TTable::IsBucketInMemory(ui32 bucket) const {
    return TableBucketsSpillers[bucket].IsInMemory();
}

bool TTable::IsSpilledBucketWaitingForExtraction(ui32 bucket) const {
    return TableBucketsSpillers[bucket].IsExtractionRequired();
}

void TTable::StartLoadingBucket(ui32 bucket) {
    MKQL_ENSURE(!TableBucketsSpillers[bucket].IsInMemory(), "Internal logic error");

    TableBucketsSpillers[bucket].StartBucketRestoration();
}

void TTable::PrepareBucket(ui64 bucket) {
    if (!TableBucketsSpillers[bucket].IsExtractionRequired()) return;
    TableBuckets[bucket] = std::move(TableBucketsSpillers[bucket].ExtractBucket());
}

// Creates new table with key columns and data columns
TTable::TTable( ui64 numberOfKeyIntColumns, ui64 numberOfKeyStringColumns,
                ui64 numberOfDataIntColumns, ui64 numberOfDataStringColumns,
                ui64 numberOfKeyIColumns, ui64 numberOfDataIColumns,
                ui64 nullsBitmapSize,  TColTypeInterface * colInterfaces, bool isAny ) :

                NumberOfKeyIntColumns(numberOfKeyIntColumns),
                NumberOfKeyStringColumns(numberOfKeyStringColumns),
                NumberOfKeyIColumns(numberOfKeyIColumns),
                NumberOfDataIntColumns(numberOfDataIntColumns),
                NumberOfDataStringColumns(numberOfDataStringColumns),
                NumberOfDataIColumns(numberOfDataIColumns),
                ColInterfaces(colInterfaces),
                NullsBitmapSize_(nullsBitmapSize),
                IsAny_(isAny)  {

    NumberOfKeyColumns = NumberOfKeyIntColumns + NumberOfKeyStringColumns + NumberOfKeyIColumns;
    NumberOfDataColumns = NumberOfDataIntColumns + NumberOfDataStringColumns + NumberOfDataIColumns;
    NumberOfColumns = NumberOfKeyColumns + NumberOfDataColumns;
    NumberOfStringColumns = NumberOfKeyStringColumns + NumberOfDataStringColumns;
    NumberOfIColumns = NumberOfKeyIColumns + NumberOfDataIColumns;

    BytesInKeyIntColumns = NumberOfKeyIntColumns * sizeof(ui64);


    TotalStringsSize = (numberOfKeyStringColumns > 0 || NumberOfKeyIColumns > 0 ) ? 1 : 0;

    HeaderSize = HashSize + NullsBitmapSize_ + NumberOfKeyIntColumns + NumberOfKeyIColumns + TotalStringsSize;

    TableBuckets.resize(NumberOfBuckets);
    TableBucketsStats.resize(NumberOfBuckets);

    const ui64 reservedSizePerTuple = (2 * DefaultTupleBytes) / sizeof(ui64);

    TempTuple.reserve( reservedSizePerTuple );
    IColumnsHashes.resize(NumberOfKeyIColumns);
    IColumnsVals.resize(NumberOfIColumns);

    const ui64 totalForTuples = DefaultTuplesNum * reservedSizePerTuple;

    for ( auto & b: TableBuckets ) {
        b.KeyIntVals.reserve( (totalForTuples * NumberOfKeyColumns) / (NumberOfColumns + 1) );
        b.StringsOffsets.reserve((totalForTuples * NumberOfStringColumns) / (NumberOfColumns + 1));
        b.DataIntVals.reserve( (totalForTuples * NumberOfDataIntColumns) / (NumberOfColumns + 1));
        b.StringsValues.reserve( (totalForTuples * NumberOfStringColumns) / (NumberOfColumns + 1) );
        b.InterfaceOffsets.reserve( (totalForTuples * NumberOfIColumns) / (NumberOfColumns + 1) );
        b.InterfaceValues.reserve( (totalForTuples * NumberOfIColumns) / (NumberOfColumns + 1));

     }

}

TTable::~TTable() {
    YQL_LOG_IF(GRACEJOIN_DEBUG, InitHashTableCount_)
        << (const void *)this << '#' << "InitHashTableCount " << InitHashTableCount_
        << " BloomLookups " << BloomLookups_ << " BloomHits " << BloomHits_ << " BloomFalsePositives " << BloomFalsePositives_
        << " HashLookups " << HashLookups_ << " HashChainTraversal " << HashO1Iterations_/(double)HashLookups_ << " HashSlotOperations " << HashSlotIterations_/(double)HashLookups_
        << " Table1 " << JoinTable1Total_ << " Table2 " << JoinTable2Total_ << " TuplesFound " << TuplesFound_
        ;
    YQL_LOG_IF(GRACEJOIN_DEBUG, JoinTable1 && JoinTable1->AnyFiltered_) << (const void *)this << '#' << "L AnyFiltered " <<  JoinTable1->AnyFiltered_;
    YQL_LOG_IF(GRACEJOIN_DEBUG, JoinTable1 && JoinTable1->BloomLookups_) << (const void *)this << '#' << "L BloomLookups " <<  JoinTable1->BloomLookups_ << " BloomHits " <<  JoinTable1->BloomHits_;
    YQL_LOG_IF(GRACEJOIN_DEBUG, JoinTable2 && JoinTable2->AnyFiltered_) << (const void *)this << '#' << "R AnyFiltered " <<  JoinTable2->AnyFiltered_;
    YQL_LOG_IF(GRACEJOIN_DEBUG, JoinTable2 && JoinTable2->BloomLookups_) << (const void *)this << '#' << "R BloomLookups " <<  JoinTable2->BloomLookups_ << " BloomHits " <<  JoinTable2->BloomHits_;
};

TTableBucketSpiller::TTableBucketSpiller(ISpiller::TPtr spiller, size_t sizeLimit)
        : StateUi64Adapter(spiller, sizeLimit)
        , StateUi32Adapter(spiller, sizeLimit)
        , StateCharAdapter(spiller, sizeLimit)
    {
    }

void TTableBucketSpiller::Update() {
    StateUi64Adapter.Update();
    StateUi32Adapter.Update();
    StateCharAdapter.Update();

    if (State == EState::Spilling) {
        ProcessBucketSpilling();
    } else if (State == EState::Finalizing) {
        ProcessFinalizing();
    } else if (State == EState::Restoring) {
        ProcessBucketRestoration();
    }
}

void TTableBucketSpiller::Finalize() {
    IsFinalizingRequested = true;
}

void TTableBucketSpiller::SpillBucket(TTableBucket&& bucket) {
    MKQL_ENSURE(NextVectorToProcess == ENextVectorToProcess::None, "Internal logic error");
    State = EState::Spilling;

    CurrentBucket = std::move(bucket);
    NextVectorToProcess = ENextVectorToProcess::KeyAndVals;

    ProcessBucketSpilling();
}

TTableBucket&& TTableBucketSpiller::ExtractBucket() {
    MKQL_ENSURE(State == EState::WaitingForExtraction, "Internal logic error");
    MKQL_ENSURE(SpilledBucketsCount == 0, "Internal logic error");
    State = EState::InMemory;
    return std::move(CurrentBucket);
}

bool TTableBucketSpiller::IsInMemory() const {
    return State == EState::InMemory;
}

bool TTableBucketSpiller::IsExtractionRequired() const {
    return State == EState::WaitingForExtraction;
}

bool TTableBucketSpiller::IsProcessingSpilling() const {
    return State == EState::Spilling;
}

bool TTableBucketSpiller::IsAcceptingDataRequests() const {
    return State == EState::AcceptingDataRequests;
}

bool TTableBucketSpiller::IsRestoring() const {
    return State == EState::Restoring;
}

void TTableBucketSpiller::StartBucketRestoration() {
    MKQL_ENSURE(State == EState::AcceptingDataRequests, "Internal logic error");
    MKQL_ENSURE(NextVectorToProcess == ENextVectorToProcess::None, "Internal logic error");

    NextVectorToProcess = ENextVectorToProcess::KeyAndVals;
    State = EState::Restoring;
    ProcessBucketRestoration();
}

void TTableBucketSpiller::ProcessBucketSpilling() {
    while (NextVectorToProcess != ENextVectorToProcess::None) {
        switch (NextVectorToProcess) {
            case ENextVectorToProcess::KeyAndVals:
                if (!StateUi64Adapter.IsAcceptingData()) return;

                StateUi64Adapter.AddData(std::move(CurrentBucket.KeyIntVals));
                NextVectorToProcess = ENextVectorToProcess::DataIntVals;
                break;
            case ENextVectorToProcess::DataIntVals:
                if (!StateUi64Adapter.IsAcceptingData()) return;

                StateUi64Adapter.AddData(std::move(CurrentBucket.DataIntVals));
                NextVectorToProcess = ENextVectorToProcess::StringsValues;
                break;
            case ENextVectorToProcess::StringsValues:
                if (!StateCharAdapter.IsAcceptingData()) return;

                StateCharAdapter.AddData(std::move(CurrentBucket.StringsValues));
                NextVectorToProcess = ENextVectorToProcess::StringsOffsets;
                break;
            case ENextVectorToProcess::StringsOffsets:
                if (!StateUi32Adapter.IsAcceptingData()) return;

                StateUi32Adapter.AddData(std::move(CurrentBucket.StringsOffsets));
                NextVectorToProcess = ENextVectorToProcess::InterfaceValues;
                break;
            case ENextVectorToProcess::InterfaceValues:
                if (!StateCharAdapter.IsAcceptingData()) return;

                StateCharAdapter.AddData(std::move(CurrentBucket.InterfaceValues));
                NextVectorToProcess = ENextVectorToProcess::InterfaceOffsets;
                break;
            case ENextVectorToProcess::InterfaceOffsets:
                if (!StateUi32Adapter.IsAcceptingData()) return;

                StateUi32Adapter.AddData(std::move(CurrentBucket.InterfaceOffsets));
                NextVectorToProcess = ENextVectorToProcess::None;
                SpilledBucketsCount++;

                break;
            default:
                return;
        }
    }

    if (IsFinalizingRequested) {
        if (!StateCharAdapter.IsAcceptingData() || !StateUi32Adapter.IsAcceptingData() || !StateUi64Adapter.IsAcceptingData()) return;
        State = EState::Finalizing;
        StateUi64Adapter.Finalize();
        StateUi32Adapter.Finalize();
        StateCharAdapter.Finalize();

        ProcessFinalizing();
        return;
    }
    State = EState::AcceptingData;
}

void TTableBucketSpiller::ProcessFinalizing() {
    if (StateCharAdapter.IsAcceptingDataRequests() && StateUi32Adapter.IsAcceptingDataRequests() && StateUi64Adapter.IsAcceptingDataRequests()) {
        State = EState::AcceptingDataRequests;
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
    while (NextVectorToProcess != ENextVectorToProcess::None) {
        switch (NextVectorToProcess) {
            case ENextVectorToProcess::KeyAndVals:
                if (StateUi64Adapter.IsDataReady()) {
                    AppendVector(CurrentBucket.KeyIntVals, StateUi64Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::DataIntVals;
                    break;
                }

                if (StateUi64Adapter.IsAcceptingDataRequests()) {
                    StateUi64Adapter.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::DataIntVals:
                if (StateUi64Adapter.IsDataReady()) {
                    AppendVector(CurrentBucket.DataIntVals, StateUi64Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::StringsValues;
                    break;
                }

                if (StateUi64Adapter.IsAcceptingDataRequests()) {
                    StateUi64Adapter.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::StringsValues:
                if (StateCharAdapter.IsDataReady()) {
                    AppendVector(CurrentBucket.StringsValues, StateCharAdapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::StringsOffsets;
                    break;
                }

                if (StateCharAdapter.IsAcceptingDataRequests()) {
                    StateCharAdapter.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::StringsOffsets:
                if (StateUi32Adapter.IsDataReady()) {
                    AppendVector(CurrentBucket.StringsOffsets, StateUi32Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::InterfaceValues;
                    break;
                }

                if (StateUi32Adapter.IsAcceptingDataRequests()) {
                    StateUi32Adapter.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::InterfaceValues:
                if (StateCharAdapter.IsDataReady()) {
                    AppendVector(CurrentBucket.InterfaceValues, StateCharAdapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::InterfaceOffsets;
                    break;
                }

                if (StateCharAdapter.IsAcceptingDataRequests()) {
                    StateCharAdapter.RequestNextVector();
                    break;
                }
                return;
            case ENextVectorToProcess::InterfaceOffsets:
                if (StateUi32Adapter.IsDataReady()) {
                    AppendVector(CurrentBucket.InterfaceOffsets, StateUi32Adapter.ExtractVector());
                    
                    SpilledBucketsCount--;
                    if (SpilledBucketsCount == 0) {
                        NextVectorToProcess = ENextVectorToProcess::None;
                        State = EState::WaitingForExtraction;
                    } else {
                        NextVectorToProcess = ENextVectorToProcess::KeyAndVals;
                    }
                    
                    break;
                }

                if (StateUi32Adapter.IsAcceptingDataRequests()) {
                    StateUi32Adapter.RequestNextVector();
                    break;
                }
                return;
            default:
                return;

        }
    }
}

}

}

}

