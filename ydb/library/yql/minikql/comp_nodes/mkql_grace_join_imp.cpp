#include "mkql_grace_join_imp.h"

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/log/log.h>

#include <contrib/libs/xxhash/xxhash.h>
#include <chrono>

namespace NKikimr {
namespace NMiniKQL {

namespace GraceJoin {



void TTable::AddTuple(  ui64 * intColumns, char ** stringColumns, ui32 * stringsSizes ) {

    TotalPacked++;

    TempTuple.clear();
    TempTuple.insert(TempTuple.end(), intColumns, intColumns + NullsBitmapSize + NumberOfKeyIntColumns);

    
    ui64 totalBytesForStrings = 0;
    ui64 totalIntsForStrings = 0;

    // Processing variable length string columns
    if ( NumberOfKeyStringColumns != 0 ) {

        for( ui64 i = 0; i < NumberOfKeyStringColumns; i++ ) {
            totalBytesForStrings += stringsSizes[i];
        }

        totalIntsForStrings = (totalBytesForStrings + sizeof(ui64) - 1) / sizeof(ui64);

        TempTuple.push_back(totalIntsForStrings);
        TempTuple.resize(TempTuple.size() + totalIntsForStrings);

        TempTuple.back() = 0;

        ui64 * startPtr = (TempTuple.data() + TempTuple.size() - totalIntsForStrings );
        char * currStrPtr = reinterpret_cast< char* > (startPtr);

        for( ui64 i = 0; i < NumberOfKeyStringColumns; i++) {
            std::memcpy(currStrPtr, stringColumns[i], stringsSizes[i] );
            currStrPtr+=stringsSizes[i];
        }

    }



    XXH64_hash_t hash = XXH64(TempTuple.data(), TempTuple.size() * sizeof(ui64), 0);

    if (!hash) hash = 1;



    ui64 bucket = hash & BucketsMask;

    TableBuckets[bucket].TuplesNum++;

    std::vector<ui64> & keyIntVals = TableBuckets[bucket].KeyIntVals;
    std::vector<ui32> & stringsOffsets = TableBuckets[bucket].StringsOffsets;
    std::vector<ui64> & dataIntVals = TableBuckets[bucket].DataIntVals;
    std::vector<char> & stringVals = TableBuckets[bucket].StringsValues;

    ui32 offset = keyIntVals.size(); // Offset of tuple inside the keyIntVals vector

    keyIntVals.push_back(hash);
    keyIntVals.insert(keyIntVals.end(), TempTuple.begin(), TempTuple.end());

    if (NumberOfStringColumns) {
        stringsOffsets.push_back(offset); // Adding offset to tuple in keyIntVals vector
        stringsOffsets.push_back(stringVals.size());  // Adding offset to string values

        // Adding strings sizes for keys and data
        stringsOffsets.insert( stringsOffsets.end(), stringsSizes, stringsSizes+NumberOfStringColumns );

    }

    // Adding data values
    ui64 * dataColumns = intColumns + NullsBitmapSize + NumberOfKeyIntColumns;
    dataIntVals.insert(dataIntVals.end(), dataColumns, dataColumns + NumberOfDataIntColumns);

    // Adding strings values for data columns
    char ** dataStringsColumns = stringColumns + NumberOfKeyStringColumns;
    ui32 * dataStringsSizes = stringsSizes + NumberOfKeyStringColumns;
      
     for( ui64 i = 0; i < NumberOfDataStringColumns; i++) {
            ui32 currStringSize = *(dataStringsSizes + i);
            stringVals.insert(stringVals.end(), *(dataStringsColumns + i), *(dataStringsColumns + i) + currStringSize);
    }


}

void TTable::ResetIterator() {
    CurrIterIndex = 0;
    CurrIterBucket = 0;
    CurrJoinIdsIterIndex = 0;
    if (IsTableJoined) {
        JoinTable1->ResetIterator();
        JoinTable2->ResetIterator();
    }
}

// Checks if there are more tuples and sets bucketId and tupleId to next valid. 
inline bool HasMoreTuples(std::vector<TTableBucket> & tableBuckets, ui64 & bucketId, ui64 & tupleId ) {

    if (bucketId >= tableBuckets.size()) return false;

    if ( tupleId >= tableBuckets[bucketId].TuplesNum ) {
        tupleId = 0;
        bucketId ++;

        if (bucketId == tableBuckets.size()) {
            return false;
        }
            
        while( tableBuckets[bucketId].TuplesNum == 0 ) {
           bucketId ++;
            if (bucketId == tableBuckets.size()) {
                return false;
            }   
        }
    }

    return true;

}


// Returns value of next tuple. Returs true if there are more tuples
bool TTable::NextTuple(TupleData & td){
    if (HasMoreTuples(TableBuckets, CurrIterBucket, CurrIterIndex )) {
        GetTupleData(CurrIterBucket, CurrIterIndex, td);
        CurrIterIndex++;
        return true;
    } else {
        td.AllNulls = true;
        return false;
    }
}


inline bool HasBitSet( ui64 * buf, ui64 Nbits ) {
    while(Nbits > sizeof(ui64)) {
        if (*buf++) return true;
        Nbits -= sizeof(ui64);
    }
    return ((*buf) << (sizeof(ui64) - Nbits));  
}

// Joins two tables and returns join result in joined table. Tuples of joined table could be received by
// joined table iterator
void TTable::Join( TTable & t1, TTable & t2, EJoinKind joinKind ) {


    JoinTable1 = &t1;
    JoinTable2 = &t2;

    JoinKind = joinKind;

 
    IsTableJoined = true;

    if (joinKind == EJoinKind::Cross) return;

    if ( JoinKind == EJoinKind::Right || JoinKind == EJoinKind::RightOnly || JoinKind == EJoinKind::RightSemi ) {
        std::swap(JoinTable1, JoinTable2);
    } 


    ui64 tuplesFound = 0;
    std::vector<ui64> joinSlots, spillSlots, slotToIdx;
    ui64 reservedSize = 6 * (DefaultTupleBytes * DefaultTuplesNum) / sizeof(ui64);
    joinSlots.reserve( reservedSize );
    spillSlots.reserve( reservedSize );
    std::vector < JoinTuplesIds > joinResults;


    for (ui64 bucket = 0; bucket < NumberOfBuckets; bucket++) {

        joinResults.clear();
        TTableBucket * bucket1 = &JoinTable1->TableBuckets[bucket];
        TTableBucket * bucket2 = &JoinTable2->TableBuckets[bucket];


        if ( bucket2->TuplesNum > bucket1->TuplesNum ) {
            std::swap(bucket1, bucket2);
       }
 
        joinResults.reserve(3 * bucket1->TuplesNum );

        ui64 headerSize = JoinTable1->HeaderSize;
        ui64 slotSize = headerSize;

        if (JoinTable1->NumberOfKeyStringColumns != 0) {
            ui64 avgStringsSize = ( 3 * (bucket2->KeyIntVals.size() - bucket2->TuplesNum * headerSize) ) / ( 2 * bucket2->TuplesNum + 1)  + 1;
            slotSize = headerSize + avgStringsSize;
        }

        ui64 nSlots = 3 * bucket2->TuplesNum + 1;
        joinSlots.clear();
        spillSlots.clear();
        slotToIdx.clear();
        joinSlots.resize(nSlots*slotSize, 0);
        slotToIdx.resize(nSlots, 0);

        ui32 tuple2Idx = 0;            
        auto it2 = bucket2->KeyIntVals.begin();
        while (it2 != bucket2->KeyIntVals.end() ) {
            ui64 valSize = (!JoinTable2->NumberOfKeyStringColumns) ? headerSize : headerSize + *(it2 + headerSize - JoinTable2->TotalStringsSize) ;
            ui64 hash = *it2;
            ui64 * nullsPtr = it2+1;
            if (!HasBitSet(nullsPtr, JoinTable1->NumberOfKeyColumns))
            {

                ui64 slotNum = hash % nSlots;
                auto slotIt = joinSlots.begin() + slotNum * slotSize;

                while (*slotIt != 0)
                {
                    slotIt += slotSize;
                    if (slotIt == joinSlots.end())
                        slotIt = joinSlots.begin();
                }

                if (valSize <= slotSize)
                {
                    std::copy_n(it2, valSize, slotIt);
                }
                else
                {
                    std::copy_n(it2, headerSize, slotIt);
                    ui64 stringsPos = spillSlots.size();
                    spillSlots.insert(spillSlots.end(), it2 + headerSize, it2 + headerSize + *(it2 + headerSize - JoinTable2->TotalStringsSize));
                    *(slotIt + headerSize) = stringsPos;
                }
                ui64 currSlotNum = (slotIt - joinSlots.begin()) / slotSize;
                slotToIdx[currSlotNum] = tuple2Idx;
            }
            it2 += valSize;
            tuple2Idx ++;
        }


        ui32 tuple1Idx = 0;
        auto it1 = bucket1->KeyIntVals.begin();
        while ( it1 < bucket1->KeyIntVals.end() ) {
        
            ui64 valSize = (!JoinTable1->NumberOfKeyStringColumns) ? headerSize : headerSize + *(it1 + headerSize - JoinTable1->TotalStringsSize);
            ui64 hash = *it1;
            ui64 * nullsPtr = it1+1;
            if (!HasBitSet(nullsPtr, JoinTable1->NumberOfKeyColumns))
            {
                ui64 slotNum = hash % nSlots;
                auto slotIt = joinSlots.begin() + slotNum * slotSize;
                ui64 collisions = 0;
                while (*slotIt != 0 && slotIt != joinSlots.end())
                {
                    bool matchFound = false;
                    if (valSize <= slotSize)
                    {
                        if (std::equal(it1, it1 + valSize, slotIt))
                        {
                            tuplesFound++;
                            matchFound = true;
                        }
                    }
                    else
                    {
                        if (std::equal(it1, it1 + headerSize, slotIt))
                        {
                            ui64 stringsPos = *(slotIt + headerSize);
                            ui64 stringsSize = *(slotIt + headerSize - 1);
                            if (std::equal(it1 + headerSize, it1 + headerSize + stringsSize, spillSlots.begin() + stringsPos))
                            {
                                tuplesFound++;
                                matchFound = true;
                            }
                        }
                    }

                    if (matchFound)
                    {
                        JoinTuplesIds joinIds;
                        joinIds.id1 = tuple1Idx;
                        joinIds.id2 = slotToIdx[(slotIt - joinSlots.begin()) / slotSize];
                        if (JoinTable2->TableBuckets[bucket].TuplesNum > JoinTable1->TableBuckets[bucket].TuplesNum)
                        {
                            std::swap(joinIds.id1, joinIds.id2);
                        }
                        joinResults.emplace_back(joinIds);
                    }

                    slotIt += slotSize;
                    if (slotIt == joinSlots.end())
                        slotIt = joinSlots.begin();
                }
            }

            it1 += valSize;
            tuple1Idx ++;
        }
        std::sort(joinResults.begin(), joinResults.end(), [](JoinTuplesIds a, JoinTuplesIds b)
        {
            if (a.id1 < b.id1) return true;
            if (a.id1 == b.id1 && (a.id2 < b.id2)) return true;
            return false;
        });

        
        TableBuckets[bucket].JoinIds = std::move(joinResults);
        if ( JoinKind == EJoinKind::Full || JoinKind == EJoinKind::Exclusion ) {
            std::vector<ui32> & rightIds = TableBuckets[bucket].RightIds;
            std::vector<JoinTuplesIds> & joinIds = TableBuckets[bucket].JoinIds;
            rightIds.clear();
            rightIds.reserve(joinIds.size());
            for (const auto & id: joinIds) {
                rightIds.emplace_back(id.id2);
            }
            std::sort(rightIds.begin(), rightIds.end());
        }
    }

}

inline void TTable::GetTupleData(ui32 bucketNum, ui32 tupleId, TupleData & td) {

    ui64 keyIntsOffset = 0;
    ui64 dataIntsOffset = 0;
    ui64 keyStringsOffset = 0;
    ui64 dataStringsOffset = 0;

    td.AllNulls = false;

    TTableBucket & tb = TableBuckets[bucketNum];
    ui64 stringsOffsetsIdx = tupleId * (NumberOfStringColumns + 2);

    if(NumberOfKeyStringColumns != 0) {
        keyIntsOffset = tb.StringsOffsets[stringsOffsetsIdx];
    } else {
        keyIntsOffset = HeaderSize * tupleId;
    }


    for ( ui64 i = 0; i < NumberOfKeyIntColumns + NullsBitmapSize; ++i) {
        td.IntColumns[i] = tb.KeyIntVals[keyIntsOffset + HashSize + i];
    }

    dataIntsOffset = NumberOfDataIntColumns * tupleId;

    for ( ui64 i = 0; i < NumberOfDataIntColumns; ++i) {
        td.IntColumns[NumberOfKeyIntColumns + NullsBitmapSize + i] = tb.DataIntVals[dataIntsOffset + i];
    }

    char *strPtr = nullptr;
    if(NumberOfKeyStringColumns != 0) {
        keyStringsOffset = tb.StringsOffsets[stringsOffsetsIdx] + HeaderSize;

        strPtr = reinterpret_cast<char *>(tb.KeyIntVals.data() + keyStringsOffset);
        
        for (ui64 i = 0; i < NumberOfKeyStringColumns; ++i)
        {
            td.StrColumns[i] = strPtr;
            td.StrSizes[i] = tb.StringsOffsets[stringsOffsetsIdx + 2 + i];
            strPtr += td.StrSizes[i];
        }
    }


    if(NumberOfDataStringColumns != 0) {
         dataStringsOffset = tb.StringsOffsets[stringsOffsetsIdx + 1];
    }

    strPtr = (tb.StringsValues.data() + dataStringsOffset);
    for ( ui64 i = 0; i < NumberOfDataStringColumns; ++i ) {
        ui32 currIdx = NumberOfKeyStringColumns + i;
        td.StrColumns[currIdx] = strPtr;
        td.StrSizes[currIdx] = tb.StringsOffsets[stringsOffsetsIdx + 2 + i];
        strPtr += td.StrSizes[currIdx];
    }


}

inline bool TTable::HasJoinedTupleId(TTable *joinedTable, ui32 &tupleId2) {

    if (joinedTable->CurrIterBucket != CurrIterBucket)
    {
        CurrIterBucket = joinedTable->CurrIterBucket;
        CurrJoinIdsIterIndex = 0;
    }
    std::vector<JoinTuplesIds> &jids = TableBuckets[CurrIterBucket].JoinIds;

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

inline bool HasRightIdMatch(ui64 currId, ui64 & rightIdIter, const std::vector<ui32> & rightIds) {

    if (rightIdIter >= rightIds.size()) return false;

    while ( rightIdIter < rightIds.size() && currId > rightIds[rightIdIter])  rightIdIter++;

    if (rightIdIter >= rightIds.size()) return false;

    if (currId == rightIds[rightIdIter]) {
        return true;
    } else {
        return false;
    }

}


bool TTable::NextJoinedData( TupleData & td1, TupleData & td2) {

    if (JoinKind == EJoinKind::Cross) {

        if (HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex))
        {
            JoinTable1->GetTupleData(JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex, td1);

            if (HasMoreTuples(JoinTable2->TableBuckets, JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex))
            {
                JoinTable2->GetTupleData(JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex, td2);
                JoinTable2->CurrIterIndex++;
                return true;
            }
            else
            {
                JoinTable2->CurrIterBucket = 0;
                JoinTable2->CurrIterIndex = 0;
                JoinTable1->CurrIterIndex++;
                return NextJoinedData(td1, td2);
            }
        }
        else
            return false;
    }

    if ( JoinKind == EJoinKind::Inner ) {
        while(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
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
        if(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td2);
                CurrJoinIdsIterIndex++;
                std::vector<JoinTuplesIds> & jids = TableBuckets[CurrIterBucket].JoinIds;
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
        td2.AllNulls = true;
        return false;
    }

    if (  JoinKind == EJoinKind::Right ) {
        if(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td1);
                CurrJoinIdsIterIndex++;
                std::vector<JoinTuplesIds> & jids = TableBuckets[CurrIterBucket].JoinIds;
                if ( (CurrJoinIdsIterIndex == jids.size()) || ( JoinTable1->CurrIterIndex != jids[CurrJoinIdsIterIndex].id1) ) JoinTable1->CurrIterIndex++;
                return true;
            } else {
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
        while(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (!HasJoinedTupleId(JoinTable1, tupleId2))
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
        while(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (!HasJoinedTupleId(JoinTable1, tupleId2))
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


    if ( JoinKind == EJoinKind::LeftSemi ) {
        while(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {

                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                td2.AllNulls = true;
                while( HasJoinedTupleId(JoinTable1, tupleId2) ) {
                    CurrJoinIdsIterIndex++;
                }                
                JoinTable1->CurrIterIndex++;
                return true;
            }
            JoinTable1->CurrIterIndex++;
        }
        return false;
    }

    if ( JoinKind == EJoinKind::RightSemi ) {
        while(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td2);
                td1.AllNulls = true;
                while( HasJoinedTupleId(JoinTable1, tupleId2) ) {
                    CurrJoinIdsIterIndex++;
                }                
                JoinTable1->CurrIterIndex++;
                return true;
            }
            JoinTable1->CurrIterIndex++;
        }
        return false;
    }

    if ( JoinKind == EJoinKind::Full ) {
        if(HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                JoinTable1->GetTupleData(CurrIterBucket, JoinTable1->CurrIterIndex, td1);
                JoinTable2->GetTupleData(CurrIterBucket, tupleId2, td2);
                CurrJoinIdsIterIndex++;
                std::vector<JoinTuplesIds> & jids = TableBuckets[CurrIterBucket].JoinIds;
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

        while (HasMoreTuples(JoinTable2->TableBuckets, JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex)) {

            if (CurrIterBucket != JoinTable2->CurrIterBucket) {
                CurrIterBucket = JoinTable2->CurrIterBucket;
                CurrJoinIdsIterIndex = 0;
            }
            std::vector<ui32> & rightIds = TableBuckets[CurrIterBucket].RightIds;
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
        while (HasMoreTuples(JoinTable1->TableBuckets, JoinTable1->CurrIterBucket, JoinTable1->CurrIterIndex)) {
            ui32 tupleId2;
            if (HasJoinedTupleId(JoinTable1, tupleId2))
            {
                CurrJoinIdsIterIndex++;
                std::vector<JoinTuplesIds> & jids = TableBuckets[CurrIterBucket].JoinIds;
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

        while (HasMoreTuples(JoinTable2->TableBuckets, JoinTable2->CurrIterBucket, JoinTable2->CurrIterIndex)) {

            if (CurrIterBucket != JoinTable2->CurrIterBucket) {
                CurrIterBucket = JoinTable2->CurrIterBucket;
                CurrJoinIdsIterIndex = 0;
            }
            std::vector<ui32> & rightIds = TableBuckets[CurrIterBucket].RightIds;
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
            TTableBucket & tb = TableBuckets[bucket];
            tb.TuplesNum = 0;
            tb.KeyIntVals.clear();
            tb.DataIntVals.clear();
            tb.StringsOffsets.clear();
            tb.StringsValues.clear();
            tb.JoinIds.clear();
        }
           

}

// Creates new table with key columns and data columns
TTable::TTable( ui64 numberOfKeyIntColumns, ui64 numberOfKeyStringColumns,
                ui64 numberOfDataIntColumns, ui64 numberOfDataStringColumns ) :

                NumberOfKeyIntColumns(numberOfKeyIntColumns),
                NumberOfKeyStringColumns(numberOfKeyStringColumns),
                NumberOfDataIntColumns(numberOfDataIntColumns),
                NumberOfDataStringColumns(numberOfDataStringColumns)  {
        
    NumberOfKeyColumns = NumberOfKeyIntColumns + NumberOfKeyStringColumns;
    NumberOfDataColumns = NumberOfDataIntColumns + NumberOfDataStringColumns;
    NumberOfColumns = NumberOfKeyColumns + NumberOfDataColumns;
    NumberOfStringColumns = NumberOfKeyStringColumns + NumberOfDataStringColumns;

    BytesInKeyIntColumns = NumberOfKeyIntColumns * sizeof(ui64);

    NullsBitmapSize = NumberOfColumns / (8 * sizeof(ui64)) + 1;

    TotalStringsSize = (numberOfKeyStringColumns > 0 ) ? 1 : 0;

    HeaderSize = HashSize + NullsBitmapSize + NumberOfKeyIntColumns + TotalStringsSize;

    TableBuckets.resize(NumberOfBuckets);

    const ui64 reservedSizePerTuple = (2 * DefaultTupleBytes) / sizeof(ui64);

    TempTuple.reserve( reservedSizePerTuple );

    for ( auto & b: TableBuckets ) {
        b.KeyIntVals.reserve(DefaultTuplesNum * reservedSizePerTuple );
        b.StringsOffsets.reserve(DefaultTuplesNum * reservedSizePerTuple);
        b.DataIntVals.reserve( DefaultTuplesNum * reservedSizePerTuple);
        b.StringsValues.reserve( DefaultTuplesNum * reservedSizePerTuple);

     }

}

}

}

}

