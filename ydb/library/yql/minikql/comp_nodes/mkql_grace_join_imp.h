#pragma once

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_vector_spiller_adapter.h>

namespace NKikimr {
namespace NMiniKQL {
namespace GraceJoin {
        
const ui64 BitsForNumberOfBuckets = 5; // 2^5 = 32
const ui64 BucketsMask = (0x00000001 << BitsForNumberOfBuckets)  - 1;
const ui64 NumberOfBuckets = (0x00000001 << BitsForNumberOfBuckets);  // Number of hashed keys buckets to distribute incoming tables tuples
const ui64 DefaultTuplesNum = 100; // Default initial number of tuples in one bucket to allocate memory
const ui64 DefaultTupleBytes = 64; // Default size of all columns in table row for estimations
const ui64 HashSize = 1; // Using ui64 hash size

/*
Table data stored in buckets. Table columns are interpreted either as integers, strings or some interface-based type,
providing IHash, IEquate, IPack and IUnpack functions.  
External clients should transform (pack) data into appropriate presentation.

Key columns always first, following int columns, string columns and interface-based columns.

Optimum presentation of table data is chosen based on locality to perform most
processing of related data in processor caches.

Structure to represent offsets in header of key records could look like the following:

struct TKeysHeader {
    ui64 hash_offset = 0; // value of hash for keys data
    ui64 nulls_offset = sizeof(ui64); // Nulls are represented in the form of bitmap array. It goes immediately after hash
    ui64 int_vals_offset; // Integer values go after nulls bitmap.  it starts at nulls_offset + sizeof(nulls bitmap array)
};


*/

struct JoinTuplesIds {
    ui32 id1 = 0; // Identifier of first table tuple as index in bucket
    ui32 id2 = 0; // Identifier of second table tuple as index in bucket
};

// To store keys values when making join only for unique keys (any join attribute)
struct KeysHashTable {
    ui64 SlotSize = 0; // Slot size in hash table
    ui64 NSlots = 0; // Total number of slots in table  
    ui64 FillCount = 0; // Number of ui64 slots which are filled
    std::vector<ui64, TMKQLAllocator<ui64>> Table;  // Table to store keys data in particular slots
    std::vector<ui64, TMKQLAllocator<ui64>> SpillData; // Vector to store long data which cannot be fit in single hash table slot.
};

struct TTableBucket {
    ui64 TuplesNum = 0;  // Total number of tuples in bucket
    std::vector<ui64, TMKQLAllocator<ui64>> KeyIntVals;  // Vector to store table key values
    std::vector<ui64, TMKQLAllocator<ui64>> DataIntVals; // Vector to store data values in bucket
    std::vector<char, TMKQLAllocator<char>> StringsValues; // Vector to store data strings values
    std::vector<ui32, TMKQLAllocator<ui32>> StringsOffsets; // Vector to store strings values sizes (offsets in StringsValues are calculated) for particular tuple.
    std::vector<char, TMKQLAllocator<char>> InterfaceValues; // Vector to store types to work through external-provided IHash, IEquate interfaces
    std::vector<ui32, TMKQLAllocator<ui32>> InterfaceOffsets; // Vector to store sizes of columns to work through IHash, IEquate interfaces
    std::vector<JoinTuplesIds, TMKQLAllocator<JoinTuplesIds>>  JoinIds;     // Results of join operations stored as index of tuples in buckets 
                                                                            // of two tables with the same number

    std::vector<ui32, TMKQLAllocator<ui32>> RightIds; // Sorted Ids of right table joined tuples to process full join and exclusion join

    std::set<ui32> AllLeftMatchedIds;  // All row ids of left join table which have matching rows in right table. To process streaming join mode.
    std::set<ui32> AllRightMatchedIds; // All row ids of right join table which matching rows in left table. To process streaming join mode. 
    KeysHashTable AnyHashTable; // Hash table to process join only for unique keys (any join attribute)

    ui64 StringValuesTotalSize = 0;
    ui64 KeyIntValsTotalSize = 0;

    size_t GetSize() const {
        return KeyIntVals.size() * sizeof(ui64) + DataIntVals.size() * sizeof(ui64) + StringsValues.size() + StringsOffsets.size() * sizeof(ui32) + InterfaceValues.size() + InterfaceOffsets.size() * sizeof(ui32);
    }
 };

struct TTableBucketSpiller {
    TTableBucketSpiller(ISpiller::TPtr spiller, size_t sizeLimit)
        : StateUi64Adapter(spiller, sizeLimit)
        , StateUi32Adapter(spiller, sizeLimit)
        , StateCharAdapter(spiller, sizeLimit) {
    }

    TVectorSpillerAdapter<ui64, TMKQLAllocator<ui64>> StateUi64Adapter;
    TVectorSpillerAdapter<ui32, TMKQLAllocator<ui32>> StateUi32Adapter;
    TVectorSpillerAdapter<char, TMKQLAllocator<char>> StateCharAdapter;


    bool HasRunningAsyncIoOperation() const {
        return StateUi64Adapter.HasRunningAsyncIoOperation()
            || StateUi32Adapter.HasRunningAsyncIoOperation()
            || StateCharAdapter.HasRunningAsyncIoOperation();
    }

    void Update() {
        StateUi64Adapter.Update();
        StateUi32Adapter.Update();
        StateCharAdapter.Update();

        if (State == EState::Spilling) {
            ProcessBucketSpilling();
        } else if (State == EState::Restoring) {
            ProcessBucketRestoration();
        }
        
    }

    void Finalize() {
        IsFinalizing = true;
    }

    bool HasMoreSpilledBuckets() const { 
        return SpilledBucketsCount > 0;
    }

    void SpillBucket(TTableBucket&& bucket) {
        MKQL_ENSURE(NextVectorToProcess == ENextVectorToProcess::None, "Internal logic error");
        State = EState::Spilling;
        
        CurrentBucket = std::move(bucket);
        NextVectorToProcess = ENextVectorToProcess::KeyAndVals;
        
        ProcessBucketSpilling();
    }

    void StartBucketRestoration() {
        MKQL_ENSURE(State == EState::Restoring, "Internal logic error");
        MKQL_ENSURE(NextVectorToProcess == ENextVectorToProcess::None, "Internal logic error");

        NextVectorToProcess = ENextVectorToProcess::KeyAndVals;
        ProcessBucketRestoration();
    }

    void ProcessBucketSpilling() {
        while (NextVectorToProcess != ENextVectorToProcess::None) {
            switch (NextVectorToProcess) {
                case ENextVectorToProcess::KeyAndVals:
                    if (StateUi64Adapter.HasRunningAsyncIoOperation() || !StateUi64Adapter.IsAcceptingData()) return;

                    StateUi64Adapter.AddData(std::move(CurrentBucket.KeyIntVals));
                    NextVectorToProcess = ENextVectorToProcess::DataIntVals;
                    break;
                case ENextVectorToProcess::DataIntVals:
                    if (StateUi64Adapter.HasRunningAsyncIoOperation() || !StateUi64Adapter.IsAcceptingData()) return;

                    StateUi64Adapter.AddData(std::move(CurrentBucket.DataIntVals));
                    NextVectorToProcess = ENextVectorToProcess::StringsValues;
                    break;
                case ENextVectorToProcess::StringsValues:
                    if (StateCharAdapter.HasRunningAsyncIoOperation() || !StateCharAdapter.IsAcceptingData()) return;

                    StateCharAdapter.AddData(std::move(CurrentBucket.StringsValues));
                    NextVectorToProcess = ENextVectorToProcess::StringsOffsets;
                    break;
                case ENextVectorToProcess::StringsOffsets:
                    if (StateUi32Adapter.HasRunningAsyncIoOperation() || !StateUi32Adapter.IsAcceptingData()) return;

                    StateUi32Adapter.AddData(std::move(CurrentBucket.StringsOffsets));
                    NextVectorToProcess = ENextVectorToProcess::InterfaceValues;
                    break;
                case ENextVectorToProcess::InterfaceValues:
                    if (StateCharAdapter.HasRunningAsyncIoOperation() || !StateCharAdapter.IsAcceptingData()) return;

                    StateCharAdapter.AddData(std::move(CurrentBucket.InterfaceValues));
                    NextVectorToProcess = ENextVectorToProcess::InterfaceOffsets;
                    break;
                case ENextVectorToProcess::InterfaceOffsets:
                    if (StateUi32Adapter.HasRunningAsyncIoOperation() || !StateUi32Adapter.IsAcceptingData()) return;

                    StateUi32Adapter.AddData(std::move(CurrentBucket.InterfaceOffsets));
                    NextVectorToProcess = ENextVectorToProcess::None;
                    SpilledBucketsCount++;

                    break;
                default:
                    return;
            }
        }
        if (!HasRunningAsyncIoOperation() && IsFinalizing) {
            State = EState::Restoring;

            StateUi64Adapter.Finalize();
            StateUi32Adapter.Finalize();
            StateCharAdapter.Finalize();
        }
    }

    template <class T>
    void AppendVector(std::vector<T, TMKQLAllocator<T>>& first, std::vector<T, TMKQLAllocator<T>>&& second) {
        if (first.empty()) {
            first = std::move(second);
            return;
        }
        first.insert(first.end(), second.begin(), second.end());
        second.clear();
    }

    void ProcessBucketRestoration() {
        while (NextVectorToProcess != ENextVectorToProcess::None) {
            switch (NextVectorToProcess) {
                case ENextVectorToProcess::KeyAndVals:
                    if (StateUi64Adapter.HasRunningAsyncIoOperation()) return;

                    if (!StateUi64Adapter.IsDataReady()) {
                        StateUi64Adapter.RequestNextVector();
                        if (StateUi64Adapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.KeyIntVals, StateUi64Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::DataIntVals;
                    break;
                case ENextVectorToProcess::DataIntVals:
                    if (StateUi64Adapter.HasRunningAsyncIoOperation()) return;

                    if (!StateUi64Adapter.IsDataReady()) {
                        StateUi64Adapter.RequestNextVector();
                        if (StateUi64Adapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.DataIntVals, StateUi64Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::StringsValues;
                    break;
                case ENextVectorToProcess::StringsValues:
                    if (StateCharAdapter.HasRunningAsyncIoOperation()) return;

                    if (!StateCharAdapter.IsDataReady()) {
                        StateCharAdapter.RequestNextVector();
                        if (StateCharAdapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.StringsValues, StateCharAdapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::StringsOffsets;
                    break;
                case ENextVectorToProcess::StringsOffsets:
                    if (StateUi32Adapter.HasRunningAsyncIoOperation()) return;

                    if (!StateUi32Adapter.IsDataReady()) {
                        StateUi32Adapter.RequestNextVector();
                        if (StateUi32Adapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.StringsOffsets, StateUi32Adapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::InterfaceValues;
                    break;
                case ENextVectorToProcess::InterfaceValues:
                    if (StateCharAdapter.HasRunningAsyncIoOperation()) return;

                    if (!StateCharAdapter.IsDataReady()) {
                        StateCharAdapter.RequestNextVector();
                        if (StateCharAdapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.InterfaceValues, StateCharAdapter.ExtractVector());
                    NextVectorToProcess = ENextVectorToProcess::InterfaceOffsets;
                    break;
                case ENextVectorToProcess::InterfaceOffsets:
                    if (StateUi32Adapter.HasRunningAsyncIoOperation()) return;

                    if (!StateUi32Adapter.IsDataReady()) {
                        StateUi32Adapter.RequestNextVector();
                        if (StateUi32Adapter.HasRunningAsyncIoOperation()) return;
                    }
                    AppendVector(CurrentBucket.InterfaceOffsets, StateUi32Adapter.ExtractVector());
                    SpilledBucketsCount--;
                    if (SpilledBucketsCount == 0) {
                        NextVectorToProcess = ENextVectorToProcess::None;
                        State = EState::InMemory;
                    } else {
                        NextVectorToProcess = ENextVectorToProcess::KeyAndVals;
                    }
                    break;
                default:
                    return;

            }
        }
    }

    TTableBucket&& ExtractBucket() {
        MKQL_ENSURE(State == EState::InMemory, "Internal logic error");
        MKQL_ENSURE(SpilledBucketsCount == 0, "Internal logic error");
        return std::move(CurrentBucket);
    }

    bool HasAnythingToProcess() const {
        return NextVectorToProcess != ENextVectorToProcess::None;
    }

    enum class EState {
        Spilling,
        Restoring,
        InMemory
    };

    enum class ENextVectorToProcess {
        KeyAndVals,
        DataIntVals,
        StringsValues,
        StringsOffsets,
        InterfaceValues,
        InterfaceOffsets,

        None
    };

    EState State = EState::InMemory;
    ENextVectorToProcess NextVectorToProcess = ENextVectorToProcess::None;

    ui64 SpilledBucketsCount = 0;

    bool IsFinalizing = false;

    TTableBucket CurrentBucket;
};


struct TupleData {
    ui64 * IntColumns = nullptr; // Array of packed int  data of the table. Caller should allocate array of NumberOfIntColumns size
    char ** StrColumns = nullptr; // Pointers to values of strings for table.  Strings are not null-terminated
    ui32 * StrSizes = nullptr; // Sizes of strings for table.
    NYql::NUdf::TUnboxedValue * IColumns = nullptr; // Array of TUboxedValues for interface-based columns. Caller should allocate array of required size.
    bool AllNulls = false; // If tuple data contains all nulls (it is required for corresponding join types)

};

// Interface to work with complex column types without "simple" byte-serialized representation (which can be used for keys comparison)
struct TColTypeInterface {
    NYql::NUdf::IHash::TPtr HashI = nullptr;  // Interface to calculate hash of column value
    NYql::NUdf::IEquate::TPtr EquateI = nullptr; // Interface to compare two column values
    std::shared_ptr<TValuePacker> Packer; // Class to pack and unpack column values
    const THolderFactory& HolderFactory; // To use during unpacking 
};


// Class which represents single table data stored in buckets
class TTable {
    ui64 NumberOfKeyIntColumns = 0; // Key int columns always first and padded to sizeof(ui64).
    ui64 NumberOfKeyStringColumns = 0; // String key columns go after key int columns
    ui64 NumberOfKeyIColumns = 0; // Number of interface - provided key columns

    ui64 NumberOfDataIntColumns = 0; //Number of integer data columns in the Table
    ui64 NumberOfDataStringColumns = 0; // Number of strings data columns in the Table
    ui64 NumberOfDataIColumns = 0; //  Number of interface - provided data columns
    
    TColTypeInterface * ColInterfaces = nullptr; // Array of interfaces to work with corresponding columns data


    ui64 NumberOfColumns = 0; // Number of columns in the Table
    ui64 NumberOfKeyColumns = 0; // Number of key columns in the Table
    ui64 NumberOfDataColumns = 0; // Number of data columns in the Table
    ui64 NumberOfStringColumns = 0; // Total number of String Columns
    ui64 NumberOfIColumns = 0; // Total number of interface-based columns
    ui64 NullsBitmapSize_ = 1; // Default size of ui64 values used for null columns bitmap.
                                // Every bit set means null value. Order of columns is equal to order in AddTuple call.
                                // First key int column is  bit 1 in bit mask, second - bit 2, etc.  Bit 0 is least significant in bitmask and tells if key columns contain nulls.
    ui64 TotalStringsSize = 0; // Bytes in tuple header reserved to store total strings size key tuple columns
    ui64 HeaderSize = HashSize + NullsBitmapSize_ + NumberOfKeyIntColumns + NumberOfKeyIColumns + TotalStringsSize; // Header of all tuples size

    ui64 BytesInKeyIntColumns = sizeof(ui64) * NumberOfKeyIntColumns;
    
    // Table data is partitioned in buckets based on key value
    std::vector<TTableBucket> TableBuckets;

    std::vector<TTableBucketSpiller> TableBucketsSpiller;

    // Temporary vector for tuples manipulation;
    std::vector<ui64> TempTuple;

    // Hashes for interface - based columns values
    std::vector<ui64> IColumnsHashes;

    // Serialized values for interface-based columns
    std::vector<std::vector<char>> IColumnsVals;

    // Current iterator index for NextTuple iterator
    ui64 CurrIterIndex = 0;

    // Index for NextJoinedData iterator
    ui64 CurrJoinIdsIterIndex = 0;

    // Current bucket for iterators
    ui64 CurrIterBucket = 0;

    // True if table joined from two other tables
    bool IsTableJoined = false;

    // Type of the join
    EJoinKind JoinKind = EJoinKind::Inner;

    // Pointers to the joined tables. Lifetime of source tables to join should be greater than joined table
    TTable * JoinTable1 = nullptr;
    TTable * JoinTable2 = nullptr;

    // Returns tuple data in td from bucket with id bucketNum.  Tuple id inside bucket is tupleId.
    inline void GetTupleData(ui32 bucketNum, ui32 tupleId, TupleData& td);

    // True if current iterator of tuple in joinedTable has corresponding joined tuple in second table. Id of joined tuple in second table returns in tupleId2.
    inline bool HasJoinedTupleId(TTable* joinedTable, ui32& tupleId2);

    // Adds keys to KeysHashTable, return true if added, false if equal key already added
    inline bool AddKeysToHashTable(KeysHashTable& t, ui64* keys);

    ui64 JoinTwoBuckets(TTableBucket * bucket1, TTableBucket * bucket2, ui64 bucket,
                            std::vector<JoinTuplesIds, TMKQLAllocator<JoinTuplesIds, EMemorySubPool::Temporary>>& joinResults,
                            std::vector<ui64, TMKQLAllocator<ui64, EMemorySubPool::Temporary>>& joinSlots,
                            std::vector<ui64, TMKQLAllocator<ui64, EMemorySubPool::Temporary>>& spillSlots,
                            std::vector<ui64, TMKQLAllocator<ui64, EMemorySubPool::Temporary>>& slotToIdx);

    ui64 TotalPacked = 0; // Total number of packed tuples
    ui64 TotalUnpacked = 0; // Total number of unpacked tuples

    bool LeftTableBatch_ = false; // True if left table is processed in batch mode
    bool RightTableBatch_ = false; // True if right table is procesed in batch mode

    bool HasMoreLeftTuples_  = false; // True if join is not completed, rows from left table are coming
    bool HasMoreRightTuples_ = false; // True if join is not completed, rows from right table are coming

    bool IsAny_ = false;  // True if key duplicates need to be removed from table (any join)

    bool Table2Initialized_ = false;    // True when iterator counters for second table already initialized

    ui64 TuplesFound_ = 0; // Total number of matching keys found during join

    ui64 NextBucketToJoin = 0;

    i64 NextBucketToSpill = NumberOfBuckets - 1;

public:

    // Adds new tuple to the table.  intColumns, stringColumns - data of columns, 
    // stringsSizes - sizes of strings columns.  Indexes of null-value columns
    // in the form of bit array should be first values of intColumns.
    void AddTuple(ui64* intColumns, char** stringColumns, ui32* stringsSizes, NYql::NUdf::TUnboxedValue * iColumns = nullptr);

    // Resets iterators. In case of join results table it also resets iterators for joined tables
    void ResetIterator();

    // Returns value of next tuple. Returs true if there are more tuples
    bool NextTuple(TupleData& td);

    // Joins two tables and stores join result in table data. Tuples of joined table could be received by
    // joined table iterator.  Life time of t1, t2 should be greater than lifetime of joined table
    // hasMoreLeftTuples, hasMoreRightTuples is true if join is partial and more rows are coming.  For final batch hasMoreLeftTuples = false, hasMoreRightTuples = false
    void Join(TTable& t1, TTable& t2, EJoinKind joinKind = EJoinKind::Inner, bool hasMoreLeftTuples = false, bool hasMoreRightTuples = false );


    // Returns next jointed tuple data. Returs true if there are more tuples
    bool NextJoinedData(TupleData& td1, TupleData& td2);

    bool IsEverythingJoined() {
        return NextBucketToJoin > NumberOfBuckets;
    }

    bool HasAnythingToProcess() {
        for (i64 bucket = NumberOfBuckets - 1; bucket > NextBucketToSpill; --bucket) {
            if (TableBucketsSpiller[bucket].HasAnythingToProcess()) return true;
        }
        return false;
    }

    // After this call either all buckets are spilled/loaded or need Yield.
    bool UpdateAndCheckIfBusy() {
        while (HasAnythingToProcess()) {
            for (i64 bucket = NumberOfBuckets - 1; bucket > NextBucketToSpill; --bucket) {
                TableBucketsSpiller[bucket].Update();
            }

            for (i64 bucket = NumberOfBuckets - 1; bucket > NextBucketToSpill; --bucket) {
                if (TableBucketsSpiller[bucket].HasRunningAsyncIoOperation()) return true;
            }
        }

        return false;
    }

    void EnsureAllSpilledBucketsAreReady() {
        for (i64 bucket = NumberOfBuckets - 1; bucket > NextBucketToSpill; --bucket) {
            MKQL_ENSURE(!TableBucketsSpiller[bucket].HasAnythingToProcess(), "MISHA ERROR");
        }
    }

    bool IsBucketInMemory(ui64 bucket) {
        return TableBucketsSpiller[bucket].State == TTableBucketSpiller::EState::InMemory;
    }

    void StartLoadingBucket(ui64 bucket) {
        TableBucketsSpiller[bucket].StartBucketRestoration();
    }

    void ExtractBucket(ui64 bucket) {
        TableBuckets[bucket] = std::move(TableBucketsSpiller[bucket].ExtractBucket());
    }

    void FinalizeSpilling() {
        for (ui64 i = 0; i < NumberOfBuckets; ++i) {
            // TODO add current bucket
            TableBucketsSpiller[i].Finalize();
        }
    }

    void InitializeBucketSpillers(std::shared_ptr<ISpillerFactory> spillerFactory) {
        for (size_t i = 0; i < NumberOfBuckets; ++i) {
            TableBucketsSpiller.emplace_back(spillerFactory->CreateSpiller(), 3_MB);
        }
    }

    ui64 GetAllBucketsSize() const {
        ui64 sum = 0;
        for (ui64 i = 0; i < NumberOfBuckets; ++i) {
            sum += TableBuckets[i].GetSize();
        }
        return sum;
    }

    ui64 GetTuplesNum() const {
        ui64 sum = 0;
        for (ui64 i = 0; i < NumberOfBuckets; ++i) {
            sum += TableBuckets[i].TuplesNum;
        }
        return sum;
    }

    bool TryToReduceMemory() {
        EnsureAllSpilledBucketsAreReady();
        for (i64 bucket = NumberOfBuckets - 1; bucket > NextBucketToSpill; --bucket) {
            if (TableBuckets[bucket].GetSize()) {
                std::cerr << std::format("[MISHA] Spilling again bucket {} of size {}\n", bucket, TableBuckets[bucket].GetSize());
                TableBucketsSpiller[bucket].SpillBucket(std::move(TableBuckets[bucket]));

                if (TableBucketsSpiller[bucket].HasRunningAsyncIoOperation()) return true;
            }
        }

        while (NextBucketToSpill >= 0) {
            i64 nowSpilling = NextBucketToSpill;
            --NextBucketToSpill;
            std::cerr << std::format("[MISHA] Spilling bucket {} of size {}\n", nowSpilling, TableBuckets[nowSpilling].GetSize());

            TableBucketsSpiller[nowSpilling].SpillBucket(std::move(TableBuckets[nowSpilling]));

            if (TableBucketsSpiller[nowSpilling].HasRunningAsyncIoOperation()) return true;
        }

        return false;
    }

    // Clears table content
    void Clear();

    // Creates new table with key columns and data columns
    TTable(ui64 numberOfKeyIntColumns = 0, ui64 numberOfKeyStringColumns = 0,
            ui64 numberOfDataIntColumns = 0, ui64 numberOfDataStringColumns = 0,
            ui64 numberOfKeyIColumns = 0, ui64 numberOfDataIColumns = 0, 
            ui64 nullsBitmapSize = 1, TColTypeInterface * colInterfaces = nullptr, bool isAny = false);
    
    ~TTable();

};



}
}
}
