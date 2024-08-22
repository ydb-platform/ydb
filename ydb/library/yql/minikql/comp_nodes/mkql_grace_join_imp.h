#pragma once

#include <ydb/library/yql/minikql/computation/mkql_vector_spiller_adapter.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>

namespace NKikimr {
namespace NMiniKQL {
namespace GraceJoin {

class TTableBucketSpiller;
#define GRACEJOIN_DEBUG INFO //DEBUG
#define GRACEJOIN_TRACE TRACE
        
const ui64 BitsForNumberOfBuckets = 5; // 2^5 = 32
const ui64 BucketsMask = (0x00000001 << BitsForNumberOfBuckets)  - 1;
const ui64 NumberOfBuckets = (0x00000001 << BitsForNumberOfBuckets);  // Number of hashed keys buckets to distribute incoming tables tuples
const ui64 DefaultTuplesNum = 101; // Default initial number of tuples in one bucket to allocate memory
const ui64 DefaultTupleBytes = 64; // Default size of all columns in table row for estimations
const ui64 HashSize = 1; // Using ui64 hash size
const ui64 SpillingSizeLimit = 1_MB; // Don't try to spill if net effect is lower than this size
const ui32 SpillingRowLimit = 1024; // Don't try to invoke spilling more often than 1 in this number of rows

constexpr ui64 CachelineBits = 9;
constexpr ui64 CachelineSize = ui64(1)<<CachelineBits;

template <typename Alloc>
class TBloomfilter {
    std::vector<ui64, Alloc> Storage_;
    ui64 *Ptr_;
    ui64 Bits_;
    bool Finalized_ = false;

    public:

    static constexpr ui64 BlockSize = CachelineSize;
    static constexpr ui64 BlockBits = CachelineBits;

    TBloomfilter() {}
    TBloomfilter(ui64 size) {
        Resize(size);
    }

    void Resize(ui64 size) {
        size = std::max(size, CachelineSize);
        Bits_ = 6;

        for (; (ui64(1)<<Bits_) < size; ++Bits_)
            ;

        Bits_ += 3; // -> multiply by 8
        size = 1u<<(Bits_ - 6);

        Storage_.clear();
        Storage_.resize(size + CachelineSize/sizeof(ui64) - 1);

        // align Ptr_ up to BlockSize
        Ptr_ = (ui64 *)((uintptr_t(Storage_.data()) + BlockSize - 1) & ~(BlockSize - 1));
        Finalized_ = false;
    }

    void Add(ui64 hash) {
        Y_DEBUG_ABORT_UNLESS(!Finalized_);

        auto bit = (hash >> (64 - Bits_));
        Ptr_[bit/64] |= (ui64(1)<<(bit % 64));
        // replace low BlockBits with next part of hash
        auto low = hash >> (64 - Bits_ - BlockBits);
        bit &= ~(BlockSize - 1);
        bit ^= low & (BlockSize - 1);
        Ptr_[bit/64] |= (ui64(1) << (bit % 64));
    }

    bool IsMissing(ui64 hash) const {
        Y_DEBUG_ABORT_UNLESS(Finalized_);

        auto bit = (hash >> (64 - Bits_));
        if (!(Ptr_[bit/64] & (ui64(1)<<(bit % 64))))
            return true;
        // replace low BlockBits with next part of hash
        auto low = hash >> (64 - Bits_ - BlockBits);
        bit &= ~(BlockSize - 1);
        bit ^= low & (BlockSize - 1);
        if (!(Ptr_[bit/64] & (ui64(1)<<(bit % 64))))
            return true;
        return false;
    }

    constexpr bool IsFinalized() const {
        return Finalized_;
    }

    void Finalize() {
        Finalized_ = true;
    }

    void Shrink() {
        Finalized_ = false;
        Bits_ = 1;
        Storage_.clear();
        Storage_.resize(1, ~ui64(0));
        Storage_.shrink_to_fit();
        Ptr_ = Storage_.data();
    }
};

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
    std::vector<ui64, TMKQLAllocator<ui64>> KeyIntVals;  // Vector to store table key values
    std::vector<ui64, TMKQLAllocator<ui64>> DataIntVals; // Vector to store data values in bucket
    std::vector<char, TMKQLAllocator<char>> StringsValues; // Vector to store data strings values
    std::vector<ui32, TMKQLAllocator<ui32>> StringsOffsets; // Vector to store strings values sizes (offsets in StringsValues are calculated) for particular tuple.
    std::vector<char, TMKQLAllocator<char>> InterfaceValues; // Vector to store types to work through external-provided IHash, IEquate interfaces
    std::vector<ui32, TMKQLAllocator<ui32>> InterfaceOffsets; // Vector to store sizes of columns to work through IHash, IEquate interfaces
    std::vector<JoinTuplesIds, TMKQLAllocator<JoinTuplesIds>>  JoinIds;     // Results of join operations stored as index of tuples in buckets 
                                                                            // of two tables with the same number
    std::vector<ui32, TMKQLAllocator<ui32>> LeftIds; // Left-side ids missing in other table

    std::vector<ui64, TMKQLAllocator<ui64>> JoinSlots;  // Hashtable
    ui64 NSlots = 0;  // Hashtable

 };

 struct TTableBucketStats {
    TBloomfilter<TMKQLAllocator<ui64>> BloomFilter;
    KeysHashTable AnyHashTable;      // Hash table to process join only for unique keys (any join attribute)
    ui64 TuplesNum = 0;             // Total number of tuples in bucket
    ui64 StringValuesTotalSize = 0; // Total size of StringsValues. Used to correctly calculate StringsOffsets.
    ui64 KeyIntValsTotalSize = 0;   // Total size of KeyIntVals. Used to correctly calculate StringsOffsets.
    ui32 SlotSize = 0;
    bool HashtableMatches = false;
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

// Class that spills bucket data.
// If, after saving, data has accumulated in the bucket again, you can spill it again.
// After restoring the entire bucket, it will contain all the data saved over different iterations.
class TTableBucketSpiller {
public:
    TTableBucketSpiller(ISpiller::TPtr spiller, size_t sizeLimit);

    // Takes the bucket and immediately starts spilling. Spilling continues until an async operation occurs.
    void SpillBucket(TTableBucket&& bucket);
    // Starts bucket restoration after spilling. Restores and unites all the buckets from different iterations. Will pause in case of async operation.
    void StartBucketRestoration();
    // Extracts bucket restored from spilling. This bucket will contain all the data from different iterations of spilling.
    TTableBucket&& ExtractBucket();

    // Updates the states of spillers. This update should be called after async operation completion to resume spilling/resoration.
    void Update();
    // Flushes all the data from inner spillers. Should be called when no more data is expected for spilling.
    void Finalize();
    // Is bucket in memory. False if spilled.
    bool IsInMemory() const;
    // Is bucket loaded to memory but still owned by spilled.
    // ExtractBucket must be called if true.
    bool IsExtractionRequired() const;
    // Is there any bucket that is being spilled right now.
    bool IsProcessingSpilling() const;
    // Is spiller ready to start loading new bucket.
    bool IsAcceptingDataRequests() const;
    // Is there any bucket that is being restored right now.
    bool IsRestoring() const;

private:
    void ProcessBucketSpilling();
    template <class T>
    void AppendVector(std::vector<T, TMKQLAllocator<T>>& first, std::vector<T, TMKQLAllocator<T>>&& second) const;
    void ProcessBucketRestoration();
    void ProcessFinalizing();

private:

    enum class EState {
        InMemory,
        Spilling,
        AcceptingData,
        Finalizing,
        AcceptingDataRequests,
        Restoring,
        WaitingForExtraction
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

    TVectorSpillerAdapter<ui64, TMKQLAllocator<ui64>> StateUi64Adapter;
    TVectorSpillerAdapter<ui32, TMKQLAllocator<ui32>> StateUi32Adapter;
    TVectorSpillerAdapter<char, TMKQLAllocator<char>> StateCharAdapter;

    EState State = EState::InMemory;
    ENextVectorToProcess NextVectorToProcess = ENextVectorToProcess::None;

    ui64 SpilledBucketsCount = 0;

    bool IsFinalizingRequested = false;

    TTableBucket CurrentBucket;
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
    // Statistics for buckets. Total number of tuples inside a single bucket and offsets.
    std::vector<TTableBucketStats> TableBucketsStats;

    std::vector<TTableBucketSpiller> TableBucketsSpillers;

    // Temporary vector for tuples manipulation;
    std::vector<ui64> TempTuple;

    // Hashes for interface - based columns values
    std::vector<ui64> IColumnsHashes;

    // Serialized values for interface-based columns
    std::vector<std::vector<char>> IColumnsVals;

    // Current iterator index for NextJoinedData iterator
    ui64 CurrIterIndex = 0;

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

    // Adds keys to KeysHashTable, return true if added, false if equal key already added
    inline bool AddKeysToHashTable(KeysHashTable& t, ui64* keys, NYql::NUdf::TUnboxedValue * iColumns);

    ui64 TotalPacked = 0; // Total number of packed tuples
    ui64 TotalUnpacked = 0; // Total number of unpacked tuples

    bool LeftTableBatch_ = false; // True if left table is processed in batch mode
    bool RightTableBatch_ = false; // True if right table is procesed in batch mode

    bool HasMoreLeftTuples_  = false; // True if join is not completed, rows from left table are coming
    bool HasMoreRightTuples_ = false; // True if join is not completed, rows from right table are coming

    bool IsAny_ = false;  // True if key duplicates need to be removed from table (any join)

    ui64 TuplesFound_ = 0; // Total number of matching keys found during join

public:


    // Resets iterators. In case of join results table it also resets iterators for joined tables
    void ResetIterator();

    // Joins two tables and stores join result in table data. Tuples of joined table could be received by
    // joined table iterator.  Life time of t1, t2 should be greater than lifetime of joined table
    // hasMoreLeftTuples, hasMoreRightTuples is true if join is partial and more rows are coming.  For final batch hasMoreLeftTuples = false, hasMoreRightTuples = false
    void Join(TTable& t1, TTable& t2, EJoinKind joinKind = EJoinKind::Inner, bool hasMoreLeftTuples = false, bool hasMoreRightTuples = false, ui32 fromBucket = 0, ui32 toBucket = NumberOfBuckets);

    // Returns next jointed tuple data. Returs true if there are more tuples
    bool NextJoinedData(TupleData& td1, TupleData& td2, ui64 bucketLimit);

    bool NextJoinedData(TupleData& td1, TupleData& td2) {
        return NextJoinedData(td1, td2, JoinTable1->TableBucketsStats.size());
    }

    // Creates buckets that support spilling.
    void InitializeBucketSpillers(ISpiller::TPtr spiller);

    // Calculates approximate size of a bucket. Used for spilling to determine the largest bucket.
    ui64 GetSizeOfBucket(ui64 bucket) const;

    // This functions wind the largest bucket and spills it to the disk.
    bool TryToReduceMemoryAndWait();

    // Update state of spilling. Must be called during each DoCalculate.
    void UpdateSpilling();

    // Flushes all the spillers.
    void FinalizeSpilling();

    // Checks if spilling has any running save operation
    bool IsSpillingFinished() const;

    // Checks if spilling ready for requesting buckets for restoration.
    bool IsSpillingAcceptingDataRequests() const;

    // Checks is spilling has any running load operation
    bool IsRestoringSpilledBuckets() const;

    // Checks if bucket fully loaded to memory and may be joined.
    bool IsBucketInMemory(ui32 bucket) const;

    // Checks if extraction of bucket is required
    bool IsSpilledBucketWaitingForExtraction(ui32 bucket) const;

    // Starts loading spilled bucket to memory.
    void StartLoadingBucket(ui32 bucket);

    // Prepares bucket for joining after spilling and restoring back.
    void PrepareBucket(ui64 bucket);

    // Clears all the data related to a single bucket
    void ClearBucket(ui64 bucket);

    // Forces bucket to release the space used for underlying containers.
    void ShrinkBucket(ui64 bucket);

    // Clears table content
    void Clear();

    // Creates new table with key columns and data columns
    TTable(ui64 numberOfKeyIntColumns = 0, ui64 numberOfKeyStringColumns = 0,
            ui64 numberOfDataIntColumns = 0, ui64 numberOfDataStringColumns = 0,
            ui64 numberOfKeyIColumns = 0, ui64 numberOfDataIColumns = 0, 
            ui64 nullsBitmapSize = 1, TColTypeInterface * colInterfaces = nullptr, bool isAny = false);

    enum class EAddTupleResult { Added, Unmatched, AnyMatch };
    // Adds new tuple to the table.  intColumns, stringColumns - data of columns,
    // stringsSizes - sizes of strings columns.  Indexes of null-value columns
    // in the form of bit array should be first values of intColumns.
    EAddTupleResult AddTuple(ui64* intColumns, char** stringColumns, ui32* stringsSizes, NYql::NUdf::TUnboxedValue * iColumns = nullptr, const TTable &other = {});
    
    ~TTable();

    ui64 InitHashTableCount_ = 0;

    ui64 HashLookups_ = 0; // hash lookups
    ui64 HashO1Iterations_ = 0; // hash chain
    ui64 HashSlotIterations_ = 0; // O(SlotSize) operations

    ui64 JoinTable1Total_ = 0;
    ui64 JoinTable2Total_ = 0;
    ui64 AnyFiltered_ = 0;

    ui64 BloomLookups_ = 0;
    ui64 BloomHits_ = 0;
    ui64 BloomFalsePositives_ = 0;
};





}
}
}
