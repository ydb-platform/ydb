// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <memory>
#include <common/StringRef.h>

#include "AggregationCommon.h"
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Columns/ColumnsHashing.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataStreams/IBlockStream_fwd.h>

namespace CH
{

/** Different data structures that can be used for aggregation
  * For efficiency, the aggregation data itself is put into the pool.
  * Data and pool ownership (states of aggregate functions)
  *  is acquired later - in `convertToBlocks` function, by the ColumnAggregateFunction object.
  *
  * Most data structures exist in two versions: normal and two-level (TwoLevel).
  * A two-level hash table works a little slower with a small number of different keys,
  *  but with a large number of different keys scales better, because it allows
  *  parallelize some operations (merging, post-processing) in a natural way.
  *
  * To ensure efficient work over a wide range of conditions,
  *  first single-level hash tables are used,
  *  and when the number of different keys is large enough,
  *  they are converted to two-level ones.
  *
  * PS. There are many different approaches to the effective implementation of parallel and distributed aggregation,
  *  best suited for different cases, and this approach is just one of them, chosen for a combination of reasons.
  */

using AggregateDataPtr = char *;
using AggregatedDataWithoutKey = AggregateDataPtr;

using AggregatedDataWithUInt8Key = FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;

using AggregatedDataWithUInt32Key = HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;

using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithKeys64 = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;


/** Variants with better hash function, using more than 32 bits for hash.
  * Using for merging phase of external aggregation, where number of keys may be far greater than 4 billion,
  *  but we keep in memory and merge only sub-partition of them simultaneously.
  * TODO We need to switch for better hash function not only for external aggregation,
  *  but also for huge aggregation results on machines with terabytes of RAM.
  */

using AggregatedDataWithUInt64KeyHash64 = HashMap<UInt64, AggregateDataPtr, DefaultHash<UInt64>>;
using AggregatedDataWithStringKeyHash64 = HashMapWithSavedHash<StringRef, AggregateDataPtr, StringRefHash>;
using AggregatedDataWithKeys128Hash64 = HashMap<UInt128, AggregateDataPtr, UInt128Hash>;
using AggregatedDataWithKeys256Hash64 = HashMap<UInt256, AggregateDataPtr, UInt256Hash>;

template <typename Base>
struct AggregationDataWithNullKey : public Base
{
    using Base::Base;

    bool & hasNullKeyData() { return has_null_key_data; }
    AggregateDataPtr & getNullKeyData() { return null_key_data; }
    bool hasNullKeyData() const { return has_null_key_data; }
    const AggregateDataPtr & getNullKeyData() const { return null_key_data; }
    size_t size() const { return Base::size() + (has_null_key_data ? 1 : 0); }
    bool empty() const { return Base::empty() && !has_null_key_data; }
    void clear()
    {
        Base::clear();
        has_null_key_data = false;
    }
    void clearAndShrink()
    {
        Base::clearAndShrink();
        has_null_key_data = false;
    }

private:
    bool has_null_key_data = false;
    AggregateDataPtr null_key_data = nullptr;
};

template <typename ... Types>
using HashTableWithNullKey = AggregationDataWithNullKey<HashMapTable<Types ...>>;
template <typename ... Types>
using StringHashTableWithNullKey = AggregationDataWithNullKey<StringHashMap<Types ...>>;

using AggregatedDataWithNullableUInt8Key = AggregationDataWithNullKey<AggregatedDataWithUInt8Key>;
using AggregatedDataWithNullableUInt16Key = AggregationDataWithNullKey<AggregatedDataWithUInt16Key>;

using AggregatedDataWithNullableUInt64Key = AggregationDataWithNullKey<AggregatedDataWithUInt64Key>;
using AggregatedDataWithNullableStringKey = AggregationDataWithNullKey<AggregatedDataWithStringKey>;


/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData,
        bool consecutive_keys_optimization = true>
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodOneNumber() = default;

    template <typename Other>
    AggregationMethodOneNumber(const Other & other) : data(other.data) {}

    /// To use one `Method` in different threads, use different `State`.
    using FixedFieldType = std::conditional_t<std::is_same_v<FieldType, char8_t>, uint8_t, FieldType>;
    using State = ColumnsHashing::HashMethodOneNumber<typename Data::value_type,
        Mapped, FixedFieldType, consecutive_keys_optimization>;

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    // Insert the key from the hash table into columns.
    template <typename ColPtr>
    static void insertKeyIntoColumns(const Key & key, const std::vector<ColPtr> & key_columns, const Sizes & /*key_sizes*/, const std::vector<arrow::Type::type>& /*typeIds*/)
    {
        insertSameSizeNumber(*key_columns[0], key);
    }
};


/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodString() = default;

    template <typename Other>
    AggregationMethodString(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename ColPtr>
    static void insertKeyIntoColumns(const StringRef & key, const std::vector<ColPtr> & key_columns, const Sizes &, const std::vector<arrow::Type::type>& /*typeIds*/)
    {
        insertString(*key_columns[0], key);
    }
};


/// Same as above but without cache
template <typename TData>
struct AggregationMethodStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodStringNoCache() = default;

    template <typename Other>
    AggregationMethodStringNoCache(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename ColPtr>
    static void insertKeyIntoColumns(const StringRef & key, const std::vector<ColPtr> & key_columns, const Sizes &, const std::vector<arrow::Type::type>& /*typeIds*/)
    {
        insertString(*key_columns[0], key);
    }
};


/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedString() = default;

    template <typename Other>
    AggregationMethodFixedString(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename ColPtr>
    static void insertKeyIntoColumns(const StringRef & key, const std::vector<ColPtr> & key_columns, const Sizes &, const std::vector<arrow::Type::type>& /*typeIds*/)
    {
        insertFixedString(*key_columns[0], key);
    }
};

/// Same as above but without cache
template <typename TData>
struct AggregationMethodFixedStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedStringNoCache() = default;

    template <typename Other>
    AggregationMethodFixedStringNoCache(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, true, false>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename ColPtr>
    static void insertKeyIntoColumns(const StringRef & key, const std::vector<ColPtr> & key_columns, const Sizes &, const std::vector<arrow::Type::type>& /*typeIds*/)
    {
        insertFixedString(*key_columns[0], key);
    }
};



/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false, bool use_cache = true>
struct AggregationMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;

    AggregationMethodKeysFixed() = default;

    template <typename Other>
    AggregationMethodKeysFixed(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodKeysFixed<
        typename Data::value_type,
        Key,
        Mapped,
        has_nullable_keys,
        false,
        use_cache>;
#if 0
    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        return State::shuffleKeyColumns(key_columns, key_sizes);
    }
#endif
    template <typename ColPtr>
    static void insertKeyIntoColumns(const Key & key, const std::vector<ColPtr> & key_columns, const Sizes & key_sizes, const std::vector<arrow::Type::type>& typeIds)
    {
        size_t keys_count = key_columns.size();

        static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        const char * key_data = reinterpret_cast<const char *>(&key) + bitmap_size;

        for (size_t i = 0; i < keys_count; ++i)
        {
            auto & observed_column = *key_columns[i];

            if constexpr (has_nullable_keys)
            {
                const char * null_bitmap = reinterpret_cast<const char *>(&key);
                size_t bucket = i / 8;
                size_t offset = i % 8;
                bool is_null = (null_bitmap[bucket] >> offset) & 1;

                if (is_null)
                {
                    observed_column.AppendNull().ok();
                    continue;
                }
            }

            insertData(observed_column, StringRef(key_data, key_sizes[i]), typeIds[i]);
            key_data += key_sizes[i];
        }
    }
};


/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodSerialized() = default;

    template <typename Other>
    AggregationMethodSerialized(const Other & other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename ColPtr>
    static void insertKeyIntoColumns(const StringRef & key, const std::vector<ColPtr> & key_columns, const Sizes &, const std::vector<arrow::Type::type>& typeIds)
    {
        const auto * pos = key.data;
        ui32 idx = 0;
        for (auto& column : key_columns) {
            pos = deserializeAndInsertFromArena(*column, pos, typeIds[idx]);
            ++idx;
        }
    }
};


class Aggregator;

using ColumnsHashing::HashMethodContext;
using ColumnsHashing::HashMethodContextPtr;

struct AggregatedDataVariants //: private boost::noncopyable
{
    /** Working with states of aggregate functions in the pool is arranged in the following (inconvenient) way:
      * - when aggregating, states are created in the pool using IAggregateFunction::create (inside - `placement new` of arbitrary structure);
      * - they must then be destroyed using IAggregateFunction::destroy (inside - calling the destructor of arbitrary structure);
      * - if aggregation is complete, then, in the Aggregator::convertToBlocks function, pointers to the states of aggregate functions
      *   are written to ColumnAggregateFunction; ColumnAggregateFunction "acquires ownership" of them, that is - calls `destroy` in its destructor.
      * - if during the aggregation, before call to Aggregator::convertToBlocks, an exception was thrown,
      *   then the states of aggregate functions must still be destroyed,
      *   otherwise, for complex states (eg, AggregateFunctionUniq), there will be memory leaks;
      * - in this case, to destroy states, the destructor calls Aggregator::destroyAggregateStates method,
      *   but only if the variable aggregator (see below) is not nullptr;
      * - that is, until you transfer ownership of the aggregate function states in the ColumnAggregateFunction, set the variable `aggregator`,
      *   so that when an exception occurs, the states are correctly destroyed.
      *
      * PS. This can be corrected by making a pool that knows about which states of aggregate functions and in which order are put in it, and knows how to destroy them.
      * But this can hardly be done simply because it is planned to put variable-length strings into the same pool.
      * In this case, the pool will not be able to know with what offsets objects are stored.
      */
    const Aggregator * aggregator = nullptr;

    size_t keys_size{};  /// Number of keys. NOTE do we need this field?
    Sizes key_sizes;     /// Dimensions of keys, if keys of fixed length

    /// Pools for states of aggregate functions. Ownership will be later transferred to ColumnAggregateFunction.
    Arenas aggregates_pools;
    Arena * aggregates_pool{};    /// The pool that is currently used for allocation.

    /** Specialization for the case when there are no keys, and for keys not fitted into max_rows_to_group_by.
      */
    AggregatedDataWithoutKey without_key = nullptr;

    // Disable consecutive key optimization for Uint8/16, because they use a FixedHashMap
    // and the lookup there is almost free, so we don't need to cache the last lookup result
    std::unique_ptr<AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>>    key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>>  key16;

    std::unique_ptr<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32Key>>         key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>>         key64;
    std::unique_ptr<AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>>        key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>>   key_fixed_string;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt16Key, false, false>>   keys16;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>>                 keys32;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>>                 keys64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128>>                   keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256>>                   keys256;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>>                serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>>   key64_hash64;
    std::unique_ptr<AggregationMethodString<AggregatedDataWithStringKeyHash64>>              key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>>         key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>>             keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>>             keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>>          serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys64, true>>              nullable_keys64;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>>             nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>>             nullable_keys256;

    /// In this and similar macros, the option without_key is not considered.
    #define APPLY_FOR_AGGREGATED_VARIANTS(M) \
        M(key8) \
        M(key16) \
        M(key32) \
        M(key64) \
        M(key_string) \
        M(key_fixed_string) \
        M(keys16) \
        M(keys32) \
        M(keys64) \
        M(keys128) \
        M(keys256) \
        M(serialized) \
        M(key64_hash64) \
        M(key_string_hash64) \
        M(key_fixed_string_hash64) \
        M(keys128_hash64) \
        M(keys256_hash64) \
        M(serialized_hash64) \
        M(nullable_keys64) \
        M(nullable_keys128) \
        M(nullable_keys256) \


    enum class Type
    {
        EMPTY = 0,
        without_key,

    #define M(NAME) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    };
    Type type = Type::EMPTY;

    AggregatedDataVariants()
        : aggregates_pools(1, std::make_shared<Arena>())
        , aggregates_pool(aggregates_pools.back().get())
    {}

    bool empty() const { return type == Type::EMPTY; }
    void invalidate() { type = Type::EMPTY; }

    ~AggregatedDataVariants();

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:       break;
            case Type::without_key: break;

        #define M(NAME) \
            case Type::NAME: NAME = std::make_unique<decltype(NAME)::element_type>(); break;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
        }

        type = type_;
    }

    /// Number of rows (different keys).
    size_t size() const
    {
        switch (type)
        {
            case Type::EMPTY:       return 0;
            case Type::without_key: return 1;

        #define M(NAME) \
            case Type::NAME: return NAME->data.size() + (without_key != nullptr);
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
        }

        __builtin_unreachable();
    }

    /// The size without taking into account the row in which data is written for the calculation of TOTALS.
    size_t sizeWithoutOverflowRow() const
    {
        switch (type)
        {
            case Type::EMPTY:       return 0;
            case Type::without_key: return 1;

            #define M(NAME) \
            case Type::NAME: return NAME->data.size();
            APPLY_FOR_AGGREGATED_VARIANTS(M)
            #undef M
        }

        __builtin_unreachable();
    }

    const char * getMethodName() const
    {
        switch (type)
        {
            case Type::EMPTY:       return "EMPTY";
            case Type::without_key: return "without_key";

        #define M(NAME) \
            case Type::NAME: return #NAME;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
        }

        __builtin_unreachable();
    }

    static HashMethodContextPtr createCache(Type type, const HashMethodContext::Settings & settings)
    {
        switch (type)
        {
            case Type::without_key: return nullptr;

            #define M(NAME) \
            case Type::NAME: \
            { \
                using TPtr ## NAME = decltype(AggregatedDataVariants::NAME); \
                using T ## NAME = typename TPtr ## NAME ::element_type; \
                return T ## NAME ::State::createContext(settings); \
            }

            APPLY_FOR_AGGREGATED_VARIANTS(M)
            #undef M

            default:
                throw Exception("Unknown aggregated data variant.");
        }
    }
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;
using ManyAggregatedDataVariantsPtr = std::shared_ptr<ManyAggregatedDataVariants>;

/** How are "total" values calculated with WITH TOTALS?
  * (For more details, see TotalsHavingTransform.)
  *
  * In the absence of group_by_overflow_mode = 'any', the data is aggregated as usual, but the states of the aggregate functions are not finalized.
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one - this will be TOTALS.
  *
  * If there is group_by_overflow_mode = 'any', the data is aggregated as usual, except for the keys that did not fit in max_rows_to_group_by.
  * For these keys, the data is aggregated into one additional row - see below under the names `overflow_row`, `overflows`...
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one,
  *  also overflow_row is added or not added (depending on the totals_mode setting) also - this will be TOTALS.
  */


/** Aggregates the source of the blocks.
  */
class Aggregator final
{
public:
    struct Params
    {
        /// Data structure of source blocks.
        Header src_header;
        /// Data structure of intermediate blocks before merge.
        Header intermediate_header;

        /// What to count.
        const ColumnNumbers keys;
        const AggregateDescriptions aggregates;
        const size_t keys_size;
        const size_t aggregates_size;

        /// The settings of approximate calculation of GROUP BY.
        const bool overflow_row;    /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
        const size_t max_rows_to_group_by = 0;
        const OverflowMode group_by_overflow_mode = OverflowMode::THROW;

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set = false;

        /// Settings is used to determine cache size. No threads are created.
        size_t max_threads;

        bool has_nullable_key = true;

        Params(const Header & src_header_,
               const Header & intermediate_header_,
               const ColumnNumbers & keys_,
               const AggregateDescriptions & aggregates_,
               bool overflow_row_,
               size_t max_threads_ = 1)
            : src_header(src_header_)
            , intermediate_header(intermediate_header_)
            , keys(keys_)
            , aggregates(aggregates_)
            , keys_size(keys.size())
            , aggregates_size(aggregates.size())
            , overflow_row(overflow_row_)
            , max_threads(max_threads_)
        {}

        Params(bool is_megre, const Header & header_,
            const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_, size_t max_threads_ = 1)
            : Params((is_megre ? Header() : header_), (is_megre ? header_ : Header()), keys_, aggregates_, overflow_row_, max_threads_)
        {}

        static Header getHeader(
            const Header & src_header,
            const Header & intermediate_header,
            const ColumnNumbers & keys,
            const AggregateDescriptions & aggregates,
            bool final);

        Header getHeader(bool final) const
        {
            return getHeader(src_header, intermediate_header, keys, aggregates, final);
        }
    };

    explicit Aggregator(const Params & params_);

    /// Aggregate the source. Get the result in the form of one of the data structures.
    void execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result);

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;

    /// Process one block. Return false if the processing should be aborted (with group_by_overflow_mode = 'break').
    bool executeOnBlock(Columns columns,
        size_t row_begin, size_t row_end,
        AggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool & no_more_keys) const;

    /// Used for aggregate projection.
    bool mergeOnBlock(Block block, AggregatedDataVariants & result, bool & no_more_keys) const;

    /** Convert the aggregation data structure into a block.
      * If overflow_row = true, then aggregates for rows that are not included in max_rows_to_group_by are put in the first block.
      *
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing).
      * If final = true, then columns with ready values are created as aggregate columns.
      */
    BlocksList convertToBlocks(AggregatedDataVariants & data_variants, bool final) const;

    ManyAggregatedDataVariants prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants) const;

    /** Merge the stream of partially aggregated blocks into one data structure.
      * (Pre-aggregate several blocks that represent the result of independent aggregations from remote servers.)
      */
    void mergeStream(const BlockInputStreamPtr & stream, AggregatedDataVariants & result);

    using BucketToBlocks = std::map<Int32, BlocksList>;
    /// Merge partially aggregated blocks separated to buckets into one data structure.
    void mergeBlocks(BucketToBlocks && bucket_to_blocks, AggregatedDataVariants & result);

    /// Merge several partially aggregated blocks into one.
    /// Precondition: for all blocks block.info.is_overflows flag must be the same.
    /// (either all blocks are from overflow data or none blocks are).
    /// The resulting block has the same value of is_overflows flag.
    Block mergeBlocks(BlocksList & blocks, bool final);

    /// Get data structure of the result.
    Header getHeader(bool final) const;

private:
    friend struct AggregatedDataVariants;
    friend class MergingAndConvertingBlockInputStream;

    Params params;

    AggregatedDataVariants::Type method_chosen;
    Sizes key_sizes;

    HashMethodContextPtr aggregation_state_cache;

    AggregateFunctionsPlainPtrs aggregate_functions;

    /** This array serves two purposes.
      *
      * Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
      * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
      */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that{};
        size_t state_offset{};
        const IColumn ** arguments{};
        const IAggregateFunction * batch_that{};
        const IColumn ** batch_arguments{};
    };

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;

    Sizes offsets_of_aggregate_states;    /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

    /** Create states of aggregate functions for one key.
      */
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(AggregatedDataVariants & result) const;


    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    void executeImpl(
        Method & method,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions,
        bool no_more_keys,
        AggregateDataPtr overflow_row) const;

    /// Specialization for a particular value no_more_keys.
    template <bool no_more_keys, typename Method>
    void executeImplBatch(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        AggregateDataPtr overflow_row) const;

    /// For case when there are no keys (all aggregate into one row).
    void executeWithoutKeyImpl(
        AggregatedDataWithoutKey & res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        Arena * arena) const;

    /// Merge NULL key data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataNullKey(
            Table & table_dst,
            Table & table_src,
            Arena * arena) const;

    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    /// Merge data from hash table `src` into `dst`, but only for keys that already exist in dst. In other cases, merge the data into `overflows`.
    template <typename Method, typename Table>
    void mergeDataNoMoreKeysImpl(
        Table & table_dst,
        AggregatedDataWithoutKey & overflows,
        Table & table_src,
        Arena * arena) const;

    /// Same, but ignores the rest of the keys.
    template <typename Method, typename Table>
    void mergeDataOnlyExistingKeysImpl(
        Table & table_dst,
        Table & table_src,
        Arena * arena) const;

    void mergeWithoutKeyDataImpl(
        ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(
        ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method, typename Table>
    void convertToBlockImpl(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(
        Mapped & mapped,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    template <typename Method, typename Table>
    void convertToBlockImplFinal(
        Method & method,
        Table & data,
        const MutableColumns & key_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    template <typename Method, typename Table>
    void convertToBlockImplNotFinal(
        Method & method,
        Table & data,
        const MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns) const;

    template <typename Filler>
    Block prepareBlockAndFill(
        AggregatedDataVariants & data_variants,
        bool final,
        size_t rows,
        Filler && filler) const;

    template <typename Method>
    Block convertOneBucketToBlock(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        size_t bucket) const;

    Block prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows = false) const;
    Block prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const;

    template <bool no_more_keys, typename Method, typename Table>
    void mergeStreamsImplCase(
        Block & block,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(
        Block & block,
        Arena * aggregates_pool,
        Method & method,
        Table & data,
        AggregateDataPtr overflow_row,
        bool no_more_keys) const;

    void mergeWithoutKeyStreamsImpl(
        Block & block,
        AggregatedDataVariants & result) const;

    template <typename Method>
    void mergeBucketImpl(
        ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena, std::atomic<bool> * is_cancelled = nullptr) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method,
        Arena * pool,
        ColumnRawPtrs & key_columns,
        const Block & source,
        std::vector<Block> & destinations) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(
        AggregatedDataVariants & result) const;


    /** Checks constraints on the maximum number of keys for aggregation.
      * If it is exceeded, then, depending on the group_by_overflow_mode, either
      * - throws an exception;
      * - returns false, which means that execution must be aborted;
      * - sets the variable no_more_keys to true.
      */
    bool checkLimits(size_t result_size, bool & no_more_keys) const;

    void prepareAggregateInstructions(
        Columns columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions) const;
};


/** Get the aggregation variant by its type. */
template <typename Method> Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME) \
    template <> inline decltype(AggregatedDataVariants::NAME)::element_type & getDataVariant<decltype(AggregatedDataVariants::NAME)::element_type>(AggregatedDataVariants & variants) { return *variants.NAME; }

APPLY_FOR_AGGREGATED_VARIANTS(M)

#undef M

}
