// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <cstddef>
#include <memory>
#include <vector>
#include <type_traits>

namespace CH
{

class Arena;
class ReadBuffer;
class WriteBuffer;

using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;
struct AggregateFunctionProperties;

/** Aggregate functions interface.
  * Instances of classes with this interface do not contain the data itself for aggregation,
  *  but contain only metadata (description) of the aggregate function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class IAggregateFunction : public std::enable_shared_from_this<IAggregateFunction>
{
public:
    IAggregateFunction(const DataTypes & argument_types_, const Array & parameters_)
        : argument_types(argument_types_), parameters(parameters_) {}

    /// Get the result type.
    virtual DataTypePtr getReturnType() const = 0;
#if 0
    /// Get the data type of internal state. By default it is AggregateFunction(name(params), argument_types...).
    virtual DataTypePtr getStateType() const;
#endif

    virtual ~IAggregateFunction() = default;

    /** Data manipulating functions. */

    /** Create empty data for aggregation with `placement new` at the specified location.
      * You will have to destroy them using the `destroy` method.
      */
    virtual void create(AggregateDataPtr __restrict place) const = 0;

    /// Delete data for aggregation.
    virtual void destroy(AggregateDataPtr __restrict place) const noexcept = 0;

    /// It is not necessary to delete data.
    virtual bool hasTrivialDestructor() const = 0;

    /// Get `sizeof` of structure with data.
    virtual size_t sizeOfData() const = 0;

    /// How the data structure should be aligned.
    virtual size_t alignOfData() const = 0;

    /** Adds a value into aggregation data on which place points to.
     *  columns points to columns containing arguments of aggregation function.
     *  row_num is number of row which should be added.
     *  Additional parameter arena should be used instead of standard memory allocator if the addition requires memory allocation.
     */
    virtual void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const = 0;

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const = 0;
#if 0
    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const = 0;
#endif
    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocatesMemoryInArena() const = 0;

    /// Inserts results into a column. This method might modify the state (e.g.
    /// sort an array), so must be called once, from single thread. The state
    /// must remain valid though, and the subsequent calls to add/merge/
    /// insertResultInto must work correctly. This kind of call sequence occurs
    /// in `runningAccumulate`, or when calculating an aggregate function as a
    /// window function.
    virtual void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena * arena) const = 0;

    /** Returns true for aggregate functions of type -State
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
      * Also returns true when the final value of this aggregate function contains State of other aggregate function inside.
      */
    virtual bool isState() const { return false; }

    /** The inner loop that uses the function pointer is better than using the virtual function.
      * The reason is that in the case of virtual functions GCC 5.1.2 generates code,
      *  which, at each iteration of the loop, reloads the function address (the offset value in the virtual function table) from memory to the register.
      * This gives a performance drop on simple queries around 12%.
      * After the appearance of better compilers, the code can be removed.
      */
    using AddFunc = void (*)(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t, Arena *);
    virtual AddFunc getAddressOfAddFunction() const = 0;

    /** Contains a loop with calls to "add" function. You can collect arguments into array "places"
      *  and do a single call to "addBatch" for devirtualization and inlining.
      */
    virtual void addBatch( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const = 0;

    virtual void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const = 0;

    /** The same for single place.
      */
    virtual void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena) const = 0;

    /** The case when the aggregation key is UInt8
      * and pointers to aggregation states are stored in AggregateDataPtr[256] lookup table.
      */
    virtual void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const uint8_t * key,
        const IColumn ** columns,
        Arena * arena) const = 0;

    /** Insert result of aggregate function into result column with batch size.
      * If destroy_place_after_insert is true. Then implementation of this method
      * must destroy aggregate place if insert state into result column was successful.
      * All places that were not inserted must be destroyed if there was exception during insert into result column.
      */
    virtual void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        MutableColumn & to,
        Arena * arena,
        bool destroy_place_after_insert) const = 0;

    /** Destroy batch of aggregate places.
      */
    virtual void destroyBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset) const noexcept = 0;

    const DataTypes & getArgumentTypes() const { return argument_types; }
    const Array & getParameters() const { return parameters; }

protected:
    DataTypes argument_types;
    Array parameters;
};


/// Implement method to obtain an address of 'add' function.
template <typename Derived, bool skip_nulls = true>
class IAggregateFunctionHelper : public IAggregateFunction
{
private:
    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const Derived &>(*that).add(place, columns, row_num, arena);
    }

public:
    IAggregateFunctionHelper(const DataTypes & argument_types_, const Array & parameters_)
        : IAggregateFunction(argument_types_, parameters_) {}

    AddFunc getAddressOfAddFunction() const override { return &addFree; }

    void addBatch( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const override
    {
        if (skip_nulls && columns && columns[0]->null_bitmap_data())
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (columns[0]->IsValid(i) && places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
                if (places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, i, arena);
        }
    }

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        for (size_t i = row_begin; i < row_end; ++i)
            if (places[i])
                static_cast<const Derived *>(this)->merge(places[i] + place_offset, rhs[i], arena);
    }

    void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena) const override
    {
        if (skip_nulls && columns && columns[0]->null_bitmap_data())
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (columns[0]->IsValid(i))
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
                static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
    }

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const uint8_t * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        static constexpr size_t UNROLL_COUNT = 8;

        size_t i = row_begin;

        size_t size_unrolled = (row_end - row_begin) / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < size_unrolled; i += UNROLL_COUNT)
        {
            AggregateDataPtr places[UNROLL_COUNT];
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                AggregateDataPtr & place = map[key[i + j]];
                if (unlikely(!place))
                    init(place);

                places[j] = place;
            }

            for (size_t j = 0; j < UNROLL_COUNT; ++j)
                static_cast<const Derived *>(this)->add(places[j] + place_offset, columns, i + j, arena);
        }

        for (; i < row_end; ++i)
        {
            AggregateDataPtr & place = map[key[i]];
            if (unlikely(!place))
                init(place);
            static_cast<const Derived *>(this)->add(place + place_offset, columns, i, arena);
        }
    }

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        MutableColumn & to,
        Arena * arena,
        bool destroy_place_after_insert) const override
    {
        size_t batch_index = row_begin;

        try
        {
            for (; batch_index < row_end; ++batch_index)
            {
                static_cast<const Derived *>(this)->insertResultInto(places[batch_index] + place_offset, to, arena);

                if (destroy_place_after_insert)
                    static_cast<const Derived *>(this)->destroy(places[batch_index] + place_offset);
            }
        }
        catch (...)
        {
            for (size_t destroy_index = batch_index; destroy_index < row_end; ++destroy_index)
                static_cast<const Derived *>(this)->destroy(places[destroy_index] + place_offset);

            throw;
        }
    }

    void destroyBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset) const noexcept override
    {
        for (size_t i = row_begin; i < row_end; ++i)
        {
            static_cast<const Derived *>(this)->destroy(places[i] + place_offset);
        }
    }
};


/// Implements several methods for manipulation with data. T - type of structure with data for aggregation.
template <typename T, typename Derived, bool skip_nulls = true>
class IAggregateFunctionDataHelper : public IAggregateFunctionHelper<Derived, skip_nulls>
{
private:
    using Base = IAggregateFunctionHelper<Derived, skip_nulls>;

protected:
    using Data = T;

    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

public:
    // Derived class can `override` this to flag that DateTime64 is not supported.
    static constexpr bool DateTime64Supported = true;

    IAggregateFunctionDataHelper(const DataTypes & argument_types_, const Array & parameters_)
        : Base(argument_types_, parameters_) {}

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) Data;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const uint8_t * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const Derived & func = *static_cast<const Derived *>(this);

        /// If the function is complex or too large, use more generic algorithm.

        if (func.allocatesMemoryInArena() || sizeof(Data) > 16 || func.sizeOfData() != sizeof(Data))
        {
            Base::addBatchLookupTable8(row_begin, row_end, map, place_offset, init, key, columns, arena);
            return;
        }

        /// Will use UNROLL_COUNT number of lookup tables.

        static constexpr size_t UNROLL_COUNT = 4;

        std::unique_ptr<Data[]> places{new Data[256 * UNROLL_COUNT]};
        bool has_data[256 * UNROLL_COUNT]{}; /// Separate flags array to avoid heavy initialization.

        size_t i = row_begin;

        /// Aggregate data into different lookup tables.

        size_t size_unrolled = (row_end - row_begin) / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < size_unrolled; i += UNROLL_COUNT)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + key[i + j];
                if (unlikely(!has_data[idx]))
                {
                    new (&places[idx]) Data;
                    has_data[idx] = true;
                }
                func.add(reinterpret_cast<char *>(&places[idx]), columns, i + j, nullptr);
            }
        }

        /// Merge data from every lookup table to the final destination.

        for (size_t k = 0; k < 256; ++k)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + k;
                if (has_data[idx])
                {
                    AggregateDataPtr & place = map[k];
                    if (unlikely(!place))
                        init(place);

                    func.merge(place + place_offset, reinterpret_cast<const char *>(&places[idx]), nullptr);
                }
            }
        }

        /// Process tails and add directly to the final destination.

        for (; i < row_end; ++i)
        {
            size_t k = key[i];
            AggregateDataPtr & place = map[k];
            if (unlikely(!place))
                init(place);

            func.add(place + place_offset, columns, i, nullptr);
        }
    }
};


/// Properties of aggregate function that are independent of argument types and parameters.
struct AggregateFunctionProperties
{
    /** When the function is wrapped with Null combinator,
      * should we return Nullable type with NULL when no values were aggregated
      * or we should return non-Nullable type with default value (example: count, countDistinct).
      */
    bool returns_default_when_only_null = false;

    /** Result varies depending on the data order (example: groupArray).
      * Some may also name this property as "non-commutative".
      */
    bool is_order_dependent = false;
};

enum class AggFunctionId {
    AGG_UNSPECIFIED = 0,
    AGG_ANY = 1,
    AGG_COUNT = 2,
    AGG_MIN = 3,
    AGG_MAX = 4,
    AGG_SUM = 5,
    AGG_AVG = 6,
    //AGG_VAR = 7,
    //AGG_COVAR = 8,
    //AGG_STDDEV = 9,
    //AGG_CORR = 10,
    //AGG_ARG_MIN = 11,
    //AGG_ARG_MAX = 12,
    //AGG_COUNT_DISTINCT = 13,
    //AGG_QUANTILES = 14,
    //AGG_TOP_COUNT = 15,
    //AGG_TOP_SUM = 16,
    AGG_NUM_ROWS = 17,
};

struct GroupByOptions : public arrow::compute::ScalarAggregateOptions {
    // We have to return aggregates + aggregate keys in result.
    // We use pair {AGG_UNSPECIFIED, result_column} to specify a key.
    // Then we could place aggregates and keys in one vector to set their order in result.
    struct Assign {
        AggFunctionId function = AggFunctionId::AGG_UNSPECIFIED;
        std::string result_column;
        std::vector<std::string> arguments;
    };

    std::shared_ptr<arrow::Schema> schema; // types and names of input arguments
    std::vector<Assign> assigns; // aggregates and keys in needed result order
    bool has_nullable_key = true;
};

AggregateFunctionPtr GetAggregateFunction(AggFunctionId, const DataTypes & argument_types);

}
