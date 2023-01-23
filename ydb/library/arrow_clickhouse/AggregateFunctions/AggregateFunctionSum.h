// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <cstring>
#include <type_traits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionWrapper.h>

namespace CH
{

/// Uses addOverflow method (if available) to avoid UB for sumWithOverflow()
///
/// Since NO_SANITIZE_UNDEFINED works only for the function itself, without
/// callers, and in case of non-POD type (i.e. Decimal) you have overwritten
/// operator+=(), which will have UB.
template <typename T>
struct AggregateFunctionSumAddOverflowImpl
{
    static void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T & lhs, const T & rhs)
    {
        lhs += rhs;
    }
};

template <typename T>
struct AggregateFunctionSumData
{
    using Impl = AggregateFunctionSumAddOverflowImpl<T>;
    T sum{};
    bool has_value = false;

    bool has() const
    {
        return has_value;
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(T value)
    {
        has_value = true;
        Impl::add(sum, value);
    }

    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE addManyImpl(const Value * __restrict ptr, size_t start, size_t end) /// NOLINT
    {
        ptr += start;
        size_t count = end - start;
        const auto * end_ptr = ptr + count;

        if constexpr (std::is_floating_point_v<T>)
        {
            /// Compiler cannot unroll this loop, do it manually.
            /// (at least for floats, most likely due to the lack of -fassociative-math)

            /// Something around the number of SSE registers * the number of elements fit in register.
            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                    Impl::add(partial_sums[i], ptr[i]);
                ptr += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        /// clang cannot vectorize the loop if accumulator is class member instead of local variable.
        T local_sum{};
        while (ptr < end_ptr)
        {
            Impl::add(local_sum, *ptr);
            ++ptr;
        }
        Impl::add(sum, local_sum);
    }

    /// Vectorized version
    template <typename Value>
    void NO_INLINE addMany(const Value * __restrict ptr, size_t start, size_t end)
    {
        addManyImpl(ptr, start, end);
    }
#if 0
    template <typename Value, bool add_if_zero>
    void NO_SANITIZE_UNDEFINED NO_INLINE addManyConditionalInternalImpl(
        const Value * __restrict ptr,
        const uint8_t * __restrict condition_map,
        size_t start,
        size_t end) /// NOLINT
    {
        ptr += start;
        size_t count = end - start;
        const auto * end_ptr = ptr + count;

        if constexpr (std::is_integral_v<T>)
        {
            /// For integers we can vectorize the operation if we replace the null check using a multiplication (by 0 for null, 1 for not null)
            /// https://quick-bench.com/q/MLTnfTvwC2qZFVeWHfOBR3U7a8I
            T local_sum{};
            while (ptr < end_ptr)
            {
                T multiplier = !*condition_map == add_if_zero;
                Impl::add(local_sum, *ptr * multiplier);
                ++ptr;
                ++condition_map;
            }
            Impl::add(sum, local_sum);
            return;
        }

        if constexpr (std::is_floating_point_v<T>)
        {
            /// For floating point we use a similar trick as above, except that now we  reinterpret the floating point number as an unsigned
            /// integer of the same size and use a mask instead (0 to discard, 0xFF..FF to keep)
            static_assert(sizeof(Value) == 4 || sizeof(Value) == 8);
            using equivalent_integer = typename std::conditional_t<sizeof(Value) == 4, UInt32, UInt64>;

            constexpr size_t unroll_count = 128 / sizeof(T);
            T partial_sums[unroll_count]{};

            const auto * unrolled_end = ptr + (count / unroll_count * unroll_count);

            while (ptr < unrolled_end)
            {
                for (size_t i = 0; i < unroll_count; ++i)
                {
                    equivalent_integer value;
                    std::memcpy(&value, &ptr[i], sizeof(Value));
                    value &= (!condition_map[i] != add_if_zero) - 1;
                    Value d;
                    std::memcpy(&d, &value, sizeof(Value));
                    Impl::add(partial_sums[i], d);
                }
                ptr += unroll_count;
                condition_map += unroll_count;
            }

            for (size_t i = 0; i < unroll_count; ++i)
                Impl::add(sum, partial_sums[i]);
        }

        T local_sum{};
        while (ptr < end_ptr)
        {
            if (!*condition_map == add_if_zero)
                Impl::add(local_sum, *ptr);
            ++ptr;
            ++condition_map;
        }
        Impl::add(sum, local_sum);
    }

    /// Vectorized version
    template <typename Value, bool add_if_zero>
    void NO_INLINE addManyConditionalInternal(const Value * __restrict ptr, const uint8_t * __restrict condition_map, size_t start, size_t end)
    {
        addManyConditionalInternalImpl<Value, add_if_zero>(ptr, condition_map, start, end);
    }

    template <typename Value>
    void ALWAYS_INLINE addManyNotNull(const Value * __restrict ptr, const uint8_t * __restrict null_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, true>(ptr, null_map, start, end);
    }

    template <typename Value>
    void ALWAYS_INLINE addManyConditional(const Value * __restrict ptr, const uint8_t * __restrict cond_map, size_t start, size_t end)
    {
        return addManyConditionalInternal<Value, false>(ptr, cond_map, start, end);
    }
#else
    template <typename Value>
    void NO_SANITIZE_UNDEFINED NO_INLINE addManyConditionalImpl(
        const Value * __restrict ptr,
        const uint8_t * __restrict condition_map,
        size_t start,
        size_t end) /// NOLINT
    {
        // TODO: optimize

        const auto * end_ptr = ptr + end;
        ptr += start;

        while (ptr < end_ptr)
        {
            if (arrow::BitUtil::GetBit(condition_map, start))
                Impl::add(sum, *ptr);
            ++ptr;
            ++start;
        }
    }

    template <typename Value>
    void ALWAYS_INLINE addManyConditional(const Value * __restrict ptr, const uint8_t * __restrict cond_map, size_t start, size_t end)
    {
        return addManyConditionalImpl<Value>(ptr, cond_map, start, end);
    }
#endif
    void NO_SANITIZE_UNDEFINED merge(const AggregateFunctionSumData & rhs)
    {
        Impl::add(sum, rhs.sum);
    }

    T get() const
    {
        return sum;
    }
};


/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>
{
public:
    using ColumnType = arrow::NumericArray<T>;
    using MutableColumnType = arrow::NumericBuilder<TResult>;

    explicit AggregateFunctionSum(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(argument_types_, {})
    {}

    AggregateFunctionSum(const IDataType & /*data_type*/, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(argument_types_, {})
    {}

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<TResult>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnType &>(*columns[0]);
        this->data(place).add(column.Value(row_num));
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *) const override
    {
        const auto & column = assert_cast<const ColumnType &>(*columns[0]);
        if (auto * flags = column.null_bitmap_data())
        {
            auto * condition_map = flags + column.offset();
            this->data(place).addManyConditional(column.raw_values(), condition_map, row_begin, row_end);
        }
        else
        {
            this->data(place).addMany(column.raw_values(), row_begin, row_end);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena *) const override
    {
        if (this->data(place).has())
            assert_cast<MutableColumnType &>(to).Append(this->data(place).get()).ok();
        else
            assert_cast<MutableColumnType &>(to).AppendNull().ok();
    }
};

class WrappedSum final : public ArrowAggregateFunctionWrapper
{
public:
    template <typename T, typename ResultT>
    using AggregateFunctionSumWithOverflow =
        AggregateFunctionSum<T, ResultT, AggregateFunctionSumData<typename arrow::TypeTraits<ResultT>::CType>>;

    WrappedSum(std::string name)
        : ArrowAggregateFunctionWrapper(std::move(name))
    {}

    AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const override
    {
        return createWithOverflow<AggregateFunctionSumWithOverflow>(argument_types);
    }

    template <template <typename, typename> typename AggFunc>
    std::shared_ptr<IAggregateFunction> createWithOverflow(const DataTypes & argument_types) const
    {
        if (argument_types.size() != 1)
            return {};

        const DataTypePtr & type = argument_types[0];

        switch (type->id()) {
            case arrow::Type::INT8:
                return std::make_shared<AggFunc<arrow::Int8Type, arrow::Int64Type>>(argument_types);
            case arrow::Type::INT16:
                return std::make_shared<AggFunc<arrow::Int16Type, arrow::Int64Type>>(argument_types);
            case arrow::Type::INT32:
                return std::make_shared<AggFunc<arrow::Int32Type, arrow::Int64Type>>(argument_types);
            case arrow::Type::INT64:
                return std::make_shared<AggFunc<arrow::Int64Type, arrow::Int64Type>>(argument_types);
            case arrow::Type::UINT8:
                return std::make_shared<AggFunc<arrow::UInt8Type, arrow::UInt64Type>>(argument_types);
            case arrow::Type::UINT16:
                return std::make_shared<AggFunc<arrow::UInt16Type, arrow::UInt64Type>>(argument_types);
            case arrow::Type::UINT32:
                return std::make_shared<AggFunc<arrow::UInt32Type, arrow::UInt64Type>>(argument_types);
            case arrow::Type::UINT64:
                return std::make_shared<AggFunc<arrow::UInt64Type, arrow::UInt64Type>>(argument_types);
            case arrow::Type::FLOAT:
                return std::make_shared<AggFunc<arrow::FloatType, arrow::DoubleType>>(argument_types);
            case arrow::Type::DOUBLE:
                return std::make_shared<AggFunc<arrow::DoubleType, arrow::DoubleType>>(argument_types);
            case arrow::Type::DURATION:
                return std::make_shared<AggFunc<arrow::DurationType, arrow::DurationType>>(argument_types);
            default:
                break;
        }

        return {};
    }
};

}
