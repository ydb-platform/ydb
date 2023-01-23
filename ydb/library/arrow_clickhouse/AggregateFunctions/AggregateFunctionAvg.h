// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <type_traits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionWrapper.h>
#include <AggregateFunctions/AggregateFunctionSum.h>

namespace CH
{

/**
 * Helper class to encapsulate values conversion for avg and avgWeighted.
 */
template <typename Numerator, typename Denominator>
struct AvgFraction
{
    Numerator numerator{0};
    Denominator denominator{0};

    double divide() const
    {
        return static_cast<double>(numerator) / denominator;
    }
};


/**
 * @tparam Derived When deriving from this class, use the child class name as in CRTP, e.g.
 *         class Self : Agg<char, bool, bool, Self>.
 */
template <typename TNumerator, typename TDenominator, typename Derived>
class AggregateFunctionAvgBase : public
        IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>
{
public:
    using Base = IAggregateFunctionDataHelper<AvgFraction<TNumerator, TDenominator>, Derived>;
    using Numerator = TNumerator;
    using Denominator = TDenominator;
    using Fraction = AvgFraction<Numerator, Denominator>;

    explicit AggregateFunctionAvgBase(const DataTypes & argument_types_,
        UInt32 num_scale_ = 0, UInt32 denom_scale_ = 0)
        : Base(argument_types_, {}), num_scale(num_scale_), denom_scale(denom_scale_) {}

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<arrow::DoubleType>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }
#if 0
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            writeVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<Denominator>)
            readVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            readBinary(this->data(place).denominator, buf);
    }
#endif
    void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena *) const override
    {
        assert_cast<MutableColumnFloat64 &>(to).Append(this->data(place).divide()).ok();
    }

private:
    UInt32 num_scale;
    UInt32 denom_scale;
};

template <typename T>
using AvgFieldType = std::conditional_t<std::is_floating_point_v<T>, T, UInt64>;

template <typename T>
class AggregateFunctionAvg : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>
{
public:
    using Base = AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionAvg<T>>;
    using Base::Base;

    using Numerator = typename Base::Numerator;
    using Denominator = typename Base::Denominator;
    using Fraction = typename Base::Fraction;

    using ColumnType = arrow::NumericArray<T>;
    using MutableColumnType = arrow::NumericBuilder<T>;

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        increment(place, static_cast<const ColumnType &>(*columns[0]).Value(row_num));
        ++this->data(place).denominator;
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *) const final
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColumnType &>(*columns[0]);
        if (auto * flags = column.null_bitmap_data())
        {
            auto * condition_map = flags + column.offset();
            auto length = row_end - row_begin;

            sum_data.addManyConditional(column.raw_values(), condition_map, row_begin, row_end);
            this->data(place).denominator += arrow::internal::CountSetBits(condition_map, row_begin, length);
        }
        else
        {
            sum_data.addMany(column.raw_values(), row_begin, row_end);
            this->data(place).denominator += (row_end - row_begin);
        }
        increment(place, sum_data.sum);
    }

private:
    void increment(AggregateDataPtr __restrict place, Numerator inc) const
    {
        this->data(place).numerator += inc;
    }
};

class WrappedAvg final : public ArrowAggregateFunctionWrapper
{
public:
    WrappedAvg(std::string name)
        : ArrowAggregateFunctionWrapper(std::move(name))
    {}

    AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const override
    {
        return createWithSameType<AggregateFunctionAvg>(argument_types);
    }

    template <template <typename> typename AggFunc>
    std::shared_ptr<IAggregateFunction> createWithSameType(const DataTypes & argument_types) const
    {
        if (argument_types.size() != 1)
            return {};

        const DataTypePtr & type = argument_types[0];

        switch (type->id()) {
            case arrow::Type::INT8:
                return std::make_shared<AggFunc<arrow::Int8Type>>(argument_types);
            case arrow::Type::INT16:
                return std::make_shared<AggFunc<arrow::Int16Type>>(argument_types);
            case arrow::Type::INT32:
                return std::make_shared<AggFunc<arrow::Int32Type>>(argument_types);
            case arrow::Type::INT64:
                return std::make_shared<AggFunc<arrow::Int64Type>>(argument_types);
            case arrow::Type::UINT8:
                return std::make_shared<AggFunc<arrow::UInt8Type>>(argument_types);
            case arrow::Type::UINT16:
                return std::make_shared<AggFunc<arrow::UInt16Type>>(argument_types);
            case arrow::Type::UINT32:
                return std::make_shared<AggFunc<arrow::UInt32Type>>(argument_types);
            case arrow::Type::UINT64:
                return std::make_shared<AggFunc<arrow::UInt64Type>>(argument_types);
            case arrow::Type::FLOAT:
                return std::make_shared<AggFunc<arrow::FloatType>>(argument_types);
            case arrow::Type::DOUBLE:
                return std::make_shared<AggFunc<arrow::DoubleType>>(argument_types);
            case arrow::Type::DURATION:
                return std::make_shared<AggFunc<arrow::DurationType>>(argument_types);
            default:
                break;
        }

        return {};
    }
};

}
