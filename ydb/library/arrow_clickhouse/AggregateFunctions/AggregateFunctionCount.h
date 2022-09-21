// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionWrapper.h>
#include <Columns/ColumnsCommon.h>

#include <array>

namespace CH
{


struct AggregateFunctionCountData
{
    UInt64 count = 0;
};


/// Simply count number of calls.
class AggregateFunctionCount final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    AggregateFunctionCount(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper(argument_types_, {})
    {}

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override
    {
        ++data(place).count;
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & filter_column = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]);
            const auto & flags = filter_column.raw_values();
            data(place).count += countBytesInFilter(flags, row_begin, row_end);
        }
        else
        {
            data(place).count += row_end - row_begin;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena *) const override
    {
        assert_cast<MutableColumnUInt64 &>(to).Append(data(place).count).ok();
    }

    /// Reset the state to specified value. This function is not the part of common interface.
    void set(AggregateDataPtr __restrict place, UInt64 new_count) const
    {
        data(place).count = new_count;
    }
};

class WrappedCount final : public ArrowAggregateFunctionWrapper
{
public:
    WrappedCount(std::string name)
        : ArrowAggregateFunctionWrapper(std::move(name))
    {}

    AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const override
    {
        return std::make_shared<AggregateFunctionCount>(argument_types);
    }
};

}
