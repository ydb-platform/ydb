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


struct AggregateFunctionNumRowsData
{
    UInt64 count = 0;
};


/// Count rows.
class AggregateFunctionNumRows final
    : public IAggregateFunctionDataHelper<AggregateFunctionNumRowsData, AggregateFunctionNumRows, /*skip_nulls=*/false>
{
public:
    AggregateFunctionNumRows(const DataTypes & argument_types_)
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
        const IColumn ** /*columns*/,
        Arena *) const override
    {
        data(place).count += row_end - row_begin;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void insertResultInto(AggregateDataPtr __restrict place, MutableColumn & to, Arena *) const override
    {
        assert_cast<MutableColumnUInt64 &>(to).Append(data(place).count).ok();
    }
};

class WrappedNumRows final : public ArrowAggregateFunctionWrapper
{
public:
    WrappedNumRows(std::string name)
        : ArrowAggregateFunctionWrapper(std::move(name))
    {}

    AggregateFunctionPtr getHouseFunction(const DataTypes & argument_types) const override
    {
        return std::make_shared<AggregateFunctionNumRows>(argument_types);
    }
};

}
