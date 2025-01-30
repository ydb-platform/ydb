#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/AggregateFunctionNumRows.h>

namespace CH
{

AggregateFunctionPtr GetAggregateFunction(AggFunctionId id, const DataTypes & argument_types)
{
    switch (id) {
        case AggFunctionId::AGG_ANY:
            return WrappedAny("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_COUNT:
            return WrappedCount("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_MIN:
            return WrappedMin("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_MAX:
            return WrappedMax("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_SUM:
            return WrappedSum("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_AVG:
            return WrappedAvg("").getHouseFunction(argument_types);
        case AggFunctionId::AGG_NUM_ROWS:
            return WrappedNumRows("").getHouseFunction(argument_types);
        default:
            break;
    }
    return {};
}

}
