class TPgAggFactory_max: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2050: return MakePgAgg_max_2050().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2115: return MakePgAgg_max_2115().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2116: return MakePgAgg_max_2116().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2117: return MakePgAgg_max_2117().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2118: return MakePgAgg_max_2118().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2119: return MakePgAgg_max_2119().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2120: return MakePgAgg_max_2120().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2122: return MakePgAgg_max_2122().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2123: return MakePgAgg_max_2123().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2124: return MakePgAgg_max_2124().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2125: return MakePgAgg_max_2125().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2126: return MakePgAgg_max_2126().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2127: return MakePgAgg_max_2127().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2128: return MakePgAgg_max_2128().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2129: return MakePgAgg_max_2129().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2130: return MakePgAgg_max_2130().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2244: return MakePgAgg_max_2244().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2797: return MakePgAgg_max_2797().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3526: return MakePgAgg_max_3526().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3564: return MakePgAgg_max_3564().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 4189: return MakePgAgg_max_4189().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 5099: return MakePgAgg_max_5099().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2050: return MakePgAgg_max_2050().PrepareCombineKeys(argsColumns, aggDesc);
    case 2115: return MakePgAgg_max_2115().PrepareCombineKeys(argsColumns, aggDesc);
    case 2116: return MakePgAgg_max_2116().PrepareCombineKeys(argsColumns, aggDesc);
    case 2117: return MakePgAgg_max_2117().PrepareCombineKeys(argsColumns, aggDesc);
    case 2118: return MakePgAgg_max_2118().PrepareCombineKeys(argsColumns, aggDesc);
    case 2119: return MakePgAgg_max_2119().PrepareCombineKeys(argsColumns, aggDesc);
    case 2120: return MakePgAgg_max_2120().PrepareCombineKeys(argsColumns, aggDesc);
    case 2122: return MakePgAgg_max_2122().PrepareCombineKeys(argsColumns, aggDesc);
    case 2123: return MakePgAgg_max_2123().PrepareCombineKeys(argsColumns, aggDesc);
    case 2124: return MakePgAgg_max_2124().PrepareCombineKeys(argsColumns, aggDesc);
    case 2125: return MakePgAgg_max_2125().PrepareCombineKeys(argsColumns, aggDesc);
    case 2126: return MakePgAgg_max_2126().PrepareCombineKeys(argsColumns, aggDesc);
    case 2127: return MakePgAgg_max_2127().PrepareCombineKeys(argsColumns, aggDesc);
    case 2128: return MakePgAgg_max_2128().PrepareCombineKeys(argsColumns, aggDesc);
    case 2129: return MakePgAgg_max_2129().PrepareCombineKeys(argsColumns, aggDesc);
    case 2130: return MakePgAgg_max_2130().PrepareCombineKeys(argsColumns, aggDesc);
    case 2244: return MakePgAgg_max_2244().PrepareCombineKeys(argsColumns, aggDesc);
    case 2797: return MakePgAgg_max_2797().PrepareCombineKeys(argsColumns, aggDesc);
    case 3526: return MakePgAgg_max_3526().PrepareCombineKeys(argsColumns, aggDesc);
    case 3564: return MakePgAgg_max_3564().PrepareCombineKeys(argsColumns, aggDesc);
    case 4189: return MakePgAgg_max_4189().PrepareCombineKeys(argsColumns, aggDesc);
    case 5099: return MakePgAgg_max_5099().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2050: return MakePgAgg_max_2050().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2115: return MakePgAgg_max_2115().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2116: return MakePgAgg_max_2116().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2117: return MakePgAgg_max_2117().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2118: return MakePgAgg_max_2118().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2119: return MakePgAgg_max_2119().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2120: return MakePgAgg_max_2120().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2122: return MakePgAgg_max_2122().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2123: return MakePgAgg_max_2123().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2124: return MakePgAgg_max_2124().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2125: return MakePgAgg_max_2125().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2126: return MakePgAgg_max_2126().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2127: return MakePgAgg_max_2127().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2128: return MakePgAgg_max_2128().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2129: return MakePgAgg_max_2129().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2130: return MakePgAgg_max_2130().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2244: return MakePgAgg_max_2244().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2797: return MakePgAgg_max_2797().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3526: return MakePgAgg_max_3526().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3564: return MakePgAgg_max_3564().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 4189: return MakePgAgg_max_4189().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 5099: return MakePgAgg_max_5099().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_min: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("min", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2051: return MakePgAgg_min_2051().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2131: return MakePgAgg_min_2131().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2132: return MakePgAgg_min_2132().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2133: return MakePgAgg_min_2133().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2134: return MakePgAgg_min_2134().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2135: return MakePgAgg_min_2135().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2136: return MakePgAgg_min_2136().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2138: return MakePgAgg_min_2138().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2139: return MakePgAgg_min_2139().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2140: return MakePgAgg_min_2140().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2141: return MakePgAgg_min_2141().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2142: return MakePgAgg_min_2142().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2143: return MakePgAgg_min_2143().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2144: return MakePgAgg_min_2144().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2145: return MakePgAgg_min_2145().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2146: return MakePgAgg_min_2146().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2245: return MakePgAgg_min_2245().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2798: return MakePgAgg_min_2798().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3527: return MakePgAgg_min_3527().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3565: return MakePgAgg_min_3565().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 4190: return MakePgAgg_min_4190().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 5100: return MakePgAgg_min_5100().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("min", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2051: return MakePgAgg_min_2051().PrepareCombineKeys(argsColumns, aggDesc);
    case 2131: return MakePgAgg_min_2131().PrepareCombineKeys(argsColumns, aggDesc);
    case 2132: return MakePgAgg_min_2132().PrepareCombineKeys(argsColumns, aggDesc);
    case 2133: return MakePgAgg_min_2133().PrepareCombineKeys(argsColumns, aggDesc);
    case 2134: return MakePgAgg_min_2134().PrepareCombineKeys(argsColumns, aggDesc);
    case 2135: return MakePgAgg_min_2135().PrepareCombineKeys(argsColumns, aggDesc);
    case 2136: return MakePgAgg_min_2136().PrepareCombineKeys(argsColumns, aggDesc);
    case 2138: return MakePgAgg_min_2138().PrepareCombineKeys(argsColumns, aggDesc);
    case 2139: return MakePgAgg_min_2139().PrepareCombineKeys(argsColumns, aggDesc);
    case 2140: return MakePgAgg_min_2140().PrepareCombineKeys(argsColumns, aggDesc);
    case 2141: return MakePgAgg_min_2141().PrepareCombineKeys(argsColumns, aggDesc);
    case 2142: return MakePgAgg_min_2142().PrepareCombineKeys(argsColumns, aggDesc);
    case 2143: return MakePgAgg_min_2143().PrepareCombineKeys(argsColumns, aggDesc);
    case 2144: return MakePgAgg_min_2144().PrepareCombineKeys(argsColumns, aggDesc);
    case 2145: return MakePgAgg_min_2145().PrepareCombineKeys(argsColumns, aggDesc);
    case 2146: return MakePgAgg_min_2146().PrepareCombineKeys(argsColumns, aggDesc);
    case 2245: return MakePgAgg_min_2245().PrepareCombineKeys(argsColumns, aggDesc);
    case 2798: return MakePgAgg_min_2798().PrepareCombineKeys(argsColumns, aggDesc);
    case 3527: return MakePgAgg_min_3527().PrepareCombineKeys(argsColumns, aggDesc);
    case 3565: return MakePgAgg_min_3565().PrepareCombineKeys(argsColumns, aggDesc);
    case 4190: return MakePgAgg_min_4190().PrepareCombineKeys(argsColumns, aggDesc);
    case 5100: return MakePgAgg_min_5100().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("min", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2051: return MakePgAgg_min_2051().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2131: return MakePgAgg_min_2131().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2132: return MakePgAgg_min_2132().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2133: return MakePgAgg_min_2133().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2134: return MakePgAgg_min_2134().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2135: return MakePgAgg_min_2135().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2136: return MakePgAgg_min_2136().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2138: return MakePgAgg_min_2138().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2139: return MakePgAgg_min_2139().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2140: return MakePgAgg_min_2140().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2141: return MakePgAgg_min_2141().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2142: return MakePgAgg_min_2142().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2143: return MakePgAgg_min_2143().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2144: return MakePgAgg_min_2144().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2145: return MakePgAgg_min_2145().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2146: return MakePgAgg_min_2146().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2245: return MakePgAgg_min_2245().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2798: return MakePgAgg_min_2798().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3527: return MakePgAgg_min_3527().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3565: return MakePgAgg_min_3565().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 4190: return MakePgAgg_min_4190().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 5100: return MakePgAgg_min_5100().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_avg: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2100: return MakePgAgg_avg_2100().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2101: return MakePgAgg_avg_2101().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2102: return MakePgAgg_avg_2102().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2103: return MakePgAgg_avg_2103().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2104: return MakePgAgg_avg_2104().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2105: return MakePgAgg_avg_2105().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2106: return MakePgAgg_avg_2106().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2100: return MakePgAgg_avg_2100().PrepareCombineKeys(argsColumns, aggDesc);
    case 2101: return MakePgAgg_avg_2101().PrepareCombineKeys(argsColumns, aggDesc);
    case 2102: return MakePgAgg_avg_2102().PrepareCombineKeys(argsColumns, aggDesc);
    case 2103: return MakePgAgg_avg_2103().PrepareCombineKeys(argsColumns, aggDesc);
    case 2104: return MakePgAgg_avg_2104().PrepareCombineKeys(argsColumns, aggDesc);
    case 2105: return MakePgAgg_avg_2105().PrepareCombineKeys(argsColumns, aggDesc);
    case 2106: return MakePgAgg_avg_2106().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2100: return MakePgAgg_avg_2100().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2101: return MakePgAgg_avg_2101().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2102: return MakePgAgg_avg_2102().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2103: return MakePgAgg_avg_2103().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2104: return MakePgAgg_avg_2104().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2105: return MakePgAgg_avg_2105().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2106: return MakePgAgg_avg_2106().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_sum: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("sum", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2107: return MakePgAgg_sum_2107().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2108: return MakePgAgg_sum_2108().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2109: return MakePgAgg_sum_2109().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2110: return MakePgAgg_sum_2110().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2111: return MakePgAgg_sum_2111().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2112: return MakePgAgg_sum_2112().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2113: return MakePgAgg_sum_2113().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2114: return MakePgAgg_sum_2114().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("sum", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2107: return MakePgAgg_sum_2107().PrepareCombineKeys(argsColumns, aggDesc);
    case 2108: return MakePgAgg_sum_2108().PrepareCombineKeys(argsColumns, aggDesc);
    case 2109: return MakePgAgg_sum_2109().PrepareCombineKeys(argsColumns, aggDesc);
    case 2110: return MakePgAgg_sum_2110().PrepareCombineKeys(argsColumns, aggDesc);
    case 2111: return MakePgAgg_sum_2111().PrepareCombineKeys(argsColumns, aggDesc);
    case 2112: return MakePgAgg_sum_2112().PrepareCombineKeys(argsColumns, aggDesc);
    case 2113: return MakePgAgg_sum_2113().PrepareCombineKeys(argsColumns, aggDesc);
    case 2114: return MakePgAgg_sum_2114().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("sum", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2107: return MakePgAgg_sum_2107().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2108: return MakePgAgg_sum_2108().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2109: return MakePgAgg_sum_2109().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2110: return MakePgAgg_sum_2110().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2111: return MakePgAgg_sum_2111().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2112: return MakePgAgg_sum_2112().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2113: return MakePgAgg_sum_2113().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2114: return MakePgAgg_sum_2114().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_count: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("count", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2147: return MakePgAgg_count_2147().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2803: return MakePgAgg_count_2803().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("count", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2147: return MakePgAgg_count_2147().PrepareCombineKeys(argsColumns, aggDesc);
    case 2803: return MakePgAgg_count_2803().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("count", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2147: return MakePgAgg_count_2147().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2803: return MakePgAgg_count_2803().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_variance: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("variance", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2148: return MakePgAgg_variance_2148().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2149: return MakePgAgg_variance_2149().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2150: return MakePgAgg_variance_2150().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2151: return MakePgAgg_variance_2151().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2152: return MakePgAgg_variance_2152().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2153: return MakePgAgg_variance_2153().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("variance", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2148: return MakePgAgg_variance_2148().PrepareCombineKeys(argsColumns, aggDesc);
    case 2149: return MakePgAgg_variance_2149().PrepareCombineKeys(argsColumns, aggDesc);
    case 2150: return MakePgAgg_variance_2150().PrepareCombineKeys(argsColumns, aggDesc);
    case 2151: return MakePgAgg_variance_2151().PrepareCombineKeys(argsColumns, aggDesc);
    case 2152: return MakePgAgg_variance_2152().PrepareCombineKeys(argsColumns, aggDesc);
    case 2153: return MakePgAgg_variance_2153().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("variance", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2148: return MakePgAgg_variance_2148().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2149: return MakePgAgg_variance_2149().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2150: return MakePgAgg_variance_2150().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2151: return MakePgAgg_variance_2151().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2152: return MakePgAgg_variance_2152().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2153: return MakePgAgg_variance_2153().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_stddev: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2154: return MakePgAgg_stddev_2154().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2155: return MakePgAgg_stddev_2155().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2156: return MakePgAgg_stddev_2156().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2157: return MakePgAgg_stddev_2157().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2158: return MakePgAgg_stddev_2158().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2159: return MakePgAgg_stddev_2159().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2154: return MakePgAgg_stddev_2154().PrepareCombineKeys(argsColumns, aggDesc);
    case 2155: return MakePgAgg_stddev_2155().PrepareCombineKeys(argsColumns, aggDesc);
    case 2156: return MakePgAgg_stddev_2156().PrepareCombineKeys(argsColumns, aggDesc);
    case 2157: return MakePgAgg_stddev_2157().PrepareCombineKeys(argsColumns, aggDesc);
    case 2158: return MakePgAgg_stddev_2158().PrepareCombineKeys(argsColumns, aggDesc);
    case 2159: return MakePgAgg_stddev_2159().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2154: return MakePgAgg_stddev_2154().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2155: return MakePgAgg_stddev_2155().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2156: return MakePgAgg_stddev_2156().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2157: return MakePgAgg_stddev_2157().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2158: return MakePgAgg_stddev_2158().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2159: return MakePgAgg_stddev_2159().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_bit_and: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2236: return MakePgAgg_bit_and_2236().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2238: return MakePgAgg_bit_and_2238().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2240: return MakePgAgg_bit_and_2240().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2242: return MakePgAgg_bit_and_2242().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2236: return MakePgAgg_bit_and_2236().PrepareCombineKeys(argsColumns, aggDesc);
    case 2238: return MakePgAgg_bit_and_2238().PrepareCombineKeys(argsColumns, aggDesc);
    case 2240: return MakePgAgg_bit_and_2240().PrepareCombineKeys(argsColumns, aggDesc);
    case 2242: return MakePgAgg_bit_and_2242().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_and", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2236: return MakePgAgg_bit_and_2236().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2238: return MakePgAgg_bit_and_2238().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2240: return MakePgAgg_bit_and_2240().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2242: return MakePgAgg_bit_and_2242().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_bit_or: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2237: return MakePgAgg_bit_or_2237().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2239: return MakePgAgg_bit_or_2239().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2241: return MakePgAgg_bit_or_2241().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2243: return MakePgAgg_bit_or_2243().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2237: return MakePgAgg_bit_or_2237().PrepareCombineKeys(argsColumns, aggDesc);
    case 2239: return MakePgAgg_bit_or_2239().PrepareCombineKeys(argsColumns, aggDesc);
    case 2241: return MakePgAgg_bit_or_2241().PrepareCombineKeys(argsColumns, aggDesc);
    case 2243: return MakePgAgg_bit_or_2243().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_or", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2237: return MakePgAgg_bit_or_2237().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2239: return MakePgAgg_bit_or_2239().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2241: return MakePgAgg_bit_or_2241().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2243: return MakePgAgg_bit_or_2243().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_array_agg: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("array_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2335: return MakePgAgg_array_agg_2335().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 4053: return MakePgAgg_array_agg_4053().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("array_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2335: return MakePgAgg_array_agg_2335().PrepareCombineKeys(argsColumns, aggDesc);
    case 4053: return MakePgAgg_array_agg_4053().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("array_agg", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2335: return MakePgAgg_array_agg_2335().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 4053: return MakePgAgg_array_agg_4053().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_bool_and: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2517: return MakePgAgg_bool_and_2517().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2517: return MakePgAgg_bool_and_2517().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bool_and", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2517: return MakePgAgg_bool_and_2517().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_bool_or: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2518: return MakePgAgg_bool_or_2518().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2518: return MakePgAgg_bool_or_2518().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bool_or", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2518: return MakePgAgg_bool_or_2518().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_every: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("every", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2519: return MakePgAgg_every_2519().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("every", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2519: return MakePgAgg_every_2519().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("every", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2519: return MakePgAgg_every_2519().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_var_samp: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2641: return MakePgAgg_var_samp_2641().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2642: return MakePgAgg_var_samp_2642().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2643: return MakePgAgg_var_samp_2643().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2644: return MakePgAgg_var_samp_2644().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2645: return MakePgAgg_var_samp_2645().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2646: return MakePgAgg_var_samp_2646().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2641: return MakePgAgg_var_samp_2641().PrepareCombineKeys(argsColumns, aggDesc);
    case 2642: return MakePgAgg_var_samp_2642().PrepareCombineKeys(argsColumns, aggDesc);
    case 2643: return MakePgAgg_var_samp_2643().PrepareCombineKeys(argsColumns, aggDesc);
    case 2644: return MakePgAgg_var_samp_2644().PrepareCombineKeys(argsColumns, aggDesc);
    case 2645: return MakePgAgg_var_samp_2645().PrepareCombineKeys(argsColumns, aggDesc);
    case 2646: return MakePgAgg_var_samp_2646().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("var_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2641: return MakePgAgg_var_samp_2641().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2642: return MakePgAgg_var_samp_2642().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2643: return MakePgAgg_var_samp_2643().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2644: return MakePgAgg_var_samp_2644().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2645: return MakePgAgg_var_samp_2645().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2646: return MakePgAgg_var_samp_2646().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_stddev_samp: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2712: return MakePgAgg_stddev_samp_2712().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2713: return MakePgAgg_stddev_samp_2713().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2714: return MakePgAgg_stddev_samp_2714().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2715: return MakePgAgg_stddev_samp_2715().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2716: return MakePgAgg_stddev_samp_2716().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2717: return MakePgAgg_stddev_samp_2717().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2712: return MakePgAgg_stddev_samp_2712().PrepareCombineKeys(argsColumns, aggDesc);
    case 2713: return MakePgAgg_stddev_samp_2713().PrepareCombineKeys(argsColumns, aggDesc);
    case 2714: return MakePgAgg_stddev_samp_2714().PrepareCombineKeys(argsColumns, aggDesc);
    case 2715: return MakePgAgg_stddev_samp_2715().PrepareCombineKeys(argsColumns, aggDesc);
    case 2716: return MakePgAgg_stddev_samp_2716().PrepareCombineKeys(argsColumns, aggDesc);
    case 2717: return MakePgAgg_stddev_samp_2717().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2712: return MakePgAgg_stddev_samp_2712().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2713: return MakePgAgg_stddev_samp_2713().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2714: return MakePgAgg_stddev_samp_2714().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2715: return MakePgAgg_stddev_samp_2715().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2716: return MakePgAgg_stddev_samp_2716().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2717: return MakePgAgg_stddev_samp_2717().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_var_pop: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2718: return MakePgAgg_var_pop_2718().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2719: return MakePgAgg_var_pop_2719().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2720: return MakePgAgg_var_pop_2720().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2721: return MakePgAgg_var_pop_2721().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2722: return MakePgAgg_var_pop_2722().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2723: return MakePgAgg_var_pop_2723().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2718: return MakePgAgg_var_pop_2718().PrepareCombineKeys(argsColumns, aggDesc);
    case 2719: return MakePgAgg_var_pop_2719().PrepareCombineKeys(argsColumns, aggDesc);
    case 2720: return MakePgAgg_var_pop_2720().PrepareCombineKeys(argsColumns, aggDesc);
    case 2721: return MakePgAgg_var_pop_2721().PrepareCombineKeys(argsColumns, aggDesc);
    case 2722: return MakePgAgg_var_pop_2722().PrepareCombineKeys(argsColumns, aggDesc);
    case 2723: return MakePgAgg_var_pop_2723().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("var_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2718: return MakePgAgg_var_pop_2718().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2719: return MakePgAgg_var_pop_2719().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2720: return MakePgAgg_var_pop_2720().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2721: return MakePgAgg_var_pop_2721().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2722: return MakePgAgg_var_pop_2722().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2723: return MakePgAgg_var_pop_2723().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_stddev_pop: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2724: return MakePgAgg_stddev_pop_2724().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2725: return MakePgAgg_stddev_pop_2725().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2726: return MakePgAgg_stddev_pop_2726().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2727: return MakePgAgg_stddev_pop_2727().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2728: return MakePgAgg_stddev_pop_2728().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2729: return MakePgAgg_stddev_pop_2729().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2724: return MakePgAgg_stddev_pop_2724().PrepareCombineKeys(argsColumns, aggDesc);
    case 2725: return MakePgAgg_stddev_pop_2725().PrepareCombineKeys(argsColumns, aggDesc);
    case 2726: return MakePgAgg_stddev_pop_2726().PrepareCombineKeys(argsColumns, aggDesc);
    case 2727: return MakePgAgg_stddev_pop_2727().PrepareCombineKeys(argsColumns, aggDesc);
    case 2728: return MakePgAgg_stddev_pop_2728().PrepareCombineKeys(argsColumns, aggDesc);
    case 2729: return MakePgAgg_stddev_pop_2729().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2724: return MakePgAgg_stddev_pop_2724().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2725: return MakePgAgg_stddev_pop_2725().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2726: return MakePgAgg_stddev_pop_2726().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2727: return MakePgAgg_stddev_pop_2727().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2728: return MakePgAgg_stddev_pop_2728().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2729: return MakePgAgg_stddev_pop_2729().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_count: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_count", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2818: return MakePgAgg_regr_count_2818().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_count", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2818: return MakePgAgg_regr_count_2818().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_count", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2818: return MakePgAgg_regr_count_2818().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_sxx: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxx", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2819: return MakePgAgg_regr_sxx_2819().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxx", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2819: return MakePgAgg_regr_sxx_2819().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxx", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2819: return MakePgAgg_regr_sxx_2819().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_syy: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_syy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2820: return MakePgAgg_regr_syy_2820().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_syy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2820: return MakePgAgg_regr_syy_2820().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_syy", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2820: return MakePgAgg_regr_syy_2820().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_sxy: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2821: return MakePgAgg_regr_sxy_2821().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2821: return MakePgAgg_regr_sxy_2821().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxy", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2821: return MakePgAgg_regr_sxy_2821().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_avgx: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgx", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2822: return MakePgAgg_regr_avgx_2822().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgx", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2822: return MakePgAgg_regr_avgx_2822().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgx", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2822: return MakePgAgg_regr_avgx_2822().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_avgy: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2823: return MakePgAgg_regr_avgy_2823().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgy", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2823: return MakePgAgg_regr_avgy_2823().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgy", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2823: return MakePgAgg_regr_avgy_2823().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_r2: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_r2", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2824: return MakePgAgg_regr_r2_2824().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_r2", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2824: return MakePgAgg_regr_r2_2824().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_r2", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2824: return MakePgAgg_regr_r2_2824().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_slope: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_slope", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2825: return MakePgAgg_regr_slope_2825().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_slope", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2825: return MakePgAgg_regr_slope_2825().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_slope", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2825: return MakePgAgg_regr_slope_2825().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_regr_intercept: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_intercept", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2826: return MakePgAgg_regr_intercept_2826().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_intercept", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2826: return MakePgAgg_regr_intercept_2826().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_intercept", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2826: return MakePgAgg_regr_intercept_2826().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_covar_pop: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2827: return MakePgAgg_covar_pop_2827().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2827: return MakePgAgg_covar_pop_2827().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("covar_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2827: return MakePgAgg_covar_pop_2827().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_covar_samp: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2828: return MakePgAgg_covar_samp_2828().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2828: return MakePgAgg_covar_samp_2828().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("covar_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2828: return MakePgAgg_covar_samp_2828().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_corr: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("corr", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2829: return MakePgAgg_corr_2829().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("corr", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 2829: return MakePgAgg_corr_2829().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("corr", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 2829: return MakePgAgg_corr_2829().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_string_agg: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("string_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 3538: return MakePgAgg_string_agg_3538().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3545: return MakePgAgg_string_agg_3545().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("string_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 3538: return MakePgAgg_string_agg_3538().PrepareCombineKeys(argsColumns, aggDesc);
    case 3545: return MakePgAgg_string_agg_3545().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("string_agg", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 3538: return MakePgAgg_string_agg_3538().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3545: return MakePgAgg_string_agg_3545().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_range_intersect_agg: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("range_intersect_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 4389: return MakePgAgg_range_intersect_agg_4389().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 4450: return MakePgAgg_range_intersect_agg_4450().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("range_intersect_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 4389: return MakePgAgg_range_intersect_agg_4389().PrepareCombineKeys(argsColumns, aggDesc);
    case 4450: return MakePgAgg_range_intersect_agg_4450().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("range_intersect_agg", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 4389: return MakePgAgg_range_intersect_agg_4389().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 4450: return MakePgAgg_range_intersect_agg_4450().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_bit_xor: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_xor", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 6164: return MakePgAgg_bit_xor_6164().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 6165: return MakePgAgg_bit_xor_6165().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 6166: return MakePgAgg_bit_xor_6166().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 6167: return MakePgAgg_bit_xor_6167().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_xor", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 6164: return MakePgAgg_bit_xor_6164().PrepareCombineKeys(argsColumns, aggDesc);
    case 6165: return MakePgAgg_bit_xor_6165().PrepareCombineKeys(argsColumns, aggDesc);
    case 6166: return MakePgAgg_bit_xor_6166().PrepareCombineKeys(argsColumns, aggDesc);
    case 6167: return MakePgAgg_bit_xor_6167().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_xor", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 6164: return MakePgAgg_bit_xor_6164().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 6165: return MakePgAgg_bit_xor_6165().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 6166: return MakePgAgg_bit_xor_6166().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 6167: return MakePgAgg_bit_xor_6167().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
class TPgAggFactory_any_value: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("any_value", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 6291: return MakePgAgg_any_value_6291().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("any_value", tupleType, argsColumns, nullptr);
    switch (aggDesc.AggId) {
    case 6291: return MakePgAgg_any_value_6291().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("any_value", tupleType, argsColumns, returnType);
    switch (aggDesc.AggId) {
    case 6291: return MakePgAgg_any_value_6291().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.AggId;
    }
}
};
registry.emplace("pg_max", std::make_unique<TPgAggFactory_max>());
registry.emplace("pg_min", std::make_unique<TPgAggFactory_min>());
registry.emplace("pg_avg", std::make_unique<TPgAggFactory_avg>());
registry.emplace("pg_sum", std::make_unique<TPgAggFactory_sum>());
registry.emplace("pg_count", std::make_unique<TPgAggFactory_count>());
registry.emplace("pg_variance", std::make_unique<TPgAggFactory_variance>());
registry.emplace("pg_stddev", std::make_unique<TPgAggFactory_stddev>());
registry.emplace("pg_bit_and", std::make_unique<TPgAggFactory_bit_and>());
registry.emplace("pg_bit_or", std::make_unique<TPgAggFactory_bit_or>());
registry.emplace("pg_array_agg", std::make_unique<TPgAggFactory_array_agg>());
registry.emplace("pg_bool_and", std::make_unique<TPgAggFactory_bool_and>());
registry.emplace("pg_bool_or", std::make_unique<TPgAggFactory_bool_or>());
registry.emplace("pg_every", std::make_unique<TPgAggFactory_every>());
registry.emplace("pg_var_samp", std::make_unique<TPgAggFactory_var_samp>());
registry.emplace("pg_stddev_samp", std::make_unique<TPgAggFactory_stddev_samp>());
registry.emplace("pg_var_pop", std::make_unique<TPgAggFactory_var_pop>());
registry.emplace("pg_stddev_pop", std::make_unique<TPgAggFactory_stddev_pop>());
registry.emplace("pg_regr_count", std::make_unique<TPgAggFactory_regr_count>());
registry.emplace("pg_regr_sxx", std::make_unique<TPgAggFactory_regr_sxx>());
registry.emplace("pg_regr_syy", std::make_unique<TPgAggFactory_regr_syy>());
registry.emplace("pg_regr_sxy", std::make_unique<TPgAggFactory_regr_sxy>());
registry.emplace("pg_regr_avgx", std::make_unique<TPgAggFactory_regr_avgx>());
registry.emplace("pg_regr_avgy", std::make_unique<TPgAggFactory_regr_avgy>());
registry.emplace("pg_regr_r2", std::make_unique<TPgAggFactory_regr_r2>());
registry.emplace("pg_regr_slope", std::make_unique<TPgAggFactory_regr_slope>());
registry.emplace("pg_regr_intercept", std::make_unique<TPgAggFactory_regr_intercept>());
registry.emplace("pg_covar_pop", std::make_unique<TPgAggFactory_covar_pop>());
registry.emplace("pg_covar_samp", std::make_unique<TPgAggFactory_covar_samp>());
registry.emplace("pg_corr", std::make_unique<TPgAggFactory_corr>());
registry.emplace("pg_string_agg", std::make_unique<TPgAggFactory_string_agg>());
registry.emplace("pg_range_intersect_agg", std::make_unique<TPgAggFactory_range_intersect_agg>());
registry.emplace("pg_bit_xor", std::make_unique<TPgAggFactory_bit_xor>());
registry.emplace("pg_any_value", std::make_unique<TPgAggFactory_any_value>());
