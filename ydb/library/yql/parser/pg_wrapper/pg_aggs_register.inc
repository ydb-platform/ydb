class TPgAggFactory_avg: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 1: return MakePgAgg_avg_1().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 2: return MakePgAgg_avg_2().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 3: return MakePgAgg_avg_3().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 4: return MakePgAgg_avg_4().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 5: return MakePgAgg_avg_5().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 6: return MakePgAgg_avg_6().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 7: return MakePgAgg_avg_7().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 1: return MakePgAgg_avg_1().PrepareCombineKeys(argsColumns, aggDesc);
    case 2: return MakePgAgg_avg_2().PrepareCombineKeys(argsColumns, aggDesc);
    case 3: return MakePgAgg_avg_3().PrepareCombineKeys(argsColumns, aggDesc);
    case 4: return MakePgAgg_avg_4().PrepareCombineKeys(argsColumns, aggDesc);
    case 5: return MakePgAgg_avg_5().PrepareCombineKeys(argsColumns, aggDesc);
    case 6: return MakePgAgg_avg_6().PrepareCombineKeys(argsColumns, aggDesc);
    case 7: return MakePgAgg_avg_7().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("avg", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 1: return MakePgAgg_avg_1().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 2: return MakePgAgg_avg_2().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 3: return MakePgAgg_avg_3().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 4: return MakePgAgg_avg_4().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 5: return MakePgAgg_avg_5().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 6: return MakePgAgg_avg_6().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 7: return MakePgAgg_avg_7().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 8: return MakePgAgg_sum_8().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 9: return MakePgAgg_sum_9().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 10: return MakePgAgg_sum_10().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 11: return MakePgAgg_sum_11().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 12: return MakePgAgg_sum_12().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 13: return MakePgAgg_sum_13().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 14: return MakePgAgg_sum_14().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 15: return MakePgAgg_sum_15().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("sum", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 8: return MakePgAgg_sum_8().PrepareCombineKeys(argsColumns, aggDesc);
    case 9: return MakePgAgg_sum_9().PrepareCombineKeys(argsColumns, aggDesc);
    case 10: return MakePgAgg_sum_10().PrepareCombineKeys(argsColumns, aggDesc);
    case 11: return MakePgAgg_sum_11().PrepareCombineKeys(argsColumns, aggDesc);
    case 12: return MakePgAgg_sum_12().PrepareCombineKeys(argsColumns, aggDesc);
    case 13: return MakePgAgg_sum_13().PrepareCombineKeys(argsColumns, aggDesc);
    case 14: return MakePgAgg_sum_14().PrepareCombineKeys(argsColumns, aggDesc);
    case 15: return MakePgAgg_sum_15().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("sum", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 8: return MakePgAgg_sum_8().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 9: return MakePgAgg_sum_9().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 10: return MakePgAgg_sum_10().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 11: return MakePgAgg_sum_11().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 12: return MakePgAgg_sum_12().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 13: return MakePgAgg_sum_13().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 14: return MakePgAgg_sum_14().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 15: return MakePgAgg_sum_15().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}
};
class TPgAggFactory_max: public IBlockAggregatorFactory {
std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineAll>> PrepareCombineAll(
    TTupleType* tupleType,
    std::optional<ui32> filterColumn,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 16: return MakePgAgg_max_16().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 17: return MakePgAgg_max_17().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 18: return MakePgAgg_max_18().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 19: return MakePgAgg_max_19().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 20: return MakePgAgg_max_20().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 21: return MakePgAgg_max_21().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 22: return MakePgAgg_max_22().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 23: return MakePgAgg_max_23().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 24: return MakePgAgg_max_24().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 25: return MakePgAgg_max_25().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 26: return MakePgAgg_max_26().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 27: return MakePgAgg_max_27().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 28: return MakePgAgg_max_28().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 29: return MakePgAgg_max_29().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 30: return MakePgAgg_max_30().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 31: return MakePgAgg_max_31().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 32: return MakePgAgg_max_32().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 33: return MakePgAgg_max_33().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 34: return MakePgAgg_max_34().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 35: return MakePgAgg_max_35().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 36: return MakePgAgg_max_36().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 16: return MakePgAgg_max_16().PrepareCombineKeys(argsColumns, aggDesc);
    case 17: return MakePgAgg_max_17().PrepareCombineKeys(argsColumns, aggDesc);
    case 18: return MakePgAgg_max_18().PrepareCombineKeys(argsColumns, aggDesc);
    case 19: return MakePgAgg_max_19().PrepareCombineKeys(argsColumns, aggDesc);
    case 20: return MakePgAgg_max_20().PrepareCombineKeys(argsColumns, aggDesc);
    case 21: return MakePgAgg_max_21().PrepareCombineKeys(argsColumns, aggDesc);
    case 22: return MakePgAgg_max_22().PrepareCombineKeys(argsColumns, aggDesc);
    case 23: return MakePgAgg_max_23().PrepareCombineKeys(argsColumns, aggDesc);
    case 24: return MakePgAgg_max_24().PrepareCombineKeys(argsColumns, aggDesc);
    case 25: return MakePgAgg_max_25().PrepareCombineKeys(argsColumns, aggDesc);
    case 26: return MakePgAgg_max_26().PrepareCombineKeys(argsColumns, aggDesc);
    case 27: return MakePgAgg_max_27().PrepareCombineKeys(argsColumns, aggDesc);
    case 28: return MakePgAgg_max_28().PrepareCombineKeys(argsColumns, aggDesc);
    case 29: return MakePgAgg_max_29().PrepareCombineKeys(argsColumns, aggDesc);
    case 30: return MakePgAgg_max_30().PrepareCombineKeys(argsColumns, aggDesc);
    case 31: return MakePgAgg_max_31().PrepareCombineKeys(argsColumns, aggDesc);
    case 32: return MakePgAgg_max_32().PrepareCombineKeys(argsColumns, aggDesc);
    case 33: return MakePgAgg_max_33().PrepareCombineKeys(argsColumns, aggDesc);
    case 34: return MakePgAgg_max_34().PrepareCombineKeys(argsColumns, aggDesc);
    case 35: return MakePgAgg_max_35().PrepareCombineKeys(argsColumns, aggDesc);
    case 36: return MakePgAgg_max_36().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("max", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 16: return MakePgAgg_max_16().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 17: return MakePgAgg_max_17().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 18: return MakePgAgg_max_18().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 19: return MakePgAgg_max_19().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 20: return MakePgAgg_max_20().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 21: return MakePgAgg_max_21().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 22: return MakePgAgg_max_22().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 23: return MakePgAgg_max_23().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 24: return MakePgAgg_max_24().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 25: return MakePgAgg_max_25().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 26: return MakePgAgg_max_26().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 27: return MakePgAgg_max_27().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 28: return MakePgAgg_max_28().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 29: return MakePgAgg_max_29().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 30: return MakePgAgg_max_30().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 31: return MakePgAgg_max_31().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 32: return MakePgAgg_max_32().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 33: return MakePgAgg_max_33().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 34: return MakePgAgg_max_34().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 35: return MakePgAgg_max_35().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 36: return MakePgAgg_max_36().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 37: return MakePgAgg_min_37().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 38: return MakePgAgg_min_38().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 39: return MakePgAgg_min_39().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 40: return MakePgAgg_min_40().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 41: return MakePgAgg_min_41().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 42: return MakePgAgg_min_42().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 43: return MakePgAgg_min_43().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 44: return MakePgAgg_min_44().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 45: return MakePgAgg_min_45().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 46: return MakePgAgg_min_46().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 47: return MakePgAgg_min_47().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 48: return MakePgAgg_min_48().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 49: return MakePgAgg_min_49().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 50: return MakePgAgg_min_50().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 51: return MakePgAgg_min_51().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 52: return MakePgAgg_min_52().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 53: return MakePgAgg_min_53().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 54: return MakePgAgg_min_54().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 55: return MakePgAgg_min_55().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 56: return MakePgAgg_min_56().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 57: return MakePgAgg_min_57().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("min", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 37: return MakePgAgg_min_37().PrepareCombineKeys(argsColumns, aggDesc);
    case 38: return MakePgAgg_min_38().PrepareCombineKeys(argsColumns, aggDesc);
    case 39: return MakePgAgg_min_39().PrepareCombineKeys(argsColumns, aggDesc);
    case 40: return MakePgAgg_min_40().PrepareCombineKeys(argsColumns, aggDesc);
    case 41: return MakePgAgg_min_41().PrepareCombineKeys(argsColumns, aggDesc);
    case 42: return MakePgAgg_min_42().PrepareCombineKeys(argsColumns, aggDesc);
    case 43: return MakePgAgg_min_43().PrepareCombineKeys(argsColumns, aggDesc);
    case 44: return MakePgAgg_min_44().PrepareCombineKeys(argsColumns, aggDesc);
    case 45: return MakePgAgg_min_45().PrepareCombineKeys(argsColumns, aggDesc);
    case 46: return MakePgAgg_min_46().PrepareCombineKeys(argsColumns, aggDesc);
    case 47: return MakePgAgg_min_47().PrepareCombineKeys(argsColumns, aggDesc);
    case 48: return MakePgAgg_min_48().PrepareCombineKeys(argsColumns, aggDesc);
    case 49: return MakePgAgg_min_49().PrepareCombineKeys(argsColumns, aggDesc);
    case 50: return MakePgAgg_min_50().PrepareCombineKeys(argsColumns, aggDesc);
    case 51: return MakePgAgg_min_51().PrepareCombineKeys(argsColumns, aggDesc);
    case 52: return MakePgAgg_min_52().PrepareCombineKeys(argsColumns, aggDesc);
    case 53: return MakePgAgg_min_53().PrepareCombineKeys(argsColumns, aggDesc);
    case 54: return MakePgAgg_min_54().PrepareCombineKeys(argsColumns, aggDesc);
    case 55: return MakePgAgg_min_55().PrepareCombineKeys(argsColumns, aggDesc);
    case 56: return MakePgAgg_min_56().PrepareCombineKeys(argsColumns, aggDesc);
    case 57: return MakePgAgg_min_57().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("min", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 37: return MakePgAgg_min_37().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 38: return MakePgAgg_min_38().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 39: return MakePgAgg_min_39().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 40: return MakePgAgg_min_40().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 41: return MakePgAgg_min_41().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 42: return MakePgAgg_min_42().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 43: return MakePgAgg_min_43().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 44: return MakePgAgg_min_44().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 45: return MakePgAgg_min_45().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 46: return MakePgAgg_min_46().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 47: return MakePgAgg_min_47().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 48: return MakePgAgg_min_48().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 49: return MakePgAgg_min_49().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 50: return MakePgAgg_min_50().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 51: return MakePgAgg_min_51().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 52: return MakePgAgg_min_52().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 53: return MakePgAgg_min_53().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 54: return MakePgAgg_min_54().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 55: return MakePgAgg_min_55().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 56: return MakePgAgg_min_56().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 57: return MakePgAgg_min_57().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 58: return MakePgAgg_count_58().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 59: return MakePgAgg_count_59().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("count", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 58: return MakePgAgg_count_58().PrepareCombineKeys(argsColumns, aggDesc);
    case 59: return MakePgAgg_count_59().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("count", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 58: return MakePgAgg_count_58().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 59: return MakePgAgg_count_59().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 60: return MakePgAgg_var_pop_60().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 61: return MakePgAgg_var_pop_61().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 62: return MakePgAgg_var_pop_62().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 63: return MakePgAgg_var_pop_63().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 64: return MakePgAgg_var_pop_64().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 65: return MakePgAgg_var_pop_65().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 60: return MakePgAgg_var_pop_60().PrepareCombineKeys(argsColumns, aggDesc);
    case 61: return MakePgAgg_var_pop_61().PrepareCombineKeys(argsColumns, aggDesc);
    case 62: return MakePgAgg_var_pop_62().PrepareCombineKeys(argsColumns, aggDesc);
    case 63: return MakePgAgg_var_pop_63().PrepareCombineKeys(argsColumns, aggDesc);
    case 64: return MakePgAgg_var_pop_64().PrepareCombineKeys(argsColumns, aggDesc);
    case 65: return MakePgAgg_var_pop_65().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("var_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 60: return MakePgAgg_var_pop_60().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 61: return MakePgAgg_var_pop_61().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 62: return MakePgAgg_var_pop_62().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 63: return MakePgAgg_var_pop_63().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 64: return MakePgAgg_var_pop_64().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 65: return MakePgAgg_var_pop_65().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 66: return MakePgAgg_var_samp_66().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 67: return MakePgAgg_var_samp_67().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 68: return MakePgAgg_var_samp_68().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 69: return MakePgAgg_var_samp_69().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 70: return MakePgAgg_var_samp_70().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 71: return MakePgAgg_var_samp_71().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("var_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 66: return MakePgAgg_var_samp_66().PrepareCombineKeys(argsColumns, aggDesc);
    case 67: return MakePgAgg_var_samp_67().PrepareCombineKeys(argsColumns, aggDesc);
    case 68: return MakePgAgg_var_samp_68().PrepareCombineKeys(argsColumns, aggDesc);
    case 69: return MakePgAgg_var_samp_69().PrepareCombineKeys(argsColumns, aggDesc);
    case 70: return MakePgAgg_var_samp_70().PrepareCombineKeys(argsColumns, aggDesc);
    case 71: return MakePgAgg_var_samp_71().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("var_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 66: return MakePgAgg_var_samp_66().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 67: return MakePgAgg_var_samp_67().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 68: return MakePgAgg_var_samp_68().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 69: return MakePgAgg_var_samp_69().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 70: return MakePgAgg_var_samp_70().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 71: return MakePgAgg_var_samp_71().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 72: return MakePgAgg_variance_72().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 73: return MakePgAgg_variance_73().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 74: return MakePgAgg_variance_74().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 75: return MakePgAgg_variance_75().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 76: return MakePgAgg_variance_76().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 77: return MakePgAgg_variance_77().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("variance", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 72: return MakePgAgg_variance_72().PrepareCombineKeys(argsColumns, aggDesc);
    case 73: return MakePgAgg_variance_73().PrepareCombineKeys(argsColumns, aggDesc);
    case 74: return MakePgAgg_variance_74().PrepareCombineKeys(argsColumns, aggDesc);
    case 75: return MakePgAgg_variance_75().PrepareCombineKeys(argsColumns, aggDesc);
    case 76: return MakePgAgg_variance_76().PrepareCombineKeys(argsColumns, aggDesc);
    case 77: return MakePgAgg_variance_77().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("variance", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 72: return MakePgAgg_variance_72().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 73: return MakePgAgg_variance_73().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 74: return MakePgAgg_variance_74().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 75: return MakePgAgg_variance_75().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 76: return MakePgAgg_variance_76().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 77: return MakePgAgg_variance_77().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 78: return MakePgAgg_stddev_pop_78().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 79: return MakePgAgg_stddev_pop_79().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 80: return MakePgAgg_stddev_pop_80().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 81: return MakePgAgg_stddev_pop_81().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 82: return MakePgAgg_stddev_pop_82().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 83: return MakePgAgg_stddev_pop_83().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 78: return MakePgAgg_stddev_pop_78().PrepareCombineKeys(argsColumns, aggDesc);
    case 79: return MakePgAgg_stddev_pop_79().PrepareCombineKeys(argsColumns, aggDesc);
    case 80: return MakePgAgg_stddev_pop_80().PrepareCombineKeys(argsColumns, aggDesc);
    case 81: return MakePgAgg_stddev_pop_81().PrepareCombineKeys(argsColumns, aggDesc);
    case 82: return MakePgAgg_stddev_pop_82().PrepareCombineKeys(argsColumns, aggDesc);
    case 83: return MakePgAgg_stddev_pop_83().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 78: return MakePgAgg_stddev_pop_78().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 79: return MakePgAgg_stddev_pop_79().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 80: return MakePgAgg_stddev_pop_80().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 81: return MakePgAgg_stddev_pop_81().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 82: return MakePgAgg_stddev_pop_82().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 83: return MakePgAgg_stddev_pop_83().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 84: return MakePgAgg_stddev_samp_84().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 85: return MakePgAgg_stddev_samp_85().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 86: return MakePgAgg_stddev_samp_86().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 87: return MakePgAgg_stddev_samp_87().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 88: return MakePgAgg_stddev_samp_88().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 89: return MakePgAgg_stddev_samp_89().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 84: return MakePgAgg_stddev_samp_84().PrepareCombineKeys(argsColumns, aggDesc);
    case 85: return MakePgAgg_stddev_samp_85().PrepareCombineKeys(argsColumns, aggDesc);
    case 86: return MakePgAgg_stddev_samp_86().PrepareCombineKeys(argsColumns, aggDesc);
    case 87: return MakePgAgg_stddev_samp_87().PrepareCombineKeys(argsColumns, aggDesc);
    case 88: return MakePgAgg_stddev_samp_88().PrepareCombineKeys(argsColumns, aggDesc);
    case 89: return MakePgAgg_stddev_samp_89().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 84: return MakePgAgg_stddev_samp_84().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 85: return MakePgAgg_stddev_samp_85().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 86: return MakePgAgg_stddev_samp_86().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 87: return MakePgAgg_stddev_samp_87().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 88: return MakePgAgg_stddev_samp_88().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 89: return MakePgAgg_stddev_samp_89().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 90: return MakePgAgg_stddev_90().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 91: return MakePgAgg_stddev_91().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 92: return MakePgAgg_stddev_92().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 93: return MakePgAgg_stddev_93().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 94: return MakePgAgg_stddev_94().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 95: return MakePgAgg_stddev_95().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("stddev", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 90: return MakePgAgg_stddev_90().PrepareCombineKeys(argsColumns, aggDesc);
    case 91: return MakePgAgg_stddev_91().PrepareCombineKeys(argsColumns, aggDesc);
    case 92: return MakePgAgg_stddev_92().PrepareCombineKeys(argsColumns, aggDesc);
    case 93: return MakePgAgg_stddev_93().PrepareCombineKeys(argsColumns, aggDesc);
    case 94: return MakePgAgg_stddev_94().PrepareCombineKeys(argsColumns, aggDesc);
    case 95: return MakePgAgg_stddev_95().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("stddev", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 90: return MakePgAgg_stddev_90().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 91: return MakePgAgg_stddev_91().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 92: return MakePgAgg_stddev_92().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 93: return MakePgAgg_stddev_93().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 94: return MakePgAgg_stddev_94().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 95: return MakePgAgg_stddev_95().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 96: return MakePgAgg_regr_count_96().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_count", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 96: return MakePgAgg_regr_count_96().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_count", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 96: return MakePgAgg_regr_count_96().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 97: return MakePgAgg_regr_sxx_97().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxx", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 97: return MakePgAgg_regr_sxx_97().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxx", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 97: return MakePgAgg_regr_sxx_97().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 98: return MakePgAgg_regr_syy_98().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_syy", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 98: return MakePgAgg_regr_syy_98().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_syy", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 98: return MakePgAgg_regr_syy_98().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 99: return MakePgAgg_regr_sxy_99().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxy", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 99: return MakePgAgg_regr_sxy_99().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_sxy", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 99: return MakePgAgg_regr_sxy_99().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 100: return MakePgAgg_regr_avgx_100().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgx", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 100: return MakePgAgg_regr_avgx_100().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgx", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 100: return MakePgAgg_regr_avgx_100().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 101: return MakePgAgg_regr_avgy_101().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgy", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 101: return MakePgAgg_regr_avgy_101().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_avgy", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 101: return MakePgAgg_regr_avgy_101().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 102: return MakePgAgg_regr_r2_102().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_r2", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 102: return MakePgAgg_regr_r2_102().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_r2", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 102: return MakePgAgg_regr_r2_102().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 103: return MakePgAgg_regr_slope_103().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_slope", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 103: return MakePgAgg_regr_slope_103().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_slope", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 103: return MakePgAgg_regr_slope_103().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 104: return MakePgAgg_regr_intercept_104().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("regr_intercept", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 104: return MakePgAgg_regr_intercept_104().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("regr_intercept", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 104: return MakePgAgg_regr_intercept_104().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 105: return MakePgAgg_covar_pop_105().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_pop", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 105: return MakePgAgg_covar_pop_105().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("covar_pop", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 105: return MakePgAgg_covar_pop_105().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 106: return MakePgAgg_covar_samp_106().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("covar_samp", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 106: return MakePgAgg_covar_samp_106().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("covar_samp", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 106: return MakePgAgg_covar_samp_106().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 107: return MakePgAgg_corr_107().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("corr", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 107: return MakePgAgg_corr_107().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("corr", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 107: return MakePgAgg_corr_107().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 108: return MakePgAgg_bool_and_108().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 108: return MakePgAgg_bool_and_108().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bool_and", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 108: return MakePgAgg_bool_and_108().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 109: return MakePgAgg_bool_or_109().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bool_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 109: return MakePgAgg_bool_or_109().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bool_or", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 109: return MakePgAgg_bool_or_109().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 110: return MakePgAgg_every_110().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("every", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 110: return MakePgAgg_every_110().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("every", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 110: return MakePgAgg_every_110().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 111: return MakePgAgg_bit_and_111().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 114: return MakePgAgg_bit_and_114().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 117: return MakePgAgg_bit_and_117().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 120: return MakePgAgg_bit_and_120().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_and", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 111: return MakePgAgg_bit_and_111().PrepareCombineKeys(argsColumns, aggDesc);
    case 114: return MakePgAgg_bit_and_114().PrepareCombineKeys(argsColumns, aggDesc);
    case 117: return MakePgAgg_bit_and_117().PrepareCombineKeys(argsColumns, aggDesc);
    case 120: return MakePgAgg_bit_and_120().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_and", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 111: return MakePgAgg_bit_and_111().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 114: return MakePgAgg_bit_and_114().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 117: return MakePgAgg_bit_and_117().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 120: return MakePgAgg_bit_and_120().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 112: return MakePgAgg_bit_or_112().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 115: return MakePgAgg_bit_or_115().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 118: return MakePgAgg_bit_or_118().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 121: return MakePgAgg_bit_or_121().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_or", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 112: return MakePgAgg_bit_or_112().PrepareCombineKeys(argsColumns, aggDesc);
    case 115: return MakePgAgg_bit_or_115().PrepareCombineKeys(argsColumns, aggDesc);
    case 118: return MakePgAgg_bit_or_118().PrepareCombineKeys(argsColumns, aggDesc);
    case 121: return MakePgAgg_bit_or_121().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_or", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 112: return MakePgAgg_bit_or_112().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 115: return MakePgAgg_bit_or_115().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 118: return MakePgAgg_bit_or_118().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 121: return MakePgAgg_bit_or_121().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 113: return MakePgAgg_bit_xor_113().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 116: return MakePgAgg_bit_xor_116().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 119: return MakePgAgg_bit_xor_119().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 122: return MakePgAgg_bit_xor_122().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("bit_xor", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 113: return MakePgAgg_bit_xor_113().PrepareCombineKeys(argsColumns, aggDesc);
    case 116: return MakePgAgg_bit_xor_116().PrepareCombineKeys(argsColumns, aggDesc);
    case 119: return MakePgAgg_bit_xor_119().PrepareCombineKeys(argsColumns, aggDesc);
    case 122: return MakePgAgg_bit_xor_122().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("bit_xor", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 113: return MakePgAgg_bit_xor_113().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 116: return MakePgAgg_bit_xor_116().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 119: return MakePgAgg_bit_xor_119().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 122: return MakePgAgg_bit_xor_122().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
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
    switch (aggDesc.InternalId) {
    case 128: return MakePgAgg_range_intersect_agg_128().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    case 129: return MakePgAgg_range_intersect_agg_129().PrepareCombineAll(filterColumn, argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorCombineKeys>> PrepareCombineKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env) const final {
    const auto& aggDesc = ResolveAggregation("range_intersect_agg", tupleType, argsColumns, nullptr);
    switch (aggDesc.InternalId) {
    case 128: return MakePgAgg_range_intersect_agg_128().PrepareCombineKeys(argsColumns, aggDesc);
    case 129: return MakePgAgg_range_intersect_agg_129().PrepareCombineKeys(argsColumns, aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}

std::unique_ptr<IPreparedBlockAggregator<IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
    TTupleType* tupleType,
    const std::vector<ui32>& argsColumns,
    const TTypeEnvironment& env,
    TType* returnType) const final {
    const auto& aggDesc = ResolveAggregation("range_intersect_agg", tupleType, argsColumns, returnType);
    switch (aggDesc.InternalId) {
    case 128: return MakePgAgg_range_intersect_agg_128().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    case 129: return MakePgAgg_range_intersect_agg_129().PrepareFinalizeKeys(argsColumns.front(), aggDesc);
    default: throw yexception() << "Unsupported agg id: " << aggDesc.InternalId;
    }
}
};
registry.emplace("pg_avg", std::make_unique<TPgAggFactory_avg>());
registry.emplace("pg_sum", std::make_unique<TPgAggFactory_sum>());
registry.emplace("pg_max", std::make_unique<TPgAggFactory_max>());
registry.emplace("pg_min", std::make_unique<TPgAggFactory_min>());
registry.emplace("pg_count", std::make_unique<TPgAggFactory_count>());
registry.emplace("pg_var_pop", std::make_unique<TPgAggFactory_var_pop>());
registry.emplace("pg_var_samp", std::make_unique<TPgAggFactory_var_samp>());
registry.emplace("pg_variance", std::make_unique<TPgAggFactory_variance>());
registry.emplace("pg_stddev_pop", std::make_unique<TPgAggFactory_stddev_pop>());
registry.emplace("pg_stddev_samp", std::make_unique<TPgAggFactory_stddev_samp>());
registry.emplace("pg_stddev", std::make_unique<TPgAggFactory_stddev>());
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
registry.emplace("pg_bool_and", std::make_unique<TPgAggFactory_bool_and>());
registry.emplace("pg_bool_or", std::make_unique<TPgAggFactory_bool_or>());
registry.emplace("pg_every", std::make_unique<TPgAggFactory_every>());
registry.emplace("pg_bit_and", std::make_unique<TPgAggFactory_bit_and>());
registry.emplace("pg_bit_or", std::make_unique<TPgAggFactory_bit_or>());
registry.emplace("pg_bit_xor", std::make_unique<TPgAggFactory_bit_xor>());
registry.emplace("pg_range_intersect_agg", std::make_unique<TPgAggFactory_range_intersect_agg>());
