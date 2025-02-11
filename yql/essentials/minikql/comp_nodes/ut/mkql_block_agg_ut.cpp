

#include "mkql_computation_node_ut.h"

#include "mkql_block_map_join_ut_utils.h"

#include "yql/essentials/minikql/computation/mkql_block_builder.h"
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <yql/essentials/minikql/comp_nodes/mkql_block_agg_factory.h>

namespace NKikimr::NMiniKQL {

namespace {

TVector<NUdf::TUnboxedValue> BatchToBlocks(TComputationContext& ctx,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values
) {
    const auto maxLength = CalcBlockLen(std::accumulate(types.cbegin(), types.cend(), 0ULL,
        [](size_t max, const TType* type) {
            return std::max(max, CalcMaxBlockItemSize(type));
        }));
    TVector<std::unique_ptr<IArrayBuilder>> builders;
    std::transform(types.cbegin(), types.cend(), std::back_inserter(builders),
        [&](const auto& type) {
            return MakeArrayBuilder(TTypeInfoHelper(), type, ctx.ArrowMemoryPool,
                                    maxLength, &ctx.Builder->GetPgBuilder());
        });
    const auto& holderFactory = ctx.HolderFactory;
    const size_t width = types.size();
    const size_t total = values.GetListLength();
    NUdf::TUnboxedValue iterator = values.GetListIterator();
    NUdf::TUnboxedValue current;
    size_t converted = 0;
    while (converted < total) {
        for (size_t i = 0; iterator.Next(current); i++, converted++) {
            (void)i;
            for (size_t j = 0; j < builders.size(); j++) {
                const NUdf::TUnboxedValuePod& item = current.GetElement(j);
                builders[j]->Add(item);
            }
        }
    }
    TVector<NUdf::TUnboxedValue> result;
    result.reserve(width);
    for (size_t i = 0; i < width; i++) {
        result.push_back(holderFactory.CreateArrowBlock(builders[i]->Build(converted >= total)));
    }
    return result;
}

struct TTypeMapperBase {
    TProgramBuilder& Pb;
    TType* ItemType;
    auto GetType() { return ItemType; }
};

template <typename Type>
struct TTypeMapper: TTypeMapperBase {
    TTypeMapper(TProgramBuilder& pb): TTypeMapperBase {pb, pb.NewDataType(NUdf::TDataType<Type>::Id) } {}
    auto GetValue(const Type& value) {
        return Pb.NewDataLiteral<Type>(value);
    }
};

template <>
struct TTypeMapper<TString>: TTypeMapperBase {
    TTypeMapper(TProgramBuilder& pb): TTypeMapperBase {pb, pb.NewDataType(NUdf::EDataSlot::String)} {}
    auto GetValue(const TString& value) {
        return Pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
    }
};

template <>
struct TTypeMapper<NYql::NDecimal::TInt128>: TTypeMapperBase {
    TTypeMapper(TProgramBuilder& pb, ui8 precision = NYql::NDecimal::MaxPrecision, ui8 scale = 10)
        : TTypeMapperBase {pb, pb.NewDecimalType(precision, scale)}
        , Precision(precision)
        , Scale(scale)
    {}

    auto GetValue(const NYql::NDecimal::TInt128& value) {
        return Pb.NewDecimalLiteral(value, Precision, Scale);
    }

    const ui8 Precision;
    const ui8 Scale;
};

template <typename TNested>
class TTypeMapper<std::optional<TNested>>: TTypeMapper<TNested> {
    using TBase = TTypeMapper<TNested>;
public:
    TTypeMapper(TProgramBuilder& pb): TBase(pb) {}
    auto GetType() { return TBase::Pb.NewOptionalType(TBase::GetType()); }
    auto GetValue(const std::optional<TNested>& value) {
        if (value == std::nullopt) {
            return TBase::Pb.NewEmptyOptional(GetType());
        } else {
            return TBase::Pb.NewOptional(TBase::GetValue(*value));
        }
    }
};

template<typename TPrimitive>
class TValueExtracter {
public:
    static TPrimitive Get(NUdf::TUnboxedValue value) {
        return value.Get<TPrimitive>();
    }
};

template<typename TNested>
class TValueExtracter<std::optional<TNested>> {
public:
    static std::optional<TNested> Get(NUdf::TUnboxedValue value) {
        if (value.HasValue()) {
            return TValueExtracter<TNested>::Get(value);
        } else {
            return std::nullopt;
        }
    }
};

template<typename... TNested>
class TValueExtracter<std::tuple<TNested...>> {
public:
    static std::tuple<TNested...> Get(NUdf::TUnboxedValue value) {
        return GetImpl(value, std::make_integer_sequence<size_t, sizeof...(TNested)>());
    }

private:
    template<size_t... I>
    static std::tuple<TNested...> GetImpl(NUdf::TUnboxedValue value, std::index_sequence<I...>) {
        return std::tuple<TNested...>{value.GetElement(I).Get<TNested>()...};
    }
};

template<typename Type>
const std::vector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const std::vector<Type>& vector
) {
    TTypeMapper<Type> mapper(pb);

    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto value) {
            return mapper.GetValue(value);
        });

    return {pb.NewList(mapper.GetType(), listItems)};
}

template<typename Type, typename... Tail>
const std::vector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const std::vector<Type>& vector, Tail... vectors
) {
    const auto frontList = BuildListNodes(pb, vector);
    const auto tailLists = BuildListNodes(pb, std::forward<Tail>(vectors)...);
    TVector<const TRuntimeNode> lists;
    lists.reserve(tailLists.size() + 1);
    lists.push_back(frontList.front());;
    for (const auto& list : tailLists) {
        lists.push_back(list);
    }
    return lists;
}

// template<typename... TVectors>
// const std::vector<NUdf::TUnboxedValue> ConvertVectorsToTuples(
//     TSetup<false>& setup, TVectors... vectors
// ) {
//     TProgramBuilder& pb = *setup.PgmBuilder;
//     const auto lists = BuildListNodes(pb, std::forward<TVectors>(vectors)...);
//     const auto tuplesNode = pb.Zip(lists);
//     const auto tuplesNodeType = tuplesNode.GetStaticType();
//     const auto tupleType = AS_TYPE(TListType, tuplesNodeType)->GetItemType();
//     const auto types = AS_TYPE(TTupleType, tupleType)->GetElements();
//     const auto tuples = setup.BuildGraph(tuplesNode)->GetValue();
//     const auto ctxProvider = setup.BuildGraph(pb.NewDataLiteral(0), {});
//     return BatchToBlocks(ctxProvider->GetContext(), types, tuples);
// }

template<typename... TArgs>
class TSchema {
public:
    TSchema(TProgramBuilder& pb)
    : Pb_(pb)
    , Types_({TTypeMapper<TArgs>(pb).GetType()...})
    {}

    TTupleType* AsTuple() const {
        return AS_TYPE(TTupleType, Pb_.NewTupleType(Types_));
    }

    const std::vector<TType*>& AsVector() const {
        return Types_;
    }

private:
    TProgramBuilder& Pb_;
    std::vector<TType*> Types_;
};

class TAggregatorStateBase {
public:
    void* GetData() {
        return Data_.data();
    }

    const void* GetData() const {
        return Data_.data();
    }

protected:
    bool InitData(ui32 stateSize) {
        if (!Inited_) {
            Data_.resize(stateSize);
            Inited_ = true;
            return false;
        } else {
            return true;
        }
    }

private:
    std::vector<char> Data_;
    bool Inited_ = false;
};


template<typename TTag, typename TResult>
class TAggregatorState;

template<typename TResult>
class TAggregatorState<TCombineAllTag, TResult> : public TAggregatorStateBase {
public:
    void AddMany(std::unique_ptr<TCombineAllTag::TAggregator>& aggregator, const std::vector<NUdf::TUnboxedValue>& columns, ui64 batchLength, std::optional<ui64> filtered) {
        if (InitData(aggregator->StateSize)) {
            aggregator->InitState(GetData());
        }
        aggregator->AddMany(GetData(), columns.data(), batchLength, filtered);
    }
};

template<typename TResult>
class TAggregatorState<TCombineKeysTag, TResult> : public TAggregatorStateBase {
public:
    void UpdateKey(std::unique_ptr<TCombineKeysTag::TAggregator>& aggregator, ui64 batchNum, const std::vector<NUdf::TUnboxedValue>& columns, ui64 row) {
        if (InitData(aggregator->StateSize)) {
            aggregator->InitKey(GetData(), batchNum, columns.data(), row);
        } else {
            aggregator->UpdateKey(GetData(), batchNum, columns.data(), row);
        }
    }
};

template<typename TResult>
class TAggregatorState<TFinalizeKeysTag, TResult> : public TAggregatorStateBase {
public:
    void UpdateState(std::unique_ptr<TFinalizeKeysTag::TAggregator>& aggregator, ui64 batchNum, const std::vector<NUdf::TUnboxedValue>& columns, ui64 row) {
        if (InitData(aggregator->StateSize)) {
            aggregator->LoadState(GetData(), batchNum, columns.data(), row);
        } else {
            aggregator->UpdateState(GetData(), batchNum, columns.data(), row);
        }
    }

    void SerializeState(std::unique_ptr<TFinalizeKeysTag::TAggregator>& aggregator, NUdf::TOutputBuffer& buffer) {
        aggregator->SerializeState(GetData(), buffer);
    }

    void DeserializeState(std::unique_ptr<TFinalizeKeysTag::TAggregator>& aggregator, NUdf::TInputBuffer& buffer) {
        InitData(aggregator->StateSize);
        aggregator->DeserializeState(GetData(), buffer);
    }

    TResult GetResult(std::unique_ptr<TFinalizeKeysTag::TAggregator>& aggregator) {
        auto resultBuilder = aggregator->MakeResultBuilder(aggregator->StateSize);
        resultBuilder->Add(GetData());
        auto result = resultBuilder->Build();
        return TValueExtracter<TResult>::Get(result);
    }
};

template<typename TTag, typename TResult>
class TAggregatorBase {
public:
    using TState = TAggregatorState<TTag, TResult>;

public:


protected:
    std::unique_ptr<typename TTag::TAggregator> Aggregator_;
};

template<typename TTag, typename TTypes, typename TResult>
class TAggregator;

template<typename TTypes, typename TResult>
class TAggregator<TCombineAllTag, TTypes, TResult> : public TAggregatorBase<TCombineAllTag, TResult> {
    using TAggregatorBase<TCombineAllTag, TResult>::Aggregator_;
    using typename TAggregatorBase<TCombineAllTag, TResult>::TState;

public:
    TAggregator(TProgramBuilder& pb, TComputationContext& ctx, const TTypes& schema, TStringBuf name, std::optional<ui32> filterColumn, std::vector<ui32> argsColumns) {
        std::vector<TType*> types = schema.AsVector();
        std::vector<TType*> blockedTypes;
        std::transform(types.begin(), types.end(), std::back_inserter(blockedTypes), [&](TType* const type) {
            return pb.NewBlockType(type, TBlockType::EShape::Many);
        });
        auto blockedSchema = TTupleType::Create(blockedTypes.size(), blockedTypes.data(), pb.GetTypeEnvironment());
        auto prepared = GetBlockAggregatorFactory(name).PrepareCombineAll(blockedSchema, filterColumn, argsColumns, pb.GetTypeEnvironment());
        Aggregator_ = prepared->Make(ctx);
    }

    void AddBatchTo(TState& state, const std::vector<NUdf::TUnboxedValue>& columns, ui64 batchLength, std::optional<ui64> filtered) {
        state.AddMany(Aggregator_, columns, batchLength, filtered);
    }
};

template<typename TTypes, typename TResult>
class TAggregator<TCombineKeysTag, TTypes, TResult> : public TAggregatorBase<TCombineKeysTag, TResult> {
    using TAggregatorBase<TCombineAllTag, TResult>::Aggregator_;
    using typename TAggregatorBase<TCombineAllTag, TResult>::TState;

public:
    TAggregator(TProgramBuilder& pb, TComputationContext& ctx, const TTypes& schema, TStringBuf name, std::vector<ui32> argsColumns) {
        std::vector<TType*> types = schema.AsVector();
        std::vector<TType*> blockedTypes;
        std::transform(types.begin(), types.end(), std::back_inserter(blockedTypes), [&](TType* const type) {
            return pb.NewBlockType(type, TBlockType::EShape::Many);
        });
        auto blockedSchema = TTupleType::Create(blockedTypes.size(), blockedTypes.data(), pb.GetTypeEnvironment());
        auto prepared = GetBlockAggregatorFactory(name).PrepareCombineKeys(blockedSchema, argsColumns, pb.GetTypeEnvironment());
        Aggregator_ = prepared->Make(ctx);
    }

    void AddBatchTo(TState& state, ui64 batchNum, const std::vector<NUdf::TUnboxedValue>& columns, ui64 batchLength) {
        for (size_t row = 0; row < batchLength; ++row) {
            state.UpdateKey(Aggregator_, batchNum, columns, row);
        }
    }
};

template<typename TTypes, typename TResult>
class TAggregator<TFinalizeKeysTag, TTypes, TResult> : public TAggregatorBase<TFinalizeKeysTag, TResult> {
    using TAggregatorBase<TFinalizeKeysTag, TResult>::Aggregator_;
    using typename TAggregatorBase<TFinalizeKeysTag, TResult>::TState;

public:
    TAggregator(TProgramBuilder& pb, TComputationContext& ctx, const TTypes& schema, TStringBuf name, std::vector<ui32> argsColumns, ui32 hint) {
        std::vector<TType*> types = schema.AsVector();
        std::vector<TType*> blockedTypes;
        std::transform(types.begin(), types.end(), std::back_inserter(blockedTypes), [&](TType* const type) {
            return pb.NewBlockType(type, TBlockType::EShape::Many);
        });
        auto blockedSchema = TTupleType::Create(blockedTypes.size(), blockedTypes.data(), pb.GetTypeEnvironment());
        TTypeMapper<TResult> mapper(pb);
        auto prepared = GetBlockAggregatorFactory(name).PrepareFinalizeKeys(blockedSchema, argsColumns, pb.GetTypeEnvironment(), mapper.GetType(), hint);
        Aggregator_ = prepared->Make(ctx);
    }

    void AddBatchTo(TState& state, ui64 batchNum, const std::vector<NUdf::TUnboxedValue>& columns, ui64 batchLength) {
        for (size_t row = 0; row < batchLength; ++row) {
            state.UpdateState(Aggregator_, batchNum, columns, row);
        }
    }

    void SerializeState(TState& state, NUdf::TOutputBuffer& buffer) {
        state.SerializeState(Aggregator_, buffer);
    }

    void DeserializeState(TState& state, NUdf::TInputBuffer& buffer) {
        state.DeserializeState(Aggregator_, buffer);
    }

    TResult GetResult(TState& state) {
        return state.GetResult(Aggregator_);
    }
};

}

Y_UNIT_TEST_SUITE(TBlockAggregatorsTest) {
    Y_UNIT_TEST(TestSerdeState_FinalizeKeys_Count) {
        using TTypes = TSchema<std::optional<i32>>;
        using TResult = ui64;
        using TAgg = TAggregator<TFinalizeKeysTag, TTypes, TResult>;
        using TState = TAggregatorState<TFinalizeKeysTag, TResult>;

        TSetup<false> setup;
        auto& pb = *setup.PgmBuilder;

        const auto schema = TTypes{pb};

        const std::vector<std::optional<i32>> values1 = {1, 2};
        const auto columns1 = ConvertVectorsToTuples(setup, values1).second;
        const std::vector<std::optional<i32>> values2 = {1, 2};
        const auto columns2 = ConvertVectorsToTuples(setup, values2);

        auto ctxProviderGraph = setup.BuildGraph(pb.NewDataLiteral(0), {});
        auto& ctx = ctxProviderGraph->GetContext();

        std::array<TAgg, 2> aggs = {
            TAgg{pb, ctx, schema, "count", {0}, 0},
            TAgg{pb, ctx, schema, "count_all", {0}, 0},
        };

        std::array<TState, 2> refStates;
        std::array<TState, 2> toSerStates;

        // Apply values1 to reference states
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].AddBatchTo(refStates[i], 0, columns1, values1.size());
        }
        // Apply values1 to states that will be serialized
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].AddBatchTo(toSerStates[i], 0, columns1, values1.size());
        }

        NUdf::TOutputBuffer outputBuffer;
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].SerializeState(toSerStates[i], outputBuffer);
        }
        auto serialized = outputBuffer.Finish();

        auto inputBuffer = NUdf::TInputBuffer(serialized);
        // Deserialize all states from UnboxedValue
        std::array<TState, 2> deserStates;
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].DeserializeState(deserStates[i], inputBuffer);
        }

        // Apply values2 to reference states
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].AddBatchTo(refStates[i], 0, columns2, values2.size());
        }
        // Apply values2 to states that was serialized
        for (size_t i = 0; i < 2; ++i) {
            aggs[i].AddBatchTo(deserStates[i], 0, columns2, values2.size());
        }

        // Verify that reference state value is equal to state that was serialized during processing
        for (size_t i = 0; i < 2; ++i) {
            auto expected = aggs[i].GetResult(refStates[i]);
            auto actual = aggs[i].GetResult(deserStates[i]);
            UNIT_ASSERT_EQUAL(expected, actual);
        }
    }
}

}


