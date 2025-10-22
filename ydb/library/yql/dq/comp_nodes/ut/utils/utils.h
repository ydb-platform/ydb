#pragma once
#include "dq_setup.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

// TODO (mfilitov): think how can we reuse the code
// Code from
// https://github.com/ydb-platform/ydb/blob/main/yql/essentials/minikql/comp_nodes/ut/mkql_block_map_join_ut_utils.h

// List<Tuple<T1, ..., Tn, Tlast>> -> List<Struct<"0": Block<T1>, ..., "n": Block<Tn>, "_yql_block_length":
// Scalar<Tlast>>>
TRuntimeNode ToBlockList(TProgramBuilder& pgmBuilder, TRuntimeNode list);

NUdf::TUnboxedValuePod ToBlocks(TComputationContext& ctx, size_t blockSize, const TArrayRef<TType* const> types,
                                const NUdf::TUnboxedValuePod& values);

TType* MakeBlockTupleType(TProgramBuilder& pgmBuilder, TType* tupleType, bool scalar);
TType* MakeJoinType(TDqProgramBuilder& pgmBuilder, EJoinKind joinKind, TType* leftStreamType,
                    const TVector<ui32>& leftKeyDrops, TType* rightListType, const TVector<ui32>& rightKeyDrops);

// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list);

// Stream<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

// List<Tuple<...>> -> WideFlow
TRuntimeNode ToWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode list);

// WideFlow -> List<Tuple<...>>
TRuntimeNode FromWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode wideFlow);

// Stream<Multi<...>> -> Stream<Tuple<...>>
TRuntimeNode FromWideStreamToTupleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list);

TVector<NUdf::TUnboxedValue> ConvertWideStreamToTupleVector(IComputationGraph& wideStream, size_t tupleSize);


struct TypeAndValue {
    TType* Type;
    NUdf::TUnboxedValue Value;
};

void CompareListsIgnoringOrder(const TType* type, const NUdf::TUnboxedValue& expected,
                               const NUdf::TUnboxedValue& gotList);
void CompareListAndStreamIgnoringOrder(const TypeAndValue& expected, IComputationGraph& gotStream);
void CompareListAndBlockStreamIgnoringOrder(const TypeAndValue& expected, IComputationGraph& gitBlockStream);

template <typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb, const TVector<Type>& vector) {
    TType* itemType;
    if constexpr (std::is_same_v<Type, std::optional<TString>>) {
        itemType = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::String));
    } else if constexpr (std::is_same_v<Type, TString>) {
        itemType = pb.NewDataType(NUdf::EDataSlot::String);
    } else if constexpr (std::is_same_v<Type, std::optional<ui64>>) {
        itemType = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::Uint64));
    } else {
        itemType = pb.NewDataType(NUdf::TDataType<Type>::Id);
    }

    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems), [&](const auto value) {
        if constexpr (std::is_same_v<Type, std::optional<TString>>) {
            if (value == std::nullopt) {
                return pb.NewEmptyOptional(itemType);
            } else {
                return pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(*value));
            }
        } else if constexpr (std::is_same_v<Type, TString>) {
            return pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
        } else if constexpr (std::is_same_v<Type, std::optional<ui64>>) {
            if (value == std::nullopt) {
                return pb.NewEmptyOptional(itemType);
            } else {
                return pb.NewOptional(pb.NewDataLiteral<ui64>(*value));
            }
        } else {
            return pb.NewDataLiteral<Type>(value);
        }
    });

    return {pb.NewList(itemType, listItems)};
}

template <typename Type, typename... Tail>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb, const TVector<Type>& vector, Tail... vectors) {
    const auto frontList = BuildListNodes(pb, vector);
    const auto tailLists = BuildListNodes(pb, std::forward<Tail>(vectors)...);
    TVector<const TRuntimeNode> lists;
    lists.reserve(tailLists.size() + 1);
    lists.push_back(frontList.front());
    for (const auto& list : tailLists) {
        lists.push_back(list);
    }
    return lists;
}

template <typename... TVectors> TypeAndValue ConvertVectorsToTuples(TDqSetup<false>& setup, TVectors... vectors) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto lists = BuildListNodes(pb, std::forward<TVectors>(vectors)...);
    const auto tuplesNode = pb.Zip(lists);
    const auto tuplesNodeType = tuplesNode.GetStaticType();
    const auto tuples = setup.BuildGraph(tuplesNode)->GetValue();
    return {tuplesNodeType, tuples};
}

template <typename... TVectors>
std::pair<TArrayRef<TType* const>, NUdf::TUnboxedValue> ConvertVectorsToRuntimeTypesAndValue(TDqSetup<false>& setup,
                                                                                             TVectors... vectors) {
    auto p = ConvertVectorsToTuples(setup, vectors...);
    return std::make_pair(AS_TYPE(TTupleType, AS_TYPE(TListType, p.Type)->GetItemType())->GetElements(), p.Value);
}

} // namespace NMiniKQL
} // namespace NKikimr
