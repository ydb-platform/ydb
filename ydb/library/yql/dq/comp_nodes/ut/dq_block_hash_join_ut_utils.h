#pragma once

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetNodeFactory();

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector
);

template<typename... TVectors>
const std::pair<TType*, NUdf::TUnboxedValue> ConvertVectorsToTuples(
    TSetup<false>& setup, TVectors... vectors
);

// Специализации для базовых типов
template<>
const TVector<const TRuntimeNode> BuildListNodes<ui64>(TProgramBuilder& pb,
    const TVector<ui64>& vector
) {
    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto value) {
            return pb.NewDataLiteral<ui64>(value);
        });

    return {pb.NewList(pb.NewDataType(NUdf::TDataType<ui64>::Id), listItems)};
}

template<>
const TVector<const TRuntimeNode> BuildListNodes<TString>(TProgramBuilder& pb,
    const TVector<TString>& vector
) {
    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto& value) {
            return pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
        });

    return {pb.NewList(pb.NewDataType(NUdf::EDataSlot::String), listItems)};
}

template<typename Type, typename... Tail>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector, Tail... vectors
) {
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

template<typename... TVectors>
const std::pair<TType*, NUdf::TUnboxedValue> ConvertVectorsToTuples(
    TSetup<false>& setup, TVectors... vectors
) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto lists = BuildListNodes(pb, std::forward<TVectors>(vectors)...);
    const auto tuplesNode = pb.Zip(lists);
    const auto tuplesNodeType = tuplesNode.GetStaticType();
    const auto tuples = setup.BuildGraph(tuplesNode)->GetValue();
    return std::make_pair(tuplesNodeType, tuples);
}

} // namespace NMiniKQL
} // namespace NKikimr 