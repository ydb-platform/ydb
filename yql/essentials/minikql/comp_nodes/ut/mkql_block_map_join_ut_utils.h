#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

inline bool IsOptionalOrNull(const TType* type) {
    return type->IsOptional() || type->IsNull() || type->IsPg();
}

TType* MakeBlockTupleType(TProgramBuilder& pgmBuilder, TType* tupleType, bool scalar);

NUdf::TUnboxedValuePod ToBlocks(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values);
NUdf::TUnboxedValuePod MakeUint64ScalarBlock(TComputationContext& ctx, size_t blockSize,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values);
NUdf::TUnboxedValuePod FromBlocks(TComputationContext& ctx,
    const TArrayRef<TType* const> types, const NUdf::TUnboxedValuePod& values);

TComputationNodeFactory GetNodeFactory();
TRuntimeNode ThrottleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);
TRuntimeNode DethrottleStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list);
void CompareResults(const TType* type, const NUdf::TUnboxedValue& expected, const NUdf::TUnboxedValue& got);

TVector<TString> GenerateValues(size_t level);
TSet<ui64> GenerateFibonacci(size_t count);

//
// Auxiliary routines to build list nodes from the given vectors.
//

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

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector
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
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector, Tail... vectors
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
