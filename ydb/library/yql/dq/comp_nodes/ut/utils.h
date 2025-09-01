#pragma once

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

namespace NKikimr {
namespace NMiniKQL {

// TODO (mfilitov): think how can we reuse the code
// Code from https://github.com/ydb-platform/ydb/blob/main/yql/essentials/minikql/comp_nodes/ut/mkql_block_map_join_ut_utils.h

// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list);

// Stream<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

// List<Tuple<...>> -> WideFlow
TRuntimeNode ToWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode list);

// WideFlow -> List<Tuple<...>>
TRuntimeNode FromWideFlow(TProgramBuilder& pgmBuilder, TRuntimeNode wideFlow);

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb, const TVector<Type>& vector);

template<typename Type, typename... Tail>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb, const TVector<Type>& vector, Tail... vectors);

template<typename... TVectors>
const std::pair<TType*, NUdf::TUnboxedValue> ConvertVectorsToTuples(TSetup<false>& setup, TVectors... vectors);

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list); 

void CompareResults(const TType* type, const NUdf::TUnboxedValue& expected, const NUdf::TUnboxedValue& got);

}
}
