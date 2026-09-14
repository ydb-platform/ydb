#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* AddMember(const TComputationNodeFactoryContext& ctx, TRuntimeNode structData, TRuntimeNode memberData, TRuntimeNode indexData);

} // namespace NKikimr::NMiniKQL
