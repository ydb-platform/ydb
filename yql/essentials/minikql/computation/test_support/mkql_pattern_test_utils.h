#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL::NTest {

TComputationNodeFactory GetListTestFactory();
TRuntimeNode CreateFlow(TProgramBuilder& pb, size_t vecSize, TCallable* list);
template <bool Wide>
TRuntimeNode CreateFilter(TProgramBuilder& pb, size_t vecSize, TCallable* list);
template <>
TRuntimeNode CreateFilter<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list);
template <>
TRuntimeNode CreateFilter<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list);

} // namespace NKikimr::NMiniKQL::NTest
