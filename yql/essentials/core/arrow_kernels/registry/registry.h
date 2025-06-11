#pragma once
#include <arrow/compute/kernel.h>
#include <memory>
#include <vector>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NYql {

std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>> LoadKernels(const TString& serialized,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    const NKikimr::NMiniKQL::TComputationNodeFactory& nodeFactory,
    TLangVersion langver = MinLangVersion);

}

