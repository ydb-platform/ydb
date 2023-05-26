#pragma once
#include <arrow/compute/kernel.h>
#include <memory>
#include <vector>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NYql {

std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>> LoadKernels(const TString& serialized,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    const NKikimr::NMiniKQL::TComputationNodeFactory& nodeFactory);

}