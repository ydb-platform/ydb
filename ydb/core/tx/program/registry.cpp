#include "registry.h"

#include <ydb/library/yql/core/arrow_kernels/registry/registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <util/system/tls.h>

namespace NKikimr::NOlap {

::NTls::TValue<TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry>> Registry;

bool TKernelsRegistry::Parse(const TString& serialized) {
    Y_ABORT_UNLESS(!!serialized);
    if (!Registry.Get()) {
        auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
        NMiniKQL::FillStaticModules(*registry.Get());
        Registry = std::move(registry);
    }

    auto nodeFactory = NMiniKQL::GetBuiltinFactory();
    auto kernels =  NYql::LoadKernels(serialized, *Registry.Get(), nodeFactory);
    Kernels.swap(kernels);
    for (const auto& kernel : Kernels) {
        arrow::compute::Arity arity(kernel->signature->in_types().size(), kernel->signature->is_varargs());
        auto func = std::make_shared<arrow::compute::ScalarFunction>("local_function", arity, nullptr);
        auto res = func->AddKernel(*kernel);
        if (!res.ok()) {
            return false;
        }
        Functions.push_back(func);
    }
    return true;
}   

NKikimr::NSsa::TFunctionPtr TKernelsRegistry::GetFunction(const size_t index) const {
    if (index < Functions.size()) {
        return Functions[index];
    }
    return nullptr;
}

}
