#include "registry.h"

#include <util/system/tls.h>
#include <yql/essentials/core/arrow_kernels/registry/registry.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr::NArrow::NSSA {

::NTls::TValue<TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry>> Registry;

bool TKernelsRegistry::Parse(const TString& serialized) {
    Y_ABORT_UNLESS(!!serialized);
    if (!Registry.Get()) {
        auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
        NMiniKQL::FillStaticModules(*registry.Get());
        Registry = std::move(registry);
    }

    auto nodeFactory = NMiniKQL::GetBuiltinFactory();
    auto kernels = NYql::LoadKernels(serialized, *Registry.Get(), nodeFactory);
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

std::shared_ptr<arrow::compute::ScalarFunction> TKernelsRegistry::GetFunction(const size_t index) const {
    if (index < Functions.size()) {
        return Functions[index];
    }
    return nullptr;
}

}   // namespace NKikimr::NOlap::NSSA
