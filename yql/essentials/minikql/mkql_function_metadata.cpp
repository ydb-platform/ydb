#include "mkql_function_metadata.h"

namespace NKikimr {

namespace NMiniKQL {

TKernelFamilyBase::TKernelFamilyBase(const arrow::compute::FunctionOptions* functionOptions)
    : TKernelFamily(functionOptions)
{}

const TKernel* TKernelFamilyBase::FindKernel(const NUdf::TDataTypeId* argTypes, size_t argTypesCount, NUdf::TDataTypeId returnType) const {
    std::vector<NUdf::TDataTypeId> args(argTypes, argTypes + argTypesCount);
    auto it = KernelMap.find({ args, returnType });
    if (it == KernelMap.end()) {
        return nullptr;
    }

    return it->second.get();
}

TVector<const TKernel*> TKernelFamilyBase::GetAllKernels() const {
    TVector<const TKernel*> ret;
    for (const auto& k : KernelMap) {
        ret.emplace_back(k.second.get());
    }

    return ret;
}

void TKernelFamilyBase::Adopt(const std::vector<NUdf::TDataTypeId>& argTypes, NUdf::TDataTypeId returnType, std::unique_ptr<TKernel>&& kernel) {
    KernelMap.emplace(std::make_pair(argTypes, returnType), std::move(kernel));
}

}
}
