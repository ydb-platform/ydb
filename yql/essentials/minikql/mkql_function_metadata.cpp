#include "mkql_function_metadata.h"

namespace NKikimr::NMiniKQL {

TKernelFamilyBase::TKernelFamilyBase(const arrow::compute::FunctionOptions* functionOptions)
    : TKernelFamily(functionOptions)
{
}

const TKernel* TKernelFamilyBase::FindKernel(const NUdf::TDataTypeId* argTypes, size_t argTypesCount, NUdf::TDataTypeId returnType) const {
    std::vector<NUdf::TDataTypeId> args(argTypes, argTypes + argTypesCount);
    auto it = KernelMap_.find({args, returnType});
    if (it == KernelMap_.end()) {
        return nullptr;
    }

    return it->second.get();
}

TVector<const TKernel*> TKernelFamilyBase::GetAllKernels() const {
    TVector<const TKernel*> ret;
    for (const auto& k : KernelMap_) {
        ret.emplace_back(k.second.get());
    }

    return ret;
}

void TKernelFamilyBase::Adopt(const std::vector<NUdf::TDataTypeId>& argTypes, NUdf::TDataTypeId returnType, std::unique_ptr<TKernel>&& kernel) {
    KernelMap_.emplace(std::make_pair(argTypes, returnType), std::move(kernel));
}

} // namespace NKikimr::NMiniKQL
