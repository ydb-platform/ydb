#include "kernels_wrapper.h"
#include "program_constructor.h"

namespace NKikimr::NTxUT {

TProgramProtoBuilder::TProgramProtoBuilder() {
    TKernelsWrapper kernels;
    kernels.Add(NYql::TKernelRequestBuilder::EBinaryOp::EndsWith, true);
    Proto.SetKernels(kernels.Serialize());
}

ui32 TProgramProtoBuilder::AddConstant(const TString& bytes) {
    auto* command = Proto.AddCommand();
    auto* constantProto = command->MutableAssign()->MutableConstant();
    constantProto->SetBytes(bytes);
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

ui32 TProgramProtoBuilder::AddOperation(const NYql::TKernelRequestBuilder::EBinaryOp op, const std::vector<ui32>& arguments) {
    auto* command = Proto.AddCommand();
    auto* functionProto = command->MutableAssign()->MutableFunction();
    functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
    functionProto->SetKernelIdx(0);
    functionProto->SetYqlOperationId((ui32)op);
    for (auto&& i : arguments) {
        functionProto->AddArguments()->SetId(i);
    }
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

void TProgramProtoBuilder::AddFilter(const ui32 colId) {
    auto* command = Proto.AddCommand();
    command->MutableFilter()->MutablePredicate()->SetId(colId);
}

}   // namespace NKikimr::NTxUT
