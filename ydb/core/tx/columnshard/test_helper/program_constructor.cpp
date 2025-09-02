#include "kernels_wrapper.h"
#include "program_constructor.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NTxUT {

ui32 TProgramProtoBuilder::AddConstant(const TString& bytes) {
    auto* command = Proto.AddCommand();
    auto* constantProto = command->MutableAssign()->MutableConstant();
    constantProto->SetBytes(bytes);
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

ui32 TProgramProtoBuilder::AddOperation(
    const NKikimrSSA::TProgram::TAssignment::EFunction op, const std::vector<ui32>& arguments) {
    auto* command = Proto.AddCommand();
    auto* functionProto = command->MutableAssign()->MutableFunction();
    for (auto&& i : arguments) {
        functionProto->AddArguments()->SetId(i);
    }
    functionProto->SetId(op);
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

ui32 TProgramProtoBuilder::AddOperation(const NYql::TKernelRequestBuilder::EBinaryOp op, const std::vector<ui32>& arguments) {
    auto it = KernelOperations.find(op);
    if (it == KernelOperations.end()) {
        it = KernelOperations.emplace(op, KernelOperations.size()).first;
        Kernels.Add(op, true);
    }

    auto* command = Proto.AddCommand();
    auto* functionProto = command->MutableAssign()->MutableFunction();
    functionProto->SetFunctionType(NKikimrSSA::TProgram::EFunctionType::TProgram_EFunctionType_YQL_KERNEL);
    functionProto->SetKernelIdx(it->second);
    functionProto->SetYqlOperationId((ui32)op);
    for (auto&& i : arguments) {
        functionProto->AddArguments()->SetId(i);
    }
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

ui32 TProgramProtoBuilder::AddOperation(const TString& kernelName, const std::vector<ui32>& arguments) {
    auto* command = Proto.AddCommand();
    auto* functionProto = command->MutableAssign()->MutableFunction();
    for (auto&& i : arguments) {
        functionProto->AddArguments()->SetId(i);
    }
    functionProto->SetId(NKikimrSSA::TProgram::TAssignment::FUNC_CMP_GREATER_EQUAL);
    functionProto->SetKernelName(kernelName);
    command->MutableAssign()->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

void TProgramProtoBuilder::AddFilter(const ui32 colId) {
    auto* command = Proto.AddCommand();
    command->MutableFilter()->MutablePredicate()->SetId(colId);
}

ui32 TProgramProtoBuilder::AddAggregation(
    const NArrow::NSSA::NAggregation::EAggregate op, const std::vector<ui32>& arguments, const std::vector<ui32>& groupByKeys) {
    auto* command = Proto.AddCommand();
    auto* groupBy = command->MutableGroupBy();
    auto* aggregate = groupBy->AddAggregates();
    for (auto&& i : arguments) {
        aggregate->MutableFunction()->AddArguments()->SetId(i);
    }
    for (auto&& i : groupByKeys) {
        groupBy->AddKeyColumns()->SetId(i);
    }
    aggregate->MutableFunction()->SetId(static_cast<ui32>(op));
    aggregate->MutableColumn()->SetId(++CurrentGenericColumnId);
    return CurrentGenericColumnId;
}

void TProgramProtoBuilder::AddProjection(const std::vector<ui32>& arguments) {
    auto* command = Proto.AddCommand();
    for (auto&& i : arguments) {
        command->MutableProjection()->AddColumns()->SetId(i);
    }
}

const NKikimrSSA::TProgram& TProgramProtoBuilder::FinishProto() {
    AFL_VERIFY(!Finished);
    Finished = true;
    Proto.SetKernels(Kernels.Serialize());
    return Proto;
}

const NKikimrSSA::TProgram& TProgramProtoBuilder::GetProto() const {
    AFL_VERIFY(Finished || KernelOperations.empty());
    return Proto;
}

}   // namespace NKikimr::NTxUT
