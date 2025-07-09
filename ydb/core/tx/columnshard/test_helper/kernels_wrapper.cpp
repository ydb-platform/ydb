#include "kernels_wrapper.h"
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr::NTxUT {

TKernelsWrapper::TKernelsWrapper() {
    auto reg = CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
    NMiniKQL::FillStaticModules(*reg);
    Reg.Reset(reg.Release());
    ReqBuilder = std::make_unique<NYql::TKernelRequestBuilder>(*Reg);
}

ui32 TKernelsWrapper::Add(NYql::TKernelRequestBuilder::EBinaryOp operation, bool scalar /*= false*/) {
    switch (operation) {
        case NYql::TKernelRequestBuilder::EBinaryOp::And: {
            auto blockResultType =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
            if (scalar) {
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::And, blockResultType, blockResultType, blockResultType);
            } else {
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::And, blockResultType, blockResultType, blockResultType);
            }
        }
        case NYql::TKernelRequestBuilder::EBinaryOp::Or: {
            auto blockResultType =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
            if (scalar) {
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Or, blockResultType, blockResultType, blockResultType);
            } else {
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Or, blockResultType, blockResultType, blockResultType);
            }
        }
        case NYql::TKernelRequestBuilder::EBinaryOp::Add: {
            auto blockInt32Type =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32));
            if (scalar) {
                auto scalarInt32Type =
                    Ctx.template MakeType<NYql::TScalarExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int32));
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, scalarInt32Type, blockInt32Type);
            } else {
                return ReqBuilder->AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Add, blockInt32Type, blockInt32Type, blockInt32Type);
            }
        }
        case NYql::TKernelRequestBuilder::EBinaryOp::StartsWith:
        case NYql::TKernelRequestBuilder::EBinaryOp::EndsWith: {
            auto blockStringType =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
            auto blockBoolType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
            if (scalar) {
                auto scalarStringType =
                    Ctx.template MakeType<NYql::TScalarExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::String));
                return ReqBuilder->AddBinaryOp(operation, blockStringType, scalarStringType, blockBoolType);
            } else {
                return ReqBuilder->AddBinaryOp(operation, blockStringType, blockStringType, blockBoolType);
            }
        }
        case NYql::TKernelRequestBuilder::EBinaryOp::StringContains: {
            auto blockStringType =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::String));
            auto blockBoolType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
            return ReqBuilder->AddBinaryOp(
                NYql::TKernelRequestBuilder::EBinaryOp::StringContains, blockStringType, blockStringType, blockBoolType);
        }
        case NYql::TKernelRequestBuilder::EBinaryOp::Equals:
        case NYql::TKernelRequestBuilder::EBinaryOp::NotEquals: {
            auto blockLeftType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Int16));
            auto blockRightType =
                Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Float));
            auto blockBoolType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool));
            return ReqBuilder->AddBinaryOp(operation, blockLeftType, blockRightType, blockBoolType);
        }
        default:
            Y_ABORT("Not implemented");
    }
}

ui32 TKernelsWrapper::AddJsonExists(bool isBinaryType /*= true*/) {
    auto blockOptJsonType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TOptionalExprType>(
        Ctx.template MakeType<NYql::TDataExprType>(isBinaryType ? NYql::EDataSlot::JsonDocument : NYql::EDataSlot::Json)));
    auto scalarStringType = Ctx.template MakeType<NYql::TScalarExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
    auto blockBoolType = Ctx.template MakeType<NYql::TBlockExprType>(
        Ctx.template MakeType<NYql::TOptionalExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Bool)));

    return ReqBuilder->JsonExists(blockOptJsonType, scalarStringType, blockBoolType);
}

ui32 TKernelsWrapper::AddJsonValue(bool isBinaryType /*= true*/, NYql::EDataSlot resultType /*= NYql::EDataSlot::Utf8*/) {
    auto blockOptJsonType = Ctx.template MakeType<NYql::TBlockExprType>(Ctx.template MakeType<NYql::TOptionalExprType>(
        Ctx.template MakeType<NYql::TDataExprType>(isBinaryType ? NYql::EDataSlot::JsonDocument : NYql::EDataSlot::Json)));
    auto scalarStringType = Ctx.template MakeType<NYql::TScalarExprType>(Ctx.template MakeType<NYql::TDataExprType>(NYql::EDataSlot::Utf8));
    auto blockResultType = Ctx.template MakeType<NYql::TBlockExprType>(
        Ctx.template MakeType<NYql::TOptionalExprType>(Ctx.template MakeType<NYql::TDataExprType>(resultType)));

    return ReqBuilder->JsonValue(blockOptJsonType, scalarStringType, blockResultType);
}

TString TKernelsWrapper::Serialize() {
    return ReqBuilder->Serialize();
}

}   // namespace NKikimr::NTxUT
