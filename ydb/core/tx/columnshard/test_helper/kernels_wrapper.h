#pragma once
#include <yql/essentials/core/arrow_kernels/request/request.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NTxUT {

class TKernelsWrapper {
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> Reg;
    std::unique_ptr<NYql::TKernelRequestBuilder> ReqBuilder;
    NYql::TExprContext Ctx;

public:
    TKernelsWrapper();

    ui32 Add(NYql::TKernelRequestBuilder::EBinaryOp operation, bool scalar = false);

    ui32 AddJsonExists(bool isBinaryType = true);

    ui32 AddJsonValue(bool isBinaryType = true, NYql::EDataSlot resultType = NYql::EDataSlot::Utf8);

    TString Serialize();
};

}   //namespace NKikimr::NTxUT
