#include "mkql_block_func.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <arrow/compute/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

const TKernel& ResolveKernel(const IBuiltinFunctionRegistry& builtins, const TString& funcName, const TVector<TType*>& inputTypes, TType* returnType) {
    std::vector<NUdf::TDataTypeId> argTypes;
    for (const auto& t : inputTypes) {
        auto asBlockType = AS_TYPE(TBlockType, t);
        bool isOptional;
        auto dataType = UnpackOptionalData(asBlockType->GetItemType(), isOptional);
        argTypes.push_back(dataType->GetSchemeType());
    }

    NUdf::TDataTypeId returnTypeId;
    {
        auto asBlockType = AS_TYPE(TBlockType, returnType);
        bool isOptional;
        auto dataType = UnpackOptionalData(asBlockType->GetItemType(), isOptional);
        returnTypeId = dataType->GetSchemeType();
    }

    auto kernel = builtins.FindKernel(funcName, argTypes.data(), argTypes.size(), returnTypeId);
    MKQL_ENSURE(kernel, "Can't find kernel for " << funcName);
    return *kernel;
}

} // namespace

IComputationNode* WrapBlockFunc(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 arg");
    const auto funcNameData = AS_VALUE(TDataLiteral, callable.GetInput(0));
    const auto funcName = TString(funcNameData->AsValue().AsStringRef());
    TComputationNodePtrVector argsNodes;
    TVector<TType*> argsTypes;
    const auto callableType = callable.GetType();
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        argsNodes.push_back(LocateNode(ctx.NodeLocator, callable, i));
        argsTypes.push_back(callableType->GetArgumentType(i));
    }

    const TKernel& kernel = ResolveKernel(*ctx.FunctionRegistry.GetBuiltins(), funcName, argsTypes, callableType->GetReturnType());
    if (kernel.IsPolymorphic()) {
        auto arrowKernel = kernel.MakeArrowKernel(argsTypes, callableType->GetReturnType());
        return new TBlockFuncNode(ctx.Mutables, funcName, std::move(argsNodes), argsTypes, *arrowKernel, arrowKernel, kernel.Family.FunctionOptions);
    } else {
        return new TBlockFuncNode(ctx.Mutables, funcName, std::move(argsNodes), argsTypes, kernel.GetArrowKernel(), {}, kernel.Family.FunctionOptions);
    }
}

}
}
