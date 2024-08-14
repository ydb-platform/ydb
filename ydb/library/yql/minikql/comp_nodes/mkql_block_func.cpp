#include "mkql_block_func.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

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

class TBlockBitCastWrapper : public TBlockFuncNode {
public:
    TBlockBitCastWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* argType, TType* to)
        : TBlockFuncNode(mutables, "BitCast", { arg }, { argType }, ResolveKernel(argType, to), {}, &CastOptions)
        , CastOptions(false)
    {
    }
private:
    static const arrow::compute::ScalarKernel& ResolveKernel(TType* from, TType* to) {
        std::shared_ptr<arrow::DataType> type;
        MKQL_ENSURE(ConvertArrowType(to, type), "can't get arrow type");

        auto function = ARROW_RESULT(arrow::compute::GetCastFunction(type));
        MKQL_ENSURE(function != nullptr, "missing function");
        MKQL_ENSURE(function->kind() == arrow::compute::Function::SCALAR, "expected SCALAR function");

        std::vector<arrow::ValueDescr> args = { ToValueDescr(from) };
        const auto kernel = ARROW_RESULT(function->DispatchExact(args));
        return *static_cast<const arrow::compute::ScalarKernel*>(kernel);
    }

    const arrow::compute::CastOptions CastOptions;
};

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
        auto arrowKernel = kernel.MakeArrowKernel();
        return new TBlockFuncNode(ctx.Mutables, funcName, std::move(argsNodes), argsTypes, *arrowKernel, arrowKernel, kernel.Family.FunctionOptions);
    } else {
        return new TBlockFuncNode(ctx.Mutables, funcName, std::move(argsNodes), argsTypes, kernel.GetArrowKernel(), {}, kernel.Family.FunctionOptions);
    }
}

IComputationNode* WrapBlockBitCast(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    auto argNode = LocateNode(ctx.NodeLocator, callable, 0);
    MKQL_ENSURE(callable.GetInput(1).GetStaticType()->IsType(), "Expected type");
    return new TBlockBitCastWrapper(ctx.Mutables,
        argNode,
        callable.GetType()->GetArgumentType(0),
        static_cast<TType*>(callable.GetInput(1).GetNode())
    );
}

}
}
