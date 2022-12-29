#include "mkql_block_func.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/mkql_functions.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec_internal.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/util/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

arrow::ValueDescr ToValueDescr(TType* type) {
    arrow::ValueDescr ret;
    MKQL_ENSURE(ConvertInputArrowType(type, ret), "can't get arrow type");
    return ret;
}

std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types) {
    std::vector<arrow::ValueDescr> res;
    res.reserve(types.size());
    for (const auto& type : types) {
        res.emplace_back(ToValueDescr(type));
    }

    return res;
}

const arrow::compute::ScalarKernel& ResolveKernel(const arrow::compute::Function& function, const std::vector<arrow::ValueDescr>& args) {
    const auto kernel = ARROW_RESULT(function.DispatchExact(args));
    return *static_cast<const arrow::compute::ScalarKernel*>(kernel);
}

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

struct TState : public TComputationValue<TState> {
    using TComputationValue::TComputationValue;

    TState(TMemoryUsageInfo* memInfo, const arrow::compute::FunctionOptions* options,
        const arrow::compute::ScalarKernel& kernel,
        const std::vector<arrow::ValueDescr>& argsValuesDescr, TComputationContext& ctx)
        : TComputationValue(memInfo)
        , Options(options)
        , ExecContext(&ctx.ArrowMemoryPool, nullptr, nullptr)
        , KernelContext(&ExecContext)
    {
        if (kernel.init) {
            State = ARROW_RESULT(kernel.init(&KernelContext, { &kernel, argsValuesDescr, options }));
            KernelContext.SetState(State.get());
        }

        Values.reserve(argsValuesDescr.size());
    }

    const arrow::compute::FunctionOptions* Options;
    arrow::compute::ExecContext ExecContext;
    arrow::compute::KernelContext KernelContext;
    std::unique_ptr<arrow::compute::KernelState> State;

    std::vector<arrow::Datum> Values;
};

class TBlockFuncWrapper : public TMutableComputationNode<TBlockFuncWrapper> {
public:
    TBlockFuncWrapper(TComputationMutables& mutables,
        const IBuiltinFunctionRegistry& builtins,
        const TString& funcName,
        TVector<IComputationNode*>&& argsNodes,
        TVector<TType*>&& argsTypes,
        TType* returnType)
        : TMutableComputationNode(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , FuncName(funcName)
        , ArgsNodes(std::move(argsNodes))
        , ArgsTypes(std::move(argsTypes))
        , ArgsValuesDescr(ToValueDescr(ArgsTypes))
        , Kernel(ResolveKernel(builtins, FuncName, ArgsTypes, returnType))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        state.Values.clear();
        for (ui32 i = 0; i < ArgsNodes.size(); ++i) {
            state.Values.emplace_back(TArrowBlock::From(ArgsNodes[i]->GetValue(ctx)).GetDatum());
            Y_VERIFY_DEBUG(ArgsValuesDescr[i] == state.Values.back().descr());
        }

        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
        ARROW_OK(executor->Init(&state.KernelContext, { &Kernel.GetArrowKernel(), ArgsValuesDescr, state.Options }));
        ARROW_OK(executor->Execute(state.Values, listener.get()));
        auto output = executor->WrapResults(state.Values, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

private:
    void RegisterDependencies() const final {
        for (const auto& arg : ArgsNodes) {
            this->DependsOn(arg);
        }
    }

    static const arrow::compute::Function& ResolveFunction(const arrow::compute::FunctionRegistry& registry, const TString& funcName) {
        auto function = ARROW_RESULT(registry.GetFunction(funcName));
        MKQL_ENSURE(function != nullptr, "missing function");
        MKQL_ENSURE(function->kind() == arrow::compute::Function::SCALAR, "expected SCALAR function");
        return *function;
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(Kernel.Family.FunctionOptions, Kernel.GetArrowKernel(), ArgsValuesDescr, ctx);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    const ui32 StateIndex;
    const TString FuncName;
    const TVector<IComputationNode*> ArgsNodes;
    const TVector<TType*> ArgsTypes;

    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const TKernel& Kernel;
};

class TBlockBitCastWrapper : public TMutableComputationNode<TBlockBitCastWrapper> {
public:
    TBlockBitCastWrapper(TComputationMutables& mutables,
        IComputationNode* arg,
        TType* argType,
        TType* to)
        : TMutableComputationNode(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , Arg(arg)
        , ArgsValuesDescr({ ToValueDescr(argType) })
        , Function(ResolveFunction(to))
        , Kernel(ResolveKernel(Function, ArgsValuesDescr))
        , CastOptions(false)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        state.Values.clear();
        state.Values.emplace_back(TArrowBlock::From(Arg->GetValue(ctx)).GetDatum());
        Y_VERIFY_DEBUG(ArgsValuesDescr[0] == state.Values.back().descr());

        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
        ARROW_OK(executor->Init(&state.KernelContext, { &Kernel, ArgsValuesDescr, state.Options }));
        ARROW_OK(executor->Execute(state.Values, listener.get()));
        auto output = executor->WrapResults(state.Values, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    static const arrow::compute::Function& ResolveFunction(TType* to) {
        std::shared_ptr<arrow::DataType> type;
        MKQL_ENSURE(ConvertArrowType(to, type), "can't get arrow type");

        auto function = ARROW_RESULT(arrow::compute::GetCastFunction(type));
        MKQL_ENSURE(function != nullptr, "missing function");
        MKQL_ENSURE(function->kind() == arrow::compute::Function::SCALAR, "expected SCALAR function");
        return *function;
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>((const arrow::compute::FunctionOptions*)&CastOptions, Kernel, ArgsValuesDescr, ctx);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    const ui32 StateIndex;
    IComputationNode* Arg;
    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const arrow::compute::Function& Function;
    const arrow::compute::ScalarKernel& Kernel;
    arrow::compute::CastOptions CastOptions;
};

}

IComputationNode* WrapBlockFunc(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 arg");
    const auto funcNameData = AS_VALUE(TDataLiteral, callable.GetInput(0));
    const auto funcName = TString(funcNameData->AsValue().AsStringRef());
    TVector<IComputationNode*> argsNodes;
    TVector<TType*> argsTypes;
    const auto callableType = callable.GetType();
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        argsNodes.push_back(LocateNode(ctx.NodeLocator, callable, i));
        argsTypes.push_back(callableType->GetArgumentType(i));
    }

    return new TBlockFuncWrapper(ctx.Mutables,
        *ctx.FunctionRegistry.GetBuiltins(),
        funcName,
        std::move(argsNodes),
        std::move(argsTypes),
        callableType->GetReturnType()
    );
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
