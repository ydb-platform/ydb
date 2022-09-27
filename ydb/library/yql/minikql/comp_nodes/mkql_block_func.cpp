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
    bool isOptional;
    arrow::ValueDescr ret;
    MKQL_ENSURE(ConvertInputArrowType(type, isOptional, ret), "can't get arrow type");
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

struct TState : public TComputationValue<TState> {
    using TComputationValue::TComputationValue;

    TState(TMemoryUsageInfo* memInfo, const arrow::compute::Function& function, const arrow::compute::FunctionOptions* options,
        const arrow::compute::ScalarKernel& kernel,
        const arrow::compute::FunctionRegistry& registry, const std::vector<arrow::ValueDescr>& argsValuesDescr, TComputationContext& ctx)
        : TComputationValue(memInfo)
        , ExecContext(&ctx.ArrowMemoryPool, nullptr, const_cast<arrow::compute::FunctionRegistry*>(&registry))
        , KernelContext(&ExecContext)
        , Executor(arrow::compute::detail::KernelExecutor::MakeScalar())
    {
        if (kernel.init) {
            State = ARROW_RESULT(kernel.init(&KernelContext, { &kernel, argsValuesDescr, options }));
            KernelContext.SetState(State.get());
        }

        ARROW_OK(Executor->Init(&KernelContext, { &kernel, argsValuesDescr, options }));
        Values.reserve(argsValuesDescr.size());
    }

    arrow::compute::ExecContext ExecContext;
    arrow::compute::KernelContext KernelContext;
    std::unique_ptr<arrow::compute::KernelState> State;
    std::unique_ptr<arrow::compute::detail::KernelExecutor> Executor;

    std::vector<arrow::Datum> Values;
};

class TBlockFuncWrapper : public TMutableComputationNode<TBlockFuncWrapper> {
public:
    TBlockFuncWrapper(TComputationMutables& mutables,
        const arrow::compute::FunctionRegistry& functionRegistry,
        const TString& funcName,
        TVector<IComputationNode*>&& argsNodes,
        TVector<TType*>&& argsTypes)
        : TMutableComputationNode(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , FuncName(funcName)
        , ArgsNodes(std::move(argsNodes))
        , ArgsTypes(std::move(argsTypes))
        , ArgsValuesDescr(ToValueDescr(ArgsTypes))
        , FunctionRegistry(functionRegistry)
        , Function(ResolveFunction(FunctionRegistry, FuncName))
        , Kernel(ResolveKernel(Function, ArgsValuesDescr))
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
        ARROW_OK(state.Executor->Execute(state.Values, listener.get()));
        auto output = state.Executor->WrapResults(state.Values, listener->values());
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
            result = ctx.HolderFactory.Create<TState>(Function, Function.default_options(), Kernel, FunctionRegistry, ArgsValuesDescr, ctx);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    const ui32 StateIndex;
    const TString FuncName;
    const TVector<IComputationNode*> ArgsNodes;
    const TVector<TType*> ArgsTypes;

    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const arrow::compute::FunctionRegistry& FunctionRegistry;
    const arrow::compute::Function& Function;
    const arrow::compute::ScalarKernel& Kernel;
};

class TBlockBitCastWrapper : public TMutableComputationNode<TBlockBitCastWrapper> {
public:
    TBlockBitCastWrapper(TComputationMutables& mutables,
        const arrow::compute::FunctionRegistry& functionRegistry,
        IComputationNode* arg,
        TType* argType,
        TType* to)
        : TMutableComputationNode(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , Arg(arg)
        , ArgsValuesDescr({ ToValueDescr(argType) })
        , FunctionRegistry(functionRegistry)
        , Function(ResolveFunction(FunctionRegistry, to))
        , Kernel(ResolveKernel(Function, ArgsValuesDescr))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        state.Values.clear();
        state.Values.emplace_back(TArrowBlock::From(Arg->GetValue(ctx)).GetDatum());
        Y_VERIFY_DEBUG(ArgsValuesDescr[0] == state.Values.back().descr());

        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        ARROW_OK(state.Executor->Execute(state.Values, listener.get()));
        auto output = state.Executor->WrapResults(state.Values, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    static const arrow::compute::Function& ResolveFunction(const arrow::compute::FunctionRegistry& registry, TType* to) {
        bool isOptional;
        std::shared_ptr<arrow::DataType> type;
        MKQL_ENSURE(ConvertArrowType(to, isOptional, type), "can't get arrow type");

        auto function = ARROW_RESULT(arrow::compute::GetCastFunction(type));
        MKQL_ENSURE(function != nullptr, "missing function");
        MKQL_ENSURE(function->kind() == arrow::compute::Function::SCALAR, "expected SCALAR function");
        return *function;
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            arrow::compute::CastOptions options(false);
            result = ctx.HolderFactory.Create<TState>(Function, (const arrow::compute::FunctionOptions*)&options, Kernel, FunctionRegistry, ArgsValuesDescr, ctx);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    const ui32 StateIndex;
    IComputationNode* Arg;
    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const arrow::compute::FunctionRegistry& FunctionRegistry;
    const arrow::compute::Function& Function;
    const arrow::compute::ScalarKernel& Kernel;
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
        *ctx.FunctionRegistry.GetBuiltins()->GetArrowFunctionRegistry(),
        funcName,
        std::move(argsNodes),
        std::move(argsTypes)
    );
}

IComputationNode* WrapBlockBitCast(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    auto argNode = LocateNode(ctx.NodeLocator, callable, 0);
    MKQL_ENSURE(callable.GetInput(1).GetStaticType()->IsType(), "Expected type");
    return new TBlockBitCastWrapper(ctx.Mutables,
        *ctx.FunctionRegistry.GetBuiltins()->GetArrowFunctionRegistry(),
        argNode,
        callable.GetType()->GetArgumentType(0),
        static_cast<TType*>(callable.GetInput(1).GetNode())
    );
}

}
}
