#include "mkql_block_func.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/arrow/mkql_functions.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/compute/exec_internal.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/util/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockFuncWrapper : public TMutableComputationNode<TBlockFuncWrapper> {
public:
    TBlockFuncWrapper(TComputationMutables& mutables,
        const TString& funcName,
        TVector<IComputationNode*>&& argsNodes,
        TVector<TType*>&& argsTypes)
        : TMutableComputationNode(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , FuncName(funcName)
        , ArgsNodes(std::move(argsNodes))
        , ArgsTypes(std::move(argsTypes))
        , ArgsValuesDescr(ToValueDescr(ArgsTypes))
        , FunctionRegistry(*arrow::compute::GetFunctionRegistry())
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

    static const arrow::compute::ScalarKernel& ResolveKernel(const arrow::compute::Function& function, const std::vector<arrow::ValueDescr>& args) {
        const auto kernel = ARROW_RESULT(function.DispatchExact(args));
        return *static_cast<const arrow::compute::ScalarKernel*>(kernel);
    }

    static arrow::ValueDescr ToValueDescr(TType* type) {
        bool isOptional;
        arrow::ValueDescr ret;
        MKQL_ENSURE(ConvertInputArrowType(type, isOptional, ret), "can't get arrow type");
        return ret;
    }

    static std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types) {
        std::vector<arrow::ValueDescr> res;
        res.reserve(types.size());
        for (const auto& type : types) {
            res.emplace_back(ToValueDescr(type));
        }

        return res;
    }

    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, const arrow::compute::Function& function, const arrow::compute::ScalarKernel& kernel,
            arrow::compute::FunctionRegistry& registry, const std::vector<arrow::ValueDescr>& argsValuesDescr, TComputationContext& ctx)
            : TComputationValue(memInfo)
            , ExecContext(&ctx.ArrowMemoryPool, nullptr, &registry)
            , KernelContext(&ExecContext)
            , Executor(arrow::compute::detail::KernelExecutor::MakeScalar())
        {
            auto options = function.default_options();
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

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(Function, Kernel, FunctionRegistry, ArgsValuesDescr, ctx);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    const ui32 StateIndex;
    const TString FuncName;
    const TVector<IComputationNode*> ArgsNodes;
    const TVector<TType*> ArgsTypes;

    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    arrow::compute::FunctionRegistry& FunctionRegistry;
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
        funcName,
        std::move(argsNodes),
        std::move(argsTypes)
    );
}

}
}
