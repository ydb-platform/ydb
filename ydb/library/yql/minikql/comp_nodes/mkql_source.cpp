#include "mkql_source.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TSourceOfWrapper : public TMutableComputationNode<TSourceOfWrapper> {
    typedef TMutableComputationNode<TSourceOfWrapper> TBaseComputation;
private:
    class TValue : public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo)
            : TComputationValue<TValue>(memInfo)
        {}

    private:
        ui32 GetTraverseCount() const override { return 0U; }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            result = NUdf::TUnboxedValuePod();
            return NUdf::EFetchStatus::Ok;
        }
    };

public:
    TSourceOfWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TValue>();
    }

private:
    void RegisterDependencies() const final {}
};

class TSourceWrapper : public TStatelessWideFlowCodegeneratorNode<TSourceWrapper> {
using TBaseComputation = TStatelessWideFlowCodegeneratorNode<TSourceWrapper>;
public:
    TSourceWrapper()
        : TStatelessWideFlowCodegeneratorNode<TSourceWrapper>(nullptr)
    {}

    EFetchResult DoCalculate(TComputationContext&, NUdf::TUnboxedValue*const*) const {
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*&) const {
        return {ConstantInt::get(Type::getInt32Ty(ctx.Codegen.GetContext()), static_cast<i32>(EFetchResult::One)), {}};
    }
#endif
private:
    void RegisterDependencies() const final {}
};

}

IComputationNode* WrapSourceOf(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args.");
    const auto type = callable.GetType()->GetReturnType();
    if (type->IsFlow()) {
        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod());
    } else if (type->IsStream()) {
        return new TSourceOfWrapper(ctx.Mutables);
    }

    THROW yexception() << "Expected flow or stream.";
}

IComputationNode* WrapSource(TCallable& callable, const TComputationNodeFactoryContext&) {
    MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args.");
    MKQL_ENSURE(!GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType())), "Expected zero width of output flow.");
    return new TSourceWrapper;
}

}
}
