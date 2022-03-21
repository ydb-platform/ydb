#include "mkql_withcontext.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_pg.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TWithContextWrapper : public TMutableComputationNode<TWithContextWrapper> {
    typedef TMutableComputationNode<TWithContextWrapper> TBaseComputation;
public:
    TWithContextWrapper(TComputationMutables& mutables, const std::string_view& contextType, IComputationNode* arg)
        : TBaseComputation(mutables)
        , ContextType(contextType)
        , Arg(arg)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto prev = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = PgInitializeContext(ContextType);
        Y_DEFER {
            PgDestroyContext(ContextType, TlsAllocState->CurrentContext);
            TlsAllocState->CurrentContext = prev;
        };

        TPAllocScope scope;
        return Arg->GetValue(compCtx).Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
    const std::string_view ContextType;
};

struct TState : public TComputationValue<TState> {

    TState(TMemoryUsageInfo* memInfo, const std::string_view& contextType)
        : TComputationValue(memInfo)
        , ContextType(contextType)
    {
        Ctx = PgInitializeContext(ContextType);
        Scope.Detach();
    }

    void Attach() {
        Scope.Attach();
        PrevContext = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = Ctx;
    }

    void Detach() {
        Scope.Detach();
        TlsAllocState->CurrentContext = PrevContext;
    }

    void Cleanup() {
        if (Ctx) {
            PgDestroyContext(ContextType, Ctx);
            Ctx = nullptr;
            Scope.Cleanup();
        }
    }

    ~TState() {
        Cleanup();
    }

    const std::string_view ContextType;
    void* Ctx = nullptr;
    TPAllocScope Scope;
    void* PrevContext = nullptr;
};

class TWithContextFlowWrapper : public TStatefulFlowComputationNode<TWithContextFlowWrapper> {
    typedef TStatefulFlowComputationNode<TWithContextFlowWrapper> TBaseComputation;
public:
    TWithContextFlowWrapper(TComputationMutables& mutables, const std::string_view& contextType,
        EValueRepresentation kind, IComputationNode* flow)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow(flow)
        , ContextType(contextType)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            stateValue = ctx.HolderFactory.Create<TState>(ContextType);
        }

        TState& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        state.Attach();
        Y_DEFER {
            state.Detach();
        };

        auto item = Flow->GetValue(ctx);
        if (item.IsFinish()) {
            state.Cleanup();
        }

        return item;
    }

private:
    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow);
    }

    IComputationNode* const Flow;
    const std::string_view ContextType;
};


}

IComputationNode* WrapWithContext(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto contextTypeData = AS_VALUE(TDataLiteral, callable.GetInput(0));
    auto contextType = contextTypeData->AsValue().AsStringRef();
    auto arg = LocateNode(ctx.NodeLocator, callable, 1);
    if (callable.GetInput(1).GetStaticType()->IsFlow()) {
        const auto type = callable.GetType()->GetReturnType();
        return new TWithContextFlowWrapper(ctx.Mutables, contextType, GetValueRepresentation(type), arg);
    } else {
        MKQL_ENSURE(!callable.GetInput(1).GetStaticType()->IsStream(), "Stream is not expected here");
        return new TWithContextWrapper(ctx.Mutables, contextType, arg);
    }
}

}
}
