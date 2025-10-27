#include "mkql_linear.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

namespace {

class TToDynamicLinearWrapper: public TMutableComputationNode<TToDynamicLinearWrapper> {
    using TSelf = TToDynamicLinearWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;

public:
    class TValue: public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo, TUnboxedValue&& value)
            : TComputationValue(memInfo)
            , Value_(std::move(value))
            , Consumed_(false)
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& result) override {
            if (Consumed_) {
                return false;
            }

            result = std::move(Value_);
            Consumed_ = true;
            return true;
        }

        TUnboxedValue Value_;
        bool Consumed_;
    };

    TToDynamicLinearWrapper(TComputationMutables& mutables, IComputationNode* source)
        : TBaseComputation(mutables)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue input = Source_->GetValue(ctx);
        return ctx.HolderFactory.Create<TValue>(std::move(input));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    IComputationNode* const Source_;
};

class TFromDynamicLinearWrapper: public TMutableComputationNode<TFromDynamicLinearWrapper> {
    using TSelf = TFromDynamicLinearWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;

public:
    TFromDynamicLinearWrapper(TComputationMutables& mutables, IComputationNode* source, const TSourcePosition& pos)
        : TBaseComputation(mutables)
        , Source_(source)
        , Pos_(pos)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue input = Source_->GetValue(ctx);
        TUnboxedValue result;
        if (input.Next(result)) {
            return result.Release();
        }

        TStringBuilder res;
        res << Pos_ << " The linear value has already been used";
        UdfTerminate(res.c_str());
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    IComputationNode* const Source_;
    const TSourcePosition Pos_;
};

} // namespace

IComputationNode* WrapToDynamicLinear(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expecting exactly one argument");
    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    return new TToDynamicLinearWrapper(ctx.Mutables, source);
}

IComputationNode* WrapFromDynamicLinear(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expecting exactly 4 arguments");
    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    return new TFromDynamicLinearWrapper(ctx.Mutables, source, NUdf::TSourcePosition(row, column, file));
}

} // namespace NMiniKQL
} // namespace NKikimr
