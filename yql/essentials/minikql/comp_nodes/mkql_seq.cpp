#include "mkql_seq.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

namespace {

class TSeqWrapper: public TMutableComputationNode<TSeqWrapper> {
    using TBaseComputation = TMutableComputationNode<TSeqWrapper>;

public:
    TSeqWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& args)
        : TBaseComputation(mutables)
        , Args_(std::move(args))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        for (size_t i = 0; i + 1 < Args_.size(); ++i) {
            Args_[i]->GetValue(ctx);
        }

        auto value = Args_.back()->GetValue(ctx);
        return value.Release();
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Args_.cbegin(), Args_.cend(), std::bind(&TSeqWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Args_;
};

} // namespace

IComputationNode* WrapSeq(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Seq: Expected at least one argument");

    TComputationNodePtrVector args;
    args.reserve(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        args.push_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    return new TSeqWrapper(ctx.Mutables, std::move(args));
}

} // namespace NKikimr::NMiniKQL
