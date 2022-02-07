#include "mkql_seq.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TSeqWrapper : public TMutableComputationNode<TSeqWrapper> {
    typedef TMutableComputationNode<TSeqWrapper> TBaseComputation;
public:
    TSeqWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& args)
        : TBaseComputation(mutables)
        , Args(std::move(args))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        for (size_t i = 0; i + 1 < Args.size(); ++i) {
            Args[i]->GetValue(ctx);
        }

        auto value = Args.back()->GetValue(ctx);
        return value.Release();
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Args.cbegin(), Args.cend(), std::bind(&TSeqWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Args;
};

}

IComputationNode* WrapSeq(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Seq: Expected at least one argument");

    TComputationNodePtrVector args;
    args.reserve(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        args.push_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    return new TSeqWrapper(ctx.Mutables, std::move(args));
}

}
}
