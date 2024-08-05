#include "registry.h"
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

#include <memory>

namespace NYql {

namespace {
    class TLoader : std::enable_shared_from_this<TLoader> {
    public:
        TLoader()
            : Alloc_(__LOCATION__)
            , Env_(Alloc_)
        {
            Alloc_.Release();
        }

        void Init(const TString& serialized,
            const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
            const NKikimr::NMiniKQL::TComputationNodeFactory& nodeFactory) {
            TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
            Pgm_ = NKikimr::NMiniKQL::DeserializeRuntimeNode(serialized, Env_);
            auto pgmTop = AS_CALLABLE("BlockAsTuple", Pgm_);
            MKQL_ENSURE(pgmTop->GetInputsCount() == 2, "Expected tuple of 2 items");
            auto argsNode = pgmTop->GetInput(0);
            MKQL_ENSURE(!argsNode.IsImmediate() && argsNode.GetNode()->GetType()->IsCallable(), "Expected callable");
            auto argsCallable = static_cast<NKikimr::NMiniKQL::TCallable*>(argsNode.GetNode());

            Explorer_.Walk(Pgm_.GetNode(), Env_);
            NKikimr::NMiniKQL::TComputationPatternOpts opts(Alloc_.Ref(), Env_, nodeFactory,
                &functionRegistry, NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", NKikimr::NMiniKQL::EGraphPerProcess::Multi);
            std::vector<NKikimr::NMiniKQL::TNode*> entryPoints;
            if (argsCallable->GetType()->GetName() == "BlockAsTuple") {
                for (ui32 i = 0; i < argsCallable->GetInputsCount(); ++i) {
                    entryPoints.emplace_back(argsCallable->GetInput(i).GetNode());
                }
            }

            Alloc_.Ref().UseRefLocking = true;
            Pattern_ = NKikimr::NMiniKQL::MakeComputationPattern(Explorer_, Pgm_, entryPoints, opts);
            RandomProvider_ = CreateDefaultRandomProvider();
            TimeProvider_ = CreateDefaultTimeProvider();

            Graph_ = Pattern_->Clone(opts.ToComputationOptions(*RandomProvider_, *TimeProvider_, &Env_));
            NKikimr::NMiniKQL::TBindTerminator terminator(Graph_->GetTerminator());
            Topology_ = Graph_->GetKernelsTopology();
            MKQL_ENSURE(Topology_->Items.size() >= 3, "Expected at least 3 kernels");
        }

        ~TLoader() {
            Alloc_.Acquire();
        }

        ui32 GetKernelsCount() const {
            return Topology_->Items.size() - 3;
        }

        const arrow::compute::ScalarKernel* GetKernel(ui32 index) const {
            MKQL_ENSURE(index < Topology_->Items.size() - 3, "Bad kernel index");
            return &Topology_->Items[index].Node->GetArrowKernel();
        }

    private:
        NKikimr::NMiniKQL::TScopedAlloc Alloc_;
        NKikimr::NMiniKQL::TTypeEnvironment Env_;
        NKikimr::NMiniKQL::TRuntimeNode Pgm_;
        NKikimr::NMiniKQL::TExploringNodeVisitor Explorer_;
        NKikimr::NMiniKQL::IComputationPattern::TPtr Pattern_;
        TIntrusivePtr<IRandomProvider> RandomProvider_;
        TIntrusivePtr<ITimeProvider> TimeProvider_;
        THolder<NKikimr::NMiniKQL::IComputationGraph> Graph_;
        const NKikimr::NMiniKQL::TArrowKernelsTopology* Topology_;
    };
}

std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>> LoadKernels(const TString& serialized,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    const NKikimr::NMiniKQL::TComputationNodeFactory& nodeFactory) {
    auto loader = std::make_shared<TLoader>();
    loader->Init(serialized, functionRegistry, nodeFactory);
    std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>> ret(loader->GetKernelsCount());
    auto deleter = [loader](const arrow::compute::ScalarKernel*) {};
    for (ui32 i = 0; i < ret.size(); ++i) {
        ret[i] = std::shared_ptr<const arrow::compute::ScalarKernel>(loader->GetKernel(ret.size() - 1 - i), deleter);
    }

    return ret;
}

}
