#include "mkql_scalar_apply.h"

#include <ydb/library/yql/public/udf/arrow/memory_pool.h>

#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TScalarApplyWrapper : public TMutableComputationNode<TScalarApplyWrapper> {
public:
    struct TAccessors {
        TAccessors(const TVector<TType*>& argsTypes, TType* returnType, const NUdf::IPgBuilder& pgBuilder)
            : PgBuilder(pgBuilder)
        {
            auto returnItemType = AS_TYPE(TBlockType, returnType)->GetItemType();
            ReturnConverter = MakeBlockItemConverter(TTypeInfoHelper(), returnItemType, pgBuilder);
            ArgsConverters.reserve(argsTypes.size());
            ArgsReaders.reserve(argsTypes.size());
            for (auto type : argsTypes) {
                ArgsConverters.emplace_back(MakeBlockItemConverter(TTypeInfoHelper(), AS_TYPE(TBlockType, type)->GetItemType(), pgBuilder));
                ArgsReaders.emplace_back(MakeBlockReader(TTypeInfoHelper(), AS_TYPE(TBlockType, type)->GetItemType()));
            }
        }

        const NUdf::IPgBuilder& PgBuilder;
        std::unique_ptr<IBlockItemConverter> ReturnConverter;
        TVector<std::unique_ptr<IBlockItemConverter>> ArgsConverters;
        TVector<std::unique_ptr<IBlockReader>> ArgsReaders;
        bool ScalarsProcessed = false;
    };

    struct TKernelState : public arrow::compute::KernelState {
        TKernelState(const TVector<TType*>& argsTypes, TType* returnType, const TComputationContext& originalContext)
            : Alloc(__LOCATION__)
            , TypeEnv(Alloc)
            , MemInfo("ScalarApply")
            , FunctionRegistry(originalContext.HolderFactory.GetFunctionRegistry()->Clone())
            , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
            , ValueBuilder(HolderFactory, NUdf::EValidatePolicy::Exception)
            , PgBuilder(NYql::CreatePgBuilder())
            , Accessors(argsTypes, returnType, *PgBuilder)
            , RandomProvider(CreateDefaultRandomProvider())
            , TimeProvider(CreateDefaultTimeProvider())
            , Ctx(HolderFactory, &ValueBuilder, TComputationOptsFull(
                nullptr, Alloc.Ref(), TypeEnv, *RandomProvider, *TimeProvider, NUdf::EValidatePolicy::Exception, originalContext.SecureParamsProvider, originalContext.CountersProvider),
                originalContext.Mutables, *NYql::NUdf::GetYqlMemoryPool())
        {
            Alloc.Ref().EnableArrowTracking = false;
            Alloc.Release();
        }

        ~TKernelState()
        {
            Alloc.Acquire();
        }

        TScopedAlloc Alloc;
        TTypeEnvironment TypeEnv;
        TMemoryUsageInfo MemInfo;
        const IFunctionRegistry::TPtr FunctionRegistry;
        THolderFactory HolderFactory;
        TDefaultValueBuilder ValueBuilder;
        std::unique_ptr<NUdf::IPgBuilder> PgBuilder;
        TAccessors Accessors;
        TIntrusivePtr<IRandomProvider> RandomProvider;
        TIntrusivePtr<ITimeProvider> TimeProvider;
        TComputationContext Ctx;
    };

    class TArrowNode : public IArrowKernelComputationNode {
    public:
        TArrowNode(const TScalarApplyWrapper* parent, TComputationContext& originalContext)
            : Parent_(parent)
            , OriginalContext_(originalContext)
            , ArgsValuesDescr_(ToValueDescr(parent->ArgsTypes_))
            , Kernel_(ConvertToInputTypes(parent->ArgsTypes_), ConvertToOutputType(parent->ReturnType_), [parent](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                auto& state = dynamic_cast<TKernelState&>(*ctx->state());
                auto guard = Guard(state.Alloc);
                TVector<TDatumProvider> providers;
                providers.reserve(batch.values.size());
                for (const auto& v : batch.values) {
                    providers.emplace_back(MakeDatumProvider(v));
                }

                *res = parent->CalculateImpl(providers, state.Accessors, *NYql::NUdf::GetYqlMemoryPool(), state.Ctx);
                return arrow::Status::OK();
            })
        {
            Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
            Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
            Kernel_.init = [parent, ctx = &OriginalContext_](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
                auto state = std::make_unique<TKernelState>(parent->ArgsTypes_, parent->ReturnType_, *ctx);
                return arrow::Result(std::move(state));
            };
        }

        TStringBuf GetKernelName() const final {
            return "ScalarApply";
        }

        const arrow::compute::ScalarKernel& GetArrowKernel() const {
            return Kernel_;
        }

        const std::vector<arrow::ValueDescr>& GetArgsDesc() const {
            return ArgsValuesDescr_;
        }

        const IComputationNode* GetArgument(ui32 index) const {
            return Parent_->Args_[index];
        }

    private:
        const TScalarApplyWrapper* Parent_;
        const TComputationContext& OriginalContext_;
        const std::vector<arrow::ValueDescr> ArgsValuesDescr_;
        arrow::compute::ScalarKernel Kernel_;
    };
    friend class TArrowNode;

    TScalarApplyWrapper(TComputationMutables& mutables, const TVector<TType*>& argsTypes, TType* returnType,
        TVector<IComputationNode*>&& args, TVector<IComputationExternalNode*>&& lambdaArgs, IComputationNode* lambdaRoot)
        : TMutableComputationNode(mutables)
        , StateIndex_(mutables.CurValueIndex++)
        , ArgsTypes_(argsTypes)
        , ReturnType_(returnType)
        , Args_(std::move(args))
        , LambdaArgs_(std::move(lambdaArgs))
        , LambdaRoot_(lambdaRoot)
    {
        MKQL_ENSURE(Args_.size() == LambdaArgs_.size(), "Mismatch args count");
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        return std::make_unique<TArrowNode>(this, ctx);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TVector<TDatumProvider> providers;
        providers.reserve(Args_.size());
        for (auto arg : Args_) {
            providers.emplace_back(MakeDatumProvider(arg, ctx));
        }

        auto& state = GetState(ctx);
        return ctx.HolderFactory.CreateArrowBlock(CalculateImpl(providers, state.Accessors, ctx.ArrowMemoryPool, ctx));
    }

    arrow::Datum CalculateImpl(const TVector<TDatumProvider>& providers, TAccessors& accessors, arrow::MemoryPool& memoryPool,
        TComputationContext& ctx) const {
        TVector<arrow::Datum> args;
        args.reserve(providers.size());
        size_t length = 1;
        for (const auto& prov : providers) {
            args.emplace_back(prov());
            if (!args.back().is_scalar()) {
                length = args.back().array()->length;
            }
        }

        auto returnItemType = AS_TYPE(TBlockType, ReturnType_)->GetItemType();
        if (AS_TYPE(TBlockType, ReturnType_)->GetShape() == TBlockType::EShape::Scalar) {
            if (!accessors.ScalarsProcessed) {
                for (ui32 j = 0; j < Args_.size(); ++j) {
                    if (!LambdaArgs_[j]) {
                        continue;
                    }

                    auto item = accessors.ArgsReaders[j]->GetScalarItem(*args[j].scalar());
                    auto value = accessors.ArgsConverters[j]->MakeValue(item, ctx.HolderFactory);
                    LambdaArgs_[j]->SetValue(ctx, value);
                }

                accessors.ScalarsProcessed = true;
            }

            auto value = LambdaRoot_->GetValue(ctx);
            return ConvertScalar(returnItemType, value, memoryPool);
        } else {
            auto builder = MakeArrayBuilder(TTypeInfoHelper(), returnItemType, memoryPool, length, &accessors.PgBuilder);
            for (size_t i = 0; i < length; ++i) {
                for (ui32 j = 0; j < Args_.size(); ++j) {
                    if (!LambdaArgs_[j]) {
                        continue;
                    }

                    if (args[j].is_scalar() && accessors.ScalarsProcessed) {
                        continue;
                    }

                    auto item = args[j].is_scalar() ?
                        accessors.ArgsReaders[j]->GetScalarItem(*args[j].scalar()) :
                        accessors.ArgsReaders[j]->GetItem(*args[j].array(), i);
                    auto value = accessors.ArgsConverters[j]->MakeValue(item, ctx.HolderFactory);
                    LambdaArgs_[j]->SetValue(ctx, value);
                }

                accessors.ScalarsProcessed = true;
                auto value = LambdaRoot_->GetValue(ctx);
                auto item = accessors.ReturnConverter->MakeItem(value);
                builder->Add(item);
            }

            return builder->Build(true);
        }
    }

private:
    void RegisterDependencies() const final {
        for (auto arg : Args_) {
            this->DependsOn(arg);
        }

        for (ui32 i = 0; i < Args_.size(); ++i) {
            Args_[i]->AddDependence(LambdaArgs_[i]);
            this->Own(LambdaArgs_[i]);
        }

        this->DependsOn(LambdaRoot_);
    }

    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, const TVector<TType*>& argsTypes, TType* returnType, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Accessors(argsTypes, returnType, pgBuilder)
        {
        }

        TAccessors Accessors;
    };

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex_];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(ArgsTypes_, ReturnType_, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

    const ui32 StateIndex_;
    const TVector<TType*> ArgsTypes_;
    TType* const ReturnType_;
    const TVector<IComputationNode*> Args_;
    const TVector<IComputationExternalNode*> LambdaArgs_;
    IComputationNode* const LambdaRoot_;
};

} // namespace

IComputationNode* WrapScalarApply(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE((callable.GetInputsCount() >= 3) && (callable.GetInputsCount() % 2 == 1), "Bad args count");
    auto lambdaRoot = LocateNode(ctx.NodeLocator, callable, callable.GetInputsCount() - 1);
    auto argsCount = (callable.GetInputsCount() - 1) / 2;
    TVector<IComputationNode*> args(argsCount);
    TVector<IComputationExternalNode*> lambdaArgs(argsCount);
    TVector<TType*> argsTypes(argsCount);
    for (ui32 i = 0; i < argsCount; ++i) {
        args[i] = LocateNode(ctx.NodeLocator, callable, i);
        lambdaArgs[i] = LocateExternalNode(ctx.NodeLocator, callable, i + argsCount);
        argsTypes[i] = callable.GetType()->GetArgumentType(i);
    }

    return new TScalarApplyWrapper(ctx.Mutables, argsTypes, callable.GetType()->GetReturnType(),
        std::move(args), std::move(lambdaArgs), lambdaRoot);
}

}
}
