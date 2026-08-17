#include "dq_pq_parsing_wrapper.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <memory>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TColumnMapping {
    TString SourceName;
    TString TargetName;
    ui32 SourceIndex;
    ui32 OutputIndex;
    ui32 StateIndex;
};

struct TWatermarkState {
    TVector<NUdf::TUnboxedValue> Values;
};

class TDqPqParsingWrapper : public TStatefulSourceComputationNode<TDqPqParsingWrapper> {
    using TBase = TStatefulSourceComputationNode<TDqPqParsingWrapper>;

public:
    class TInputStreamValue : public TComputationValue<TInputStreamValue> {
    public:
        using TBase = TComputationValue<TInputStreamValue>;

        TInputStreamValue(
            TMemoryUsageInfo* memInfo,
            TComputationContext& ctx,
            NUdf::TUnboxedValue&& input,
            ui32 lambdaInputWidth,
            TVector<TMaybe<ui32>>&& sourceToLambdaInput,
            TVector<TColumnMapping>&& metadata,
            std::shared_ptr<TWatermarkState> state)
            : TBase(memInfo)
            , Ctx_(ctx)
            , Input_(std::move(input))
            , LambdaInputWidth_(lambdaInputWidth)
            , SourceToLambdaInput_(std::move(sourceToLambdaInput))
            , Metadata_(std::move(metadata))
            , State_(std::move(state))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            NUdf::TUnboxedValue row;
            const auto status = Input_.Fetch(row);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            for (const auto& column : Metadata_) {
                State_->Values[column.StateIndex] = row.GetElement(column.SourceIndex);
            }

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            result = Ctx_.HolderFactory.CreateDirectArrayHolder(LambdaInputWidth_, itemsPtr);

            for (ui32 index = 0; index < LambdaInputWidth_; ++index) {
                if (const auto sourceIndex = SourceToLambdaInput_[index]) {
                    itemsPtr[index] = row.GetElement(*sourceIndex);
                }
            }

            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& Ctx_;
        const NUdf::TUnboxedValue Input_;
        const ui32 LambdaInputWidth_;
        const TVector<TMaybe<ui32>> SourceToLambdaInput_;
        const TVector<TColumnMapping> Metadata_;
        const std::shared_ptr<TWatermarkState> State_;
    };

    class TOutputStreamValue : public TComputationValue<TOutputStreamValue> {
    public:
        using TBase = TComputationValue<TOutputStreamValue>;

        TOutputStreamValue(
            TMemoryUsageInfo* memInfo,
            TComputationContext& ctx,
            NUdf::TUnboxedValue&& input,
            ui32 outputWidth,
            TVector<TMaybe<ui32>>&& lambdaOutputToOutput,
            TVector<TColumnMapping>&& metadata,
            std::shared_ptr<TWatermarkState> state)
            : TBase(memInfo)
            , Ctx_(ctx)
            , Input_(std::move(input))
            , OutputWidth_(outputWidth)
            , LambdaOutputToOutput_(std::move(lambdaOutputToOutput))
            , Metadata_(std::move(metadata))
            , State_(std::move(state))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            NUdf::TUnboxedValue row;
            const auto status = Input_.Fetch(row);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            NUdf::TUnboxedValue* itemsPtr = nullptr;
            result = Ctx_.HolderFactory.CreateDirectArrayHolder(OutputWidth_, itemsPtr);

            for (ui32 index = 0; index < OutputWidth_; ++index) {
                if (const auto sourceIndex = LambdaOutputToOutput_[index]) {
                    itemsPtr[index] = row.GetElement(*sourceIndex);
                }
            }

            for (const auto& column : Metadata_) {
                itemsPtr[column.OutputIndex] = State_->Values[column.StateIndex];
            }

            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& Ctx_;
        const NUdf::TUnboxedValue Input_;
        const ui32 OutputWidth_;
        const TVector<TMaybe<ui32>> LambdaOutputToOutput_;
        const TVector<TColumnMapping> Metadata_;
        const std::shared_ptr<TWatermarkState> State_;
    };

    TDqPqParsingWrapper(
        TComputationMutables& mutables,
        IComputationNode* input,
        IComputationExternalNode* lambdaArg,
        IComputationNode* lambdaBody,
        ui32 lambdaInputWidth,
        ui32 outputWidth,
        TVector<TMaybe<ui32>>&& sourceToLambdaInput,
        TVector<TMaybe<ui32>>&& lambdaOutputToOutput,
        TVector<TColumnMapping>&& metadata)
        : TBase(mutables)
        , Input_(input)
        , LambdaArg_(lambdaArg)
        , LambdaBody_(lambdaBody)
        , LambdaInputWidth_(lambdaInputWidth)
        , OutputWidth_(outputWidth)
        , SourceToLambdaInput_(std::move(sourceToLambdaInput))
        , LambdaOutputToOutput_(std::move(lambdaOutputToOutput))
        , Metadata_(std::move(metadata))
    {
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const override {
        NUdf::TUnboxedValue& valueRef = ValueRef(ctx);
        if (valueRef.IsInvalid()) {
            valueRef = CallLambda(ctx);
        } else if (valueRef.HasValue()) {
            MKQL_ENSURE(valueRef.IsBoxed(), "Expected boxed value");
            if (valueRef.HasListItems()) {
                NUdf::TUnboxedValue stream = CallLambda(ctx);
                stream.Load2(valueRef);
                valueRef = stream;
            }
        }

        return valueRef;
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Input_);
        Own(LambdaArg_);
        DependsOn(LambdaBody_);
    }

    NUdf::TUnboxedValue CallLambda(TComputationContext& ctx) const {
        auto state = std::make_shared<TWatermarkState>();
        state->Values.resize(Metadata_.size());

        LambdaArg_->SetValue(ctx, ctx.HolderFactory.Create<TInputStreamValue>(
            ctx,
            Input_->GetValue(ctx),
            LambdaInputWidth_,
            TVector<TMaybe<ui32>>(SourceToLambdaInput_),
            TVector<TColumnMapping>(Metadata_),
            state));

        return ctx.HolderFactory.Create<TOutputStreamValue>(
            ctx,
            LambdaBody_->GetValue(ctx),
            OutputWidth_,
            TVector<TMaybe<ui32>>(LambdaOutputToOutput_),
            TVector<TColumnMapping>(Metadata_),
            std::move(state));
    }

private:
    IComputationNode* const Input_;
    IComputationExternalNode* const LambdaArg_;
    IComputationNode* const LambdaBody_;
    const ui32 LambdaInputWidth_;
    const ui32 OutputWidth_;
    const TVector<TMaybe<ui32>> SourceToLambdaInput_;
    const TVector<TMaybe<ui32>> LambdaOutputToOutput_;
    const TVector<TColumnMapping> Metadata_;
};

TVector<TMaybe<ui32>> BuildStructMapping(
    const TStructType& sourceType,
    const TStructType& targetType
) {
    TVector<TMaybe<ui32>> result(targetType.GetMembersCount());
    for (ui32 index = 0; index < targetType.GetMembersCount(); ++index) {
        if (const auto sourceIndex = sourceType.FindMemberIndex(targetType.GetMemberName(index))) {
            result[index] = *sourceIndex;
        }
    }
    return result;
}

TVector<TColumnMapping> BuildMetadataMapping(
    TCallable& callable,
    const TStructType& sourceType,
    const TStructType& outputType
) {
    const auto metadataMapping = AS_VALUE(TListLiteral, callable.GetInput(3));
    MKQL_ENSURE(metadataMapping->GetItemsCount() % 2 == 0, "Expected even metadata mapping item count");

    TVector<TColumnMapping> result;
    result.reserve(metadataMapping->GetItemsCount() / 2);
    for (ui32 index = 0; index < metadataMapping->GetItemsCount(); index += 2) {
        const auto sourceName = AS_VALUE(TDataLiteral, metadataMapping->GetItems()[index])->AsValue().AsStringRef();
        const auto targetName = AS_VALUE(TDataLiteral, metadataMapping->GetItems()[index + 1])->AsValue().AsStringRef();

        result.push_back(TColumnMapping{
            TString(sourceName),
            TString(targetName),
            sourceType.GetMemberIndex(TStringBuf(sourceName)),
            outputType.GetMemberIndex(TStringBuf(targetName)),
            index / 2,
        });
    }
    return result;
}

} // anonymous namespace

IComputationNode* WrapDqPqParsingWrapper(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx
) {
    const auto inputStreamType = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
    const auto sourceItemType = AS_TYPE(TStructType, inputStreamType->GetItemType());
    const auto lambdaInputStreamType = AS_TYPE(TStreamType, callable.GetInput(1).GetStaticType());
    const auto lambdaInputItemType = AS_TYPE(TStructType, lambdaInputStreamType->GetItemType());
    const auto lambdaOutputStreamType = AS_TYPE(TStreamType, callable.GetInput(2).GetStaticType());
    const auto lambdaOutputItemType = AS_TYPE(TStructType, lambdaOutputStreamType->GetItemType());
    const auto outputStreamType = AS_TYPE(TStreamType, callable.GetType()->GetReturnType());
    const auto outputItemType = AS_TYPE(TStructType, outputStreamType->GetItemType());

    return new TDqPqParsingWrapper(
        ctx.Mutables,
        LocateNode(ctx.NodeLocator, callable, 0),
        LocateExternalNode(ctx.NodeLocator, callable, 1),
        LocateNode(ctx.NodeLocator, callable, 2),
        lambdaInputItemType->GetMembersCount(),
        outputItemType->GetMembersCount(),
        BuildStructMapping(*sourceItemType, *lambdaInputItemType),
        BuildStructMapping(*lambdaOutputItemType, *outputItemType),
        BuildMetadataMapping(callable, *sourceItemType, *outputItemType)
    );
}

} // namespace NKikimr::NMiniKQL
