#include "mkql_blocks.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/util/bitmap.h>
#include <arrow/util/bit_util.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockBuilder {
public:
    explicit TBlockBuilder(TComputationContext& ctx)
        : ItemType(arrow::uint64())
        , MaxLength_(MaxBlockSizeInBytes / TypeSize(*ItemType))
        , Ctx(ctx)
        , Builder(&Ctx.ArrowMemoryPool)
    {
        ARROW_OK(Builder.Reserve(MaxLength_));
    }

    void Add(NUdf::TUnboxedValue& value) {
        Y_VERIFY_DEBUG(Builder.length() < MaxLength_);
        if (value) {
            Builder.UnsafeAppend(value.Get<ui64>());
        } else {
            Builder.UnsafeAppendNull();
        }
    }

    inline size_t MaxLength() const noexcept {
        return MaxLength_;
    }

    NUdf::TUnboxedValuePod Build() {
        std::shared_ptr<arrow::ArrayData> result;
        ARROW_OK(Builder.FinishInternal(&result));
        return Ctx.HolderFactory.CreateArrowBlock(std::move(result));
    }

private:
    static int64_t TypeSize(arrow::DataType& itemType) {
        const auto bits = static_cast<const arrow::FixedWidthType&>(itemType).bit_width();
        return arrow::BitUtil::BytesForBits(bits);
    }

private:
    static constexpr size_t MaxBlockSizeInBytes = 1_MB;

private:
    std::shared_ptr<arrow::DataType> ItemType;
    const size_t MaxLength_;
    TComputationContext& Ctx;
    arrow::UInt64Builder Builder;
};

class TToBlocksWrapper: public TStatelessFlowComputationNode<TToBlocksWrapper> {
public:
    explicit TToBlocksWrapper(IComputationNode* flow)
        : TStatelessFlowComputationNode(flow, EValueRepresentation::Boxed)
        , Flow(flow)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto builder = TBlockBuilder(ctx);

        for (size_t i = 0; i < builder.MaxLength(); ++i) {
            auto result = Flow->GetValue(ctx);
            if (result.IsFinish() || result.IsYield()) {
                if (i == 0) {
                    return result.Release();
                }
                break;
            }
            builder.Add(result);
        }

        return builder.Build();
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

private:
    IComputationNode* const Flow;
};

class TWideToBlocksWrapper : public TStatefulWideFlowComputationNode<TWideToBlocksWrapper> {
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        size_t width)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Embedded)
        , Flow(flow)
        , Width(width)
    {
        Y_VERIFY_DEBUG(Width > 0);
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto builders = std::vector<TBlockBuilder>();
        builders.reserve(Width);
        for (size_t i = 0; i < Width; ++i) {
            builders.push_back(TBlockBuilder(ctx));
        }
        size_t maxLength = builders.front().MaxLength();
        for (size_t i = 1; i < Width; ++i) {
            maxLength = Min(maxLength, builders[i].MaxLength());
        }

        auto& s = GetState(state, ctx);
        for (size_t i = 0; i < maxLength; ++i) {
            if (const auto result = Flow->FetchValues(ctx, s.ValuePointers.data()); EFetchResult::One != result) {
                if (i == 0) {
                    return result;
                }
                break;
            }
            for (size_t j = 0; j < Width; ++j) {
                if (output[j] != nullptr) {
                    builders[j].Add(s.Values[j]);
                }
            }
        }

        for (size_t i = 0; i < Width; ++i) {
            if (auto* out = output[i]; out != nullptr) {
                *out = builders[i].Build();
            }
        }

        return EFetchResult::One;
    }

private:
    struct TState: public TComputationValue<TState> {
        std::vector<NUdf::TUnboxedValue> Values;
        std::vector<NUdf::TUnboxedValue*> ValuePointers;

        TState(TMemoryUsageInfo* memInfo, size_t width)
            : TComputationValue(memInfo)
            , Values(width)
            , ValuePointers(width)
        {
            for (size_t i = 0; i < width; ++i) {
                ValuePointers[i] = &Values[i];
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow;
    const size_t Width;
};

class TFromBlocksWrapper : public TMutableComputationNode<TFromBlocksWrapper> {
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TMutableComputationNode(mutables)
        , Flow(flow)
        , StateIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        if (state.Array == nullptr || state.Index == state.Array->length) {
            auto result = Flow->GetValue(ctx);
            if (result.IsFinish()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (result.IsYield()) {
                return NUdf::TUnboxedValue::MakeYield();
            }
            state.Array = TArrowBlock::From(result).GetDatum().array();
            state.Index = 0;
        }

        const auto result = state.GetValue();
        ++state.Index;
        return result;
    }

private:
    struct TState: public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        NUdf::TUnboxedValuePod GetValue() const {
            const auto nullCount = Array->GetNullCount();

            return nullCount == Array->length || (nullCount > 0 && !HasValue())
                ? NUdf::TUnboxedValuePod()
                : DoGetValue();
        }

    private:
        NUdf::TUnboxedValuePod DoGetValue() const {
            return NUdf::TUnboxedValuePod(Array->GetValues<ui64>(1)[Index]);
        }

        bool HasValue() const {
            return arrow::BitUtil::GetBit(Array->GetValues<uint8_t>(0), Index + Array->offset);
        }

    public:
        std::shared_ptr<arrow::ArrayData> Array{nullptr};
        size_t Index{0};
    };

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow);
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>();
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow;
    const ui32 StateIndex;
};

arrow::Datum ExtractLiteral(TRuntimeNode n) {
    if (n.GetStaticType()->IsOptional()) {
        const auto* dataLiteral = AS_VALUE(TOptionalLiteral, n);
        if (!dataLiteral->HasItem()) {
            return arrow::MakeNullScalar(arrow::uint64());
        }
        n = dataLiteral->GetItem();
    }

    const auto* dataLiteral = AS_VALUE(TDataLiteral, n);
    return arrow::Datum(static_cast<uint64_t>(dataLiteral->AsValue().Get<ui64>()));
}

}

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TToBlocksWrapper(LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto* flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto* tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto* wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksWrapper(ctx.Mutables,
        wideFlow,
        tupleType->GetElementsCount());
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapAsSingle(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    auto value = ExtractLiteral(callable.GetInput(0U));
    return ctx.NodeFactory.CreateImmutableNode(ctx.HolderFactory.CreateArrowBlock(std::move(value)));
}

}
}
