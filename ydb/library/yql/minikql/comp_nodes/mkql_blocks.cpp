#include "mkql_blocks.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"
#include "mkql_block_impl.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <arrow/scalar.h>
#include <arrow/array.h>
#include <arrow/datum.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TToBlocksWrapper : public TStatelessFlowComputationNode<TToBlocksWrapper> {
public:
    explicit TToBlocksWrapper(IComputationNode* flow, TType* itemType)
        : TStatelessFlowComputationNode(flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , ItemType_(itemType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto maxLen = CalcBlockLen(CalcMaxBlockItemSize(ItemType_));
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), ItemType_, ctx.ArrowMemoryPool, maxLen, &ctx.Builder->GetPgBuilder());

        for (size_t i = 0; i < builder->MaxLength(); ++i) {
            auto result = Flow_->GetValue(ctx);
            if (result.IsFinish() || result.IsYield()) {
                if (i == 0) {
                    return result.Release();
                }
                break;
            }
            builder->Add(result);
        }

        return ctx.HolderFactory.CreateArrowBlock(builder->Build(true));
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
};

class TWideToBlocksWrapper : public TStatefulWideFlowBlockComputationNode<TWideToBlocksWrapper> {
public:
    TWideToBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, types.size() + 1)
        , Flow_(flow)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (; s.Rows_ < s.MaxLength_; ++s.Rows_) {
            if (const auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data()); EFetchResult::One != result) {
                if (EFetchResult::Finish == result) {
                    s.IsFinished_ = true;
                }

                if (EFetchResult::Yield == result || s.Rows_ == 0) {
                    return result;
                }

                break;
            }
            for (size_t j = 0; j < Width_; ++j) {
                if (output[j] != nullptr) {
                    s.Builders_[j]->Add(s.Values_[j]);
                }
            }
        }

        for (size_t i = 0; i < Width_; ++i) {
            if (auto* out = output[i]; out != nullptr) {
                *out = ctx.HolderFactory.CreateArrowBlock(s.Builders_[i]->Build(s.IsFinished_));
            }
        }

        if (auto* out = output[Width_]; out != nullptr) {
            *out = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(s.Rows_)));
        }

        s.Rows_ = 0;
        return EFetchResult::One;
    }

private:
    struct TState : public TComputationValue<TState> {
        std::vector<NUdf::TUnboxedValue> Values_;
        std::vector<NUdf::TUnboxedValue*> ValuePointers_;
        std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
        size_t MaxLength_;
        size_t Rows_ = 0;
        bool IsFinished_ = false;

        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<TType*>& types)
            : TComputationValue(memInfo)
            , Values_(types.size())
            , ValuePointers_(types.size())
        {
            size_t maxBlockItemSize = 0;
            for (size_t i = 0; i < types.size(); ++i) {
                maxBlockItemSize = std::max(CalcMaxBlockItemSize(types[i]), maxBlockItemSize);
            }
            MaxLength_ = CalcBlockLen(maxBlockItemSize);

            for (size_t i = 0; i < types.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
                Builders_.push_back(MakeArrayBuilder(TTypeInfoHelper(), types[i], ctx.ArrowMemoryPool, MaxLength_, &ctx.Builder->GetPgBuilder()));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(ctx, Types_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<TType*> Types_;
    const size_t Width_;
};

class TFromBlocksWrapper : public TMutableComputationNode<TFromBlocksWrapper> {
public:
    TFromBlocksWrapper(TComputationMutables& mutables, IComputationNode* flow, TType* itemType)
        : TMutableComputationNode(mutables)
        , Flow_(flow)
        , ItemType_(itemType)
        , StateIndex_(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = GetState(ctx);

        for (;;) {
            auto item = state.GetValue(ctx);
            if (item) {
                return *item;
            }

            auto input = Flow_->GetValue(ctx);
            if (input.IsFinish()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (input.IsYield()) {
                return NUdf::TUnboxedValue::MakeYield();
            }

            state.Reset(TArrowBlock::From(input).GetDatum());
        }
    }

private:
    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, TType* itemType, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Reader_(MakeBlockReader(TTypeInfoHelper(), itemType))
            , Converter_(MakeBlockItemConverter(TTypeInfoHelper(), itemType, pgBuilder))
        {
        }

        TMaybe<NUdf::TUnboxedValuePod> GetValue(TComputationContext& ctx) {
            for (;;) {
                if (Arrays_.empty()) {
                    return {};
                }
                if (Index_ < ui64(Arrays_.front()->length)) {
                    break;
                }
                Index_ = 0;
                Arrays_.pop_front();
            }
            return Converter_->MakeValue(Reader_->GetItem(*Arrays_.front(), Index_++), ctx.HolderFactory);
        }

        void Reset(const arrow::Datum& datum) {
            MKQL_ENSURE(datum.is_arraylike(), "Expecting array as FromBlocks argument");
            MKQL_ENSURE(Arrays_.empty(), "Not all input is processed");
            if (datum.is_array()) {
                Arrays_.push_back(datum.array());
            } else {
                for (auto& chunk : datum.chunks()) {
                    Arrays_.push_back(chunk->data());
                }
            }
            Index_ = 0;
        }

    private:
        const std::unique_ptr<IBlockReader> Reader_;
        const std::unique_ptr<IBlockItemConverter> Converter_;
        TDeque<std::shared_ptr<arrow::ArrayData>> Arrays_;
        size_t Index_ = 0;
    };

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }

    TState& GetState(TComputationContext& ctx) const {
        auto& result = ctx.MutableValues[StateIndex_];
        if (!result.HasValue()) {
            result = ctx.HolderFactory.Create<TState>(ItemType_, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(result.AsBoxed().Get());
    }

private:
    IComputationNode* const Flow_;
    TType* ItemType_;
    const ui32 StateIndex_;
};

class TWideFromBlocksWrapper : public TStatefulWideFlowComputationNode<TWideFromBlocksWrapper> {
public:
    TWideFromBlocksWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        TVector<TType*>&& types)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        while (s.Index_ == s.Count_) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result != EFetchResult::One) {
                return result;
            }

            s.Index_ = 0;
            s.Count_ = TArrowBlock::From(s.Values_[Width_]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
        }

        for (size_t i = 0; i < Width_; ++i) {
            if (!output[i]) {
                continue;
            }

            const auto& datum = TArrowBlock::From(s.Values_[i]).GetDatum();
            TBlockItem item;
            if (datum.is_scalar()) {
                item = s.Readers_[i]->GetScalarItem(*datum.scalar());
            } else {
                MKQL_ENSURE(datum.is_array(), "Expecting array");
                item = s.Readers_[i]->GetItem(*datum.array(), s.Index_);
            }

            *(output[i]) = s.Converters_[i]->MakeValue(item, ctx.HolderFactory);
        }

        ++s.Index_;
        return EFetchResult::One;
    }

private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<std::unique_ptr<IBlockReader>> Readers_;
        TVector<std::unique_ptr<IBlockItemConverter>> Converters_;
        size_t Count_ = 0;
        size_t Index_ = 0;

        TState(TMemoryUsageInfo* memInfo, const TVector<TType*>& types, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Values_(types.size() + 1)
            , ValuePointers_(types.size() + 1)
        {
            for (size_t i = 0; i < types.size() + 1; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            for (size_t i = 0; i < types.size(); ++i) {
                Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), types[i]));
                Converters_.push_back(MakeBlockItemConverter(TTypeInfoHelper(), types[i], pgBuilder));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Types_, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    IComputationWideFlowNode* Flow_;
    const TVector<TType*> Types_;
    const size_t Width_;
};

class TAsScalarWrapper : public TMutableComputationNode<TAsScalarWrapper> {
public:
    class TArrowNode : public IArrowKernelComputationNode {
    public:
        TArrowNode(const arrow::Datum& datum)
            : Kernel_({}, datum.scalar()->type, [datum](arrow::compute::KernelContext*, const arrow::compute::ExecBatch&, arrow::Datum* res) {
                *res = datum;
                return arrow::Status::OK();
            })
        {
            Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
            Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
        }

        TStringBuf GetKernelName() const final {
            return "AsScalar";
        }

        const arrow::compute::ScalarKernel& GetArrowKernel() const {
            return Kernel_;
        }

        const std::vector<arrow::ValueDescr>& GetArgsDesc() const {
            return EmptyDesc_;
        }

        const IComputationNode* GetArgument(ui32 index) const {
            Y_UNUSED(index);
            ythrow yexception() << "No input arguments";
        }

    private:
        arrow::compute::ScalarKernel Kernel_;
        const std::vector<arrow::ValueDescr> EmptyDesc_;
    };

    TAsScalarWrapper(TComputationMutables& mutables, IComputationNode* arg, TType* type)
        : TMutableComputationNode(mutables)
        , Arg_(arg)
        , Type_(type)
    {
        std::shared_ptr<arrow::DataType> arrowType;
        MKQL_ENSURE(ConvertArrowType(Type_, arrowType), "Unsupported type of scalar");
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result = ConvertScalar(Type_, value, ctx.ArrowMemoryPool);
        return ctx.HolderFactory.CreateArrowBlock(std::move(result));
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        auto value = Arg_->GetValue(ctx);
        arrow::Datum result = ConvertScalar(Type_, value, ctx.ArrowMemoryPool);
        return std::make_unique<TArrowNode>(result);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg_);
    }

private:
    IComputationNode* const Arg_;
    TType* Type_;
};

class TBlockExpandChunkedWrapper : public TStatefulWideFlowBlockComputationNode<TBlockExpandChunkedWrapper> {
public:
    TBlockExpandChunkedWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, ui32 width)
        : TStatefulWideFlowBlockComputationNode(mutables, flow, width)
        , Flow_(flow)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue*const* output) const
    {
        Y_UNUSED(state);
        return Flow_->FetchValues(ctx, output);
    }

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* Flow_;
};


} // namespace

IComputationNode* WrapToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    return new TToBlocksWrapper(LocateNode(ctx.NodeLocator, callable, 0), flowType->GetItemType());
}

IComputationNode* WrapWideToBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    TVector<TType*> items(wideComponents.begin(), wideComponents.end());
    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideToBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto blockType = AS_TYPE(TBlockType, flowType->GetItemType());
    return new TFromBlocksWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), blockType->GetItemType());
}

IComputationNode* WrapWideFromBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");
    TVector<TType*> items;
    for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
        const auto blockType = AS_TYPE(TBlockType, wideComponents[i]);
        items.push_back(blockType->GetItemType());
    }

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TWideFromBlocksWrapper(ctx.Mutables, wideFlow, std::move(items));
}

IComputationNode* WrapAsScalar(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    return new TAsScalarWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), callable.GetInput(0).GetStaticType());
}

IComputationNode* WrapBlockExpandChunked(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    return new TBlockExpandChunkedWrapper(ctx.Mutables, wideFlow, wideComponents.size());
}

}
}
