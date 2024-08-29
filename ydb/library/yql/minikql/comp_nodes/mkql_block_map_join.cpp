#include "mkql_map_join.h"

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

size_t CalcMaxBlockLength(const TVector<TType*>& items) {
    return CalcBlockLen(std::accumulate(items.cbegin(), items.cend(), 0ULL,
        [](size_t max, const TType* type) {
            const TType* itemType = AS_TYPE(TBlockType, type)->GetItemType();
            return std::max(max, CalcMaxBlockItemSize(itemType));
        }));
}

template <bool RightRequired>
class TBlockJoinState : public TBlockState {
public:
    TBlockJoinState(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                    const TVector<TType*>& inputItems,
                    const TVector<TType*> outputItems,
                    NUdf::TUnboxedValue**const fields)
        : TBlockState(memInfo, outputItems.size())
        , InputWidth_(inputItems.size() - 1)
        , OutputWidth_(outputItems.size() - 1)
        , Inputs_(inputItems.size())
        , InputsDescr_(ToValueDescr(inputItems))
    {
        const auto& pgBuilder = ctx.Builder->GetPgBuilder();
        MaxLength_ = CalcMaxBlockLength(outputItems);
        for (size_t i = 0; i < inputItems.size(); i++) {
            fields[i] = &Inputs_[i];
            const TType* blockItemType = AS_TYPE(TBlockType, inputItems[i])->GetItemType();
            Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
            Converters_.push_back(MakeBlockItemConverter(TTypeInfoHelper(), blockItemType, pgBuilder));
        }
        // The last output column (i.e. block length) doesn't require a block builder.
        for (size_t i = 0; i < OutputWidth_; i++) {
            const TType* blockItemType = AS_TYPE(TBlockType, outputItems[i])->GetItemType();
            Builders_.push_back(MakeArrayBuilder(TTypeInfoHelper(), blockItemType, ctx.ArrowMemoryPool, MaxLength_, &pgBuilder, &BuilderAllocatedSize_));
        }
        MaxBuilderAllocatedSize_ = MaxAllocatedFactor_ * BuilderAllocatedSize_;
    }

    void CopyRow() {
        // Copy items from the "left" flow.
        for (size_t i = 0; i < InputWidth_; i++) {
            AddItem(GetItem(i), i);
        }
        OutputRows_++;
    }

    void MakeRow(const NUdf::TUnboxedValuePod& value) {
        // Copy items from the "left" flow.
        for (size_t i = 0; i < InputWidth_; i++) {
            AddItem(GetItem(i), i);
        }
        // Convert and append items from the "right" dict.
        if constexpr (RightRequired) {
            for (size_t i = InputWidth_, j = 0; i < OutputWidth_; i++, j++) {
                AddValue(value.GetElement(j), i);
            }
        } else {
            if (value) {
                for (size_t i = InputWidth_, j = 0; i < OutputWidth_; i++, j++) {
                    AddValue(value.GetElement(j), i);
                }
            } else {
                for (size_t i = InputWidth_; i < OutputWidth_; i++) {
                    AddValue(value, i);
                }
            }
        }
        OutputRows_++;
    }

    void MakeBlocks(const THolderFactory& holderFactory) {
        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(OutputRows_)));
        OutputRows_ = 0;
        BuilderAllocatedSize_ = 0;

        for (size_t i = 0; i < Builders_.size(); i++) {
            Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(IsFinished_));
        }
        FillArrays();
    }

    TBlockItem GetItem(size_t idx) const {
        const auto& datum = TArrowBlock::From(Inputs_[idx]).GetDatum();
        ARROW_DEBUG_CHECK_DATUM_TYPES(InputsDescr_[idx], datum.descr());
        if (datum.is_scalar()) {
            return Readers_[idx]->GetScalarItem(*datum.scalar());
        }
        MKQL_ENSURE(datum.is_array(), "Expecting array");
        return Readers_[idx]->GetItem(*datum.array(), Current_);
    }

    NUdf::TUnboxedValuePod GetValue(const THolderFactory& holderFactory, size_t idx) const {
        return Converters_[idx]->MakeValue(GetItem(idx), holderFactory);
    }

    void Reset() {
        Next_ = 0;
        InputRows_ = GetBlockCount(Inputs_.back());
    }

    void Finish() {
        IsFinished_ = true;
    }

    bool NextRow() {
        if (Next_ >= InputRows_) {
            return false;
        }
        Current_ = Next_++;
        return true;
    }

    bool IsNotFull() const {
        return OutputRows_ < MaxLength_
            && BuilderAllocatedSize_ <= MaxBuilderAllocatedSize_;
    }

    bool IsEmpty() const {
        return OutputRows_ == 0;
    }

    bool IsFinished() const {
        return IsFinished_;
    }

private:
    void AddItem(const TBlockItem& item, size_t idx) {
        Builders_[idx]->Add(item);
    }

    void AddValue(const NUdf::TUnboxedValuePod& value, size_t idx) {
        Builders_[idx]->Add(value);
    }

    size_t Current_ = 0;
    size_t Next_ = 0;
    bool IsFinished_ = false;
    size_t MaxLength_;
    size_t BuilderAllocatedSize_ = 0;
    size_t MaxBuilderAllocatedSize_ = 0;
    static const size_t MaxAllocatedFactor_ = 4;
    size_t InputRows_ = 0;
    size_t OutputRows_ = 0;
    size_t InputWidth_;
    size_t OutputWidth_;
    TUnboxedValueVector Inputs_;
    const std::vector<arrow::ValueDescr> InputsDescr_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<std::unique_ptr<IBlockItemConverter>> Converters_;
    TVector<std::unique_ptr<IArrayBuilder>> Builders_;
};

template <bool WithoutRight, bool RightRequired>
class TBlockWideMapJoinWrapper : public TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<WithoutRight, RightRequired>>
{
using TBaseComputation = TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<WithoutRight, RightRequired>>;
using TState = TBlockJoinState<RightRequired>;
public:
    TBlockWideMapJoinWrapper(TComputationMutables& mutables,
        const TVector<TType*>&& resultJoinItems, const TVector<TType*>&& leftFlowItems,
        TVector<ui32>&& leftKeyColumns,
        IComputationWideFlowNode* flow, IComputationNode* dict)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , ResultJoinItems_(std::move(resultJoinItems))
        , LeftFlowItems_(std::move(leftFlowItems))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , Flow_(flow)
        , Dict_(dict)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(LeftFlowItems_.size()))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;
        const auto dict = Dict_->GetValue(ctx);

        do {
            while (s.IsNotFull() && s.NextRow()) {
                const auto key = MakeKeysTuple(ctx, s, LeftKeyColumns_);
                if constexpr (WithoutRight) {
                    if (key && dict.Contains(key) == RightRequired) {
                        s.CopyRow();
                    }
                } else if constexpr (RightRequired) {
                    if (NUdf::TUnboxedValue lookup; key && (lookup = dict.Lookup(key))) {
                        s.MakeRow(lookup);
                    }
                } else {
                    s.MakeRow(dict.Lookup(key));
                }
            }
            if (!s.IsFinished()) {
                switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::One:
                    s.Reset();
                    continue;
                case EFetchResult::Finish:
                    s.Finish();
                    break;
                }
            }
            // Leave the outer loop, if no values left in the flow.
            Y_DEBUG_ABORT_UNLESS(s.IsFinished());
            break;
        } while (true);

        if (s.IsEmpty()) {
            return EFetchResult::Finish;
        }
        s.MakeBlocks(ctx.HolderFactory);
        const auto sliceSize = s.Slice();

        for (size_t i = 0; i < ResultJoinItems_.size(); i++) {
            if (const auto out = output[i]) {
                *out = s.Get(sliceSize, ctx.HolderFactory, i);
            }
        }

        return EFetchResult::One;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            this->DependsOn(flow, Dict_);
        }
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, LeftFlowItems_, ResultJoinItems_, ctx.WideFields.data() + WideFieldsIndex_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    NUdf::TUnboxedValue MakeKeysTuple(const TComputationContext& ctx, const TState& state, const TVector<ui32>& keyColumns) const {
        // TODO: Handle complex key.
        // TODO: Handle converters.
        return state.GetValue(ctx.HolderFactory, keyColumns.front());
    }

    const TVector<TType*> ResultJoinItems_;
    const TVector<TType*> LeftFlowItems_;
    const TVector<ui32> LeftKeyColumns_;
    IComputationWideFlowNode* const Flow_;
    IComputationNode* const Dict_;
    ui32 WideFieldsIndex_;
};

} // namespace

IComputationNode* WrapBlockMapJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting stream");
    const auto joinFlowType = AS_TYPE(TFlowType, joinType);
    MKQL_ENSURE(joinFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinFlowType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftFlowNode = callable.GetInput(0);
    MKQL_ENSURE(leftFlowNode.GetStaticType()->IsFlow(),
                "Expected WideFlow as a left stream");
    const auto leftFlowType = AS_TYPE(TFlowType, leftFlowNode);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a left stream item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightDictNode = callable.GetInput(1);
    MKQL_ENSURE(rightDictNode.GetStaticType()->IsDict(),
                "Expected Dict as a right join part");
    const auto rightDictType = AS_TYPE(TDictType, rightDictNode);
    MKQL_ENSURE(rightDictType->GetPayloadType()->IsVoid() ||
                rightDictType->GetPayloadType()->IsTuple(),
                "Expected Void or Tuple as a right dict item type");

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    Y_ENSURE(joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left ||
             joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly);

    const auto tupleLiteral = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(tupleLiteral->GetValuesCount());
    for (ui32 i = 0; i < tupleLiteral->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, tupleLiteral->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    // TODO: Handle multi keys.
    Y_ENSURE(leftKeyColumns.size() == 1);

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto dict = LocateNode(ctx.NodeLocator, callable, 1);

    switch (joinKind) {
    case EJoinKind::Inner:
        return new TBlockWideMapJoinWrapper<false, true>(ctx.Mutables,
            std::move(joinItems), std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    case EJoinKind::Left:
        return new TBlockWideMapJoinWrapper<false, false>(ctx.Mutables,
            std::move(joinItems), std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    case EJoinKind::LeftSemi:
        return new TBlockWideMapJoinWrapper<true, true>(ctx.Mutables,
            std::move(joinItems), std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    case EJoinKind::LeftOnly:
        return new TBlockWideMapJoinWrapper<true, false>(ctx.Mutables,
            std::move(joinItems), std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    default:
        Y_ABORT();
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
