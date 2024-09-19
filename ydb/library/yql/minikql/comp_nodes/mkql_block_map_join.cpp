#include "mkql_map_join.h"

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <util/generic/serialized_enum.h>

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
                    const TVector<ui32>& leftIOMap,
                    const TVector<TType*> outputItems,
                    NUdf::TUnboxedValue**const fields)
        : TBlockState(memInfo, outputItems.size())
        , InputWidth_(inputItems.size() - 1)
        , OutputWidth_(outputItems.size() - 1)
        , Inputs_(inputItems.size())
        , LeftIOMap_(leftIOMap)
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
        // Use the mapping from input fields to output ones to
        // produce a tight loop to copy row items.
        for (size_t i = 0; i < LeftIOMap_.size(); i++) {
            AddItem(GetItem(LeftIOMap_[i]), i);
        }
        OutputRows_++;
    }

    void MakeRow(const NUdf::TUnboxedValuePod& value) {
        size_t builderIndex = 0;
        // Copy items from the "left" flow.
        // Use the mapping from input fields to output ones to
        // produce a tight loop to copy row items.
        for (size_t i = 0; i < LeftIOMap_.size(); i++, builderIndex++) {
            AddItem(GetItem(LeftIOMap_[i]), i);
        }
        // Convert and append items from the "right" dict.
        // Since the keys are copied to the output only from the
        // "left" flow, process all values unconditionally.
        if constexpr (RightRequired) {
            for (size_t i = 0; builderIndex < OutputWidth_; i++) {
                AddValue(value.GetElement(i), builderIndex++);
            }
        } else {
            if (value) {
                for (size_t i = 0; builderIndex < OutputWidth_; i++) {
                    AddValue(value.GetElement(i), builderIndex++);
                }
            } else {
                while (builderIndex < OutputWidth_) {
                    AddValue(value, builderIndex++);
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

    bool HasBlocks() {
        return Count > 0;
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

    NUdf::TUnboxedValue* GetRawInputFields() {
        return Inputs_.data();
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
    const TVector<ui32> LeftIOMap_;
    const std::vector<arrow::ValueDescr> InputsDescr_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<std::unique_ptr<IBlockItemConverter>> Converters_;
    TVector<std::unique_ptr<IArrayBuilder>> Builders_;
};

template <bool WithoutRight, bool RightRequired, bool IsTuple>
class TBlockWideMapJoinWrapper : public TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<WithoutRight, RightRequired, IsTuple>>
{
using TBaseComputation = TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<WithoutRight, RightRequired, IsTuple>>;
using TState = TBlockJoinState<RightRequired>;
public:
    TBlockWideMapJoinWrapper(TComputationMutables& mutables,
        const TVector<TType*>&& resultJoinItems, const TVector<TType*>&& leftStreamItems,
        const TVector<ui32>&& leftKeyColumns, const TVector<ui32>&& leftIOMap,
        IComputationWideFlowNode* flow, IComputationNode* dict)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , ResultJoinItems_(std::move(resultJoinItems))
        , LeftStreamItems_(std::move(leftStreamItems))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , LeftIOMap_(leftIOMap)
        , Flow_(flow)
        , Dict_(dict)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(LeftStreamItems_.size()))
        , KeyTuple_(mutables)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& blockState = GetState(state, ctx);
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;
        const auto dict = Dict_->GetValue(ctx);

        while (!blockState.HasBlocks()) {
            while (blockState.IsNotFull() && blockState.NextRow()) {
                const auto key = MakeKeysTuple(ctx, blockState);
                if constexpr (WithoutRight) {
                    if (key && dict.Contains(key) == RightRequired) {
                        blockState.CopyRow();
                    }
                } else if constexpr (RightRequired) {
                    if (NUdf::TUnboxedValue lookup; key && (lookup = dict.Lookup(key))) {
                        blockState.MakeRow(lookup);
                    }
                } else {
                    blockState.MakeRow(dict.Lookup(key));
                }
            }
            if (blockState.IsNotFull() && !blockState.IsFinished()) {
                switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::One:
                    blockState.Reset();
                    continue;
                case EFetchResult::Finish:
                    blockState.Finish();
                    break;
                }
                // Leave the loop, if no values left in the flow.
                Y_DEBUG_ABORT_UNLESS(blockState.IsFinished());
            }
            if (blockState.IsEmpty()) {
                return EFetchResult::Finish;
            }
            blockState.MakeBlocks(ctx.HolderFactory);
        }

        const auto sliceSize = blockState.Slice();

        for (size_t i = 0; i < ResultJoinItems_.size(); i++) {
            if (const auto out = output[i]) {
                *out = blockState.Get(sliceSize, ctx.HolderFactory, i);
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
        state = ctx.HolderFactory.Create<TState>(ctx, LeftStreamItems_, LeftIOMap_,
                                                 ResultJoinItems_, ctx.WideFields.data() + WideFieldsIndex_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    NUdf::TUnboxedValue MakeKeysTuple(TComputationContext& ctx, const TState& state) const {
        // TODO: Handle converters.
        if constexpr (!IsTuple) {
            return state.GetValue(ctx.HolderFactory, LeftKeyColumns_.front());
        }

        NUdf::TUnboxedValue* items = nullptr;
        const auto keys = KeyTuple_.NewArray(ctx, LeftKeyColumns_.size(), items);
        for (size_t i = 0; i < LeftKeyColumns_.size(); i++) {
            items[i] = state.GetValue(ctx.HolderFactory, LeftKeyColumns_[i]);
        }
        return keys;
    }

    const TVector<TType*> ResultJoinItems_;
    const TVector<TType*> LeftStreamItems_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<ui32> LeftIOMap_;
    IComputationWideFlowNode* const Flow_;
    IComputationNode* const Dict_;
    ui32 WideFieldsIndex_;
    const TContainerCacheOnContext KeyTuple_;
};

template<bool RightRequired, bool IsTuple>
class TBlockWideMultiMapJoinWrapper : public TPairStateWideFlowComputationNode<TBlockWideMultiMapJoinWrapper<RightRequired, IsTuple>>
{
using TBaseComputation = TPairStateWideFlowComputationNode<TBlockWideMultiMapJoinWrapper<RightRequired, IsTuple>>;
using TState = TBlockJoinState<RightRequired>;
public:
    TBlockWideMultiMapJoinWrapper(TComputationMutables& mutables,
        const TVector<TType*>&& resultJoinItems, const TVector<TType*>&& leftStreamItems,
        const TVector<ui32>&& leftKeyColumns, const TVector<ui32>&& leftIOMap,
        IComputationWideFlowNode* flow, IComputationNode* dict)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Boxed)
        , ResultJoinItems_(std::move(resultJoinItems))
        , LeftStreamItems_(std::move(leftStreamItems))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , LeftIOMap_(leftIOMap)
        , Flow_(flow)
        , Dict_(dict)
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(LeftStreamItems_.size()))
        , KeyTuple_(mutables)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, NUdf::TUnboxedValue& iterator, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& blockState = GetState(state, ctx);
        auto& iterState = GetIterator(iterator, ctx);
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;
        const auto dict = Dict_->GetValue(ctx);

        while (!blockState.HasBlocks()) {
            if (iterState) {
                NUdf::TUnboxedValue lookupItem;
                // Process the remaining items from the iterator.
                while (blockState.IsNotFull() && iterState.Next(lookupItem)) {
                    blockState.MakeRow(lookupItem);
                }
            }
            if (blockState.IsNotFull() && blockState.NextRow()) {
                const auto key = MakeKeysTuple(ctx, blockState);
                // Lookup the item in the right dict. If the lookup succeeds,
                // reset the iterator and proceed the execution from the
                // beginning of the outer loop. Otherwise, the iterState is
                // already invalidated (i.e. finished), so the execution will
                // process the next tuple from the left flow.
                if (NUdf::TUnboxedValue lookup; key && (lookup = dict.Lookup(key))) {
                    iterState.Reset(std::move(lookup));
                } else if constexpr (!RightRequired) {
                    blockState.MakeRow(NUdf::TUnboxedValue());
                }
                continue;
            }
            if (blockState.IsNotFull() && !blockState.IsFinished()) {
                switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::One:
                    blockState.Reset();
                    continue;
                case EFetchResult::Finish:
                    blockState.Finish();
                    break;
                }
                // Leave the loop, if no values left in the flow.
                Y_DEBUG_ABORT_UNLESS(blockState.IsFinished());
            }
            if (blockState.IsEmpty()) {
                return EFetchResult::Finish;
            }
            blockState.MakeBlocks(ctx.HolderFactory);
        }

        const auto sliceSize = blockState.Slice();

        for (size_t i = 0; i < ResultJoinItems_.size(); i++) {
            if (const auto out = output[i]) {
                *out = blockState.Get(sliceSize, ctx.HolderFactory, i);
            }
        }

        return EFetchResult::One;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_))
            this->DependsOn(flow, Dict_);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ctx, LeftStreamItems_, LeftIOMap_,
                                                 ResultJoinItems_, ctx.WideFields.data() + WideFieldsIndex_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    class TIterator : public TComputationValue<TIterator> {
    using TBase = TComputationValue<TIterator>;
        NUdf::TUnboxedValue List_;
        NUdf::TUnboxedValue Iterator_;
        NUdf::TUnboxedValue Current_;

    public:
        TIterator(TMemoryUsageInfo* memInfo)
            : TBase(memInfo)
            , List_(NUdf::TUnboxedValue::Invalid())
            , Iterator_(NUdf::TUnboxedValue::Invalid())
            , Current_(NUdf::TUnboxedValue::Invalid())
        {}

        inline explicit operator bool() const { return !Iterator_.IsInvalid(); }
        void Reset(const NUdf::TUnboxedValue&& list) {
            List_ = std::move(list);
            Iterator_ = List_.GetListIterator();
        }
        bool Next(NUdf::TUnboxedValue& item) {
            const auto found = Iterator_.Next(Current_);
            item = Current_;
            return found;
        }
    };

    void MakeIterator(TComputationContext& ctx, NUdf::TUnboxedValue& iterator) const {
        iterator = ctx.HolderFactory.Create<TIterator>();
    }

    TIterator& GetIterator(NUdf::TUnboxedValue& iterator, TComputationContext& ctx) const {
        if (iterator.IsInvalid()) {
            MakeIterator(ctx, iterator);
        }
        return *static_cast<TIterator*>(iterator.AsBoxed().Get());
    }

    NUdf::TUnboxedValue MakeKeysTuple(TComputationContext& ctx, const TState& state) const {
        // TODO: Handle converters.
        if constexpr (!IsTuple) {
            return state.GetValue(ctx.HolderFactory, LeftKeyColumns_.front());
        }

        NUdf::TUnboxedValue* items = nullptr;
        const auto keys = KeyTuple_.NewArray(ctx, LeftKeyColumns_.size(), items);
        for (size_t i = 0; i < LeftKeyColumns_.size(); i++) {
            items[i] = state.GetValue(ctx.HolderFactory, LeftKeyColumns_[i]);
        }
        return keys;
    }

    const TVector<TType*> ResultJoinItems_;
    const TVector<TType*> LeftStreamItems_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<ui32> LeftIOMap_;
    IComputationWideFlowNode* const Flow_;
    IComputationNode* const Dict_;
    ui32 WideFieldsIndex_;
    const TContainerCacheOnContext KeyTuple_;
};

} // namespace

IComputationNode* WrapBlockMapJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    MKQL_ENSURE(leftStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> leftStreamItems(leftStreamComponents.cbegin(), leftStreamComponents.cend());

    const auto rightDictNode = callable.GetInput(1);
    MKQL_ENSURE(rightDictNode.GetStaticType()->IsDict(),
                "Expected Dict as a right join part");
    const auto rightDictType = AS_TYPE(TDictType, rightDictNode)->GetPayloadType();
    const auto isMulti = rightDictType->IsList();
    const auto rightDictItemType = isMulti
                                 ? AS_TYPE(TListType, rightDictType)->GetItemType()
                                 : rightDictType;
    MKQL_ENSURE(rightDictItemType->IsVoid() || rightDictItemType->IsTuple(),
                "Expected Void or Tuple as a right dict item type");

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    Y_ENSURE(joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left ||
             joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly);

    const auto keyColumnsLiteral = callable.GetInput(3);
    const auto keyColumnsTuple = AS_VALUE(TTupleLiteral, keyColumnsLiteral);
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(keyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < keyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, keyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    const bool isTupleKey = leftKeyColumns.size() > 1;

    const auto keyDropsLiteral = callable.GetInput(4);
    const auto keyDropsTuple = AS_VALUE(TTupleLiteral, keyDropsLiteral);
    THashSet<ui32> leftKeyDrops;
    leftKeyDrops.reserve(keyDropsTuple->GetValuesCount());
    for (ui32 i = 0; i < keyDropsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, keyDropsTuple->GetValue(i));
        leftKeyDrops.emplace(item->AsValue().Get<ui32>());
    }

    const THashSet<ui32> leftKeySet(leftKeyColumns.cbegin(), leftKeyColumns.cend());
    for (const auto& drop : leftKeyDrops) {
        MKQL_ENSURE(leftKeySet.contains(drop),
                    "Only key columns has to be specified in drop column set");

    }

    TVector<ui32> leftIOMap;
    // XXX: Mind the last wide item, containing block length.
    for (size_t i = 0; i < leftStreamItems.size() - 1; i++) {
        if (leftKeyDrops.contains(i)) {
            continue;
        }
        leftIOMap.push_back(i);
    }

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto dict = LocateNode(ctx.NodeLocator, callable, 1);

#define DISPATCH_JOIN(IS_TUPLE) do {                                                \
    switch (joinKind) {                                                             \
    case EJoinKind::Inner:                                                          \
        if (isMulti) {                                                              \
            return new TBlockWideMultiMapJoinWrapper<true, IS_TUPLE>(ctx.Mutables,  \
                std::move(joinItems), std::move(leftStreamItems),                   \
                std::move(leftKeyColumns), std::move(leftIOMap),                    \
                static_cast<IComputationWideFlowNode*>(flow), dict);                \
        }                                                                           \
        return new TBlockWideMapJoinWrapper<false, true, IS_TUPLE>(ctx.Mutables,    \
            std::move(joinItems), std::move(leftStreamItems),                       \
            std::move(leftKeyColumns), std::move(leftIOMap),                        \
            static_cast<IComputationWideFlowNode*>(flow), dict);                    \
    case EJoinKind::Left:                                                           \
        if (isMulti) {                                                              \
            return new TBlockWideMultiMapJoinWrapper<false, IS_TUPLE>(ctx.Mutables, \
                std::move(joinItems), std::move(leftStreamItems),                   \
                std::move(leftKeyColumns), std::move(leftIOMap),                    \
                static_cast<IComputationWideFlowNode*>(flow), dict);                \
        }                                                                           \
        return new TBlockWideMapJoinWrapper<false, false, IS_TUPLE>(ctx.Mutables,   \
            std::move(joinItems), std::move(leftStreamItems),                       \
            std::move(leftKeyColumns), std::move(leftIOMap),                        \
            static_cast<IComputationWideFlowNode*>(flow), dict);                    \
    case EJoinKind::LeftSemi:                                                       \
        return new TBlockWideMapJoinWrapper<true, true, IS_TUPLE>(ctx.Mutables,     \
            std::move(joinItems), std::move(leftStreamItems),                       \
            std::move(leftKeyColumns), std::move(leftIOMap),                        \
            static_cast<IComputationWideFlowNode*>(flow), dict);                    \
    case EJoinKind::LeftOnly:                                                       \
        return new TBlockWideMapJoinWrapper<true, false, IS_TUPLE>(ctx.Mutables,    \
            std::move(joinItems), std::move(leftStreamItems),                       \
            std::move(leftKeyColumns), std::move(leftIOMap),                        \
            static_cast<IComputationWideFlowNode*>(flow), dict);                    \
    default:                                                                        \
        /* TODO: Display the human-readable join kind name. */                      \
        MKQL_ENSURE(false, "BlockMapJoinCore doesn't support join type #"           \
                    << static_cast<ui32>(joinKind));                                \
    }                                                                               \
} while(0)

    if (isTupleKey) {
        DISPATCH_JOIN(true);
    } else {
        DISPATCH_JOIN(false);
    }

#undef DISPATCH_JOIN
}

} // namespace NMiniKQL
} // namespace NKikimr
