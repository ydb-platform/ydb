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
class TBlockWideMapJoinWrapper : public TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<RightRequired>>
{
using TBaseComputation = TStatefulWideFlowComputationNode<TBlockWideMapJoinWrapper<RightRequired>>;
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
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(ResultJoinItems_.size()))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;
        const auto dict = Dict_->GetValue(ctx);

        do {
            while (s.IsNotFull() && s.NextRow()) {
                const auto key = MakeKeysTuple(ctx, s, LeftKeyColumns_);
                if (key && dict.Contains(key) == RightRequired) {
                    s.CopyRow();
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
        s.MakeBlocks();
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

    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
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
        TVector<std::deque<std::shared_ptr<arrow::ArrayData>>> Deques;
        TVector<std::shared_ptr<arrow::ArrayData>> Arrays;
        TVector<std::unique_ptr<IBlockReader>> Readers_;
        TVector<std::unique_ptr<IBlockItemConverter>> Converters_;
        TVector<std::unique_ptr<IArrayBuilder>> Builders_;

    public:
        TState(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
            const TVector<TType*>& inputItems, const TVector<TType*> outputItems,
            NUdf::TUnboxedValue**const fields)
            : TBase(memInfo)
            , InputWidth_(inputItems.size() - 1)
            , OutputWidth_(outputItems.size() - 1)
            , Inputs_(inputItems.size())
            , InputsDescr_(ToValueDescr(inputItems))
            , Deques(OutputWidth_)
            , Arrays(OutputWidth_)
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

        bool IsNotFull() {
            return OutputRows_ < MaxLength_
                && BuilderAllocatedSize_ <= MaxBuilderAllocatedSize_;
        }

        bool IsEmpty() {
            return OutputRows_ == 0;
        }

        bool IsFinished() {
            return IsFinished_;
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

        void AddValue(const NUdf::TUnboxedValuePod& value, size_t idx) {
            Builders_[idx]->Add(value);
        }

        void AddItem(const TBlockItem& item, size_t idx) {
            Builders_[idx]->Add(item);
        }

        void CopyRow() {
            // Copy items from the "left" flow.
            for (size_t i = 0; i < InputWidth_; i++) {
                AddItem(GetItem(i), i);
            }
            OutputRows_++;
        }

        void CopyArray(size_t idx, size_t popCount, const ui8* sparseBitmap, size_t bitmapSize) {
            const auto& datum = TArrowBlock::From(Inputs_[idx]).GetDatum();
            Y_ENSURE(datum.is_array());
            Builders_[idx]->AddMany(*datum.array(), popCount, sparseBitmap, bitmapSize);
        }

        void MakeBlocks() {
            if (OutputRows_ == 0) {
                return;
            }
            BuilderAllocatedSize_ = 0;

            for (size_t i = 0; i < Builders_.size(); i++) {
                const auto& datum = Builders_[i]->Build(IsFinished_);
                Deques[i].clear();
                MKQL_ENSURE(datum.is_arraylike(), "Unexpected block type (expecting array or chunked array)");
                ForEachArrayData(datum, [this, i](const auto& arrayData) {
                    Deques[i].push_back(arrayData);
                });
            }
        }

        ui64 Slice() {
            auto sliceSize = OutputRows_;
            for (size_t i = 0; i < Deques.size(); i++) {
                const auto& arrays = Deques[i];
                if (arrays.empty()) {
                    continue;
                }
                Y_ABORT_UNLESS(ui64(arrays.front()->length) <= OutputRows_);
                sliceSize = std::min<ui64>(sliceSize, arrays.front()->length);
            }

            for (size_t i = 0; i < Arrays.size(); i++) {
                auto& arrays = Deques[i];
                if (arrays.empty()) {
                    continue;
                }
                if (auto& head = arrays.front(); ui64(head->length) == sliceSize) {
                    Arrays[i] = std::move(head);
                    arrays.pop_front();
                } else {
                    Arrays[i] = Chop(head, sliceSize);
                }
            }

            OutputRows_ -= sliceSize;
            return sliceSize;
        }

        NUdf::TUnboxedValuePod Get(const ui64 sliceSize, const THolderFactory& holderFactory, const size_t idx) const {
            MKQL_ENSURE(idx <= OutputWidth_, "Deques index overflow");
            // Return the slice length as the last column value (i.e. block length).
            if (idx == OutputWidth_) {
                return holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(sliceSize)));
            }
            if (auto array = Arrays[idx]) {
                return holderFactory.CreateArrowBlock(std::move(array));
            } else {
                return NUdf::TUnboxedValuePod();
            }
        }

    };

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
    // TODO: Handle other join types.
    Y_ENSURE(joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly);

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
    case EJoinKind::LeftSemi:
        return new TBlockWideMapJoinWrapper<true>(ctx.Mutables, std::move(joinItems),
            std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    case EJoinKind::LeftOnly:
        return new TBlockWideMapJoinWrapper<false>(ctx.Mutables, std::move(joinItems),
            std::move(leftFlowItems), std::move(leftKeyColumns),
            static_cast<IComputationWideFlowNode*>(flow), dict);
    default:
        Y_ABORT();
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
