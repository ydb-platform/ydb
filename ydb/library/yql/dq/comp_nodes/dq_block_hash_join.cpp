#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

// Include block infrastructure from BlockMapJoinCore
#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>

namespace NKikimr::NMiniKQL {

namespace {

// Helper function from BlockMapJoinCore
size_t CalcMaxBlockLength(const TVector<TType*>& items) {
    return std::accumulate(items.cbegin(), items.cend(), size_t(0),
        [](size_t max, const TType* type) {
            return std::max(max, CalcBlockLen(type));
        });
}

// Block join state for hash join (reuse infrastructure from BlockMapJoinCore)
template <bool RightRequired = true>
class TBlockHashJoinState : public TBlockState {
public:
    TBlockHashJoinState(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                    const TVector<TType*>& inputItems,
                    const TVector<ui32>& leftIOMap,
                    const TVector<TType*> outputItems)
        : TBlockState(memInfo, outputItems.size())
        , InputWidth_(inputItems.size() - 1)
        , OutputWidth_(outputItems.size() - 1)
        , Inputs_(inputItems.size())
        , LeftIOMap_(leftIOMap)
        , InputsDescr_(ToValueDescr(inputItems))
    {
        const ui64 maxLength = CalcMaxBlockLength(inputItems);
        MaxLength_ = std::min<ui64>(maxLength, DEFAULT_BLOCK_LEN);
        Readers_.resize(inputItems.size());
        Converters_.resize(inputItems.size());
        Builders_.resize(outputItems.size());
        Hashers_.resize(inputItems.size());

        for (size_t i = 0; i < inputItems.size(); i++) {
            Readers_[i] = MakeBlockReader(TTypeInfoHelper(), inputItems[i]);
            Hashers_[i] = MakeBlockItemHasher(TTypeInfoHelper(), inputItems[i]);
        }

        for (size_t i = 0; i < outputItems.size(); i++) {
            Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), outputItems[i], arrow::default_memory_pool(), MaxLength_, nullptr);
        }
    }

    void CopyRow() {
        for (size_t i = 0; i < InputWidth_; i++) {
            const auto item = GetItem(LeftIOMap_[i]);
            AddItem(item, i);
        }
        OutputRows_++;
    }

    void MakeRow(const std::vector<NYql::NUdf::TBlockItem>& rightColumns) {
        // First, copy left side
        for (size_t i = 0; i < LeftIOMap_.size(); i++) {
            const auto item = GetItem(LeftIOMap_[i]);
            AddItem(item, i);
        }
        // Then add right side
        for (size_t i = 0; i < rightColumns.size(); i++) {
            AddItem(rightColumns[i], LeftIOMap_.size() + i);
        }
        OutputRows_++;
    }

    void MakeBlocks(const THolderFactory& holderFactory) {
        TBlockState::FillArrays(Builders_, OutputRows_);
        TBlockState::FlushArrays(Builders_);
    }

    TBlockItem GetItem(size_t idx, size_t offset = 0) const {
        return Readers_[idx]->GetItem(*TBlockState::GetArray(idx), Current_ + offset);
    }

    std::pair<TBlockItem, ui64> GetItemWithHash(size_t idx, size_t offset) const {
        const auto item = GetItem(idx, offset);
        const auto hash = Hashers_[idx]->Hash(item);
        return std::make_pair(item, hash);
    }

    NUdf::TUnboxedValuePod GetValue(const THolderFactory& holderFactory, size_t idx) const {
        return holderFactory.CreateArrowBlock(Datum(TBlockState::GetArray(idx)));
    }

    void Reset() {
        Current_ = 0;
        OutputRows_ = 0;
        InputRows_ = 0;
        TBlockState::ResetArrays(Builders_);
    }

    void Finish() {
        IsFinished_ = true;
    }

    void NextRow() {
        Current_++;
    }

    bool HasBlocks() {
        return TBlockState::GetValuesCount() > 0;
    }

    bool IsNotFull() const {
        return OutputRows_ < MaxLength_;
    }

    bool IsEmpty() const {
        return OutputRows_ == 0;
    }

    bool IsFinished() const {
        return IsFinished_;
    }

    size_t RemainingRowsCount() const {
        return InputRows_ - Current_;
    }

    NUdf::TUnboxedValue* GetRawInputFields() {
        return Inputs_.data();
    }

    size_t GetInputWidth() const {
        return InputWidth_;
    }

    size_t GetOutputWidth() const {
        return OutputWidth_;
    }

    void AddValue(const NUdf::TUnboxedValuePod& value, size_t idx) {
        Inputs_[idx] = value;
        auto datum = TArrowBlock::From(value).GetDatum();
        TBlockState::AddArray(idx, datum);
        if (0 == idx) {
            InputRows_ = TBlockState::GetArray(idx)->length;
        }
    }

private:
    void AddItem(const TBlockItem& item, size_t idx) {
        Builders_[idx]->Add(item);
    }

    size_t Current_ = 0;
    bool IsFinished_ = false;
    size_t MaxLength_;
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
    TVector<NYql::NUdf::IBlockItemHasher::TPtr> Hashers_;
};

class TBlockHashJoinWrapper : public TMutableComputationNode<TBlockHashJoinWrapper> {
private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWrapper>;
    using TJoinState = TBlockHashJoinState<>;

public:
    TBlockHashJoinWrapper(
        TComputationMutables&   mutables,
        const TVector<TType*>&& resultItemTypes,
        const TVector<TType*>&& leftItemTypes,
        const TVector<ui32>&&   leftKeyColumns,
        const TVector<TType*>&& rightItemTypes,
        const TVector<ui32>&&   rightKeyColumns,
        IComputationNode*       leftStream,
        IComputationNode*       rightStream
    )
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftItemTypes_(std::move(leftItemTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightItemTypes_(std::move(rightItemTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , LeftStream_(leftStream)
        , RightStream_(rightStream)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        // Create IO mapping - for now, include all left columns, then all right columns
        TVector<ui32> leftIOMap;
        for (size_t i = 0; i < LeftItemTypes_.size() - 1; i++) { // -1 for block length
            leftIOMap.push_back(i);
        }

        const auto joinState = ctx.HolderFactory.Create<TJoinState>(
            ctx,
            LeftItemTypes_,
            leftIOMap,
            ResultItemTypes_
        );

        return ctx.HolderFactory.Create<TStreamValue>(
            ctx.HolderFactory,
            std::move(joinState),
            LeftKeyColumns_,
            RightKeyColumns_,
            std::move(LeftStream_->GetValue(ctx)),
            std::move(RightStream_->GetValue(ctx))
        );
    }

private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(
            TMemoryUsageInfo*       memInfo,
            const THolderFactory&   holderFactory,
            NUdf::TUnboxedValue&&   joinState,
            const TVector<ui32>&    leftKeyColumns,
            const TVector<ui32>&    rightKeyColumns,
            NUdf::TUnboxedValue&&   leftStream,
            NUdf::TUnboxedValue&&   rightStream
        )
            : TBase(memInfo)
            , JoinState_(joinState)
            , LeftKeyColumns_(leftKeyColumns)
            , RightKeyColumns_(rightKeyColumns)
            , LeftStream_(std::move(leftStream))
            , RightStream_(std::move(rightStream))
            , HolderFactory_(holderFactory)
        { }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

            auto* inputFields = joinState.GetRawInputFields();
            const size_t inputWidth = joinState.GetInputWidth();
            const size_t outputWidth = joinState.GetOutputWidth();

            MKQL_ENSURE(width == outputWidth,
                        "The given width doesn't equal to the result type size");

            while (!joinState.HasBlocks()) {
                // Phase 1: Process left stream completely
                if (!LeftFinished_) {
                    if (joinState.IsNotFull() && !joinState.IsFinished()) {
                        switch (LeftStream_.WideFetch(inputFields, inputWidth)) {
                        case NUdf::EFetchStatus::Yield:
                            return NUdf::EFetchStatus::Yield;
                        case NUdf::EFetchStatus::Ok:
                            // Add the input block to join state
                            for (size_t i = 0; i < inputWidth; i++) {
                                joinState.AddValue(inputFields[i], i);
                            }
                            // Copy all rows from current block
                            while (joinState.RemainingRowsCount() > 0 && joinState.IsNotFull()) {
                                joinState.CopyRow();
                                joinState.NextRow();
                            }
                            if (joinState.RemainingRowsCount() == 0) {
                                joinState.Reset();
                            }
                            continue;
                        case NUdf::EFetchStatus::Finish:
                            // Process remaining rows from current block if any
                            while (joinState.RemainingRowsCount() > 0 && joinState.IsNotFull()) {
                                joinState.CopyRow();
                                joinState.NextRow();
                            }
                            if (joinState.RemainingRowsCount() == 0) {
                                LeftFinished_ = true;
                                joinState.Reset();
                            }
                            break;
                        }
                    }
                }

                // Phase 2: Process right stream completely  
                if (LeftFinished_ && !RightFinished_) {
                    if (joinState.IsNotFull() && !joinState.IsFinished()) {
                        switch (RightStream_.WideFetch(inputFields, inputWidth)) {
                        case NUdf::EFetchStatus::Yield:
                            return NUdf::EFetchStatus::Yield;
                        case NUdf::EFetchStatus::Ok:
                            // Add the input block to join state
                            for (size_t i = 0; i < inputWidth; i++) {
                                joinState.AddValue(inputFields[i], i);
                            }
                            // Copy all rows from current block
                            while (joinState.RemainingRowsCount() > 0 && joinState.IsNotFull()) {
                                joinState.CopyRow();
                                joinState.NextRow();
                            }
                            if (joinState.RemainingRowsCount() == 0) {
                                joinState.Reset();
                            }
                            continue;
                        case NUdf::EFetchStatus::Finish:
                            // Process remaining rows from current block if any
                            while (joinState.RemainingRowsCount() > 0 && joinState.IsNotFull()) {
                                joinState.CopyRow();
                                joinState.NextRow();
                            }
                            if (joinState.RemainingRowsCount() == 0) {
                                RightFinished_ = true;
                                joinState.Finish();
                            }
                            break;
                        }
                    }
                }

                // Both streams finished or output buffer full
                if ((LeftFinished_ && RightFinished_) || !joinState.IsNotFull()) {
                    if (joinState.IsEmpty()) {
                        return NUdf::EFetchStatus::Finish;
                    }
                    joinState.MakeBlocks(HolderFactory_);
                }
            }

            const auto sliceSize = joinState.Slice();

            for (size_t i = 0; i < outputWidth; i++) {
                output[i] = joinState.Get(sliceSize, HolderFactory_, i);
            }

            return NUdf::EFetchStatus::Ok;
        }

    private:
        bool LeftFinished_ = false;
        bool RightFinished_ = false;

        NUdf::TUnboxedValue                      JoinState_;
        const TVector<ui32>&                     LeftKeyColumns_;
        const TVector<ui32>&                     RightKeyColumns_;
        NUdf::TUnboxedValue                      LeftStream_;
        NUdf::TUnboxedValue                      RightStream_;
        const THolderFactory&                    HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

private:
    const TVector<TType*>   ResultItemTypes_;
    const TVector<TType*>   LeftItemTypes_;
    const TVector<ui32>     LeftKeyColumns_;
    const TVector<TType*>   RightItemTypes_;
    const TVector<ui32>     RightKeyColumns_;
    IComputationNode*       LeftStream_;
    IComputationNode*       RightStream_;
};

} // namespace

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
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

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    MKQL_ENSURE(rightStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> rightStreamItems(rightStreamComponents.cbegin(), rightStreamComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    MKQL_ENSURE(joinKind == EJoinKind::Inner,
                "Only inner join is supported in block hash join prototype");

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(leftKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    TVector<ui32> rightKeyColumns;
    rightKeyColumns.reserve(rightKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        rightKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);

    return new TBlockHashJoinWrapper(
        ctx.Mutables,
        std::move(joinItems),
        std::move(leftStreamItems),
        std::move(leftKeyColumns),
        std::move(rightStreamItems),
        std::move(rightKeyColumns),
        leftStream,
        rightStream
    );
}

} // namespace NKikimr::NMiniKQL

