#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

// Include block infrastructure from BlockMapJoinCore
#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>

#include <algorithm>
#include <arrow/scalar.h>

namespace NKikimr::NMiniKQL {

namespace {

// Core block hash join implementation - works only with blocks
class TBlockHashJoinWrapper : public TMutableComputationNode<TBlockHashJoinWrapper> {
private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWrapper>;

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
        return ctx.HolderFactory.Create<TStreamValue>(
            ctx.HolderFactory,
            std::move(LeftStream_->GetValue(ctx)),
            std::move(RightStream_->GetValue(ctx)),
            LeftItemTypes_.size(),
            RightItemTypes_.size(),
            ResultItemTypes_
        );
    }

private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(
            TMemoryUsageInfo*       memInfo,
            const THolderFactory&   holderFactory,
            NUdf::TUnboxedValue&&   leftStream,
            NUdf::TUnboxedValue&&   rightStream,
            size_t                  leftStreamWidth,
            size_t                  rightStreamWidth,
            const TVector<TType*>&  resultItemTypes
        )
            : TBase(memInfo)
            , HolderFactory_(holderFactory)
            , LeftStream_(std::move(leftStream))
            , RightStream_(std::move(rightStream))
            , LeftStreamWidth_(leftStreamWidth)
            , RightStreamWidth_(rightStreamWidth)
            , ResultItemTypes_(resultItemTypes)
        { }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            Y_DEBUG_ABORT_UNLESS(width == ResultItemTypes_.size());
            
            // Simple concatenation logic for now: read left stream first, then right stream
            if (!LeftFinished_) {
                TVector<NUdf::TUnboxedValue> leftInput(LeftStreamWidth_);
                auto status = LeftStream_.WideFetch(leftInput.data(), LeftStreamWidth_);
                
                if (status == NUdf::EFetchStatus::Ok) {
                    CopyBlockToOutput(leftInput, output, width);
                    return NUdf::EFetchStatus::Ok;
                } else if (status == NUdf::EFetchStatus::Yield) {
                    return NUdf::EFetchStatus::Yield;
                } else {
                    LeftFinished_ = true;
                }
            }
            
            if (!RightFinished_) {
                TVector<NUdf::TUnboxedValue> rightInput(RightStreamWidth_);
                auto status = RightStream_.WideFetch(rightInput.data(), RightStreamWidth_);
                
                if (status == NUdf::EFetchStatus::Ok) {
                    CopyBlockToOutput(rightInput, output, width);
                    return NUdf::EFetchStatus::Ok;
                } else if (status == NUdf::EFetchStatus::Yield) {
                    return NUdf::EFetchStatus::Yield;
                } else {
                    RightFinished_ = true;
                }
            }
            
            return NUdf::EFetchStatus::Finish;
        }

        void CopyBlockToOutput(const TVector<NUdf::TUnboxedValue>& input, NUdf::TUnboxedValue* output, ui32 width) {
            // Copy block data - assume all types are blocks
            size_t dataCols = std::min(static_cast<size_t>(width), input.size()) - 1;
            for (size_t i = 0; i < dataCols; i++) {
                output[i] = input[i];
            }
            
            // Copy block length to the last position
            if (width > 0) {
                output[width - 1] = input[input.size() - 1];
            }
            
            // Fill remaining columns with empty blocks
            for (size_t i = dataCols; i < width - 1; i++) {
                auto blockItemType = AS_TYPE(TBlockType, ResultItemTypes_[i])->GetItemType();
                std::shared_ptr<arrow::DataType> arrowType;
                MKQL_ENSURE(ConvertArrowType(blockItemType, arrowType), "Failed to convert type to arrow");
                auto emptyArray = arrow::MakeArrayOfNull(arrowType, 0);
                ARROW_OK(emptyArray.status());
                output[i] = HolderFactory_.CreateArrowBlock(arrow::Datum(emptyArray.ValueOrDie()));
            }
        }

    private:
        bool LeftFinished_ = false;
        bool RightFinished_ = false;
        
        const THolderFactory& HolderFactory_;
        NUdf::TUnboxedValue LeftStream_;
        NUdf::TUnboxedValue RightStream_;
        const size_t LeftStreamWidth_;
        const size_t RightStreamWidth_;
        const TVector<TType*>& ResultItemTypes_;
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

    // Simple block hash join - expects all inputs and outputs to be blocks
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

