
#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>

#include <algorithm>
#include <arrow/scalar.h>

namespace NKikimr::NMiniKQL {

namespace {

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
            LeftKeyColumns_,
            RightKeyColumns_,
            std::move(LeftStream_->GetValue(ctx)),
            std::move(RightStream_->GetValue(ctx)),
            ResultItemTypes_,
            LeftItemTypes_.size(),   // Left stream width  
            RightItemTypes_.size()   // Right stream width
        );
    }

private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(
            TMemoryUsageInfo*       memInfo,
            const THolderFactory&   holderFactory,
            const TVector<ui32>&    leftKeyColumns,
            const TVector<ui32>&    rightKeyColumns,
            NUdf::TUnboxedValue&&   leftStream,
            NUdf::TUnboxedValue&&   rightStream,
            const TVector<TType*>&  resultItemTypes,
            size_t                  leftStreamWidth,
            size_t                  rightStreamWidth
        )
            : TBase(memInfo)
            , LeftKeyColumns_(leftKeyColumns)
            , RightKeyColumns_(rightKeyColumns)
            , LeftStream_(std::move(leftStream))
            , RightStream_(std::move(rightStream))
            , HolderFactory_(holderFactory)
            , ResultItemTypes_(resultItemTypes)
            , LeftStreamWidth_(leftStreamWidth)
            , RightStreamWidth_(rightStreamWidth)
        { }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            Y_DEBUG_ABORT_UNLESS(width == ResultItemTypes_.size());
            
            Cerr << "WideFetch called: width=" << width 
                 << " leftWidth=" << LeftStreamWidth_ 
                 << " rightWidth=" << RightStreamWidth_
                 << " leftFinished=" << LeftFinished_
                 << " rightFinished=" << RightFinished_ << Endl;
            
            
            if (!LeftFinished_) {
                TVector<NUdf::TUnboxedValue> leftInput(LeftStreamWidth_);
                Cerr << "Trying to read left stream with width " << LeftStreamWidth_ << Endl;
                auto status = LeftStream_.WideFetch(leftInput.data(), LeftStreamWidth_);
                Cerr << "Left stream status: " << (int)status << Endl;
                
                switch (status) {
                case NUdf::EFetchStatus::Ok: {
                    Cerr << "Left stream read successful!" << Endl;
                    
                    size_t dataCols = std::min(static_cast<size_t>(width), LeftStreamWidth_) - 1;
                    for (size_t i = 0; i < dataCols; i++) {
                        Cerr << "Copying leftInput[" << i << "] IsBoxed=" << leftInput[i].IsBoxed() 
                             << " IsSpecial=" << leftInput[i].IsSpecial()
                             << " IsInvalid=" << leftInput[i].IsInvalid() << Endl;
                        output[i] = std::move(leftInput[i]);
                        Cerr << "Successfully copied leftInput[" << i << "]" << Endl;
                    }
                    
                    if (width > 0) {
                        size_t blockLengthSrcIdx = LeftStreamWidth_ - 1;
                        size_t blockLengthDstIdx = width - 1;
                        Cerr << "Copying block length from leftInput[" << blockLengthSrcIdx << "] to output[" << blockLengthDstIdx << "] IsBoxed=" 
                             << leftInput[blockLengthSrcIdx].IsBoxed() 
                             << " IsEmpty=" << !leftInput[blockLengthSrcIdx]
                             << " IsEmbedded=" << leftInput[blockLengthSrcIdx].IsEmbedded() << Endl;
                        
                        output[blockLengthDstIdx] = std::move(leftInput[blockLengthSrcIdx]);
                    }
                    
                    for (size_t i = dataCols; i < width - 1; i++) {
                        Cerr << "Creating empty array for output[" << i << "]" << Endl;
                        auto blockItemType = AS_TYPE(TBlockType, ResultItemTypes_[i])->GetItemType();
                        std::shared_ptr<arrow::DataType> arrowType;
                        MKQL_ENSURE(ConvertArrowType(blockItemType, arrowType), "Failed to convert type to arrow");
                        auto emptyArray = arrow::MakeArrayOfNull(arrowType, 0);
                        ARROW_OK(emptyArray.status());
                        output[i] = HolderFactory_.CreateArrowBlock(arrow::Datum(emptyArray.ValueOrDie()));
                    }
                    return NUdf::EFetchStatus::Ok;
                }
                    
                case NUdf::EFetchStatus::Yield:
                    return NUdf::EFetchStatus::Yield;
                    
                case NUdf::EFetchStatus::Finish:
                    Cerr << "Left stream finished!" << Endl;
                    LeftFinished_ = true;
                    break;
                }
            }
            
            if (!RightFinished_) {
                TVector<NUdf::TUnboxedValue> rightInput(RightStreamWidth_);
                auto status = RightStream_.WideFetch(rightInput.data(), RightStreamWidth_);
                
                switch (status) {
                case NUdf::EFetchStatus::Ok: {
                    Cerr << "Right stream read successful!" << Endl;
                    size_t dataCols = std::min(static_cast<size_t>(width), RightStreamWidth_) - 1;
                    for (size_t i = 0; i < dataCols; i++) {
                        Cerr << "Copying rightInput[" << i << "] IsBoxed=" << rightInput[i].IsBoxed() << Endl;
                        output[i] = std::move(rightInput[i]);
                    }
                    
                    if (width > 0) {
                        size_t blockLengthSrcIdx = RightStreamWidth_ - 1;
                        size_t blockLengthDstIdx = width - 1;
                        Cerr << "Copying block length from rightInput[" << blockLengthSrcIdx << "] to output[" << blockLengthDstIdx << "] IsBoxed=" 
                             << rightInput[blockLengthSrcIdx].IsBoxed() 
                             << " IsEmpty=" << !rightInput[blockLengthSrcIdx]
                             << " IsEmbedded=" << rightInput[blockLengthSrcIdx].IsEmbedded() << Endl;
                        
                        output[blockLengthDstIdx] = std::move(rightInput[blockLengthSrcIdx]);
                    }
                    
                    for (size_t i = dataCols; i < width - 1; i++) {
                        Cerr << "Creating empty array for output[" << i << "]" << Endl;
                        auto blockItemType = AS_TYPE(TBlockType, ResultItemTypes_[i])->GetItemType();
                        std::shared_ptr<arrow::DataType> arrowType;
                        MKQL_ENSURE(ConvertArrowType(blockItemType, arrowType), "Failed to convert type to arrow");
                        auto emptyArray = arrow::MakeArrayOfNull(arrowType, 0);
                        ARROW_OK(emptyArray.status());
                        output[i] = HolderFactory_.CreateArrowBlock(arrow::Datum(emptyArray.ValueOrDie()));
                    }
                    return NUdf::EFetchStatus::Ok;
                }
                    
                case NUdf::EFetchStatus::Yield:
                    return NUdf::EFetchStatus::Yield;
                    
                case NUdf::EFetchStatus::Finish:
                    Cerr << "Right stream finished!" << Endl;
                    RightFinished_ = true;
                    break;
                }
            }
            
            Cerr << "Both streams finished, returning Finish" << Endl;
            return NUdf::EFetchStatus::Finish;
        }

    private:
        bool LeftFinished_ = false;
        bool RightFinished_ = false;
        
        [[maybe_unused]] const TVector<ui32>&    LeftKeyColumns_;
        [[maybe_unused]] const TVector<ui32>&    RightKeyColumns_;
        NUdf::TUnboxedValue                      LeftStream_;
        NUdf::TUnboxedValue                      RightStream_;
        const THolderFactory&                    HolderFactory_;
        const TVector<TType*>&                   ResultItemTypes_;
        const size_t                             LeftStreamWidth_;
        const size_t                             RightStreamWidth_;
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


