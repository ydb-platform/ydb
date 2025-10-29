

#include "dq_scalar_hash_join.h"
#include "dq_join_common.h"
#include <dq_hash_join_table.h>
#include <hash_join_utils/scalar_layout_converter.h>
#include <ranges>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

namespace NKikimr::NMiniKQL {

namespace {
// Packed tuple source for scalar join
class TScalarPackedTupleSource : public NNonCopyable::TMoveOnly {
  public:
    TScalarPackedTupleSource(TComputationContext& ctx, IComputationWideFlowNode* flow, const std::vector<TType*>& types, IScalarLayoutConverter* converter)
        : Ctx_(&ctx)
        , Flow_(flow)
        , ConsumeBuff_(types.size())
        , Pointers_(types.size())
        , Converter_(converter)
    {
        for (int index = 0; index < std::ssize(types); ++index) {
            Pointers_[index] = &ConsumeBuff_[index];
        }
    }

    bool Finished() const {
        return Finished_;
    }

    int UserDataCols() const {
        return ConsumeBuff_.size();
    }

    FetchResult<IScalarLayoutConverter::TPackResult> FetchRow() {
        if (Finished()) {
            return Finish{};
        }
        auto res = Flow_->FetchValues(*Ctx_, Pointers_.data());
        if (res != EFetchResult::One) {
            if (res == EFetchResult::Finish) {
                Finished_ = true;
                return Finish{};
            }
            return Yield{};
        }
        
        IScalarLayoutConverter::TPackResult result;
        Converter_->Pack(ConsumeBuff_.data(), result);
        return One{std::move(result)};
    }

  private:
    bool Finished_ = false;
    TComputationContext* Ctx_;
    IComputationWideFlowNode* Flow_;
    std::vector<NYql::NUdf::TUnboxedValue> ConsumeBuff_;
    std::vector<NYql::NUdf::TUnboxedValue*> Pointers_;
    IScalarLayoutConverter* Converter_;
};

// Output for packed scalar tuples with unpacking to UnboxedValues
struct TScalarPackedOutput : NNonCopyable::TMoveOnly {
    TScalarPackedOutput(TDqUserRenames renames, TSides<IScalarLayoutConverter*> converters, const THolderFactory& holderFactory)
        : Renames_(std::move(renames))
        , Converters_(converters)
        , HolderFactory_(holderFactory)
    {}

    int Columns() const {
        return Renames_.size();
    }

    i64 SizeTuples() const {
        return Output_.NItems;
    }

    struct PackedTuplesData {
        std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
        std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
    };

    struct TuplePairs {
        i64 NItems = 0;
        TSides<PackedTuplesData> Data;
    };

    auto MakeConsumeFn() {
        return [this](TSides<NJoinTable::TNeumannJoinTable::Tuple> tuples) {
            ForEachSide([&](ESide side) {
                Converters_.SelectSide(side)->GetTupleLayout()->TupleDeepCopy(
                    tuples.SelectSide(side).PackedData, tuples.SelectSide(side).OverflowBegin,
                    Output_.Data.SelectSide(side).PackedTuples, Output_.Data.SelectSide(side).Overflow);
            });
            Output_.NItems++;
        };
    }

    void FlushToBuffer() {
        if (Output_.NItems == 0) {
            return;
        }
        
        // Unpack all tuples and store in OutputBuffer
        for (i64 tupleIdx = 0; tupleIdx < Output_.NItems; ++tupleIdx) {
            TSides<std::vector<NYql::NUdf::TUnboxedValue>> sideValues;
            
            ForEachSide([&](ESide side) {
                IScalarLayoutConverter::TPackResult packResult;
                packResult.PackedTuples = Output_.Data.SelectSide(side).PackedTuples;
                packResult.Overflow = Output_.Data.SelectSide(side).Overflow;
                packResult.NTuples = Output_.NItems;
                
                auto converter = Converters_.SelectSide(side);
                auto& values = sideValues.SelectSide(side);
                
                // Get column count for this side
                size_t columnCount = 0;
                if (side == ESide::Build) {
                    for (const auto& rename : Renames_) {
                        if (rename.Side == EJoinSide::kRight) {
                            columnCount = std::max(columnCount, static_cast<size_t>(rename.Index + 1));
                        }
                    }
                } else {
                    for (const auto& rename : Renames_) {
                        if (rename.Side == EJoinSide::kLeft) {
                            columnCount = std::max(columnCount, static_cast<size_t>(rename.Index + 1));
                        }
                    }
                }
                
                values.resize(columnCount);
                converter->Unpack(packResult, tupleIdx, values.data(), HolderFactory_);
            });
            
            // Apply renames and add to buffer
            for (const auto& rename : Renames_) {
                if (rename.Side == EJoinSide::kLeft) {
                    OutputBuffer.push_back(sideValues.Probe[rename.Index]);
                } else {
                    OutputBuffer.push_back(sideValues.Build[rename.Index]);
                }
            }
        }
        
        // Clear output
        Output_.NItems = 0;
        Output_.Data.Build.PackedTuples.clear();
        Output_.Data.Build.Overflow.clear();
        Output_.Data.Probe.PackedTuples.clear();
        Output_.Data.Probe.Overflow.clear();
    }
    
    int TupleSize() const {
        return Renames_.size();
    }
    
    int SizeTuplesInBuffer() const {
        MKQL_ENSURE(OutputBuffer.size() % TupleSize() == 0, "buffer contains tuple parts??");
        return OutputBuffer.size() / TupleSize();
    }
    
    std::vector<NYql::NUdf::TUnboxedValue> OutputBuffer;
    TuplePairs Output_;

private:
    TDqUserRenames Renames_;
    TSides<IScalarLayoutConverter*> Converters_;
    const THolderFactory& HolderFactory_;
};

template <EJoinKind Kind> class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState<Kind>> {
  public:
    TScalarHashJoinState(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                               IComputationWideFlowNode* leftFlow,
                               IComputationWideFlowNode* rightFlow, 
                               [[maybe_unused]] const std::vector<ui32>& leftKeyColumns,
                               [[maybe_unused]] const std::vector<ui32>& rightKeyColumns, 
                               const std::vector<TType*>& leftColumnTypes,
                               const std::vector<TType*>& rightColumnTypes, 
                               TSides<std::unique_ptr<IScalarLayoutConverter>> converters,
                               NUdf::TLoggerPtr logger, TString componentName,
                               TDqUserRenames renames, const THolderFactory& holderFactory)
        : NKikimr::NMiniKQL::TComputationValue<TScalarHashJoinState>(memInfo)
        , Converters_(std::move(converters))
        , Join_(TSides<TScalarPackedTupleSource>{
                    .Build = TScalarPackedTupleSource{ctx, rightFlow, rightColumnTypes, Converters_.Build.get()},
                    .Probe = TScalarPackedTupleSource{ctx, leftFlow, leftColumnTypes, Converters_.Probe.get()}
                },
                logger, componentName,
                TSides<const NPackedTuple::TTupleLayout*>{
                    .Build = Converters_.Build->GetTupleLayout(),
                    .Probe = Converters_.Probe->GetTupleLayout()
                })
        , Output_(std::move(renames), 
                  TSides<IScalarLayoutConverter*>{.Build = Converters_.Build.get(), .Probe = Converters_.Probe.get()},
                  holderFactory)
    {}

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        while (Output_.SizeTuplesInBuffer() == 0) {
            auto res = Join_.MatchRows(ctx, Output_.MakeConsumeFn());
            switch (res) {
            case EFetchResult::Finish:
                // Flush any remaining packed tuples
                Output_.FlushToBuffer();
                if (Output_.SizeTuplesInBuffer() == 0) {
                    return EFetchResult::Finish;
                }
                break;
            case EFetchResult::Yield:
                return EFetchResult::Yield;
            case EFetchResult::One:
                break;
            }
            
            // Flush packed tuples to buffer when we have enough
            if (Output_.SizeTuples() > 0) {
                Output_.FlushToBuffer();
            }
        }
        
        // Return one tuple from buffer
        const int outputTupleSize = Output_.TupleSize();
        MKQL_ENSURE(std::ssize(Output_.OutputBuffer) >= outputTupleSize, "Output_ must contain at least one tuple");
        
        for (int index = 0; index < outputTupleSize; ++index) {
            int bufferIndex = std::ssize(Output_.OutputBuffer) - outputTupleSize + index;
            *output[index] = Output_.OutputBuffer[bufferIndex];
        }
        Output_.OutputBuffer.resize(std::ssize(Output_.OutputBuffer) - outputTupleSize);
        
        return EFetchResult::One;
    }

  private:
    TSides<std::unique_ptr<IScalarLayoutConverter>> Converters_;
    TJoinPackedTuples<TScalarPackedTupleSource> Join_;
    TScalarPackedOutput Output_;
};

template <EJoinKind Kind>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Kind>> {
  private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

  public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* leftFlow,
                           IComputationWideFlowNode* rightFlow,
                           TVector<TType*>&& resultItemTypes,
                           TVector<TType*>&& leftColumnTypes,
                           TVector<ui32>&& leftKeyColumns,
                           TVector<TType*>&& rightColumnTypes,
                           TVector<ui32>&& rightKeyColumns, TDqUserRenames renames)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , LeftFlow_(leftFlow)
        , RightFlow_(rightFlow)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftColumnTypes_(std::move(leftColumnTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightColumnTypes_(std::move(rightColumnTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , Renames_(std::move(renames))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        
        return static_cast<TScalarHashJoinState<Kind>*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }

  private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();

        // Create layout converters for both sides
        TTypeInfoHelper helper;
        TSides<std::unique_ptr<IScalarLayoutConverter>> layouts;
        
        ForEachSide([&](ESide side) {
            const auto& columnTypes = (side == ESide::Probe) ? LeftColumnTypes_ : RightColumnTypes_;
            const auto& keyColumns = (side == ESide::Probe) ? LeftKeyColumns_ : RightKeyColumns_;
            
            TVector<NPackedTuple::EColumnRole> roles(columnTypes.size(), NPackedTuple::EColumnRole::Payload);
            for (ui32 column : keyColumns) {
                roles[column] = NPackedTuple::EColumnRole::Key;
            }
            
            layouts.SelectSide(side) = MakeScalarLayoutConverter(helper, columnTypes, roles);
        });
        
        state = ctx.HolderFactory.Create<TScalarHashJoinState<Kind>>(
            ctx, LeftFlow_, RightFlow_, LeftKeyColumns_, RightKeyColumns_, 
            LeftColumnTypes_, RightColumnTypes_, 
            std::move(layouts), logger, "ScalarHashJoin", Renames_, ctx.HolderFactory);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOnBoth(LeftFlow_, RightFlow_);
    }

  private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    const TVector<TType*> ResultItemTypes_;
    const TVector<TType*> LeftColumnTypes_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<TType*> RightColumnTypes_;
    const TVector<ui32> RightKeyColumns_;
    const TDqUserRenames Renames_;
};

} // namespace

IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting flow");
    const auto joinComponents = GetWideComponents(joinType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(), "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(), "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(rightFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> rightFlowItems(rightFlowComponents.cbegin(), rightFlowComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);

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

    const auto leftFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    const auto rightFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));

    MKQL_ENSURE(leftFlow, "Expected WideFlow as a left input");
    MKQL_ENSURE(rightFlow, "Expected WideFlow as a right input");
    MKQL_ENSURE(joinKind == EJoinKind::Inner, "Only inner is supported, see gh#26780 for details.");

    TDqUserRenames renames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));
    ValidateRenames(renames, joinKind, std::ssize(leftFlowItems), std::ssize(rightFlowItems));
    return new TScalarHashJoinWrapper<EJoinKind::Inner>(ctx.Mutables, leftFlow, rightFlow, std::move(joinItems),
                                                        std::move(leftFlowItems), std::move(leftKeyColumns),
                                                        std::move(rightFlowItems), std::move(rightKeyColumns), renames);
}
} // namespace NKikimr::NMiniKQL


