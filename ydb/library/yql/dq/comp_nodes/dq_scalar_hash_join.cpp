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
class TScalarRowSource : public NNonCopyable::TMoveOnly {
  public:
    TScalarRowSource(IComputationWideFlowNode* flow, const std::vector<TType*>& types)
        : Flow_(flow)
        , ConsumeBuff_(types.size())
        , Pointers_(types.size())
    {
        for (int index = 0; index < std::ssize(types); ++index) {
            Pointers_[index] = &ConsumeBuff_[index];
        }
        MKQL_ENSURE(std::ranges::is_permutation(
                        ConsumeBuff_ | std::views::transform([](auto& value) { return &value; }), Pointers_),
                    "Pointers_ should be a permutation of ConsumeBuff_ addresses");
    }

    bool Finished() const {
        return Finished_;
    }

    int UserDataSize() const {
        return ConsumeBuff_.size();
    }

    NYql::NUdf::EFetchStatus ForEachRow(TComputationContext& ctx, std::invocable<NJoinTable::TTuple> auto consume) {
        auto res = Flow_->FetchValues(ctx, Pointers_.data());
        switch (res) {
        case EFetchResult::Finish: {
            Finished_ = true;
            return NYql::NUdf::EFetchStatus::Finish;
        }
        case EFetchResult::Yield: {
            return NYql::NUdf::EFetchStatus::Yield;
        }
        case EFetchResult::One: {
            consume(ConsumeBuff_.data());
            return NYql::NUdf::EFetchStatus::Ok;
        }
        }
    }

  private:
    bool Finished_ = false;
    IComputationWideFlowNode* Flow_;
    std::vector<NYql::NUdf::TUnboxedValue> ConsumeBuff_;
    std::vector<NYql::NUdf::TUnboxedValue*> Pointers_;
};

// New packed tuple source for scalar join
class TScalarPackedTupleSource : public NNonCopyable::TMoveOnly {
  public:
    TScalarPackedTupleSource(IComputationWideFlowNode* flow, const std::vector<TType*>& types, IScalarLayoutConverter* converter)
        : Flow_(flow)
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

    FetchResult<IScalarLayoutConverter::TPackResult> FetchRow(TComputationContext& ctx) {
        if (Finished()) {
            return Finish{};
        }
        auto res = Flow_->FetchValues(ctx, Pointers_.data());
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

    std::vector<NYql::NUdf::TUnboxedValue> FlushAndApplyRenames() {
        std::vector<NYql::NUdf::TUnboxedValue> result;
        result.reserve(Renames_.size() * Output_.NItems);
        
        // Unpack tuples from both sides
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
                // We need to know how many columns to unpack
                // For now, we'll infer from the tuple layout
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
            
            // Apply renames
            for (const auto& rename : Renames_) {
                if (rename.Side == EJoinSide::kLeft) {
                    result.push_back(sideValues.Probe[rename.Index]);
                } else {
                    result.push_back(sideValues.Build[rename.Index]);
                }
            }
        }
        
        // Clear output
        Output_.NItems = 0;
        Output_.Data.Build.PackedTuples.clear();
        Output_.Data.Build.Overflow.clear();
        Output_.Data.Probe.PackedTuples.clear();
        Output_.Data.Probe.Overflow.clear();
        
        return result;
    }

private:
    TuplePairs Output_;
    TDqUserRenames Renames_;
    TSides<IScalarLayoutConverter*> Converters_;
    const THolderFactory& HolderFactory_;
};

// Old version using standard join table (kept for compatibility)
template <EJoinKind Kind> class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState<Kind>> {
  public:
    TScalarHashJoinState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* leftFlow,
                         IComputationWideFlowNode* rightFlow, const std::vector<ui32>& leftKeyColumns,
                         const std::vector<ui32>& rightKeyColumns, const std::vector<TType*>& leftColumnTypes,
                         const std::vector<TType*>& rightColumnTypes, NUdf::TLoggerPtr logger, TString componentName,
                         TDqUserRenames renames)
        : NKikimr::NMiniKQL::TComputationValue<TScalarHashJoinState>(memInfo)
        , Join_(memInfo, TScalarRowSource{leftFlow, leftColumnTypes}, TScalarRowSource{rightFlow, rightColumnTypes},
                TJoinMetadata{TColumnsMetadata{rightKeyColumns, rightColumnTypes},
                              TColumnsMetadata{leftKeyColumns, leftColumnTypes},
                              KeyTypesFromColumns(leftColumnTypes, leftKeyColumns)}, logger, componentName)
        , Output_(std::move(renames), leftColumnTypes, rightColumnTypes)
    {}

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        while (Output_.SizeTuples() == 0) {
            auto res = Join_.MatchRows(ctx, Output_.MakeConsumeFn());
            switch (res) {
            case EFetchResult::Finish:
                return res;
            case EFetchResult::Yield:
                return res;
            case EFetchResult::One:
                break;
            }
        }
        const int outputTupleSize = Output_.TupleSize();
        MKQL_ENSURE(std::ssize(Output_.OutputBuffer) >= outputTupleSize, "Output_ must contain at least one tuple");
        for (int index = 0; index < outputTupleSize; ++index) {
            int myIndex = std::ssize(Output_.OutputBuffer) - outputTupleSize + index;
            int theirIndex = index;
            *output[theirIndex] = Output_.OutputBuffer[myIndex];
        }
        Output_.OutputBuffer.resize(std::ssize(Output_.OutputBuffer) - outputTupleSize);
        return EFetchResult::One;
    }

  private:
    TJoin<TScalarRowSource, Kind> Join_;
    TRenamedOutput<Kind> Output_;
};

// New version using packed tuples with layout converter
template <EJoinKind Kind> class TScalarPackedHashJoinState : public TComputationValue<TScalarPackedHashJoinState<Kind>> {
  public:
    TScalarPackedHashJoinState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* leftFlow,
                               IComputationWideFlowNode* rightFlow, 
                               [[maybe_unused]] const std::vector<ui32>& leftKeyColumns,
                               [[maybe_unused]] const std::vector<ui32>& rightKeyColumns, 
                               const std::vector<TType*>& leftColumnTypes,
                               const std::vector<TType*>& rightColumnTypes, 
                               TSides<std::unique_ptr<IScalarLayoutConverter>> converters,
                               NUdf::TLoggerPtr logger, TString componentName,
                               TDqUserRenames renames, const THolderFactory& holderFactory)
        : NKikimr::NMiniKQL::TComputationValue<TScalarPackedHashJoinState>(memInfo)
        , Converters_(std::move(converters))
        , Join_(memInfo, 
                TScalarPackedTupleSource{leftFlow, leftColumnTypes, Converters_.Probe.get()}, 
                TScalarPackedTupleSource{rightFlow, rightColumnTypes, Converters_.Build.get()},
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
        while (Output_.SizeTuples() == 0) {
            auto res = Join_.MatchRows(ctx, Output_.MakeConsumeFn());
            switch (res) {
            case EFetchResult::Finish:
                if (Output_.SizeTuples() == 0) {
                    return EFetchResult::Finish;
                }
                break;
            case EFetchResult::Yield:
                return EFetchResult::Yield;
            case EFetchResult::One:
                break;
            }
        }
        
        auto outputBuffer = Output_.FlushAndApplyRenames();
        const int outputTupleSize = Output_.Columns();
        MKQL_ENSURE(std::ssize(outputBuffer) >= outputTupleSize, "Output_ must contain at least one tuple");
        
        for (int index = 0; index < outputTupleSize; ++index) {
            *output[index] = outputBuffer[index];
        }
        
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
                           TVector<ui32>&& rightKeyColumns, TDqUserRenames renames,
                           bool usePacked = true)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , LeftFlow_(leftFlow)
        , RightFlow_(rightFlow)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftColumnTypes_(std::move(leftColumnTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightColumnTypes_(std::move(rightColumnTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , Renames_(std::move(renames))
        , UsePacked_(usePacked)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        
        if (UsePacked_) {
            return static_cast<TScalarPackedHashJoinState<Kind>*>(state.AsBoxed().Get())->FetchValues(ctx, output);
        } else {
            return static_cast<TScalarHashJoinState<Kind>*>(state.AsBoxed().Get())->FetchValues(ctx, output);
        }
    }

  private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();

        if (UsePacked_) {
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
            
            state = ctx.HolderFactory.Create<TScalarPackedHashJoinState<Kind>>(
                LeftFlow_, RightFlow_, LeftKeyColumns_, RightKeyColumns_, 
                LeftColumnTypes_, RightColumnTypes_, 
                std::move(layouts), logger, "ScalarHashJoin", Renames_, ctx.HolderFactory);
        } else {
            state = ctx.HolderFactory.Create<TScalarHashJoinState<Kind>>(
                LeftFlow_, RightFlow_, LeftKeyColumns_, RightKeyColumns_, 
                LeftColumnTypes_, RightColumnTypes_, logger, "ScalarHashJoin", Renames_);
        }
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
    const bool UsePacked_;
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
