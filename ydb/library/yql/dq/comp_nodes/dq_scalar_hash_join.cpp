#include "dq_scalar_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <ydb/library/yql/dq/comp_nodes/dq_join_common.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <algorithm>
#include <numeric>

namespace NKikimr::NMiniKQL {

namespace {

using TDqJoinImplRenames = TDqRenames<ESide>;

struct TDqScalarJoinMetadata {
    TSides<TVector<TType*>> InputTypes;
    TSides<TVector<ui32>> KeyColumns;
    TVector<TType*> ResultItemTypes;
    TDqJoinImplRenames Renames;
    EJoinKind Kind;
    TSides<TVector<TType*>> UserTypes;
    TSides<TVector<int>> ColumnPermutation;
};

class TScalarPackedTupleSource : public NNonCopyable::TMoveOnly {
public:
    TScalarPackedTupleSource(TComputationContext& ctx, IComputationWideFlowNode* flow, IScalarLayoutConverter* converter,
                             int columns, const TVector<int>& columnPermutation)
        : Ctx_(&ctx)
        , Flow_(flow)
        , Buff_(columns)
        , Pointers_(columns)
        , Converter_(converter)
        , Columns_(columns)
        , ColumnPermutation_(columnPermutation)
    {
        for (int index = 0; index < columns; ++index) {
            Pointers_[index] = &Buff_[index];
        }
        BatchValues_.reserve(static_cast<size_t>(Columns_) * BatchSize_);
    }

    bool Finished() const {
        return Finished_;
    }

    FetchResult<TPackResult> FetchRow() {
        while (true) {
            if (Finished_ && BatchCount_ == 0) {
                return Finish{};
            }
            if (BatchCount_ >= BatchSize_) {
                return FlushBatch();
            }
            auto res = Flow_->FetchValues(*Ctx_, Pointers_.data());
            switch (res) {
            case EFetchResult::Finish:
                Finished_ = true;
                if (BatchCount_ > 0) {
                    return FlushBatch();
                }
                return Finish{};
            case EFetchResult::Yield:
                if (BatchCount_ > 0) {
                    return FlushBatch();
                }
                return Yield{};
            case EFetchResult::One: {
                if (ColumnPermutation_.empty()) {
                    for (int i = 0; i < Columns_; ++i) {
                        BatchValues_.push_back(Buff_[i]);
                    }
                } else {
                    for (int i = 0; i < Columns_; ++i) {
                        BatchValues_.push_back(Buff_[ColumnPermutation_[i]]);
                    }
                }
                ++BatchCount_;
                // loop to accumulate more until batch full or finish
                break;
            }
            }
        }
        MKQL_ENSURE(false, "unreachable");
    }

private:
    FetchResult<TPackResult> FlushBatch() {
        MKQL_ENSURE(BatchCount_ > 0, "INTERNAL LOGIC ERROR");
        TPackResult packed;
        Converter_->PackBatch(BatchValues_.data(), BatchCount_, packed);
        BatchValues_.clear();
        BatchCount_ = 0;
        return One<TPackResult>{std::move(packed)};
    }

    bool Finished_ = false;
    TComputationContext* Ctx_;
    IComputationWideFlowNode* Flow_;
    TMKQLVector<NYql::NUdf::TUnboxedValue> Buff_;
    TMKQLVector<NYql::NUdf::TUnboxedValue*> Pointers_;
    IScalarLayoutConverter* Converter_;
    int Columns_;
    TVector<int> ColumnPermutation_;
    static constexpr int BatchSize_ = 1024;
    TMKQLVector<NYql::NUdf::TUnboxedValue> BatchValues_;
    int BatchCount_ = 0;
};

template <EJoinKind Kind>
struct TRenamesScalarOutput : NNonCopyable::TMoveOnly {
    struct TFlushResult {
        TVector<NUdf::TUnboxedValue> Buffer;
        TSides<TPackResult> Packs;
    };

    struct Empty {};
    using BuildNullIfNeeded = std::conditional_t<Kind == EJoinKind::Left, TPackResult, Empty>;

    TRenamesScalarOutput(const TDqScalarJoinMetadata* meta, TSides<IScalarLayoutConverter*> converters)
        : Renames_(&meta->Renames)
        , Converters_(converters)
        , BuildWidth_(std::ssize(meta->InputTypes.Build))
        , ProbeWidth_(std::ssize(meta->InputTypes.Probe))
    {
        if constexpr (!std::is_same_v<decltype(Nulls_), Empty>) {
            TMKQLVector<NUdf::TUnboxedValue> nulls(BuildWidth_);
            Converters_.Build->Pack(nulls.data(), Nulls_);
        }
    }

    int Columns() const {
        return Renames_->size();
    }

    i64 SizeTuples() const {
        return Output_.Probe.NTuples;
    }

    auto MakeConsumeFn() {
        struct ConsumeFn {
            TRenamesScalarOutput& Self;

            void operator()(TSides<TSingleTuple> tuples) {
                for (ESide side : EachSide) {
                    Self.Output_.SelectSide(side).AppendTuple(
                        tuples.SelectSide(side), Self.Converters_.SelectSide(side)->GetTupleLayout());
                }
            }

            void operator()(TSingleTuple tuple) {
                if constexpr (Kind == EJoinKind::Left) {
                    TSingleTuple null{.PackedData = Self.Nulls_.PackedTuples.data(),
                                      .OverflowBegin = Self.Nulls_.Overflow.data()};
                    this->operator()(TSides<TSingleTuple>{.Build = null, .Probe = tuple});
                } else if constexpr (SemiOrOnlyJoin(Kind)) {
                    Self.Output_.Probe.AppendTuple(tuple, Self.Converters_.Probe->GetTupleLayout());
                }
            }
        };
        return ConsumeFn{*this};
    }

    TFlushResult Flush() {
        TFlushResult res;
        const i64 nItems = Output_.Probe.NTuples;
        res.Packs.Build = std::move(Output_.Build);
        res.Packs.Probe = std::move(Output_.Probe);

        res.Buffer.reserve(nItems * Columns());

        if constexpr (LeftSemiOrOnly(Kind)) {
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 tupleIndex = 0; tupleIndex < nItems; ++tupleIndex) {
                Converters_.Probe->Unpack(res.Packs.Probe, tupleIndex, probeValues.data());
                for (auto rename : *Renames_) {
                    MKQL_ENSURE(rename.Side == ESide::Probe,
                                "renames in Semi or Only Left Join shouldn't contain columns from right side");
                    res.Buffer.push_back(probeValues[rename.Index]);
                }
            }
        } else {
            TMKQLVector<NUdf::TUnboxedValue> buildValues(BuildWidth_);
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 tupleIndex = 0; tupleIndex < nItems; ++tupleIndex) {
                Converters_.Build->Unpack(res.Packs.Build, tupleIndex, buildValues.data());
                Converters_.Probe->Unpack(res.Packs.Probe, tupleIndex, probeValues.data());
                for (auto rename : *Renames_) {
                    if (rename.Side == ESide::Build) {
                        res.Buffer.push_back(buildValues[rename.Index]);
                    } else {
                        res.Buffer.push_back(probeValues[rename.Index]);
                    }
                }
            }
        }

        return res;
    }

private:
    const TDqJoinImplRenames* Renames_;
    TSides<IScalarLayoutConverter*> Converters_;
    const int BuildWidth_;
    const int ProbeWidth_;
    TSides<TPackResult> Output_;
    BuildNullIfNeeded Nulls_;
};

template <EJoinKind Kind>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Kind>> {
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, TDqScalarJoinMetadata meta,
                           TSides<IComputationWideFlowNode*> flows)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , Meta_(std::make_unique<TDqScalarJoinMetadata>(std::move(meta)))
        , Flows_(flows)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return static_cast<TStreamState*>(state.AsBoxed().Get())->FetchValues(output);
    }

private:
    class TStreamState : public TComputationValue<TStreamState> {
        using TBase = TComputationValue<TStreamState>;
        using JoinType = NJoinPackedTuples::THybridHashJoin<TScalarPackedTupleSource, TestStorageSettings, Kind>;

    public:
        TStreamState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, TSides<IComputationWideFlowNode*> flows,
                     TSides<std::unique_ptr<IScalarLayoutConverter>> converters, const TDqScalarJoinMetadata* meta)
            : TBase(memInfo)
            , Meta_(meta)
            , Converters_(std::move(converters))
            , JoinCtx_(&ctx)
            , Join_(TSides<TScalarPackedTupleSource>{
                        .Build = {ctx, flows.Build, Converters_.Build.get(),
                                  static_cast<int>(std::ssize(Meta_->InputTypes.Build)),
                                  Meta_->ColumnPermutation.Build},
                        .Probe = {ctx, flows.Probe, Converters_.Probe.get(),
                                  static_cast<int>(std::ssize(Meta_->InputTypes.Probe)),
                                  Meta_->ColumnPermutation.Probe}},
                    ctx, "ScalarHashJoinPacked",
                    TSides<const NPackedTuple::TTupleLayout*>{.Build = Converters_.Build->GetTupleLayout(),
                                                              .Probe = Converters_.Probe->GetTupleLayout()})
            , Output_(meta, {.Build = Converters_.Build.get(), .Probe = Converters_.Probe.get()})
        {}

        EFetchResult FetchValues(NUdf::TUnboxedValue* const* output) {
            const int expectedWidth = Output_.Columns();
            if (!Buffer_.has_value()) {
                auto res = FillBuffer();
                if (res != EFetchResult::One) {
                    return res;
                }
            }
            if (!HasRow()) {
                auto res = FillBuffer();
                if (res != EFetchResult::One) {
                    return res;
                }
            }
            for (int index = 0; index < expectedWidth; ++index) {
                *output[index] = Buffer_->Buffer[BufferPos_ + index];
            }
            BufferPos_ += expectedWidth;
            if (BufferPos_ >= Buffer_->Buffer.size()) {
                Buffer_.reset();
                BufferPos_ = 0;
            }
            return EFetchResult::One;
        }

    private:
        bool HasRow() const {
            return Buffer_.has_value() && BufferPos_ + Output_.Columns() <= Buffer_->Buffer.size();
        }

        EFetchResult FillBuffer() {
            auto outputIsFull = [&]() {
                return Output_.SizeTuples() >= Threshold_;
            };
            while (!outputIsFull()) {
                auto res = Join_.MatchRows(*JoinCtx_, Output_.MakeConsumeFn(), outputIsFull);
                switch (res) {
                case EFetchResult::Finish: {
                    if (Output_.SizeTuples() == 0) {
                        return EFetchResult::Finish;
                    }
                    Buffer_ = Output_.Flush();
                    return EFetchResult::One;
                }
                case EFetchResult::Yield: {
                    if (Output_.SizeTuples() == 0) {
                        return EFetchResult::Yield;
                    }
                    Buffer_ = Output_.Flush();
                    return EFetchResult::One;
                }
                case EFetchResult::One: {
                    break;
                }
                default:
                    MKQL_ENSURE(false, "unexpected fetch result");
                }
            }
            Buffer_ = Output_.Flush();
            return EFetchResult::One;
        }

    private:
        const TDqScalarJoinMetadata* Meta_;
        TSides<std::unique_ptr<IScalarLayoutConverter>> Converters_;
        TComputationContext* JoinCtx_;
        JoinType Join_;
        TRenamesScalarOutput<Kind> Output_;
        std::optional<typename TRenamesScalarOutput<Kind>::TFlushResult> Buffer_;
        size_t BufferPos_ = 0;
        const int Threshold_ = 10000;
    };

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        TSides<std::unique_ptr<IScalarLayoutConverter>> converters;
        TTypeInfoHelper helper;
        for(ESide side: EachSide) {
            TVector<NPackedTuple::EColumnRole> roles(std::ssize(Meta_->UserTypes.SelectSide(side)),
                                                    NPackedTuple::EColumnRole::Payload);
            for (int column : Meta_->KeyColumns.SelectSide(side)) {
                roles[column] = NPackedTuple::EColumnRole::Key;
            }
            converters.SelectSide(side) =
                MakeScalarLayoutConverter(helper, Meta_->UserTypes.SelectSide(side), roles, ctx.HolderFactory);
        }

        state = ctx.HolderFactory.Create<TStreamState>(ctx, Flows_, std::move(converters), Meta_.get());
    }

    void RegisterDependencies() const final {
        this->FlowDependsOnBoth(Flows_.Build, Flows_.Probe);
    }

private:
    std::unique_ptr<const TDqScalarJoinMetadata> Meta_;
    TSides<IComputationWideFlowNode*> Flows_;
};

} // namespace

IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting flow");
    const auto joinComponents = GetWideComponents(joinType);
    MKQL_ENSURE(!joinComponents.empty(), "Expected at least one column");

    TDqScalarJoinMetadata meta;
    for (auto* type : joinComponents) {
        meta.ResultItemTypes.push_back(type);
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(), "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(!leftFlowComponents.empty(), "Expected at least one column");
    for (auto* type : leftFlowComponents) {
        meta.InputTypes.Probe.push_back(type);
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(), "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(!rightFlowComponents.empty(), "Expected at least one column");
    for (auto* type : rightFlowComponents) {
        meta.InputTypes.Build.push_back(type);
    }

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    meta.Kind = joinKind;

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        meta.KeyColumns.Probe.emplace_back(item->AsValue().Get<ui32>());
    }

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        meta.KeyColumns.Build.emplace_back(item->AsValue().Get<ui32>());
    }

    MKQL_ENSURE(meta.KeyColumns.Build.size() == meta.KeyColumns.Probe.size(), "Key columns mismatch");

    const auto leftFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    const auto rightFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));
    MKQL_ENSURE(leftFlow, "Expected WideFlow as a left input");
    MKQL_ENSURE(rightFlow, "Expected WideFlow as a right input");

    TDqUserRenames userRenames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));
    ValidateRenames(userRenames, joinKind, std::ssize(meta.InputTypes.Probe), std::ssize(meta.InputTypes.Build));

    for (auto rename : userRenames) {
        ESide side = rename.Side == EJoinSide::kLeft ? ESide::Probe : ESide::Build;
        meta.Renames.push_back({.Index = rename.Index, .Side = side});
    }

    for (ESide side : EachSide) {
        auto& keyColumns = meta.KeyColumns.SelectSide(side);
        const int numDataCols = std::ssize(meta.InputTypes.SelectSide(side));
        const int numKeys = std::ssize(keyColumns);

        bool needsReorder = false;
        for (int i = 0; i < numKeys; ++i) {
            if (keyColumns[i] != static_cast<ui32>(i)) {
                needsReorder = true;
                break;
            }
        }
        if (!needsReorder) {
            continue;
        }

        TVector<int> perm(numDataCols);
        std::iota(perm.begin(), perm.end(), 0);
        for (int i = 0; i < numKeys; ++i) {
            const int keyColumn = static_cast<int>(keyColumns[i]);
            MKQL_ENSURE(keyColumn >= 0 && keyColumn < numDataCols,
                        Sprintf("key column index %i on %s side is out of range [0, %i)", keyColumn,
                                AsString(side), numDataCols));
            auto it = std::find(perm.begin() + i, perm.end(), keyColumn);
            MKQL_ENSURE(it != perm.end(),
                        Sprintf("key column index %i on %s side is duplicated or could not be placed",
                                keyColumn, AsString(side)));
            std::swap(perm[i], *it);
        }

        meta.ColumnPermutation.SelectSide(side) = perm;

        const auto origTypes = meta.InputTypes.SelectSide(side);
        for (int i = 0; i < numDataCols; ++i) {
            meta.InputTypes.SelectSide(side)[i] = origTypes[perm[i]];
        }

        TVector<int> inv(numDataCols);
        for (int i = 0; i < numDataCols; ++i) {
            inv[perm[i]] = i;
        }
        for (auto& rename : meta.Renames) {
            if (rename.Side == side) {
                rename.Index = inv[rename.Index];
            }
        }

        for (int i = 0; i < numKeys; ++i) {
            keyColumns[i] = i;
        }
    }

    for (ESide side : EachSide) {
        for (TType* thisType : meta.InputTypes.SelectSide(side)) {
            if (joinKind == EJoinKind::Left && side == ESide::Build && !thisType->IsOptional()) {
                meta.UserTypes.SelectSide(side).push_back(TOptionalType::Create(thisType, ctx.Env));
            } else {
                meta.UserTypes.SelectSide(side).push_back(thisType);
            }
        }
    }

    using enum EJoinKind;
    const TSides<IComputationWideFlowNode*> flows{.Build = rightFlow, .Probe = leftFlow};
    if (joinKind == Inner) {
        return new TScalarHashJoinWrapper<Inner>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == LeftOnly) {
        return new TScalarHashJoinWrapper<LeftOnly>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == LeftSemi) {
        return new TScalarHashJoinWrapper<LeftSemi>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == Left) {
        return new TScalarHashJoinWrapper<Left>(ctx.Mutables, std::move(meta), flows);
    } else {
        MKQL_ENSURE(false, "unsupported join type in scalar hash join, see gh#26780 for details.");
    }
}

} // namespace NKikimr::NMiniKQL
