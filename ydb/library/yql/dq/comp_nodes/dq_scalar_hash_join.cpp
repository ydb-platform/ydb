#include "dq_scalar_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <ydb/library/yql/dq/comp_nodes/dq_join_common.h>
#include <ydb/library/yql/dq/comp_nodes/dq_join_filters.h>

namespace NKikimr::NMiniKQL {

namespace {

using TDqJoinImplRenames = TDqRenames<ESide>;

constexpr ui32 BaseInputs = 7;

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
        if (Columns_ == 0) {
            packed.PackedTuples.resize(Converter_->GetTupleLayout()->TotalRowSize * BatchCount_, 0);
            packed.NTuples = BatchCount_;
        } else {
            Converter_->PackBatch(BatchValues_.data(), BatchCount_, packed);
            BatchValues_.clear();
        }
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

template <TPhysicalJoin Join>
struct TRenamesScalarOutput : TPackedTupleOutputBase<Join, IScalarLayoutConverter> {
    using TBase = TPackedTupleOutputBase<Join, IScalarLayoutConverter>;

    struct TFlushResult {
        TVector<NUdf::TUnboxedValue> Buffer;
        TSides<TPackResult> Packs;
    };

    TRenamesScalarOutput(const TDqScalarJoinMetadata* meta, TSides<IScalarLayoutConverter*> converters)
        : TBase(&meta->Renames, converters)
        , BuildWidth_(std::ssize(meta->InputTypes.Build))
        , ProbeWidth_(std::ssize(meta->InputTypes.Probe))
    {
        if constexpr (!std::is_same_v<typename TBase::BuildNullIfNeeded, typename TBase::Empty>) {
            TMKQLVector<NUdf::TUnboxedValue> nulls(BuildWidth_);
            this->Converters_.Build->Pack(nulls.data(), this->Nulls_);
        }
    }

    TFlushResult Flush() {
        TFlushResult res;
        const i64 nItems = this->Output_.Probe.NTuples;
        res.Packs.Build = std::move(this->Output_.Build);
        res.Packs.Probe = std::move(this->Output_.Probe);

        res.Buffer.reserve(nItems * this->Columns());

        if constexpr (LeftSemiOrOnly(Join.Kind)) {
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 tupleIndex = 0; tupleIndex < nItems; ++tupleIndex) {
                this->Converters_.Probe->Unpack(res.Packs.Probe, tupleIndex, probeValues.data());
                for (auto rename : *this->Renames_) {
                    MKQL_ENSURE(rename.Side == ESide::Probe,
                                "renames in Semi or Only Left Join shouldn't contain columns from right side");
                    res.Buffer.push_back(probeValues[rename.Index]);
                }
            }
        } else {
            TMKQLVector<NUdf::TUnboxedValue> buildValues(BuildWidth_);
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 tupleIndex = 0; tupleIndex < nItems; ++tupleIndex) {
                this->Converters_.Build->Unpack(res.Packs.Build, tupleIndex, buildValues.data());
                this->Converters_.Probe->Unpack(res.Packs.Probe, tupleIndex, probeValues.data());
                for (auto rename : *this->Renames_) {
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
    const int BuildWidth_;
    const int ProbeWidth_;
};

template <TPhysicalJoin Join>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Join>> {
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, TDqScalarJoinMetadata meta,
                           TSides<IComputationWideFlowNode*> flows, TJoinFilters filters)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , Meta_(std::make_unique<TDqScalarJoinMetadata>(std::move(meta)))
        , Flows_(flows)
        , Filters_(std::move(filters))
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
        using JoinType = NJoinPackedTuples::THybridHashJoin<TScalarPackedTupleSource, TestStorageSettings, Join>;

    public:
        TStreamState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, TSides<IComputationWideFlowNode*> flows,
                     TSides<std::unique_ptr<IScalarLayoutConverter>> converters, const TDqScalarJoinMetadata* meta,
                     std::optional<TPackedTuplePairFilter> pairFilter)
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
            , PairFilter_(std::move(pairFilter))
        {}

        EFetchResult FetchValues(NUdf::TUnboxedValue* const* output) {
            const int width = Output_.Columns();
            if (!HasRow()) {
                auto res = FillBuffer();
                if (res != EFetchResult::One) {
                    return res;
                }
            }
            for (int index = 0; index < width; ++index) {
                *output[index] = Buffer_->Buffer[BufferPos_ + index];
            }
            BufferPos_ += width ? width : 1;
            if (!HasRow()) {
                Buffer_.reset();
                BufferPos_ = 0;
            }
            return EFetchResult::One;
        }

    private:
        bool HasRow() const {
            if (!Buffer_.has_value()) {
                return false;
            }
            const int width = Output_.Columns();
            return width ? BufferPos_ + width <= Buffer_->Buffer.size()
                         : BufferPos_ < static_cast<size_t>(Buffer_->Packs.Probe.NTuples);
        }

        EFetchResult FillBuffer() {
            const auto flushSink = [&](auto flush) {
                Buffer_ = std::move(flush);
                BufferPos_ = 0;
            };
            return RunPackedHashJoinBatch<OutputThreshold_>(*JoinCtx_, Join_, Output_, flushSink,
                                                            PairFilter_ ? &*PairFilter_ : nullptr);
        }

    private:
        const TDqScalarJoinMetadata* Meta_;
        TSides<std::unique_ptr<IScalarLayoutConverter>> Converters_;
        TComputationContext* JoinCtx_;
        JoinType Join_;
        TRenamesScalarOutput<Join> Output_;
        std::optional<TPackedTuplePairFilter> PairFilter_;
        std::optional<typename TRenamesScalarOutput<Join>::TFlushResult> Buffer_;
        size_t BufferPos_ = 0;
        static constexpr i64 OutputThreshold_ = 10000;
    };

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        TSides<std::unique_ptr<IScalarLayoutConverter>> converters;
        TTypeInfoHelper helper;
        for(ESide side: EachSide) {
            const auto roles =
                MakeColumnRoles(Meta_->UserTypes.SelectSide(side).size(), Meta_->KeyColumns.SelectSide(side));
            converters.SelectSide(side) =
                MakeScalarLayoutConverter(helper, Meta_->UserTypes.SelectSide(side), roles, ctx.HolderFactory);
        }

        state = ctx.HolderFactory.Create<TStreamState>(
            ctx, Flows_, std::move(converters), Meta_.get(),
            TPackedTuplePairFilter::TryCreate(ctx, Filters_, Meta_->UserTypes, Meta_->KeyColumns,
                                              Meta_->ColumnPermutation));
    }

    void RegisterDependencies() const final {
        const auto flow = this->FlowDependsOnBoth(Flows_.Build, Flows_.Probe);
        Filters_.RegisterDependencies([this, flow](IComputationNode* node) { this->DependsOn(flow, node); },
                                      [this, flow](IComputationExternalNode* node) { this->Own(flow, node); });
    }

private:
    std::unique_ptr<const TDqScalarJoinMetadata> Meta_;
    TSides<IComputationWideFlowNode*> Flows_;
    TJoinFilters Filters_;
};

} // namespace

IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= BaseInputs, "Expected at least " << BaseInputs << " args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting flow");
    const auto joinComponents = GetWideComponents(joinType);

    TDqScalarJoinMetadata meta;
    for (auto* type : joinComponents) {
        meta.ResultItemTypes.push_back(type);
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(), "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    for (auto* type : leftFlowComponents) {
        meta.InputTypes.Probe.push_back(type);
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(), "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    for (auto* type : rightFlowComponents) {
        meta.InputTypes.Build.push_back(type);
    }

    const auto parsed = ParseCommonHashJoinArgs(callable);
    const auto joinKind = parsed.Kind;
    meta.Kind = joinKind;
    meta.KeyColumns = parsed.KeyColumns;

    if (joinKind != EJoinKind::Cross) {
        MKQL_ENSURE(!joinComponents.empty(), "Expected at least one column");
        MKQL_ENSURE(!leftFlowComponents.empty(), "Expected at least one column");
        MKQL_ENSURE(!rightFlowComponents.empty(), "Expected at least one column");
    }

    const auto leftFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    const auto rightFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));
    MKQL_ENSURE(leftFlow, "Expected WideFlow as a left input");
    MKQL_ENSURE(rightFlow, "Expected WideFlow as a right input");

    ValidateRenames(parsed.UserRenames, joinKind, std::ssize(meta.InputTypes.Probe), std::ssize(meta.InputTypes.Build));
    meta.Renames = BuildImplRenames(parsed.UserRenames);

    ApplyKeyColumnPermutation(meta.KeyColumns, meta.InputTypes, /* trailingColumns */ 0, meta.Renames,
                              meta.ColumnPermutation);
    meta.UserTypes = ForceOptionalOnNullableSide(meta.InputTypes, joinKind, ESide::Build, ctx.Env);

    const TSides<IComputationWideFlowNode*> flows{.Build = rightFlow, .Probe = leftFlow};

    return DispatchHashJoinByKind<TScalarHashJoinWrapper, IComputationWideFlowNode>(
        joinKind, ESide::Probe, "unsupported join type in scalar hash join, see gh#26780 for details.", ctx.Mutables,
        std::move(meta), flows, ParseJoinFilters(ctx, callable, BaseInputs));
}

} // namespace NKikimr::NMiniKQL
