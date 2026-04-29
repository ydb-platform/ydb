#include "dq_scalar_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <numeric>

#include <ydb/library/yql/dq/comp_nodes/dq_join_common.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

namespace NKikimr::NMiniKQL {

namespace {

using TDqJoinImplRenames = TDqRenames<ESide>;

struct TDqScalarJoinContext {
    TSides<TVector<TType*>> InputTypes;
    TSides<TVector<int>> KeyColumns;
    TVector<TType*> ResultItemTypes;
    TDqJoinImplRenames Renames;
    EJoinKind Kind = EJoinKind::Inner;
    TBlockHashJoinSettings Settings;
    TSides<TVector<TType*>> UserTypes;
    TSides<TVector<int>> ColumnPermutation;
};

class TScalarPackedTupleSource : public NNonCopyable::TMoveOnly {
public:
    TScalarPackedTupleSource(TComputationContext& ctx, IComputationWideFlowNode* flow, const TDqScalarJoinContext* meta,
                             IScalarLayoutConverter* converter, ESide side)
        : Ctx_(&ctx)
        , Flow_(flow)
        , Buff_(meta->InputTypes.SelectSide(side).size())
        , Pointers_(Buff_.size())
        , Converter_(converter)
        , Columns_(static_cast<int>(Buff_.size()))
        , ColumnPermutation_(meta->ColumnPermutation.SelectSide(side))
    {
        for (int index = 0; index < Columns_; ++index) {
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
                if (!ColumnPermutation_.empty()) {
                    TMKQLVector<NYql::NUdf::TUnboxedValue> tmp(Columns_);
                    for (int i = 0; i < Columns_; ++i) {
                        tmp[i] = Buff_[ColumnPermutation_[i]];
                    }
                    for (int i = 0; i < Columns_; ++i) {
                        Buff_[i] = tmp[i];
                    }
                }
                for (int i = 0; i < Columns_; ++i) {
                    BatchValues_.push_back(Buff_[i]);
                }
                ++BatchCount_;
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
    };

    TRenamesScalarOutput(const TDqScalarJoinContext* meta, TSides<IScalarLayoutConverter*> converters)
        : Renames_(&meta->Renames)
        , Converters_(converters)
        , BuildWidth_(std::ssize(meta->InputTypes.Build))
        , ProbeWidth_(std::ssize(meta->InputTypes.Probe))
        , LeftIsBuild_(meta->Settings.LeftIsBuild())
    {
        if constexpr (Kind == EJoinKind::Left) {
            const auto& userNullTypes =
                meta->Settings.LeftIsBuild() ? meta->UserTypes.Probe : meta->UserTypes.Build;
            TMKQLVector<NYql::NUdf::TUnboxedValue> nullCells(userNullTypes.size());
            if (LeftIsBuild_) {
                Converters_.Probe->Pack(nullCells.data(), NullRowPacked_);
            } else {
                Converters_.Build->Pack(nullCells.data(), NullRowPacked_);
            }
        }
    }

    int Columns() const {
        return Renames_->size();
    }

    i64 SizeTuples() const {
        AssertSizeIsSane();
        return Output_.Probe.NTuples;
    }

    auto MakeConsumeFn() {
        struct ConsumeFn {
            TRenamesScalarOutput& self;
            void operator()(TSides<TSingleTuple> tuples) {
                for (ESide side : EachSide) {
                    self.Output_.SelectSide(side).AppendTuple(tuples.SelectSide(side),
                        self.Converters_.SelectSide(side)->GetTupleLayout());
                }
            }
            void operator()(TSingleTuple tuple) {
                if constexpr (Kind == EJoinKind::Left) {
                    TSingleTuple null{.PackedData = self.NullRowPacked_.PackedTuples.data(),
                                      .OverflowBegin = self.NullRowPacked_.Overflow.data()};
                    if (self.LeftIsBuild_) {
                        (*this)(TSides<TSingleTuple>{.Build = tuple, .Probe = null});
                    } else {
                        (*this)(TSides<TSingleTuple>{.Build = null, .Probe = tuple});
                    }
                } else if constexpr (SemiOrOnlyJoin(Kind)) {
                    self.Output_.Probe.AppendTuple(tuple, self.Converters_.Probe->GetTupleLayout());
                }
            }
        };
        return ConsumeFn{*this};
    }

    TFlushResult Flush() {
        TFlushResult res;
        if constexpr (LeftSemiOrOnly(Kind)) {
            const i64 n = Output_.Probe.NTuples;
            res.Buffer.reserve(static_cast<size_t>(n * Columns()));
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 i = 0; i < n; ++i) {
                Converters_.Probe->Unpack(Output_.Probe, static_cast<ui32>(i), probeValues.data());
                for (auto rename : *Renames_) {
                    MKQL_ENSURE(rename.Side == ESide::Probe,
                        "renames in Semi or Only Left Join shouldn't contain columns from build side");
                    res.Buffer.push_back(probeValues[rename.Index]);
                }
            }
            Output_.Probe.Clear();
        } else {
            const i64 n = Output_.Probe.NTuples;
            MKQL_ENSURE(n == Output_.Build.NTuples, "Inner and Left join types must collect same amount of tuples");
            res.Buffer.reserve(static_cast<size_t>(n * Columns()));
            TMKQLVector<NUdf::TUnboxedValue> buildValues(BuildWidth_);
            TMKQLVector<NUdf::TUnboxedValue> probeValues(ProbeWidth_);
            for (i64 i = 0; i < n; ++i) {
                Converters_.Build->Unpack(Output_.Build, static_cast<ui32>(i), buildValues.data());
                Converters_.Probe->Unpack(Output_.Probe, static_cast<ui32>(i), probeValues.data());
                for (auto rename : *Renames_) {
                    if (rename.Side == ESide::Build) {
                        res.Buffer.push_back(buildValues[rename.Index]);
                    } else {
                        res.Buffer.push_back(probeValues[rename.Index]);
                    }
                }
            }
            Output_.Build.Clear();
            Output_.Probe.Clear();
        }
        return res;
    }

private:
    using TuplePairs = TSides<TPackResult>;

    void AssertSizeIsSane() const {
        if constexpr (Kind == EJoinKind::LeftOnly || Kind == EJoinKind::LeftSemi) {
            MKQL_ENSURE(Output_.Build.NTuples == 0,
                "Left Only and Left Semi join types shouldn't collect any Build tuples");
        } else if constexpr (Kind == EJoinKind::Left || Kind == EJoinKind::Inner) {
            MKQL_ENSURE(Output_.Build.NTuples == Output_.Probe.NTuples,
                "Inner and Left join types must collect same amount of tuples from build and probe");
        }
    }

    TuplePairs Output_;
    const TDqJoinImplRenames* Renames_;
    TSides<IScalarLayoutConverter*> Converters_;
    const int BuildWidth_;
    const int ProbeWidth_;
    bool LeftIsBuild_;
    TPackResult NullRowPacked_;
};

template <EJoinKind Kind>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Kind>> {
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, TDqScalarJoinContext meta,
                           TSides<IComputationWideFlowNode*> flows)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , Meta_(std::make_unique<TDqScalarJoinContext>(std::move(meta)))
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
                     TSides<std::unique_ptr<IScalarLayoutConverter>> converters, const TDqScalarJoinContext* meta)
            : TBase(memInfo)
            , Meta_(meta)
            , Converters_(std::move(converters))
            , JoinCtx_(&ctx)
            , Join_(TSides<TScalarPackedTupleSource>{
                        .Build = {ctx, flows.Build, meta, Converters_.Build.get(), ESide::Build},
                        .Probe = {ctx, flows.Probe, meta, Converters_.Probe.get(), ESide::Probe}},
                    ctx, "ScalarHashJoin",
                    TSides<const NPackedTuple::TTupleLayout*>{.Build = Converters_.Build->GetTupleLayout(),
                                                              .Probe = Converters_.Probe->GetTupleLayout()},
                    meta->Settings)
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
        const TDqScalarJoinContext* Meta_;
        TSides<std::unique_ptr<IScalarLayoutConverter>> Converters_;
        TComputationContext* JoinCtx_;
        JoinType Join_;
        TRenamesScalarOutput<Kind> Output_;
        std::optional<typename TRenamesScalarOutput<Kind>::TFlushResult> Buffer_;
        size_t BufferPos_ = 0;
        static constexpr int Threshold_ = 10000;
    };

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        TSides<std::unique_ptr<IScalarLayoutConverter>> converters;
        TTypeInfoHelper helper;
        for (ESide side : EachSide) {
            TVector<NPackedTuple::EColumnRole> roles(std::ssize(Meta_->UserTypes.SelectSide(side)),
                                                     NPackedTuple::EColumnRole::Payload);
            for (int column : Meta_->KeyColumns.SelectSide(side)) {
                roles[column] = NPackedTuple::EColumnRole::Key;
            }
            converters.SelectSide(side) = MakeScalarLayoutConverter(helper, Meta_->UserTypes.SelectSide(side), roles,
                ctx.HolderFactory);
        }

        state = ctx.HolderFactory.Create<TStreamState>(ctx, Flows_, std::move(converters), Meta_.get());
    }

    void RegisterDependencies() const final {
        this->FlowDependsOnBoth(Flows_.Build, Flows_.Probe);
    }

private:
    std::unique_ptr<const TDqScalarJoinContext> Meta_;
    TSides<IComputationWideFlowNode*> Flows_;
};

} // namespace

IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 8, "Expected 8 args");

    TDqScalarJoinContext meta;

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow() || joinType->IsStream(),
        "Expected WideFlow or WideStream as join output");
    const auto joinComponents = GetWideComponents(joinType);
    MKQL_ENSURE(!joinComponents.empty(), "Expected at least one column");
    for (auto* type : joinComponents) {
        meta.ResultItemTypes.push_back(type);
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow() || leftType->IsStream(),
        "Expected WideFlow or WideStream as left input");
    MKQL_ENSURE(GetWideComponentsCount(leftType) > 0, "Expected at least one left column");
    const auto leftFlowComponents = GetWideComponents(leftType);
    MKQL_ENSURE(!leftFlowComponents.empty(), "Expected at least one column");
    for (auto* type : leftFlowComponents) {
        meta.InputTypes.Probe.push_back(type);
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow() || rightType->IsStream(),
        "Expected WideFlow or WideStream as right input");
    MKQL_ENSURE(GetWideComponentsCount(rightType) > 0, "Expected at least one right column");
    const auto rightFlowComponents = GetWideComponents(rightType);
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
        meta.KeyColumns.Probe.emplace_back(static_cast<int>(item->AsValue().Get<ui32>()));
    }

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        meta.KeyColumns.Build.emplace_back(static_cast<int>(item->AsValue().Get<ui32>()));
    }

    MKQL_ENSURE(meta.KeyColumns.Build.size() == meta.KeyColumns.Probe.size(), "Key columns mismatch");

    TDqUserRenames userRenames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));

    const auto leftFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    const auto rightFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));
    MKQL_ENSURE(leftFlow, "Expected WideFlow as a left input");
    MKQL_ENSURE(rightFlow, "Expected WideFlow as a right input");

    ValidateRenames(userRenames, joinKind, std::ssize(meta.InputTypes.Probe), std::ssize(meta.InputTypes.Build));

    for (auto rename : userRenames) {
        ESide thisSide = rename.Side == EJoinSide::kLeft ? ESide::Probe : ESide::Build;
        meta.Renames.push_back({.Index = rename.Index, .Side = thisSide});
    }

    {
        const auto settingsTuple = AS_VALUE(TTupleLiteral, callable.GetInput(7));
        if (settingsTuple->GetValuesCount() >= 1) {
            meta.Settings.BuildSide =
                static_cast<EBuildSide>(AS_VALUE(TDataLiteral, settingsTuple->GetValue(0))->AsValue().Get<ui32>());
        }
    }

    if (meta.Settings.LeftIsBuild()) {
        std::swap(meta.InputTypes.Build, meta.InputTypes.Probe);
        std::swap(meta.KeyColumns.Build, meta.KeyColumns.Probe);
        for (auto& rename : meta.Renames) {
            rename.Side = (rename.Side == ESide::Build) ? ESide::Probe : ESide::Build;
        }
    }

    for (ESide side : EachSide) {
        auto& keyColumns = meta.KeyColumns.SelectSide(side);
        const int numDataCols = std::ssize(meta.InputTypes.SelectSide(side));
        const int numKeys = std::ssize(keyColumns);

        bool needsReorder = false;
        for (int i = 0; i < numKeys; ++i) {
            if (keyColumns[i] != i) {
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
            auto it = std::find(perm.begin(), perm.end(), keyColumns[i]);
            MKQL_ENSURE(it != perm.end(), "bad key column index");
            std::swap(perm[i], *it);
        }

        meta.ColumnPermutation.SelectSide(side) = perm;

        auto origTypes = TVector<TType*>(meta.InputTypes.SelectSide(side).begin(),
            meta.InputTypes.SelectSide(side).begin() + numDataCols);
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

    const ESide nullableSide = meta.Settings.LeftIsBuild() ? ESide::Probe : ESide::Build;
    for (ESide side : EachSide) {
        for (int index = 0; index < std::ssize(meta.InputTypes.SelectSide(side)); ++index) {
            TType* thisType = meta.InputTypes.SelectSide(side)[index];
            if (meta.Kind == EJoinKind::Left && side == nullableSide && !thisType->IsOptional()) {
                meta.UserTypes.SelectSide(side).push_back(TOptionalType::Create(thisType, ctx.Env));
            } else {
                meta.UserTypes.SelectSide(side).push_back(thisType);
            }
        }
    }

    const auto flows = meta.Settings.LeftIsBuild()
        ? TSides<IComputationWideFlowNode*>{.Build = leftFlow, .Probe = rightFlow}
        : TSides<IComputationWideFlowNode*>{.Build = rightFlow, .Probe = leftFlow};

    using enum EJoinKind;
    if (joinKind == Inner) {
        return new TScalarHashJoinWrapper<Inner>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == LeftOnly) {
        return new TScalarHashJoinWrapper<LeftOnly>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == LeftSemi) {
        return new TScalarHashJoinWrapper<LeftSemi>(ctx.Mutables, std::move(meta), flows);
    } else if (joinKind == Left) {
        return new TScalarHashJoinWrapper<Left>(ctx.Mutables, std::move(meta), flows);
    }
    MKQL_ENSURE(false, "unsupported join type in scalar hash join");
}

} // namespace NKikimr::NMiniKQL
