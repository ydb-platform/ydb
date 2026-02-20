#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <arrow/scalar.h>

#include "dq_join_common.h"

namespace NKikimr::NMiniKQL {

namespace {

using TDqJoinImplRenames = TDqRenames<ESide>;

struct TDqBlockJoinContext {
    TSides<TVector<TBlockType*>> InputTypes;
    TSides<TVector<int>> KeyColumns;
    TVector<TBlockType*> ResultItemTypes;
    TDqJoinImplRenames Renames;
    EJoinKind Kind;
    TSides<i32> TempStateIndes;
    // Pre-computed during graph construction in WrapDqBlockHashJoin using the
    // program's TTypeEnvironment.  This avoids creating TOptionalType objects
    // at runtime (inside DoCalculate) whose lifetime depends on the
    // TComputationContext – which may differ between iterations/retries.
    TSides<TVector<TType*>> UserTypes;
};

class TBlockPackedTupleSource : public NNonCopyable::TMoveOnly {
  public:
    TBlockPackedTupleSource(TComputationContext& ctx, TSides<IComputationNode*> stream,
                            const TDqBlockJoinContext* meta,
                            TSides<std::unique_ptr<IBlockLayoutConverter>>& converters, ESide side)
        : Side_(side)
        , Stream_(stream.SelectSide(side))
        , StreamValues_(Stream_->GetValue(ctx))
        , Buff_(ctx.MutableValues.get() + meta->TempStateIndes.SelectSide(side), meta->InputTypes.SelectSide(side).size())
        , ArrowBlockToInternalConverter_(converters.SelectSide(side).get())
    {}

    bool Finished() const {
        return Finished_;
    }

    int UserDataCols() const {
        return Buff_.size() - 1;
    }

    FetchResult<IBlockLayoutConverter::TPackResult> FetchRow() {
        if (Finished()) {
            return Finish{};
        }
        auto res = StreamValues_.WideFetch(Buff_.data(), Buff_.size());
        if (res != NYql::NUdf::EFetchStatus::Ok) {
            if (res == NYql::NUdf::EFetchStatus::Finish) {
                Finished_ = true;
                return Finish{};
            }
            return Yield{};
        }
        const size_t cols = UserDataCols();
        TVector<arrow::Datum> columns = ArrowFromUV({Buff_.data(), cols});
        IBlockLayoutConverter::TPackResult result;
        ArrowBlockToInternalConverter_->Pack(columns, result);
        return One{std::move(result)};
    }

  private:
    TVector<arrow::Datum> ArrowFromUV(std::span<const NYql::NUdf::TUnboxedValue> UVs) {
        TVector<arrow::Datum> arrow;
        for (const auto& uv : UVs) {
            arrow.push_back(TArrowBlock::From(uv).GetDatum());
        }
        return arrow;
    }

    bool Finished_ = false;
    [[maybe_unused]]ESide Side_;
    IComputationNode* Stream_;
    NYql::NUdf::TUnboxedValue StreamValues_;
    std::span<NYql::NUdf::TUnboxedValue> Buff_;
    IBlockLayoutConverter* ArrowBlockToInternalConverter_;
};

template<EJoinKind Kind>
struct TRenamesPackedTupleOutput : NNonCopyable::TMoveOnly {
    TRenamesPackedTupleOutput(const TDqBlockJoinContext* meta, TSides<IBlockLayoutConverter*> converters,
                              const TVector<TType*>& userBuildTypes, arrow::MemoryPool& arrowPool)
        : Renames_(&meta->Renames)
        , Converters_(converters)
    {
        if constexpr (!std::is_same_v<decltype(Nulls_), Empty>) {
            TVector<arrow::Datum> nulls;
            for(auto* type:userBuildTypes) {
                auto strname = type->GetKindAsStr();
                MKQL_ENSURE(type->IsOptional(), Sprintf("expected every type of right side to be optional when join type is Left, got type №%i: %s  ", nulls.size()+1, strname.data()));
                int blockSize = NMiniKQL::CalcBlockLen(NMiniKQL::CalcMaxBlockItemSize(type));
                auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, arrowPool, blockSize, nullptr);
                builder->Add(NYql::NUdf::TBlockItem{});
                nulls.push_back(builder->Build(true));
            }
            Converters_.Build->Pack(nulls,Nulls_);
        }
    }

    int Columns() const {
        return Renames_->size();
    }

    i64 SizeTuples() const {
        AssertSizeIsSane();
        return Output_.Probe.NTuples;
    }



    using TuplePairs = TSides<TPackResult>;
    struct Empty {};
    using BuildNullIfNeeded = std::conditional_t<Kind==EJoinKind::Left, TPackResult, Empty>;

    auto MakeConsumeFn() {
        struct ConsumeFn {
            TRenamesPackedTupleOutput& self;
            void operator()(TSides<TSingleTuple> tuples) {
                for(ESide side: EachSide) {
                    self.Output_.SelectSide(side).AppendTuple(tuples.SelectSide(side), self.Converters_.SelectSide(side)->GetTupleLayout());
                }
            }
            void operator()(TSingleTuple tuple) {
                if constexpr (Kind == EJoinKind::Left) {
                    TSingleTuple null{.PackedData = self.Nulls_.PackedTuples.data(), .OverflowBegin = self.Nulls_.Overflow.data() };
                    this->operator()(TSides<TSingleTuple>{.Build = null, .Probe = tuple});
                } else if constexpr(SemiOrOnlyJoin(Kind)) {
                    self.Output_.Probe.AppendTuple(tuple, self.Converters_.Probe->GetTupleLayout());
                }
            }
        };
        return ConsumeFn{*this};
    }

    TVector<arrow::Datum> FlushAndApplyRenames() {
        if constexpr(LeftSemiOrOnly(Kind)) {
            TVector<arrow::Datum> out;
            Converters_.Probe->Unpack(Output_.Probe, out);
            Output_.Probe.Clear();
            TVector<arrow::Datum> renamed;
            for(auto rename: *Renames_){
                MKQL_ENSURE(rename.Side == ESide::Probe, "renames in Semi or Only Left Join shouldn't contain columns from right side");
                renamed.push_back(out[rename.Index]);
            }
            return renamed;
        } else {
            TSides<TVector<arrow::Datum>> sides;
            for(ESide side: EachSide) {
                Converters_.SelectSide(side)->Unpack(Output_.SelectSide(side), sides.SelectSide(side));
                Output_.SelectSide(side).Clear();
            }
            TVector<arrow::Datum> renamed;
            for (auto rename : *Renames_) {
                renamed.push_back(sides.SelectSide(rename.Side)[rename.Index]);
            }
            return renamed;
        }
    }

  private:
    TSides<TVector<arrow::Datum>> Flush() {
        TSides<TVector<arrow::Datum>> out;
        for(ESide side: EachSide) {

            Converters_.SelectSide(side)->Unpack(Output_.SelectSide(side), out.SelectSide(side));
            Output_.SelectSide(side).Clear();

        }

        return out;
    }
    void AssertSizeIsSane() const{
        if constexpr (Kind == EJoinKind::LeftOnly || Kind==EJoinKind::LeftSemi) {
            MKQL_ENSURE(Output_.Build.NTuples == 0, "Left Only and Left Semi join types shouldn't collect any Build(right) tuples");
        } else if constexpr (Kind == EJoinKind::Left || Kind == EJoinKind::Inner) {
            MKQL_ENSURE(Output_.Build.NTuples == Output_.Probe.NTuples, "Inner and Left join types must collect same amount of tuples from build and probe");
        }
    }

    TuplePairs Output_;
    const TDqJoinImplRenames* Renames_;
    TSides<IBlockLayoutConverter*> Converters_;
    BuildNullIfNeeded Nulls_;
};

template <EJoinKind Kind> class TBlockHashJoinWrapper : public TMutableComputationNode<TBlockHashJoinWrapper<Kind>> {
  private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWrapper>;

  public:
    TBlockHashJoinWrapper(TComputationMutables& mutables, TDqBlockJoinContext meta, TSides<IComputationNode*> streams)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Meta_(std::make_unique<TDqBlockJoinContext>(meta))
        , Streams_(streams)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {

        TTypeInfoHelper helper;
        TSides<std::unique_ptr<IBlockLayoutConverter>> layouts;
        const auto& userTypes = Meta_->UserTypes;
        for(ESide side: EachSide) {
            TVector<NPackedTuple::EColumnRole> roles(userTypes.SelectSide(side).size(), NPackedTuple::EColumnRole::Payload);
            for (int column : Meta_->KeyColumns.SelectSide(side)) {
                roles[column] = NPackedTuple::EColumnRole::Key;
            }
            layouts.SelectSide(side) = MakeBlockLayoutConverter(helper, userTypes.SelectSide(side), roles, &ctx.ArrowMemoryPool);
        }
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Streams_, std::move(layouts), Meta_.get(), userTypes.Build);
    }

  private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;
        using JoinType = NJoinPackedTuples::THybridHashJoin<TBlockPackedTupleSource, TestStorageSettings, Kind>;

      public:
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, TSides<IComputationNode*> streams,
                     TSides<std::unique_ptr<IBlockLayoutConverter>> converters, const TDqBlockJoinContext* meta,
                     const TVector<TType*>& userBuildTypes)
            : TBase(memInfo)
            , Meta_(meta)
            , Converters_(std::move(converters))
            , Join_(TSides<TBlockPackedTupleSource>{.Build = {ctx, streams, meta, Converters_, ESide::Build},
                                                    .Probe = {ctx, streams, meta, Converters_, ESide::Probe}},
                    ctx, "BlockHashJoin",
                    TSides<const NPackedTuple::TTupleLayout*>{.Build = Converters_.Build->GetTupleLayout(),
                                                              .Probe = Converters_.Probe->GetTupleLayout()})
            , Ctx_(&ctx)
            , Output_(meta, {.Build = Converters_.Build.get(), .Probe = Converters_.Probe.get()}, userBuildTypes, ctx.ArrowMemoryPool)
        {}

        NUdf::EFetchStatus FlushTo(NUdf::TUnboxedValue* output) {
            MKQL_ENSURE(Output_.SizeTuples() != 0, "make sure we are flushing something, not empty set of tuples");
            i64 rows = Output_.SizeTuples();
            TVector<arrow::Datum> arrowOutput = Output_.FlushAndApplyRenames();
            for (int colIndex = 0; colIndex < Output_.Columns(); ++colIndex) {
                output[colIndex] = Ctx_->HolderFactory.CreateArrowBlock(std::move(arrowOutput[colIndex]));
            }
            output[Output_.Columns()] = Ctx_->HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(rows)));

            MKQL_ENSURE(Output_.SizeTuples() == 0, "something left after flush??");
            return NYql::NUdf::EFetchStatus::Ok;
        }

      private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            size_t expectedSize = Meta_->Renames.size() + 1;
            MKQL_ENSURE(width == expectedSize,
                        Sprintf("runtime(%i) vs compile-time(%i) tuple width mismatch", width, expectedSize));
            if (Finished_) {
                return NYql::NUdf::EFetchStatus::Finish;
            }
            while (Output_.SizeTuples() < Threshold_) {
                auto res = Join_.MatchRows(*Ctx_, Output_.MakeConsumeFn());
                switch (res) {
                case EFetchResult::Finish: {
                    if (Output_.SizeTuples() == 0) {
                        return NYql::NUdf::EFetchStatus::Finish;
                    }
                    Finished_ = true;
                    return FlushTo(output);
                }
                case EFetchResult::Yield:
                    return NYql::NUdf::EFetchStatus::Yield;
                case EFetchResult::One:
                    break;
                }
            }
            return FlushTo(output);
        }

      private:
        const TDqBlockJoinContext* Meta_;
        TSides<std::unique_ptr<IBlockLayoutConverter>> Converters_;
        JoinType Join_;
        TComputationContext* Ctx_;
        TRenamesPackedTupleOutput<Kind> Output_;
        const int Threshold_ = 10000;
        bool Finished_ = false;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Streams_.Build);
        this->DependsOn(Streams_.Probe);
    }

    std::unique_ptr<TDqBlockJoinContext> Meta_;
    TSides<IComputationNode*> Streams_;
};

} // namespace

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");
    TDqBlockJoinContext meta;

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(), "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    for (auto* blockType : joinComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of result stream");
        meta.ResultItemTypes.push_back(AS_TYPE(TBlockType, blockType));
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(), "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    MKQL_ENSURE(leftStreamComponents.size() > 0, "Expected at least one column");
    for (auto* blockType : leftStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of left stream");
        meta.InputTypes.Probe.push_back(AS_TYPE(TBlockType, blockType));
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(), "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    MKQL_ENSURE(rightStreamComponents.size() > 0, "Expected at least one column");
    for (auto* blockType : rightStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of right stream");
        meta.InputTypes.Build.push_back(AS_TYPE(TBlockType, blockType));
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
    TDqUserRenames userRenames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));

    MKQL_ENSURE(meta.KeyColumns.Build.size() == meta.KeyColumns.Probe.size(), "Key columns mismatch");

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);
    ValidateRenames(userRenames, joinKind, std::ssize(meta.InputTypes.Probe) - 1,
                    std::ssize(meta.InputTypes.Build) - 1);
    for(ESide side: EachSide) {
        int size = std::ssize(meta.InputTypes.SelectSide(side));
        for(int index = 0; index < size; ++index) {
            TBlockType* thisType = meta.InputTypes.SelectSide(side)[index];
            if (index == size - 1) {
                MKQL_ENSURE(thisType->GetShape() == TBlockType::EShape::Scalar, Sprintf("expected last(%i) column in %s side to be scalar size",index, AsString(side)));
            } else {
                MKQL_ENSURE(thisType->GetShape() == TBlockType::EShape::Many, Sprintf("expected %i column in %s side to be block data",index, AsString(side)));
            }
        }
    }

    for (auto rename : userRenames) {
        ESide thisSide = [&] {
            if (rename.Side == EJoinSide::kLeft) {
                return ESide::Probe;
            } else {
                return ESide::Build;
            }
        }();
        meta.Renames.push_back({.Index = rename.Index, .Side = thisSide});
    }
    for(ESide side: EachSide) {
        meta.TempStateIndes.SelectSide(side) = std::exchange(ctx.Mutables.CurValueIndex, meta.InputTypes.SelectSide(side).size() + ctx.Mutables.CurValueIndex);
    }

    for (ESide side : EachSide) {
        for (int index = 0; index < std::ssize(meta.InputTypes.SelectSide(side)) - 1; ++index) {
            TType* thisType = meta.InputTypes.SelectSide(side)[index]->GetItemType();
            if (meta.Kind == EJoinKind::Left && side == ESide::Build && !thisType->IsOptional()) {
                meta.UserTypes.SelectSide(side).push_back(TOptionalType::Create(thisType, ctx.Env));
            } else {
                meta.UserTypes.SelectSide(side).push_back(thisType);
            }
        }
    }

    using enum EJoinKind;
    if (joinKind == Inner) {
        return new TBlockHashJoinWrapper<Inner>(ctx.Mutables, meta, {.Build = rightStream, .Probe = leftStream});
    } else if (joinKind == LeftOnly) {
        return new TBlockHashJoinWrapper<LeftOnly>(ctx.Mutables, meta, {.Build = rightStream, .Probe = leftStream});
    } else if (joinKind == LeftSemi) {
        return new TBlockHashJoinWrapper<LeftSemi>(ctx.Mutables, meta, {.Build = rightStream, .Probe = leftStream});
    } else if (joinKind == Left) {
        return new TBlockHashJoinWrapper<Left>(ctx.Mutables, meta, {.Build = rightStream, .Probe = leftStream});
    } else {
        MKQL_ENSURE(false, "unsupported join type in block hash join" );
    }
}

} // namespace NKikimr::NMiniKQL
