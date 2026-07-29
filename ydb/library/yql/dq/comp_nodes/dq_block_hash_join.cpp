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
    TSides<TVector<ui32>> KeyColumns;
    TVector<TBlockType*> ResultItemTypes;
    TDqJoinImplRenames Renames;
    EJoinKind Kind;
    TSides<i32> TempStateIndes;
    TBlockHashJoinSettings Settings;
    // Pre-computed during graph construction in WrapDqBlockHashJoin using the
    // program's TTypeEnvironment.  This avoids creating TOptionalType objects
    // at runtime (inside DoCalculate) whose lifetime depends on the
    // TComputationContext – which may differ between iterations/retries.
    TSides<TVector<TType*>> UserTypes;
    TSides<TVector<int>> ColumnPermutation;
};

class TBlockPackedTupleSource : public NNonCopyable::TMoveOnly {
  public:
    TBlockPackedTupleSource(TComputationContext& ctx, TSides<IComputationNode*> stream,
                            const TDqBlockJoinContext* meta,
                            TSides<std::unique_ptr<IBlockLayoutConverter>>& converters, ESide side)
        : Side_(side)
        , Meta_(meta)
        , ArrowPool_(&ctx.ArrowMemoryPool)
        , Stream_(stream.SelectSide(side))
        , StreamValues_(Stream_->GetValue(ctx))
        , Buff_(ctx.MutableValues.get() + meta->TempStateIndes.SelectSide(side), meta->InputTypes.SelectSide(side).size())
        , ArrowBlockToInternalConverter_(converters.SelectSide(side).get())
        , ColumnPermutation_(meta->ColumnPermutation.SelectSide(side))
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
        if (!ColumnPermutation_.empty()) {
            TVector<arrow::Datum> permuted(cols);
            for (size_t j = 0; j < cols; ++j) {
                permuted[j] = std::move(columns[ColumnPermutation_[j]]);
            }
            columns = std::move(permuted);
        }
        NormalizeScalarColumns(columns);
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

    void NormalizeScalarColumns(TVector<arrow::Datum>& columns) {
        bool hasScalar = false;
        for (const auto& column : columns) {
            if (column.is_scalar()) {
                hasScalar = true;
                break;
            }
        }
        if (!hasScalar) {
            return;
        }

        const ui64 blockLen = GetBlockCount(Buff_[UserDataCols()]);
        MKQL_ENSURE(blockLen > 0, "Got a scalar column in a zero-length block");

        const auto& inputTypes = Meta_->InputTypes.SelectSide(Side_);
        for (size_t j = 0; j < columns.size(); ++j) {
            if (columns[j].is_scalar()) {
                TType* itemType = inputTypes[j]->GetItemType();
                columns[j] = MakeArrayFromScalar(*columns[j].scalar(), blockLen, itemType, *ArrowPool_);
            }
        }
    }

    bool Finished_ = false;
    ESide Side_;
    const TDqBlockJoinContext* Meta_;
    arrow::MemoryPool* ArrowPool_;
    IComputationNode* Stream_;
    NYql::NUdf::TUnboxedValue StreamValues_;
    std::span<NYql::NUdf::TUnboxedValue> Buff_;
    IBlockLayoutConverter* ArrowBlockToInternalConverter_;
    TVector<int> ColumnPermutation_;
};

template<EJoinKind Kind>
struct TRenamesPackedTupleOutput : TPackedTupleOutputBase<Kind, IBlockLayoutConverter> {
    using TBase = TPackedTupleOutputBase<Kind, IBlockLayoutConverter>;

    TRenamesPackedTupleOutput(const TDqBlockJoinContext* meta, TSides<IBlockLayoutConverter*> converters,
                              const TVector<TType*>& userNullTypes, arrow::MemoryPool& arrowPool)
        : TBase(&meta->Renames, converters, meta->Settings.LeftIsBuild())
    {
        if constexpr (!std::is_same_v<typename TBase::BuildNullIfNeeded, typename TBase::Empty>) {
            TVector<arrow::Datum> nulls;
            for(auto* type:userNullTypes) {
                auto strname = type->GetKindAsStr();
                MKQL_ENSURE(type->IsOptional(), Sprintf("expected every type of right side to be optional when join type is Left, got type №%i: %s  ", nulls.size()+1, strname.data()));
                int blockSize = NMiniKQL::CalcBlockLen(NMiniKQL::CalcMaxBlockItemSize(type));
                auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, arrowPool, blockSize, nullptr);
                builder->Add(NYql::NUdf::TBlockItem{});
                nulls.push_back(builder->Build(true));
            }
            if (this->LeftIsBuild_) {
                this->Converters_.Probe->Pack(nulls, this->Nulls_);
            } else {
                this->Converters_.Build->Pack(nulls, this->Nulls_);
            }
        }
    }

    struct TFlushResult {
        TVector<arrow::Datum> Columns;
        i64 Rows;
    };

    TFlushResult Flush() {
        TFlushResult res;
        res.Rows = this->SizeTuples();
        res.Columns = FlushAndApplyRenames();
        return res;
    }

    TVector<arrow::Datum> FlushAndApplyRenames() {
        if constexpr(LeftSemiOrOnly(Kind)) {
            TVector<arrow::Datum> out;
            this->Converters_.Probe->Unpack(this->Output_.Probe, out);
            this->Output_.Probe.Clear();
            TVector<arrow::Datum> renamed;
            for(auto rename: *this->Renames_){
                MKQL_ENSURE(rename.Side == ESide::Probe, "renames in Semi or Only Left Join shouldn't contain columns from right side");
                renamed.push_back(out[rename.Index]);
            }
            return renamed;
        } else {
            TSides<TVector<arrow::Datum>> sides;
            for(ESide side: EachSide) {
                this->Converters_.SelectSide(side)->Unpack(this->Output_.SelectSide(side), sides.SelectSide(side));
                this->Output_.SelectSide(side).Clear();
            }
            TVector<arrow::Datum> renamed;
            for (auto rename : *this->Renames_) {
                renamed.push_back(sides.SelectSide(rename.Side)[rename.Index]);
            }
            return renamed;
        }
    }
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
            const auto roles = MakeColumnRoles(userTypes.SelectSide(side).size(), Meta_->KeyColumns.SelectSide(side));
            layouts.SelectSide(side) = MakeBlockLayoutConverter(helper, userTypes.SelectSide(side), roles, &ctx.ArrowMemoryPool);
        }
        const auto& userNullTypes = (Kind == EJoinKind::Left && Meta_->Settings.LeftIsBuild()) ? userTypes.Probe : userTypes.Build;
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Streams_,
            std::move(layouts), Meta_.get(), userNullTypes);
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
                                                              .Probe = Converters_.Probe->GetTupleLayout()},
                    meta->Settings)
            , Ctx_(&ctx)
            , Output_(meta, {.Build = Converters_.Build.get(), .Probe = Converters_.Probe.get()}, userBuildTypes, ctx.ArrowMemoryPool)
        {}

        void WriteFlushToOutput(NUdf::TUnboxedValue* output, typename TRenamesPackedTupleOutput<Kind>::TFlushResult flush) {
            const int cols = Output_.Columns();
            for (int colIndex = 0; colIndex < cols; ++colIndex) {
                output[colIndex] = Ctx_->HolderFactory.CreateArrowBlock(std::move(flush.Columns[colIndex]), Ctx_->RuntimeSettings.DatumValidation.Get());
            }
            output[cols] = Ctx_->HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(flush.Rows)), Ctx_->RuntimeSettings.DatumValidation.Get());
        }

      private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            size_t expectedSize = Meta_->Renames.size() + 1;
            MKQL_ENSURE(width == expectedSize,
                        Sprintf("runtime(%i) vs compile-time(%i) tuple width mismatch", width, expectedSize));
            switch (RunPackedHashJoinBatch<MaxOutputRows_>(
                *Ctx_, Join_, Output_, [&](auto flush) { WriteFlushToOutput(output, std::move(flush)); })) {
            case EFetchResult::One:
                return NYql::NUdf::EFetchStatus::Ok;
            case EFetchResult::Yield:
                return NYql::NUdf::EFetchStatus::Yield;
            case EFetchResult::Finish:
                return NYql::NUdf::EFetchStatus::Finish;
            default:
                MKQL_ENSURE(false, "unexpected fetch result");
            }
            Y_UNREACHABLE();
        }

      private:
        const TDqBlockJoinContext* Meta_;
        TSides<std::unique_ptr<IBlockLayoutConverter>> Converters_;
        JoinType Join_;
        TComputationContext* Ctx_;
        TRenamesPackedTupleOutput<Kind> Output_;
        static constexpr i64 MaxOutputRows_ = 10000;
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
    MKQL_ENSURE(callable.GetInputsCount() == 8, "Expected 8 args");
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
    const auto parsed = ParseCommonHashJoinArgs(callable);
    const auto joinKind = parsed.Kind;
    meta.Kind = joinKind;
    meta.KeyColumns = parsed.KeyColumns;

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);
    ValidateRenames(parsed.UserRenames, joinKind, std::ssize(meta.InputTypes.Probe) - 1,
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

    meta.Renames = BuildImplRenames(parsed.UserRenames);

    {
        const auto settingsTuple = AS_VALUE(TTupleLiteral, callable.GetInput(7));
        if (settingsTuple->GetValuesCount() >= 1) {
            meta.Settings.BuildSide = static_cast<EBuildSide>(AS_VALUE(TDataLiteral, settingsTuple->GetValue(0))->AsValue().Get<ui32>());
        }
    }
    if (meta.Settings.LeftIsBuild()) {
        std::swap(meta.InputTypes.Build, meta.InputTypes.Probe);
        std::swap(meta.KeyColumns.Build, meta.KeyColumns.Probe);
        for (auto& rename : meta.Renames) {
            rename.Side = (rename.Side == ESide::Build) ? ESide::Probe : ESide::Build;
        }
    }

    ApplyKeyColumnPermutation(meta.KeyColumns, meta.InputTypes, /* trailingColumns */ 1, meta.Renames,
                              meta.ColumnPermutation);

    for(ESide side: EachSide) {
        meta.TempStateIndes.SelectSide(side) = std::exchange(ctx.Mutables.CurValueIndex, meta.InputTypes.SelectSide(side).size() + ctx.Mutables.CurValueIndex);
    }

    TSides<TVector<TType*>> itemTypes;
    for (ESide side : EachSide) {
        for (int index = 0; index < std::ssize(meta.InputTypes.SelectSide(side)) - 1; ++index) {
            itemTypes.SelectSide(side).push_back(meta.InputTypes.SelectSide(side)[index]->GetItemType());
        }
    }
    const ESide nullableSide = meta.Settings.LeftIsBuild() ? ESide::Probe : ESide::Build;
    meta.UserTypes = ForceOptionalOnNullableSide(itemTypes, meta.Kind, nullableSide, ctx.Env);

    const auto streams = meta.Settings.LeftIsBuild()
        ? TSides<IComputationNode*>{.Build = leftStream, .Probe = rightStream}
        : TSides<IComputationNode*>{.Build = rightStream, .Probe = leftStream};

    return DispatchHashJoinByKind<TBlockHashJoinWrapper, IComputationNode>(
        joinKind, "unsupported join type in block hash join", ctx.Mutables, std::move(meta), streams);
}

} // namespace NKikimr::NMiniKQL
