
#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/comp_nodes/mkql_blocks.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <algorithm>
#include <arrow/scalar.h>

#include "dq_join_common.h"

namespace NKikimr::NMiniKQL {

namespace {

class TBlockRowSource : public NNonCopyable::TMoveOnly {
  public:
    TBlockRowSource(TComputationContext& ctx, IComputationNode* stream, const std::vector<TType*>& types)
        : Stream_(stream)
        , Values_(Stream_->GetValue(ctx))
        , Buff_(types.size())
    {
        TTypeInfoHelper typeInfoHelper;
        for (int index = 0; index < std::ssize(InputReaders); ++index) {
            InputReaders[index] = NYql::NUdf::MakeBlockReader(typeInfoHelper, types[index]);
            InputItemConverters[index] =
                MakeBlockItemConverter(typeInfoHelper, types[index], ctx.Builder->GetPgBuilder());
        }
    }

    bool Finished() const {
        return Finished_;
    }

    int UserDataSize() const {
        return Buff_.size() - 1;
    }

    NYql::NUdf::EFetchStatus ForEachRow(TComputationContext& ctx, std::invocable<NJoinTable::TTuple> auto consume) {
        auto res = Values_.WideFetch(Buff_.data(), Buff_.size());
        if (res != NYql::NUdf::EFetchStatus::Ok) {
            if (res == NYql::NUdf::EFetchStatus::Finish) {
                Finished_ = true;
            }
            return res;
        }
        const int cols = UserDataSize();

        for (int index = 0; index < cols; ++index) {
            Blocks_[index] = &TArrowBlock::From(Buff_[index]).GetDatum();
        }

        const int rows = ArrowScalarAsInt(TArrowBlock::From(Buff_.back()));

        for (int rowIndex = 0; rowIndex < rows; ++rowIndex) {
            for (int colIndex = 0; colIndex < cols; ++colIndex) {
                ConsumeBuff_[colIndex] = InputItemConverters[colIndex]->MakeValue(
                    InputReaders[colIndex]->GetItem(*Blocks_[colIndex]->array(), rowIndex), ctx.HolderFactory);
            }
            consume(ConsumeBuff_.data());
        }
        return NYql::NUdf::EFetchStatus::Ok;
    }

  private:
    bool Finished_ = false;
    IComputationNode* Stream_;
    NYql::NUdf::TUnboxedValue Values_;
    TUnboxedValueVector Buff_;
    TUnboxedValueVector ConsumeBuff_{Buff_.size() - 1};
    std::vector<const arrow::Datum*> Blocks_{Buff_.size() - 1};
    std::vector<std::unique_ptr<IBlockReader>> InputReaders{Buff_.size() - 1};
    std::vector<std::unique_ptr<IBlockItemConverter>> InputItemConverters{Buff_.size() - 1};
};

template <EJoinKind Kind> class TBlockHashJoinWrapper : public TMutableComputationNode<TBlockHashJoinWrapper<Kind>> {
  private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWrapper>;

  public:
    TBlockHashJoinWrapper(TComputationMutables& mutables, const TVector<TType*>&& resultItemTypes,
                          const TVector<TType*>&& leftItemTypes, const TVector<ui32>&& leftKeyColumns,
                          const TVector<TType*>&& rightItemTypes, const TVector<ui32>&& rightKeyColumns,
                          IComputationNode* leftStream, IComputationNode* rightStream, TDqRenames renames)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftItemTypes_(std::move(leftItemTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightItemTypes_(std::move(rightItemTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , Renames_(std::move(renames))
        , LeftStream_(leftStream)
        , RightStream_(rightStream)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, LeftKeyColumns_, RightKeyColumns_, LeftStream_, RightStream_,
                                                      LeftItemTypes_, RightItemTypes_, ResultItemTypes_, Renames_);
    }

  private:
    class TStreamValue : public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

      public:
        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const TVector<ui32>& leftKeyColumns,
                     const TVector<ui32>& rightKeyColumns, IComputationNode* leftStream, IComputationNode* rightStream,
                     const TVector<TType*>& leftStreamTypes, const TVector<TType*>& rightStreamTypes,
                     const TVector<TType*>& resultStreamTypes, TDqRenames renames)
            : TBase(memInfo)
            , Join_(memInfo, TBlockRowSource{ctx, leftStream, leftStreamTypes},
                    TBlockRowSource{ctx, rightStream, rightStreamTypes},
                    TJoinMetadata{TColumnsMetadata{rightKeyColumns, rightStreamTypes},
                                  TColumnsMetadata{leftKeyColumns, leftStreamTypes},
                                  KeyTypesFromColumns(leftStreamTypes, leftKeyColumns)}, ctx.MakeLogger(),
                    "BlockHashJoin")
            , Ctx_(&ctx)
            , Output_(std::move(renames), leftStreamTypes, rightStreamTypes)
            , OutputTypes_(resultStreamTypes)
        {
            TTypeInfoHelper typeInfoHelper;
            for (auto outType : OutputTypes_) {
                OutputItemConverters_.push_back(
                    MakeBlockItemConverter(typeInfoHelper, outType, ctx.Builder->GetPgBuilder()));
            }
        }

        NUdf::EFetchStatus FlushTo(NUdf::TUnboxedValue* output) {
            MKQL_ENSURE(!Output_.OutputBuffer.empty(), "make sure we are flushing something, not empty set of tuples");
            TTypeInfoHelper helper;
            std::vector<std::unique_ptr<NYql::NUdf::IArrayBuilder>> blockBuilders;
            int rows = Output_.SizeTuples();
            for (int i = 0; i < Output_.TupleSize(); ++i) {
                blockBuilders.push_back(MakeArrayBuilder(helper, OutputTypes_[i], Ctx_->ArrowMemoryPool, rows,
                                                         &Ctx_->Builder->GetPgBuilder()));
            }

            for (int rowIndex = 0; rowIndex < rows; ++rowIndex) {
                for (int colIndex = 0; colIndex < Output_.TupleSize(); ++colIndex) {
                    int valueIndex = colIndex + rowIndex * Output_.TupleSize();
                    blockBuilders[colIndex]->Add(
                        OutputItemConverters_[colIndex]->MakeItem(Output_.OutputBuffer[valueIndex]));
                }
            }
            Output_.OutputBuffer.clear();
            for (int colIndex = 0; colIndex < Output_.TupleSize(); ++colIndex) {
                output[colIndex] = Ctx_->HolderFactory.CreateArrowBlock(blockBuilders[colIndex]->Build(true));
            }
            output[Output_.TupleSize()] =
                Ctx_->HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(rows)));
            MKQL_ENSURE(Output_.OutputBuffer.empty(), "something left after flush??");
            return NYql::NUdf::EFetchStatus::Ok;
        }

      private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            MKQL_ENSURE(width == OutputTypes_.size(),
                        Sprintf("runtime(%i) vs compile-time(%i) tuple width mismatch", width, OutputTypes_.size()));
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
        TJoin<TBlockRowSource, Kind> Join_;
        TComputationContext* Ctx_;
        TRenamedOutput<Kind> Output_;
        std::vector<std::unique_ptr<IBlockItemConverter>> OutputItemConverters_;
        const std::vector<TType*> OutputTypes_;
        const int Threshold_ = 10000;
        bool Finished_ = false;
    };

    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

  private:
    const TVector<TType*> ResultItemTypes_;
    const TVector<TType*> LeftItemTypes_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<TType*> RightItemTypes_;
    const TVector<ui32> RightKeyColumns_;
    const TDqRenames Renames_;
    IComputationNode* LeftStream_;
    IComputationNode* RightStream_;
};

} // namespace

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(), "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    TVector<TType*> joinItems;
    for (auto* blockType : joinComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of result stream");
        joinItems.push_back(AS_TYPE(TBlockType, blockType)->GetItemType());
    }

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(), "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    MKQL_ENSURE(leftStreamComponents.size() > 0, "Expected at least one column");
    TVector<TType*> leftStreamItems;
    for (auto* blockType : leftStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of left stream");
        leftStreamItems.push_back(AS_TYPE(TBlockType, blockType)->GetItemType());
    }

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(), "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    MKQL_ENSURE(rightStreamComponents.size() > 0, "Expected at least one column");
    TVector<TType*> rightStreamItems;
    for (auto* blockType : rightStreamComponents) {
        MKQL_ENSURE(blockType->IsBlock(), "Expected block types as wide components of right stream");
        rightStreamItems.push_back(AS_TYPE(TBlockType, blockType)->GetItemType());
    }

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
    TDqRenames renames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);
    MKQL_ENSURE(joinKind == EJoinKind::Inner, "Only inner is supported, see gh#26780 for details.");
    ValidateRenames(renames, joinKind, std::ssize(leftStreamItems) - 1, std::ssize(rightStreamItems) - 1);

    return new TBlockHashJoinWrapper<EJoinKind::Inner>(ctx.Mutables, std::move(joinItems), std::move(leftStreamItems),
                                                       std::move(leftKeyColumns), std::move(rightStreamItems),
                                                       std::move(rightKeyColumns), leftStream, rightStream, renames);
}

} // namespace NKikimr::NMiniKQL
