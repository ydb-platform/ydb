#include "mkql_block_top.h"
#include "mkql_block_impl.h"
#include "mkql_block_reader.h"
#include "mkql_block_builder.h"

#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/utils/sort.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool Sort, bool HasCount>
class TTopOrSortBlocksWrapper : public TStatefulWideFlowBlockComputationNode<TTopOrSortBlocksWrapper<Sort, HasCount>> {
    using TSelf = TTopOrSortBlocksWrapper<Sort, HasCount>;
    using TBase = TStatefulWideFlowBlockComputationNode<TSelf>;
    using TChunkedArrayIndex = TVector<IArrayBuilder::TArrayDataItem>;
public:
    TTopOrSortBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TArrayRef<TType* const> wideComponents, IComputationNode* count,
        TVector<IComputationNode*>&& directions, TVector<ui32>&& keyIndicies)
        : TBase(mutables, flow, wideComponents.size())
        , Flow_(flow)
        , Count_(count)
        , Directions_(std::move(directions))
        , KeyIndicies_(std::move(keyIndicies))
    {
        for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
            Columns_.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        }
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_VERIFY(output[Columns_.size()]);

        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        if (!s.PreparedCountAndDirections_) {
            if constexpr (HasCount) {
                s.Count_ = Count_->GetValue(ctx).Get<ui64>();
            }

            for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                s.Directions_[k] = Directions_[k]->GetValue(ctx).Get<bool>();
            }

            s.PreparedCountAndDirections_ = true;
        }

        if constexpr (HasCount) {
            if (!s.Count_) {
                s.IsFinished_ = true;
                return EFetchResult::Finish;
            }
        }

        if (!s.PreparedBuilders_) {
            s.AllocateBuilders(Columns_, ctx);
            s.PreparedBuilders_ = true;
        }

        if (s.WritingOutput_) {
            if (s.Written_ >= s.OutputLength_) {
                s.IsFinished_ = true;
                return EFetchResult::Finish;
            }

            s.FillSortOutputPart(Columns_, output, ctx);
            return EFetchResult::One;
        }

        for (;;) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result == EFetchResult::Yield) {
                return result;
            } else if (result == EFetchResult::One) {
                ui64 blockLen = TArrowBlock::From(s.Values_.back()).GetDatum().template scalar_as<arrow::UInt64Scalar>().value;

                if (!s.ScalarsFilled_) {
                    for (ui32 i = 0; i < Columns_.size(); ++i) {
                        if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                            s.ScalarValues_[i] = s.Values_[i];
                        }
                    }

                    s.ScalarsFilled_ = true;
                }

                if constexpr (!HasCount) {
                    for (ui32 i = 0; i < Columns_.size(); ++i) {
                        auto datum = TArrowBlock::From(s.Values_[i]).GetDatum();
                        if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                            s.SortInput_[i].emplace_back(datum);
                        }
                    }

                    s.OutputLength_ += blockLen;
                    continue;
                }

                // shrink input block
                TMaybe<TVector<ui64>> blockIndicies;
                if (blockLen > s.Count_) {
                    blockIndicies.ConstructInPlace();
                    blockIndicies->reserve(blockLen);
                    for (ui64 row = 0; row < blockLen; ++row) {
                        blockIndicies->emplace_back(row);
                    }

                    TVector<TChunkedArrayIndex> arrayIndicies(Columns_.size());
                    for (ui32 i = 0; i < Columns_.size(); ++i) {
                        if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                            auto datum = TArrowBlock::From(s.Values_[i]).GetDatum();
                            arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                        }
                    }

                    TBlockLess cmp(KeyIndicies_, s, arrayIndicies);
                    NYql::FastNthElement(blockIndicies->begin(), blockIndicies->begin() + s.Count_, blockIndicies->end(), cmp);
                }

                // copy all to builders
                s.AddTop(Columns_, blockIndicies, blockLen);
                if (s.BuilderLength_ + s.Count_ > s.BuilderMaxLength_) {
                    s.CompressBuilders(false, Columns_, KeyIndicies_, ctx);
                }

            } else {
                if constexpr (!HasCount) {
                    if (!s.OutputLength_) {
                        s.IsFinished_ = true;
                        return EFetchResult::Finish;
                    }

                    s.SortAll(Columns_, KeyIndicies_);
                    s.WritingOutput_ = true;
                    s.FillSortOutputPart(Columns_, output, ctx);
                    return EFetchResult::One;
                }

                s.IsFinished_ = true;
                if (!s.BuilderLength_) {
                    return EFetchResult::Finish;
                }

                if (s.BuilderLength_ > s.Count_ || Sort) {
                    s.CompressBuilders(Sort, Columns_, KeyIndicies_, ctx);
                }

                s.FillOutput(Columns_, output, ctx);
                return EFetchResult::One;
            }
        }
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            if constexpr (HasCount) {
                this->DependsOn(flow, Count_);
            }

            for (auto dir : Directions_) {
                this->DependsOn(flow, dir);
            }
        }
    }

    class TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        bool WritingOutput_ = false;
        ui64 OutputLength_ = 0;
        ui64 Written_ = 0;
        TVector<TVector<arrow::Datum>> SortInput_;
        TVector<ui64> SortPermutation_;
        TVector<TChunkedArrayIndex> SortArrays_;

        bool IsFinished_ = false;
        bool PreparedCountAndDirections_ = false;
        ui64 Count_ = 0;
        TVector<bool> Directions_;
        bool ScalarsFilled_ = false;
        TVector<NUdf::TUnboxedValue> ScalarValues_;
        TVector<std::unique_ptr<IBlockReader>> LeftReaders_;
        TVector<std::unique_ptr<IBlockReader>> RightReaders_;
        bool PreparedBuilders_ = false;
        TVector<std::unique_ptr<IArrayBuilder>> Builders_;
        ui64 BuilderMaxLength_ = 0;
        ui64 BuilderLength_ = 0;
        TVector<NUdf::IBlockItemComparator::TPtr> Comparators_; // by key columns only

        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;

        TState(TMemoryUsageInfo* memInfo, const TVector<ui32>& keyIndicies, const TVector<TBlockType*>& columns)
            : TBase(memInfo)
        {
            Directions_.resize(keyIndicies.size());
            LeftReaders_.resize(columns.size());
            RightReaders_.resize(columns.size());
            Builders_.resize(columns.size());
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                LeftReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
                RightReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
            }

            Values_.resize(columns.size() + 1);
            ValuePointers_.resize(columns.size() + 1);
            for (ui32 i = 0; i <= columns.size(); ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            Comparators_.resize(keyIndicies.size());
            for (ui32 k = 0; k < keyIndicies.size(); ++k) {
                Comparators_[k] = TBlockTypeHelper().MakeComparator(columns[keyIndicies[k]]->GetItemType());
            }

            SortInput_.resize(columns.size());
            SortArrays_.resize(columns.size());
        }

        ui64 GetStorageLength() const {
            Y_VERIFY(PreparedCountAndDirections_);
            return 2 * Count_;
        }

        void AllocateBuilders(const TVector<TBlockType*>& columns, TComputationContext& ctx) {
            BuilderMaxLength_ = GetStorageLength();
            size_t maxBlockItemSize = 0;
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                maxBlockItemSize = Max(maxBlockItemSize, CalcMaxBlockItemSize(columns[i]->GetItemType()));
            };

            BuilderMaxLength_ = Max(BuilderMaxLength_, CalcBlockLen(maxBlockItemSize));

            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), columns[i]->GetItemType(), ctx.ArrowMemoryPool, BuilderMaxLength_, &ctx.Builder->GetPgBuilder());
            }
        }

        void CompressBuilders(bool sort, const TVector<TBlockType*>& columns, const TVector<ui32>& keyIndicies, TComputationContext&) {
            Y_VERIFY(ScalarsFilled_);
            TVector<TChunkedArrayIndex> arrayIndicies(columns.size());
            TVector<arrow::Datum> tmpDatums(columns.size());
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = Builders_[i]->Build(false);
                    arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                    tmpDatums[i] = std::move(datum);
                }
            }

            TVector<ui64> blockIndicies;
            blockIndicies.reserve(BuilderLength_);
            for (ui64 row = 0; row < BuilderLength_; ++row) {
                blockIndicies.push_back(row);
            }

            ui64 blockLen = Min(BuilderLength_, Count_);
            TBlockLess cmp(keyIndicies, *this, arrayIndicies);
            if (BuilderLength_ <= Count_) {
                if (sort) {
                    std::sort(blockIndicies.begin(), blockIndicies.end(), cmp);
                }
            } else {
                if (sort) {
                    NYql::FastPartialSort(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                } else {
                    NYql::FastNthElement(blockIndicies.begin(), blockIndicies.begin() + blockLen, blockIndicies.end(), cmp);
                }
            }

            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                auto& arrayIndex = arrayIndicies[i];
                Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies.data(), blockLen);
            }

            BuilderLength_ = blockLen;
        }

        void SortAll(const TVector<TBlockType*>& columns, const TVector<ui32>& keyIndicies) {
            SortPermutation_.reserve(OutputLength_);
            for (ui64 i = 0; i < OutputLength_; ++i) {
                SortPermutation_.emplace_back(i);
            }

            for (ui32 i = 0; i < columns.size(); ++i) {
                ui64 offset = 0;
                for (const auto& datum : SortInput_[i]) {
                    if (datum.is_scalar()) {
                        continue;
                    } else if (datum.is_array()) {
                        auto arrayData = datum.array();
                        SortArrays_[i].push_back({ arrayData.get(), offset });
                        offset += arrayData->length;
                    } else {
                        auto chunks = datum.chunks();
                        for (auto& chunk : chunks) {
                            auto arrayData = chunk->data();
                            SortArrays_[i].push_back({ arrayData.get(), offset });
                            offset += arrayData->length;
                        }
                    }
                }
            }

            TBlockLess cmp(keyIndicies, *this, SortArrays_);
            std::sort(SortPermutation_.begin(), SortPermutation_.end(), cmp);
        }

        void FillSortOutputPart(const TVector<TBlockType*>& columns, NUdf::TUnboxedValue*const* output, TComputationContext& ctx) {
            auto blockLen = Min(BuilderMaxLength_, OutputLength_ - Written_);
            const bool isLast = (Written_ + blockLen == OutputLength_);

            for (ui32 i = 0; i < columns.size(); ++i) {
                if (!output[i]) {
                    continue;
                }

                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    *output[i] = ScalarValues_[i];
                } else {
                    Builders_[i]->AddMany(SortArrays_[i].data(), SortArrays_[i].size(), SortPermutation_.data() + Written_, blockLen);
                    *output[i] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(isLast)));
                }
            }

            *output[columns.size()] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));

            Written_ += blockLen;
        }

        void FillOutput(const TVector<TBlockType*>& columns, NUdf::TUnboxedValue*const* output, TComputationContext& ctx) {
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (!output[i]) {
                    continue;
                }

                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    *output[i] = ScalarValues_[i];
                } else {
                    *output[i] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(true)));
                }
            }

            *output[columns.size()] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BuilderLength_)));
        }

        void AddTop(const TVector<TBlockType*>& columns, const TMaybe<TVector<ui64>>& blockIndicies, ui64 blockLen) {
            for (ui32 i = 0; i < columns.size(); ++i) {
                if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    continue;
                }

                const auto& datum = TArrowBlock::From(Values_[i]).GetDatum();
                auto arrayIndex = MakeChunkedArrayIndex(datum);
                if (blockIndicies) {
                    Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies->data(), Count_);
                } else {
                    Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), ui64(0), blockLen);
                }
            }

            if (blockIndicies) {
                BuilderLength_ += Count_;
            } else {
                BuilderLength_ += blockLen;
            }
        }
    };

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(KeyIndicies_, Columns_);
        }

        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    static TChunkedArrayIndex MakeChunkedArrayIndex(const arrow::Datum& datum) {
        TChunkedArrayIndex result;
        if (datum.is_array()) {
            result.push_back({datum.array().get(), 0});
        } else {
            auto chunks = datum.chunks();
            ui64 offset = 0;
            for (auto& chunk : chunks) {
                auto arrayData = chunk->data();
                result.push_back({arrayData.get(), offset});
                offset += arrayData->length;
            }
        }
        return result;
    }

    class TBlockLess {
    public:
        TBlockLess(const TVector<ui32>& keyIndicies, const TState& state, const TVector<TChunkedArrayIndex>& arrayIndicies)
            : KeyIndicies_(keyIndicies)
            , ArrayIndicies_(arrayIndicies)
            , State_(state)
        {}

        bool operator()(ui64 lhs, ui64 rhs) const {
            if (KeyIndicies_.size() == 1) {
                auto i = KeyIndicies_[0];
                auto& arrayIndex = ArrayIndicies_[i];
                if (arrayIndex.empty()) {
                    // scalar
                    return false;
                }

                auto leftItem = GetBlockItem(*State_.LeftReaders_[i], arrayIndex, lhs);
                auto rightItem = GetBlockItem(*State_.RightReaders_[i], arrayIndex, rhs);
                if (State_.Directions_[0]) {
                    return State_.Comparators_[0]->Less(leftItem, rightItem);
                } else {
                    return State_.Comparators_[0]->Greater(leftItem, rightItem);
                }
            } else {
                for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                    auto i = KeyIndicies_[k];
                    auto& arrayIndex = ArrayIndicies_[i];
                    if (arrayIndex.empty()) {
                        // scalar
                        continue;
                    }

                    auto leftItem = GetBlockItem(*State_.LeftReaders_[i], arrayIndex, lhs);
                    auto rightItem = GetBlockItem(*State_.RightReaders_[i], arrayIndex, rhs);
                    auto cmp = State_.Comparators_[k]->Compare(leftItem, rightItem);
                    if (cmp == 0) {
                        continue;
                    }

                    if (State_.Directions_[k]) {
                        return cmp < 0;
                    } else {
                        return cmp > 0;
                    }
                }

                return false;
            }
        }

    private:
        static TBlockItem GetBlockItem(IBlockReader& reader, const TChunkedArrayIndex& arrayIndex, ui64 idx) {
            Y_VERIFY_DEBUG(!arrayIndex.empty());
            if (arrayIndex.size() == 1) {
                return reader.GetItem(*arrayIndex.front().Data, idx);
            }

            auto it = LookupArrayDataItem(arrayIndex.data(), arrayIndex.size(), idx);
            return reader.GetItem(*it->Data, idx);
        }

        const TVector<ui32>& KeyIndicies_;
        const TVector<TChunkedArrayIndex> ArrayIndicies_;
        const TState& State_;
    };

    IComputationWideFlowNode* Flow_;
    IComputationNode* Count_;
    const TVector<IComputationNode*> Directions_;
    const TVector<ui32> KeyIndicies_;
    TVector<TBlockType*> Columns_;
};

template <bool Sort, bool HasCount>
IComputationNode* WrapTopOrSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    constexpr ui32 offset = HasCount ? 0 : 1;
    const ui32 inputsWithCount = callable.GetInputsCount() + offset;
    MKQL_ENSURE(inputsWithCount > 2U && !(inputsWithCount % 2U), "Expected more arguments.");

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto wideComponents = GetWideComponents(flowType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    IComputationNode* count = nullptr;
    if constexpr (HasCount) {
        const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
        MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");
        count = LocateNode(ctx.NodeLocator, callable, 1);
    }

    TVector<IComputationNode*> directions;
    TVector<ui32> keyIndicies;
    for (ui32 i = 2; i < inputsWithCount; i += 2) {
        ui32 keyIndex = AS_VALUE(TDataLiteral, callable.GetInput(i - offset))->AsValue().Get<ui32>();
        MKQL_ENSURE(keyIndex + 1 < wideComponents.size(), "Wrong key index");
        keyIndicies.push_back(keyIndex);
        directions.push_back(LocateNode(ctx.NodeLocator, callable, i + 1 - offset));
    }

    return new TTopOrSortBlocksWrapper<Sort, HasCount>(ctx.Mutables, wideFlow, wideComponents, count, std::move(directions), std::move(keyIndicies));
}

} //namespace

IComputationNode* WrapWideTopBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<false, true>(callable, ctx);
}

IComputationNode* WrapWideTopSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, true>(callable, ctx);
}

IComputationNode* WrapWideSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, false>(callable, ctx);
}

}
}
