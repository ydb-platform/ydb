#include "mkql_block_top.h"

#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>

#include <yql/essentials/public/udf/arrow/block_item_comparator.h>

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <yql/essentials/utils/sort.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

using TChunkedArrayIndex = std::vector<IArrayBuilder::TArrayDataItem>;

TChunkedArrayIndex MakeChunkedArrayIndex(const arrow::Datum& datum) {
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

template <bool Sort, bool HasCount>
class TTopOrSortBlocksState: public TBlockState {
public:
    bool WritingOutput_ = false;
    bool IsFinished_ = false;

    ui64 OutputLength_ = 0;
    ui64 Written_ = 0;
    const std::vector<bool> Directions_;
    const ui64 Count_;
    const std::vector<TBlockType*> Columns_;
    const std::vector<ui32> KeyIndicies_;
    std::vector<std::vector<arrow::Datum>> SortInput_;
    std::vector<ui64> SortPermutation_;
    std::vector<TChunkedArrayIndex> SortArrays_;

    bool ScalarsFilled_ = false;
    TUnboxedValueVector ScalarValues_;
    std::vector<std::unique_ptr<IBlockReader>> LeftReaders_;
    std::vector<std::unique_ptr<IBlockReader>> RightReaders_;
    std::vector<std::unique_ptr<IArrayBuilder>> Builders_;
    ui64 BuilderMaxLength_ = 0;
    ui64 BuilderLength_ = 0;
    std::vector<NUdf::IBlockItemComparator::TPtr> Comparators_; // by key columns only

    class TBlockLess {
    public:
        TBlockLess(const std::vector<ui32>& keyIndicies, const TTopOrSortBlocksState<Sort, HasCount>& state, const std::vector<TChunkedArrayIndex>& arrayIndicies)
            : KeyIndicies_(keyIndicies)
            , ArrayIndicies_(arrayIndicies)
            , State_(state)
        {
        }

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
            Y_DEBUG_ABORT_UNLESS(!arrayIndex.empty());
            if (arrayIndex.size() == 1) {
                return reader.GetItem(*arrayIndex.front().Data, idx);
            }

            auto it = LookupArrayDataItem(arrayIndex.data(), arrayIndex.size(), idx);
            return reader.GetItem(*it->Data, idx);
        }

        const std::vector<ui32>& KeyIndicies_;
        const std::vector<TChunkedArrayIndex> ArrayIndicies_;
        const TTopOrSortBlocksState<Sort, HasCount>& State_;
    };

    TTopOrSortBlocksState(TMemoryUsageInfo* memInfo, TComputationContext& ctx, const std::vector<ui32>& keyIndicies, const std::vector<TBlockType*>& columns, const bool* directions, ui64 count)
        : TBlockState(memInfo, columns.size() + 1U)
        , IsFinished_(HasCount && !count)
        , Directions_(directions, directions + keyIndicies.size())
        , Count_(count)
        , Columns_(columns)
        , KeyIndicies_(keyIndicies)
        , SortInput_(Columns_.size())
        , SortArrays_(Columns_.size())
        , ScalarValues_(Columns_.size())
        , LeftReaders_(Columns_.size())
        , RightReaders_(Columns_.size())
        , Builders_(Columns_.size())
        , Comparators_(KeyIndicies_.size())
    {
        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            LeftReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
            RightReaders_[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
        }

        for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
            Comparators_[k] = TBlockTypeHelper().MakeComparator(Columns_[KeyIndicies_[k]]->GetItemType());
        }

        BuilderMaxLength_ = GetStorageLength();
        size_t maxBlockItemSize = 0;
        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            maxBlockItemSize = Max(maxBlockItemSize, CalcMaxBlockItemSize(Columns_[i]->GetItemType()));
        };

        BuilderMaxLength_ = Max(BuilderMaxLength_, CalcBlockLen(maxBlockItemSize));

        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            Builders_[i] = MakeArrayBuilder(TTypeInfoHelper(), Columns_[i]->GetItemType(), ctx.ArrowMemoryPool, BuilderMaxLength_, &ctx.Builder->GetPgBuilder());
        }
    }

    void Add(const NUdf::TUnboxedValuePod value, size_t idx) {
        Values[idx] = value;
    }

    void ProcessInput() {
        const ui64 blockLen = TArrowBlock::From(Values.back()).GetDatum().template scalar_as<arrow::UInt64Scalar>().value;

        if (!ScalarsFilled_) {
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    ScalarValues_[i] = std::move(Values[i]);
                }
            }

            ScalarsFilled_ = true;
        }

        if constexpr (!HasCount) {
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = TArrowBlock::From(Values[i]).GetDatum();
                    SortInput_[i].emplace_back(datum);
                }
            }

            OutputLength_ += blockLen;
            Values.assign(Values.size(), NUdf::TUnboxedValuePod());
            return;
        }

        // shrink input block
        std::optional<std::vector<ui64>> blockIndicies;
        if (blockLen > Count_) {
            blockIndicies.emplace();
            blockIndicies->reserve(blockLen);
            for (ui64 row = 0; row < blockLen; ++row) {
                blockIndicies->emplace_back(row);
            }

            std::vector<TChunkedArrayIndex> arrayIndicies(Columns_.size());
            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = TArrowBlock::From(Values[i]).GetDatum();
                    arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                }
            }

            const TBlockLess cmp(KeyIndicies_, *this, arrayIndicies);
            NYql::FastNthElement(blockIndicies->begin(), blockIndicies->begin() + Count_, blockIndicies->end(), cmp);
        }

        // copy all to builders
        AddTop(Columns_, blockIndicies, blockLen);
        if (BuilderLength_ + Count_ > BuilderMaxLength_) {
            CompressBuilders(false);
        }

        Values.assign(Values.size(), NUdf::TUnboxedValuePod());
    }

    ui64 GetStorageLength() const {
        return 2 * Count_;
    }

    void CompressBuilders(bool sort) {
        Y_ABORT_UNLESS(ScalarsFilled_);
        std::vector<TChunkedArrayIndex> arrayIndicies(Columns_.size());
        std::vector<arrow::Datum> tmpDatums(Columns_.size());
        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() != TBlockType::EShape::Scalar) {
                auto datum = Builders_[i]->Build(false);
                arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                tmpDatums[i] = std::move(datum);
            }
        }

        std::vector<ui64> blockIndicies;
        blockIndicies.reserve(BuilderLength_);
        for (ui64 row = 0; row < BuilderLength_; ++row) {
            blockIndicies.push_back(row);
        }

        const ui64 blockLen = Min(BuilderLength_, Count_);
        const TBlockLess cmp(KeyIndicies_, *this, arrayIndicies);
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

        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            auto& arrayIndex = arrayIndicies[i];
            Builders_[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies.data(), blockLen);
        }

        BuilderLength_ = blockLen;
    }

    void SortAll() {
        SortPermutation_.reserve(OutputLength_);
        for (ui64 i = 0; i < OutputLength_; ++i) {
            SortPermutation_.emplace_back(i);
        }

        for (ui32 i = 0; i < Columns_.size(); ++i) {
            ui64 offset = 0;
            for (const auto& datum : SortInput_[i]) {
                if (datum.is_scalar()) {
                    continue;
                } else if (datum.is_array()) {
                    auto arrayData = datum.array();
                    SortArrays_[i].push_back({arrayData.get(), offset});
                    offset += arrayData->length;
                } else {
                    auto chunks = datum.chunks();
                    for (auto& chunk : chunks) {
                        auto arrayData = chunk->data();
                        SortArrays_[i].push_back({arrayData.get(), offset});
                        offset += arrayData->length;
                    }
                }
            }
        }

        TBlockLess cmp(KeyIndicies_, *this, SortArrays_);
        std::sort(SortPermutation_.begin(), SortPermutation_.end(), cmp);
    }

    bool FillOutput(const THolderFactory& holderFactory) {
        if (WritingOutput_) {
            FillSortOutputPart(holderFactory);
        } else if constexpr (!HasCount) {
            if (!OutputLength_) {
                IsFinished_ = true;
                return false;
            }

            SortAll();
            WritingOutput_ = true;
            FillSortOutputPart(holderFactory);
        } else {
            IsFinished_ = true;
            if (!BuilderLength_) {
                return false;
            }

            if (BuilderLength_ > Count_ || Sort) {
                CompressBuilders(Sort);
            }

            for (ui32 i = 0; i < Columns_.size(); ++i) {
                if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                    Values[i] = ScalarValues_[i];
                } else {
                    Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(true)));
                }
            }

            Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BuilderLength_)));
        }
        FillArrays();
        return true;
    }

    void FillSortOutputPart(const THolderFactory& holderFactory) {
        auto blockLen = Min(BuilderMaxLength_, OutputLength_ - Written_);
        const bool isLast = (Written_ + blockLen == OutputLength_);

        for (ui32 i = 0; i < Columns_.size(); ++i) {
            if (Columns_[i]->GetShape() == TBlockType::EShape::Scalar) {
                Values[i] = ScalarValues_[i];
            } else {
                Builders_[i]->AddMany(SortArrays_[i].data(), SortArrays_[i].size(), SortPermutation_.data() + Written_, blockLen);
                Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders_[i]->Build(isLast)));
            }
        }

        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
        Written_ += blockLen;
        if (Written_ >= OutputLength_) {
            IsFinished_ = true;
        }
    }

    void AddTop(const std::vector<TBlockType*>& columns, const std::optional<std::vector<ui64>>& blockIndicies, ui64 blockLen) {
        for (ui32 i = 0; i < columns.size(); ++i) {
            if (columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            const auto& datum = TArrowBlock::From(Values[i]).GetDatum();
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

template <bool Sort, bool HasCount>
class TTopOrSortBlocksStreamWrapper: public TMutableComputationNode<TTopOrSortBlocksStreamWrapper<Sort, HasCount>> {
    using TBaseComputation = TMutableComputationNode<TTopOrSortBlocksStreamWrapper>;
    using TState = TTopOrSortBlocksState<Sort, HasCount>;

public:
    TTopOrSortBlocksStreamWrapper(TComputationMutables& mutables,
                                  IComputationNode* stream,
                                  TArrayRef<TType* const> wideComponents,
                                  IComputationNode* count,
                                  TComputationNodePtrVector&& directions,
                                  std::vector<ui32>&& keyIndicies)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
        , Count_(count)
        , Directions_(std::move(directions))
        , KeyIndicies_(std::move(keyIndicies))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(wideComponents.size()))
    {
        for (ui32 i = 0; i < wideComponents.size() - 1; ++i) {
            Columns_.push_back(AS_TYPE(TBlockType, wideComponents[i]));
        }
    }

    NUdf::TUnboxedValue MakeState(TComputationContext& ctx) const {
        std::vector<bool> dirs(Directions_.size());
        std::transform(Directions_.cbegin(), Directions_.cend(), dirs.begin(), [&ctx](IComputationNode* dir) { return dir->GetValue(ctx).Get<bool>(); });
        if constexpr (HasCount) {
            return ctx.HolderFactory.Create<TState>(ctx, KeyIndicies_, Columns_, dirs.data(), Count_->GetValue(ctx).Get<ui64>());
        } else {
            return ctx.HolderFactory.Create<TState>(ctx, KeyIndicies_, Columns_, dirs.data(), 0);
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto state = MakeState(ctx);
        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(state),
                                                      std::move(Stream_->GetValue(ctx)));
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo,
                     const THolderFactory& holderFactory,
                     NUdf::TUnboxedValue&& blockState,
                     NUdf::TUnboxedValue&& stream)
            : TBase(memInfo)
            , BlockState_(std::move(blockState))
            , Stream_(std::move(stream))
            , HolderFactory_(holderFactory)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
            Y_DEBUG_ABORT_UNLESS(blockState.Values.size() == width);
            Y_DEBUG_ABORT_UNLESS(blockState.Values.size() == blockState.Columns_.size() + 1);
            auto* inputFields = blockState.Pointer;

            if (!blockState.Count) {
                if (blockState.IsFinished_) {
                    return NUdf::EFetchStatus::Finish;
                }

                if (!blockState.WritingOutput_) {
                    while (true) {
                        switch (Stream_.WideFetch(inputFields, width)) {
                            case NUdf::EFetchStatus::Yield:
                                return NUdf::EFetchStatus::Yield;
                            case NUdf::EFetchStatus::Ok:
                                blockState.ProcessInput();
                                continue;
                            case NUdf::EFetchStatus::Finish:
                                break;
                        }
                        break;
                    }
                }

                if (!blockState.FillOutput(HolderFactory_)) {
                    return NUdf::EFetchStatus::Finish;
                }
            }

            const auto sliceSize = blockState.Slice();
            for (size_t i = 0; i < width; ++i) {
                output[i] = blockState.Get(sliceSize, HolderFactory_, i);
            }
            return NUdf::EFetchStatus::Ok;
        }

        NUdf::TUnboxedValue BlockState_;
        NUdf::TUnboxedValue Stream_;
        const THolderFactory& HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        this->DependsOn(Count_);
        for (auto dir : Directions_) {
            this->DependsOn(dir);
        }
    }

    IComputationNode* const Stream_;
    IComputationNode* const Count_;
    const TComputationNodePtrVector Directions_;
    const std::vector<ui32> KeyIndicies_;
    std::vector<TBlockType*> Columns_;
    const size_t WideFieldsIndex_;
};

template <bool Sort, bool HasCount>
IComputationNode* WrapTopOrSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    constexpr ui32 offset = HasCount ? 0 : 1;
    const ui32 inputsWithCount = callable.GetInputsCount() + offset;
    MKQL_ENSURE(inputsWithCount > 2U && !(inputsWithCount % 2U), "Expected more arguments.");
    const TType* const inputType = callable.GetInput(0).GetStaticType();

    MKQL_ENSURE(inputType->IsStream(), "Expected WideStream as an input");

    const auto wideComponents = GetWideComponents(inputType);
    MKQL_ENSURE(wideComponents.size() > 0, "Expected at least one column");

    auto node = LocateNode(ctx.NodeLocator, callable, 0);

    IComputationNode* count = nullptr;
    if constexpr (HasCount) {
        const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
        MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");
        count = LocateNode(ctx.NodeLocator, callable, 1);
    }

    TComputationNodePtrVector directions;
    std::vector<ui32> keyIndicies;
    for (ui32 i = 2; i < inputsWithCount; i += 2) {
        ui32 keyIndex = AS_VALUE(TDataLiteral, callable.GetInput(i - offset))->AsValue().Get<ui32>();
        MKQL_ENSURE(keyIndex + 1 < wideComponents.size(), "Wrong key index");
        keyIndicies.push_back(keyIndex);
        directions.push_back(LocateNode(ctx.NodeLocator, callable, i + 1 - offset));
    }

    return new TTopOrSortBlocksStreamWrapper<Sort, HasCount>(ctx.Mutables, node, wideComponents, count, std::move(directions), std::move(keyIndicies));
}

} // namespace

IComputationNode* WrapWideTopBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<false, true>(callable, ctx);
}

IComputationNode* WrapWideTopSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, true>(callable, ctx);
}

IComputationNode* WrapWideSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapTopOrSort<true, false>(callable, ctx);
}

} // namespace NMiniKQL
} // namespace NKikimr
