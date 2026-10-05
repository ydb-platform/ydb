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

namespace NKikimr::NMiniKQL {

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
    bool WritingOutput = false;
    bool IsFinished = false;

    ui64 OutputLength = 0;
    ui64 Written = 0;
    const std::vector<bool> Directions;
    const ui64 TopCount;
    const std::vector<TBlockType*> Columns;
    const std::vector<ui32> KeyIndicies;
    std::vector<std::vector<arrow::Datum>> SortInput;
    std::vector<ui64> SortPermutation;
    std::vector<TChunkedArrayIndex> SortArrays;

    bool ScalarsFilled = false;
    TUnboxedValueVector ScalarValues;
    std::vector<std::unique_ptr<IBlockReader>> LeftReaders;
    std::vector<std::unique_ptr<IBlockReader>> RightReaders;
    std::vector<std::unique_ptr<IArrayBuilder>> Builders;
    ui64 BuilderMaxLength = 0;
    ui64 BuilderLength = 0;
    std::vector<NUdf::IBlockItemComparator::TPtr> Comparators; // by key columns only

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

                auto leftItem = GetBlockItem(*State_.LeftReaders[i], arrayIndex, lhs);
                auto rightItem = GetBlockItem(*State_.RightReaders[i], arrayIndex, rhs);
                if (State_.Directions[0]) {
                    return State_.Comparators[0]->Less(leftItem, rightItem);
                } else {
                    return State_.Comparators[0]->Greater(leftItem, rightItem);
                }
            } else {
                for (ui32 k = 0; k < KeyIndicies_.size(); ++k) {
                    auto i = KeyIndicies_[k];
                    auto& arrayIndex = ArrayIndicies_[i];
                    if (arrayIndex.empty()) {
                        // scalar
                        continue;
                    }

                    auto leftItem = GetBlockItem(*State_.LeftReaders[i], arrayIndex, lhs);
                    auto rightItem = GetBlockItem(*State_.RightReaders[i], arrayIndex, rhs);
                    auto cmp = State_.Comparators[k]->Compare(leftItem, rightItem);
                    if (cmp == 0) {
                        continue;
                    }

                    if (State_.Directions[k]) {
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
        , IsFinished(HasCount && !count)
        , Directions(directions, directions + keyIndicies.size())
        , TopCount(count)
        , Columns(columns)
        , KeyIndicies(keyIndicies)
        , SortInput(Columns.size())
        , SortArrays(Columns.size())
        , ScalarValues(Columns.size())
        , LeftReaders(Columns.size())
        , RightReaders(Columns.size())
        , Builders(Columns.size())
        , Comparators(KeyIndicies.size())
    {
        for (ui32 i = 0; i < Columns.size(); ++i) {
            if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            LeftReaders[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
            RightReaders[i] = MakeBlockReader(TTypeInfoHelper(), columns[i]->GetItemType());
        }

        for (ui32 k = 0; k < KeyIndicies.size(); ++k) {
            Comparators[k] = TBlockTypeHelper().MakeComparator(Columns[KeyIndicies[k]]->GetItemType());
        }

        BuilderMaxLength = GetStorageLength();
        size_t maxBlockItemSize = 0;
        for (auto Column : Columns) {
            if (Column->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            maxBlockItemSize = Max(maxBlockItemSize, CalcMaxBlockItemSize(Column->GetItemType()));
        };

        BuilderMaxLength = Max(BuilderMaxLength, CalcBlockLen(maxBlockItemSize));

        for (ui32 i = 0; i < Columns.size(); ++i) {
            if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            Builders[i] = MakeArrayBuilder(TTypeInfoHelper(), Columns[i]->GetItemType(), ctx.ArrowMemoryPool, BuilderMaxLength, &ctx.Builder->GetPgBuilder());
        }
    }

    void Add(const NUdf::TUnboxedValuePod value, size_t idx) {
        Values[idx] = value;
    }

    void ProcessInput() {
        const ui64 blockLen = TArrowBlock::From(Values.back()).GetDatum().template scalar_as<arrow::UInt64Scalar>().value;

        if (!ScalarsFilled) {
            for (ui32 i = 0; i < Columns.size(); ++i) {
                if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    ScalarValues[i] = std::move(Values[i]);
                }
            }

            ScalarsFilled = true;
        }

        if constexpr (!HasCount) {
            for (ui32 i = 0; i < Columns.size(); ++i) {
                if (Columns[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = TArrowBlock::From(Values[i]).GetDatum();
                    SortInput[i].emplace_back(datum);
                }
            }

            OutputLength += blockLen;
            Values.assign(Values.size(), NUdf::TUnboxedValuePod());
            return;
        }

        // shrink input block
        std::optional<std::vector<ui64>> blockIndicies;
        if (blockLen > TopCount) {
            blockIndicies.emplace();
            blockIndicies->reserve(blockLen);
            for (ui64 row = 0; row < blockLen; ++row) {
                blockIndicies->emplace_back(row);
            }

            std::vector<TChunkedArrayIndex> arrayIndicies(Columns.size());
            for (ui32 i = 0; i < Columns.size(); ++i) {
                if (Columns[i]->GetShape() != TBlockType::EShape::Scalar) {
                    auto datum = TArrowBlock::From(Values[i]).GetDatum();
                    arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                }
            }

            const TBlockLess cmp(KeyIndicies, *this, arrayIndicies);
            NYql::FastNthElement(blockIndicies->begin(), blockIndicies->begin() + TopCount, blockIndicies->end(), cmp);
        }

        // copy all to builders
        AddTop(Columns, blockIndicies, blockLen);
        if (BuilderLength + TopCount > BuilderMaxLength) {
            CompressBuilders(false);
        }

        Values.assign(Values.size(), NUdf::TUnboxedValuePod());
    }

    ui64 GetStorageLength() const {
        return 2 * TopCount;
    }

    void CompressBuilders(bool sort) {
        Y_ABORT_UNLESS(ScalarsFilled);
        std::vector<TChunkedArrayIndex> arrayIndicies(Columns.size());
        std::vector<arrow::Datum> tmpDatums(Columns.size());
        for (ui32 i = 0; i < Columns.size(); ++i) {
            if (Columns[i]->GetShape() != TBlockType::EShape::Scalar) {
                auto datum = Builders[i]->Build(false);
                arrayIndicies[i] = MakeChunkedArrayIndex(datum);
                tmpDatums[i] = std::move(datum);
            }
        }

        std::vector<ui64> blockIndicies;
        blockIndicies.reserve(BuilderLength);
        for (ui64 row = 0; row < BuilderLength; ++row) {
            blockIndicies.push_back(row);
        }

        const ui64 blockLen = Min(BuilderLength, TopCount);
        const TBlockLess cmp(KeyIndicies, *this, arrayIndicies);
        if (BuilderLength <= TopCount) {
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

        for (ui32 i = 0; i < Columns.size(); ++i) {
            if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                continue;
            }

            auto& arrayIndex = arrayIndicies[i];
            Builders[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies.data(), blockLen);
        }

        BuilderLength = blockLen;
    }

    void SortAll() {
        SortPermutation.reserve(OutputLength);
        for (ui64 i = 0; i < OutputLength; ++i) {
            SortPermutation.emplace_back(i);
        }

        for (ui32 i = 0; i < Columns.size(); ++i) {
            ui64 offset = 0;
            for (const auto& datum : SortInput[i]) {
                if (datum.is_scalar()) {
                    continue;
                } else if (datum.is_array()) {
                    auto arrayData = datum.array();
                    SortArrays[i].push_back({arrayData.get(), offset});
                    offset += arrayData->length;
                } else {
                    auto chunks = datum.chunks();
                    for (auto& chunk : chunks) {
                        auto arrayData = chunk->data();
                        SortArrays[i].push_back({arrayData.get(), offset});
                        offset += arrayData->length;
                    }
                }
            }
        }

        TBlockLess cmp(KeyIndicies, *this, SortArrays);
        std::sort(SortPermutation.begin(), SortPermutation.end(), cmp);
    }

    bool FillOutput(const THolderFactory& holderFactory, NYql::EDatumValidationMode validationMode) {
        if (WritingOutput) {
            FillSortOutputPart(holderFactory, validationMode);
        } else if constexpr (!HasCount) {
            if (!OutputLength) {
                IsFinished = true;
                return false;
            }

            SortAll();
            WritingOutput = true;
            FillSortOutputPart(holderFactory, validationMode);
        } else {
            IsFinished = true;
            if (!BuilderLength) {
                return false;
            }

            if (BuilderLength > TopCount || Sort) {
                CompressBuilders(Sort);
            }

            for (ui32 i = 0; i < Columns.size(); ++i) {
                if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                    Values[i] = ScalarValues[i];
                } else {
                    Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders[i]->Build(true)), validationMode);
                }
            }

            Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BuilderLength)), validationMode);
        }
        FillArrays();
        return true;
    }

    void FillSortOutputPart(const THolderFactory& holderFactory, NYql::EDatumValidationMode validationMode) {
        auto blockLen = Min(BuilderMaxLength, OutputLength - Written);
        const bool isLast = (Written + blockLen == OutputLength);

        for (ui32 i = 0; i < Columns.size(); ++i) {
            if (Columns[i]->GetShape() == TBlockType::EShape::Scalar) {
                Values[i] = ScalarValues[i];
            } else {
                Builders[i]->AddMany(SortArrays[i].data(), SortArrays[i].size(), SortPermutation.data() + Written, blockLen);
                Values[i] = holderFactory.CreateArrowBlock(arrow::Datum(Builders[i]->Build(isLast)), validationMode);
            }
        }

        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)), validationMode);
        Written += blockLen;
        if (Written >= OutputLength) {
            IsFinished = true;
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
                Builders[i]->AddMany(arrayIndex.data(), arrayIndex.size(), blockIndicies->data(), TopCount);
            } else {
                Builders[i]->AddMany(arrayIndex.data(), arrayIndex.size(), ui64(0), blockLen);
            }
        }

        if (blockIndicies) {
            BuilderLength += TopCount;
        } else {
            BuilderLength += blockLen;
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
                                                      std::move(Stream_->GetValue(ctx)),
                                                      ctx.RuntimeSettings.DatumValidation.Get());
    }

private:
    class TStreamValue: public TComputationValue<TStreamValue> {
        using TBase = TComputationValue<TStreamValue>;

    public:
        TStreamValue(TMemoryUsageInfo* memInfo,
                     const THolderFactory& holderFactory,
                     NUdf::TUnboxedValue&& blockState,
                     NUdf::TUnboxedValue&& stream,
                     NYql::EDatumValidationMode validationMode)
            : TBase(memInfo)
            , BlockState_(std::move(blockState))
            , Stream_(std::move(stream))
            , HolderFactory_(holderFactory)
            , ValidationMode_(validationMode)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
            auto& blockState = *static_cast<TState*>(BlockState_.AsBoxed().Get());
            Y_DEBUG_ABORT_UNLESS(blockState.Values.size() == width);
            Y_DEBUG_ABORT_UNLESS(blockState.Values.size() == blockState.Columns.size() + 1);
            auto* inputFields = blockState.Pointer;

            if (!blockState.Count) {
                if (blockState.IsFinished) {
                    return NUdf::EFetchStatus::Finish;
                }

                if (!blockState.WritingOutput) {
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

                if (!blockState.FillOutput(HolderFactory_, ValidationMode_)) {
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
        const NYql::EDatumValidationMode ValidationMode_;
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
    MKQL_ENSURE(!wideComponents.empty(), "Expected at least one column");

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

} // namespace NKikimr::NMiniKQL
