#include "mkql_block_grace_join.h"

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/block_layout_converter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_resource_meter.h>
#include <yql/essentials/minikql/computation/mkql_vector_spiller_adapter.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_block_grace_join_policy.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/accumulator.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/cardinality.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/neumann_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/page_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/robin_hood_table.h>

#include <util/generic/serialized_enum.h>
#include <util/digest/numeric.h>

#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/arrow/block_item_hasher.h>

#include <arrow/array/data.h>
#include <arrow/datum.h>

#include <chrono>
#include <optional>

namespace NKikimr::NMiniKQL {

namespace {

using namespace std::chrono_literals;
using THash = ui64;

// -------------------------------------------------------------------
[[maybe_unused]] constexpr size_t KB = 1024;
[[maybe_unused]] constexpr size_t MB = KB * KB;
[[maybe_unused]] constexpr size_t L1_CACHE_SIZE = 32 * KB;
[[maybe_unused]] constexpr size_t L2_CACHE_SIZE = 256 * KB;
[[maybe_unused]] constexpr size_t L3_CACHE_SIZE = 16 * MB;

// -------------------------------------------------------------------
TDefaultBlockGraceJoinPolicy globalDefaultPolicy{};

// -------------------------------------------------------------------
size_t CalcMaxBlockLength(const TVector<TType*>& items, bool isBlockType = true) {
    return CalcBlockLen(std::accumulate(items.cbegin(), items.cend(), 0ULL,
        [isBlockType](size_t max, const TType* type) {
            if (isBlockType) {
                const TType* itemType = AS_TYPE(TBlockType, type)->GetItemType();
                return std::max(max, CalcMaxBlockItemSize(itemType));
            } else {
                return std::max(max, CalcMaxBlockItemSize(type));
            }
        }));
}

THash CalculateTupleHash(const TVector<THash>& hashes) {
    THash hash = 0;
    for (size_t i = 0; i < hashes.size(); i++) {
        if (!hashes[i]) {
            return 0;
        }
        hash = CombineHashes(hash, hashes[i]);
    }
    return hash;
}

// -------------------------------------------------------------------
using TRobinHoodTable = NPackedTuple::TRobinHoodHashBase<true>;
using TNeumannTable = NPackedTuple::TNeumannHashTable<false>;
using TPageTable = NPackedTuple::TPageHashTableImpl<NSimd::TSimdSSE42Traits>;

size_t CalculateExpectedOverflowSize(const NPackedTuple::TTupleLayout* layout, size_t nTuples) {
    size_t varSizedCount = 0;
    for (const auto& column: layout->Columns) {
        if (column.SizeType == NPackedTuple::EColumnSizeType::Variable) {
            varSizedCount++;
        }
    }

    if (varSizedCount == 0) {
        return 0;
    }

    // Some weird heuristic.
    // Lets expect that there will be no more than 10% of var sized values with length
    // bigger than 64 bytes.
    return varSizedCount * nTuples * 64 / 10;
}

// -------------------------------------------------------------------
// Class is used as temporary storage for join algorithm quick start.
// Quick start is stage of hash join algo when join compute node is trying to
// fetch some data from left and right stream and decide what to do: start grace hash join or
// hash join.
// Also this class collects some initial statistics about data, like sizes and cardinality.
class TTempJoinStorage : public TComputationValue<TTempJoinStorage> {
private:
    using TBase = TComputationValue<TTempJoinStorage>;
    using THasherPtr = NYql::NUdf::IBlockItemHasher::TPtr;
    using TReaderPtr = std::unique_ptr<IBlockReader>;

public:
    // Fetched block
    struct TBlock {
        size_t Size; // count of elements in one column
        TVector<arrow::Datum> Columns;

        TBlock() = default;
        TBlock(size_t size, TVector<arrow::Datum>&& columns)
            : Size(size)
            , Columns(std::move(columns))
        {}
    };

public:
    TTempJoinStorage(
        TMemoryUsageInfo*       memInfo,
        const TVector<TType*>&  leftItemTypesArg,
        const TVector<ui32>&    leftKeyColumns,
        NUdf::TUnboxedValue     leftStream,
        const TVector<TType*>&  rightItemTypesArg,
        const TVector<ui32>&    rightKeyColumns,
        NUdf::TUnboxedValue     rightStream,
        IBlockGraceJoinPolicy*  policy,
        arrow::MemoryPool*      pool
    )
        : TBase(memInfo)
        , LeftStream_(leftStream)
        , LeftInputs_(leftItemTypesArg.size())
        , LeftKeyColumns_(leftKeyColumns)
        , RightStream_(rightStream)
        , RightInputs_(rightItemTypesArg.size())
        , RightKeyColumns_(rightKeyColumns)
        , Policy_(policy)
    {
        TBlockTypeHelper helper;
        TVector<TType*> leftItemTypes;
        for (size_t i = 0; i < leftItemTypesArg.size() - 1; i++) { // ignore last column, because this is block size
            TType* blockItemType = AS_TYPE(TBlockType, leftItemTypesArg[i])->GetItemType();
            leftItemTypes.push_back(blockItemType);
            LeftHashers_.push_back(helper.MakeHasher(blockItemType));
            LeftReaders_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
        }
        TVector<NPackedTuple::EColumnRole> leftRoles(LeftInputs_.size() - 1, NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: leftKeyColumns) {
            leftRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        LeftConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), leftItemTypes, leftRoles, pool);

        TVector<TType*> rightItemTypes;
        for (size_t i = 0; i < rightItemTypesArg.size() - 1; i++) { // ignore last column, because this is block size
            TType* blockItemType = AS_TYPE(TBlockType, rightItemTypesArg[i])->GetItemType();
            rightItemTypes.push_back(blockItemType);
            RightHashers_.push_back(helper.MakeHasher(blockItemType));
            RightReaders_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
        }
        TVector<NPackedTuple::EColumnRole> rightRoles(RightInputs_.size() - 1, NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: rightKeyColumns) {
            rightRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        RightConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), rightItemTypes, rightRoles, pool);
    }

    NUdf::EFetchStatus FetchStreams() {
        auto maxFetchedSize = Policy_->GetMaximumInitiallyFetchedData();

        /// TODO: include overflow in estimated size calculation

        auto resultLeft = NUdf::EFetchStatus::Finish;
        if (!LeftIsFinished_ && LeftEstimatedSize_ < maxFetchedSize) {
            resultLeft = LeftStream_.WideFetch(LeftInputs_.data(), LeftInputs_.size());
            if (resultLeft == NUdf::EFetchStatus::Ok) {
                TBlock leftBlock;
                ExtractBlock(LeftInputs_, leftBlock);
                LeftEstimatedSize_ += EstimateBlockSize(leftBlock, LeftConverter_->GetTupleLayout());
                LeftFetchedTuples_ += leftBlock.Size;
                SampleBlock(leftBlock, LeftKeyColumns_, LeftHashers_, LeftReaders_, LeftSamples_);
                LeftData_.push_back(std::move(leftBlock));
            } else if (resultLeft == NUdf::EFetchStatus::Finish) {
                LeftIsFinished_ = true;
            }
        }
        
        auto resultRight = NUdf::EFetchStatus::Finish;
        if (!RightIsFinished_ && RightEstimatedSize_ < maxFetchedSize) {
            resultRight = RightStream_.WideFetch(RightInputs_.data(), RightInputs_.size());
            if (resultRight == NUdf::EFetchStatus::Ok) {
                TBlock rightBlock;
                ExtractBlock(RightInputs_, rightBlock);
                RightEstimatedSize_ += EstimateBlockSize(rightBlock, RightConverter_->GetTupleLayout());
                RightFetchedTuples_ += rightBlock.Size;
                SampleBlock(rightBlock, RightKeyColumns_, RightHashers_, RightReaders_, RightSamples_);
                RightData_.push_back(std::move(rightBlock));
            } else if (resultRight == NUdf::EFetchStatus::Finish) {
                RightIsFinished_ = true;
            }
        }

        if (resultLeft == NUdf::EFetchStatus::Yield || resultRight == NUdf::EFetchStatus::Yield) {
            return NUdf::EFetchStatus::Yield;
        }
        return NUdf::EFetchStatus::Finish; // Finish here doesn't mean that there is nothing to fetch anymore
    }

    bool IsReady() {
        const auto maxFetchedSize = Policy_->GetMaximumInitiallyFetchedData();
        
        const bool leftReady = LeftIsFinished_ || LeftEstimatedSize_ > maxFetchedSize;
        const bool rightReady = RightIsFinished_ || RightEstimatedSize_ > maxFetchedSize;
        return leftReady & rightReady;
    }

    std::pair<size_t, size_t> GetFetchedTuples() const {
        return {LeftFetchedTuples_, RightFetchedTuples_};
    }

    std::pair<size_t, size_t> GetPayloadSizes() const {
        return {
            LeftConverter_->GetTupleLayout()->PayloadSize,
            RightConverter_->GetTupleLayout()->PayloadSize};
    }

    // This estimation is rough and depends on selectivity, so use it as a bootstrap
    ui64 EstimateCardinality() const {
        using std::max;

        // TODO: change this values to stream sizes given from optimizer
        auto [lTuples, rTuples] = GetFetchedTuples();
        // Another weird heuristic to get number of buckets for cardinality estimation
        auto buckets = max<ui64>(max<ui64>(lTuples, rTuples) / 2000, 1); // 1/20 (5%) * 1/100 (step) -> 1/2000
        NPackedTuple::CardinalityEstimator estimator{buckets};
        return estimator.Estimate(lTuples, LeftSamples_, rTuples, RightSamples_);
    }

    std::pair<bool, bool> IsFinished() const {
        return {LeftIsFinished_, RightIsFinished_};
    }

    // After the method is called FetchStreams cannot be called anymore
    std::pair<TDeque<TBlock>, TDeque<TBlock>> DetachData() {
        return {std::move(LeftData_), std::move(RightData_)};
    }

private:
    // Extract block from TUnboxedValueVector
    void ExtractBlock(const TUnboxedValueVector& input, TBlock& block) {
        TVector<arrow::Datum> blockColumns;
        for (size_t i = 0; i < input.size() - 1; i++) {
            auto& datum = TArrowBlock::From(input[i]).GetDatum();
            blockColumns.push_back(datum.array());
        }
        auto blockSize = ::GetBlockCount(input[input.size() - 1]);
        block.Size = blockSize;
        block.Columns = std::move(blockColumns);
    }

    // Calculate block size in tuple layout to estimate memory consumption for hash table
    size_t EstimateBlockSize(const TBlock& block, const NPackedTuple::TTupleLayout* layout) {
        return block.Size * layout->TotalRowSize;
    }

    // Make and save hashes of given block samples to estimate Join cardinality
    // Step should be large enough to not affect performance
    void SampleBlock(
        const TBlock& block, const TVector<ui32>& keyColumns,
        TVector<THasherPtr>& hashers, TVector<TReaderPtr>& readers,
        TVector<THash>& samples, size_t step = 100)
    {
        TVector<THash> hashes(keyColumns.size());
        for (size_t i = 0; i < block.Size; i += step) {
            for (size_t j = 0; j < keyColumns.size(); ++j) {
                auto col = keyColumns[j];
                const auto& reader = readers[col];
                const auto& hasher = hashers[col];
                const auto& array = block.Columns[col].array();
                hashes[j] = hasher->Hash(reader->GetItem(*array, i));
            }
            auto keyHash = CalculateTupleHash(hashes);
            samples.push_back(keyHash);
        }
    }

private:
    NUdf::TUnboxedValue LeftStream_;
    TUnboxedValueVector LeftInputs_;
    TVector<ui32>       LeftKeyColumns_;
    TDeque<TBlock>      LeftData_;
    size_t              LeftFetchedTuples_{0}; // count of fetched tuples
    size_t              LeftEstimatedSize_{0}; // size in tuple layout represenation
    bool                LeftIsFinished_{false};
    IBlockLayoutConverter::TPtr LeftConverter_; // Converters here are used only for size estimation via info in TupleLayout class
    TVector<THash>      LeftSamples_; // Samples for cardinality estimation
    TVector<THasherPtr> LeftHashers_; // Hashers to calculate hash of block's key items
    TVector<TReaderPtr> LeftReaders_; // Readers to read blcok's keu items

    NUdf::TUnboxedValue RightStream_;
    TUnboxedValueVector RightInputs_;
    TVector<ui32>       RightKeyColumns_;
    TDeque<TBlock>      RightData_;
    size_t              RightFetchedTuples_{0}; // count of fetched tuples
    size_t              RightEstimatedSize_{0}; // size in tuple layout represenation
    bool                RightIsFinished_{false};
    IBlockLayoutConverter::TPtr RightConverter_;
    TVector<THash>      RightSamples_;
    TVector<THasherPtr> RightHashers_;
    TVector<TReaderPtr> RightReaders_;

    IBlockGraceJoinPolicy*  Policy_;
};

// -------------------------------------------------------------------
// This is storage for payload columns used when payload part of a tuple is big.
// So we don't want to carry this useless data during conversion and join algorithm.
// This storage can save some block and restore payload by index array.
class TExternalPayloadStorage : public TComputationValue<TExternalPayloadStorage> {
    private:
        using TBase = TComputationValue<TExternalPayloadStorage>;
        using TBlock = TTempJoinStorage::TBlock;

    public:
        TExternalPayloadStorage(
            TMemoryUsageInfo*       memInfo,
            TComputationContext&    ctx,
            const TVector<TType*>&  payloadItemTypes,
            bool                    nonClearable = false // if true Clear() method will do nothing. Used for build-stream storage
        )
            : TBase(memInfo)
            , NonClearable_(nonClearable)
        {
            const auto& pgBuilder = ctx.Builder->GetPgBuilder();
            // WARNING: we can not properly track the number of output rows due to uninterruptible for loop in DoBatchLookup,
            // so add some heuristic to prevent overflow in AddMany builder's method.
            auto maxBlockLen = CalcMaxBlockLength(payloadItemTypes, false) * 2;

            for (size_t i = 0; i < payloadItemTypes.size(); i++) {
                Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), payloadItemTypes[i]));
                // FIXME: monitor amount of allocated memory like in BlockMapJoin to prevent overflow
                Builders_.push_back(MakeArrayBuilder(
                    TTypeInfoHelper(), payloadItemTypes[i], ctx.ArrowMemoryPool, maxBlockLen, &pgBuilder));
            }

            // Init indirection indexes datum only once
            auto ui64Type = ctx.TypeEnv.GetUi64Lazy();
            auto maxBufferSize = CalcBlockLen(CalcMaxBlockItemSize(ui64Type));
            std::shared_ptr<arrow::DataType> type;
            ConvertArrowType(ui64Type, type);
            std::shared_ptr<arrow::Buffer> nullBitmap;
            auto dataBuffer = NUdf::AllocateResizableBuffer(sizeof(ui64) * maxBufferSize, &ctx.ArrowMemoryPool);
            IndirectionIndexes = arrow::ArrayData::Make(std::move(type), maxBufferSize, {std::move(nullBitmap), std::move(dataBuffer)});
        }

        ui32 Size() const {
            return PayloadColumnsStorage_.size();
        }

        void AddBlock(TBlock&& block) {
            PayloadColumnsStorage_.push_back(std::move(block));
        }

        void Clear() {
            if (NonClearable_) {
                return;
            }
            PayloadColumnsStorage_.clear();
        }

        TVector<arrow::Datum> RestorePayload(const arrow::Datum& indexes, ui32 length) {
            auto rawIndexes = indexes.array()->GetMutableValues<ui64>(1);

            TVector<arrow::Datum> result;
            for (size_t i = 0; i < Builders_.size(); ++i) {
                auto& builder = Builders_[i];
                auto& reader = Readers_[i];

                for (size_t j = 0; j < length; ++j) {
                    auto blockIndex = static_cast<ui32>(rawIndexes[j] >> 32);
                    auto elemIndex = static_cast<ui32>(rawIndexes[j] & 0xFFFFFFFF);

                    const auto& array = PayloadColumnsStorage_[blockIndex].Columns[i].array();
                    builder->Add(reader->GetItem(*array, elemIndex));
                }

                result.push_back(builder->Build(false));
            }

            return result;
        }

        // Split block on two blocks
        // Lhs contains all key columns and indirection index, rhs contains all payload columns
        static std::pair<TBlock, TBlock> SplitBlock(
            const TBlock& block, TExternalPayloadStorage& payloadStorage, const THashSet<ui32>& keyColumnsSet)
        {
            TBlock keyBlock;
            TBlock payloadBlock;
            for (size_t i = 0; i < block.Columns.size(); ++i) {
                const auto& datum = block.Columns[i];
                if (keyColumnsSet.contains(i)) {
                    keyBlock.Columns.push_back(datum.array());
                } else {
                    payloadBlock.Columns.push_back(datum.array());
                }
            }
            keyBlock.Size = block.Size;
            payloadBlock.Size = block.Size;

            // Init index column
            auto* rawDataBuffer = payloadStorage.IndirectionIndexes.array()->GetMutableValues<ui64>(1);
            ui32 blockIndex = payloadStorage.Size();
            for (size_t i = 0; i < keyBlock.Size; ++i) {
                rawDataBuffer[i] = (static_cast<ui64>(blockIndex) << 32) | i; // indirected index column has such layout: 32 higher bits for block number and 32 bits for offset in block
            }
            payloadStorage.IndirectionIndexes.array()->length = keyBlock.Size;

            // Add index column to fetched key block
            keyBlock.Columns.push_back(payloadStorage.IndirectionIndexes);

            return {std::move(keyBlock), std::move(payloadBlock)};
        }

    public:
        arrow::Datum IndirectionIndexes;

    private:
        TVector<TBlock> PayloadColumnsStorage_;
        TVector<std::unique_ptr<IBlockReader>> Readers_;
        TVector<std::unique_ptr<IArrayBuilder>> Builders_;
        bool NonClearable_;
    };

    void MakeTupleConverter(TComputationContext &ctx, bool externalPayload,
                            const TVector<TType *> &itemTypesArg,
                            const THashSet<ui32> &keyColumnsSet,
                            TVector<TType *> *itemTypes,
                            NUdf::TUnboxedValue *externalPayloadStorage,
                            IBlockLayoutConverter::TPtr *converter) {
      itemTypes->clear();
      if (externalPayload) {
        // split types on two lists: key and payload
        TVector<TType *> leftPayloadItemTypes;
        for (size_t i = 0; i < itemTypesArg.size() - 1; i++) {
          if (keyColumnsSet.contains(i)) {
            itemTypes->push_back(
                AS_TYPE(TBlockType, itemTypesArg[i])->GetItemType());
          } else {
            leftPayloadItemTypes.push_back(
                AS_TYPE(TBlockType, itemTypesArg[i])->GetItemType());
          }
        }

        // add indirection index column as payload column to converter
        TType* ui64Type = ctx.TypeEnv.GetUi64Lazy();
        itemTypes->push_back(ui64Type);

        // create external payload storage for payload columns
        *externalPayloadStorage =
            ctx.HolderFactory.Create<TExternalPayloadStorage>(
                ctx, leftPayloadItemTypes, true);
      } else {
        for (size_t i = 0; i < itemTypesArg.size() - 1;
             i++) { // ignore last column, because this is block size
          itemTypes->push_back(
              AS_TYPE(TBlockType, itemTypesArg[i])->GetItemType());
        }
      }
      TVector<NPackedTuple::EColumnRole> leftRoles(
          itemTypes->size(), NPackedTuple::EColumnRole::Payload);
      for (auto keyCol : keyColumnsSet) {
        leftRoles[keyCol] = NPackedTuple::EColumnRole::Key;
      }
      *converter = MakeBlockLayoutConverter(TTypeInfoHelper(), *itemTypes,
                                           leftRoles, &ctx.ArrowMemoryPool);
    }

// -------------------------------------------------------------------
// State of joined output.
struct TJoinState : public TBlockState {
public:
    TJoinState(
        TMemoryUsageInfo*           memInfo,
        const TVector<TType*>*      resultItemTypes,
        IBlockLayoutConverter*      buildConverter,
        IBlockLayoutConverter*      probeConverter,
        const TVector<ui32>&        leftIOMap,
        const TVector<ui32>&        rightIOMap,
        TExternalPayloadStorage*    buildPayloadStorage, // can be nullptr
        TExternalPayloadStorage*    probePayloadStorage, // can be nullptr
        bool                        wasSwapped
    )
        : TBlockState(memInfo, resultItemTypes->size())
        , MaxLength_(CalcMaxBlockLength(*resultItemTypes))
        , WasSwapped_(wasSwapped)
        , LeftIOMap_(leftIOMap)
        , RightIOMap_(rightIOMap)
    {
        LeftPackedTuple_ = &BuildPackedOutput;
        LeftOverflow_ = &BuildPackedInput.Overflow;
        LeftConverter_ = buildConverter;
        LeftPayloadStorage_ = buildPayloadStorage;

        RightPackedTuple_ = &ProbePackedOutput;
        RightOverflow_ = &ProbePackedInput.Overflow;
        RightConverter_ = probeConverter;
        RightPayloadStorage_ = probePayloadStorage;

        // Check if was swapped.
        // If was not swapped, left stream is build and right is probe
        if (wasSwapped) {
            using std::swap;
            swap(LeftPackedTuple_, RightPackedTuple_);
            swap(LeftOverflow_, RightOverflow_);
            swap(LeftConverter_, RightConverter_);
            swap(LeftPayloadStorage_, RightPayloadStorage_);
        }
    }

    bool GetSwapped() const {
        return WasSwapped_;
    }

    void SetSwapped(bool wasSwapped) {
        if (wasSwapped != WasSwapped_) {
            using std::swap;
            swap(LeftPackedTuple_, RightPackedTuple_);
            swap(LeftOverflow_, RightOverflow_);
            WasSwapped_ = wasSwapped;
        }
    }

    void MakeBlocks(const THolderFactory& holderFactory) {
        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(OutputRows)));

        size_t index = 0;
        IBlockLayoutConverter::TPackResult leftPackResult{std::move(*LeftPackedTuple_), std::move(*LeftOverflow_), OutputRows};
        TVector<arrow::Datum> leftColumns;
        LeftConverter_->Unpack(leftPackResult, leftColumns);
        if (LeftPayloadStorage_) {
            auto payload = LeftPayloadStorage_->RestorePayload(leftColumns.back(), OutputRows);
            leftColumns.pop_back();
            leftColumns.insert(leftColumns.end(), payload.begin(), payload.end());
        }
        for (size_t i = 0; i < LeftIOMap_.size(); i++, index++) {
            Values[index] = holderFactory.CreateArrowBlock(std::move(leftColumns[LeftIOMap_[i]]));
        }

        IBlockLayoutConverter::TPackResult rightPackResult{std::move(*RightPackedTuple_), std::move(*RightOverflow_), OutputRows};
        TVector<arrow::Datum> rightColumns;
        RightConverter_->Unpack(rightPackResult, rightColumns);
        if (RightPayloadStorage_) {
            auto payload = RightPayloadStorage_->RestorePayload(rightColumns.back(), OutputRows);
            rightColumns.pop_back();
            rightColumns.insert(rightColumns.end(), payload.begin(), payload.end());
        }
        for (size_t i = 0; i < RightIOMap_.size(); i++, index++) {
            Values[index] = holderFactory.CreateArrowBlock(std::move(rightColumns[RightIOMap_[i]]));
        }

        FillArrays();
        // Move values back from packed view
        *LeftPackedTuple_ = std::move(leftPackResult.PackedTuples);
        *LeftOverflow_ = std::move(leftPackResult.Overflow);
        *RightPackedTuple_ = std::move(rightPackResult.PackedTuples);
        *RightOverflow_ = std::move(rightPackResult.Overflow);
    }

    bool IsNotFull() const {
        // WARNING: we can not properly track the number of output rows due to uninterruptible for loop in DoBatchLookup,
        // so add some heuristic to prevent overflow in AddMany builder's method.
        return OutputRows * 5 < MaxLength_ * 4;
    }

    bool HasEnoughMemory() const {
        /// TODO: vector size probably should be compared with extrenal parameter
        ///       not with its own capacity
        return ProbePackedInput.Overflow.capacity() == 0 ||
               ProbePackedInput.Overflow.size() * 5 <
                   ProbePackedInput.Overflow.capacity() * 4;
    }

    bool HasBlocks() const {
        return Count > 0;
    }

    void ResetBuildInput() {
        BuildPackedInput.PackedTuples.clear();
        BuildPackedInput.Overflow.clear();
        BuildPackedInput.NTuples = 0;
    }

    void ResetProbeInput() {
        ProbePackedInput.PackedTuples.clear();
        ProbePackedInput.Overflow.clear();
        ProbePackedInput.NTuples = 0;
        // Do not clear build input, because it is constant for all DoProbe calls
        if (LeftPayloadStorage_) {
            LeftPayloadStorage_->Clear();
        }
        if (RightPayloadStorage_) {
            RightPayloadStorage_->Clear();
        }
    }

    void ResetOutput() {
        OutputRows = 0;
        BuildPackedOutput.clear();
        ProbePackedOutput.clear();
    }

public:
    IBlockLayoutConverter::TPackResult BuildPackedInput;   // converted data right after fetch
    IBlockLayoutConverter::TPackResult ProbePackedInput;

    IBlockLayoutConverter::TPackedTuple BuildPackedOutput;   // packed output after join operation
    IBlockLayoutConverter::TPackedTuple ProbePackedOutput;

    ui32 OutputRows{0};

private:
    ui32 MaxLength_{0};
    bool WasSwapped_;

    IBlockLayoutConverter*                  LeftConverter_;
    IBlockLayoutConverter::TPackedTuple*    LeftPackedTuple_;
    IBlockLayoutConverter::TOverflow*       LeftOverflow_;
    const TVector<ui32>&                    LeftIOMap_;
    TExternalPayloadStorage*                LeftPayloadStorage_; // can be nullptr

    IBlockLayoutConverter*                  RightConverter_;
    IBlockLayoutConverter::TPackedTuple*    RightPackedTuple_;
    IBlockLayoutConverter::TOverflow*       RightOverflow_;
    const TVector<ui32>&                    RightIOMap_;
    TExternalPayloadStorage*                RightPayloadStorage_; // can be nullptr
};

// -------------------------------------------------------------------
class THashJoin : public TComputationValue<THashJoin> {
private:
    using TBase = TComputationValue<THashJoin>;
    using TBlock = TTempJoinStorage::TBlock;
    using TTable = TNeumannTable; // According to benchmarks it is always better to use Neumann HT in HashJoin, due to small build size

public:
    THashJoin(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const char *            joinName,
        const TVector<TType*>*  resultItemTypes,
        NUdf::TUnboxedValue*    leftStream,
        const TVector<TType*>*  leftItemTypesArg,
        const TVector<ui32>*    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        NUdf::TUnboxedValue*    rightStream,
        const TVector<TType*>*  rightItemTypesArg,
        const TVector<ui32>*    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        IBlockGraceJoinPolicy*  policy,
        NUdf::TUnboxedValue     tempStorageValue
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , JoinName_(joinName)
        , ResultItemTypes_(resultItemTypes)
    {
        using EJoinAlgo = IBlockGraceJoinPolicy::EJoinAlgo;

        auto& tempStorage = *static_cast<TTempJoinStorage*>(tempStorageValue.AsBoxed().Get());
        auto [leftFetchedTuples, rightFetchedTuples] = tempStorage.GetFetchedTuples();
        auto [leftPSz, rightPSz] = tempStorage.GetPayloadSizes();
        auto cardinality = tempStorage.EstimateCardinality(); // bootstrap value, may be far from truth
        auto [isLeftFinished, isRightFinished] = tempStorage.IsFinished();
        auto [leftData, rightData] = tempStorage.DetachData();
        bool wasSwapped = false;
        // assume that finished stream has less size than unfinished
        if ((!isLeftFinished && isRightFinished) ||
            (isLeftFinished && isRightFinished && (leftFetchedTuples > rightFetchedTuples)))
        {
            using std::swap;
            swap(leftStream, rightStream); // so swap them
            swap(leftData, rightData);
            swap(leftItemTypesArg, rightItemTypesArg);
            swap(leftKeyColumns, rightKeyColumns);
            swap(leftPSz, rightPSz);
            swap(leftFetchedTuples, rightFetchedTuples);
            wasSwapped = true;
        }

        BuildData_ = std::move(leftData);
        BuildKeyColumns_ = leftKeyColumns;
        BuildKeyColumnsSet_ = THashSet<ui32>(BuildKeyColumns_->begin(), BuildKeyColumns_->end());
        // Use or not external payload depends on the policy
        IsBuildIndirected_ = policy->UseExternalPayload(
            EJoinAlgo::HashJoin, leftPSz, leftFetchedTuples / cardinality);

        ProbeStream_ = *rightStream;
        ProbeData_ = std::move(rightData);
        ProbeKeyColumns_ = rightKeyColumns;
        ProbeInputs_.resize(rightItemTypesArg->size());
        ProbeKeyColumnsSet_ = THashSet<ui32>(ProbeKeyColumns_->begin(), ProbeKeyColumns_->end());
        // Use or not external payload depends on the policy
        IsProbeIndirected_ = policy->UseExternalPayload(
            EJoinAlgo::HashJoin, rightPSz, rightFetchedTuples / cardinality);

        // Create converters
        TVector<TType*> leftItemTypes;
        MakeTupleConverter(ctx, IsBuildIndirected_, *leftItemTypesArg, BuildKeyColumnsSet_, &leftItemTypes, &BuildExternalPayloadStorage_, &BuildConverter_);

        TVector<TType*> rightItemTypes;
        MakeTupleConverter(ctx, IsProbeIndirected_, *rightItemTypesArg, ProbeKeyColumnsSet_, &rightItemTypes, &ProbeExternalPayloadStorage_, &ProbeConverter_);

        Table_.SetTupleLayout(BuildConverter_->GetTupleLayout());

        // Prepare pointers to external payload storage for Join state
        auto buildPayloadStorage = IsBuildIndirected_ ? static_cast<TExternalPayloadStorage*>(BuildExternalPayloadStorage_.AsBoxed().Get()) : nullptr;
        auto probePayloadStorage = IsProbeIndirected_ ? static_cast<TExternalPayloadStorage*>(ProbeExternalPayloadStorage_.AsBoxed().Get()) : nullptr;

        // Create inner hash join state
        JoinState_ = Ctx_.HolderFactory.Create<TJoinState>(
            ResultItemTypes_, BuildConverter_.get(), ProbeConverter_.get(), leftIOMap, rightIOMap,
            buildPayloadStorage, probePayloadStorage, wasSwapped);
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        // Reserve buffers for overflow
        size_t nTuplesBuild = 0;
        for (auto& block: BuildData_) {
            nTuplesBuild += block.Size;
        }
        joinState.BuildPackedInput.Overflow.reserve(
            CalculateExpectedOverflowSize(BuildConverter_->GetTupleLayout(), nTuplesBuild));

        size_t nTuplesProbe = CalcMaxBlockLength(rightItemTypes, false) * 4; // Lets assume that average join selectivity eq 25%, so we have to fetch 4 blocks in general to fill output properly
        joinState.ProbePackedInput.Overflow.reserve(
            CalculateExpectedOverflowSize(ProbeConverter_->GetTupleLayout(), nTuplesProbe));

        // Reserve memory for probe input
        joinState.ProbePackedInput.PackedTuples.reserve(
            CalcMaxBlockLength(rightItemTypes, false) * ProbeConverter_->GetTupleLayout()->TotalRowSize);

        // Reserve memory for output
        joinState.BuildPackedOutput.reserve(
            CalcMaxBlockLength(leftItemTypes, false) * BuildConverter_->GetTupleLayout()->TotalRowSize);
        joinState.ProbePackedOutput.reserve(
            CalcMaxBlockLength(rightItemTypes, false) * ProbeConverter_->GetTupleLayout()->TotalRowSize);
    }

    void BuildIndex() {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateStageSpentTime(JoinName_, "Build", spent);
        };

        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
        auto payloadStorage = IsBuildIndirected_ ? static_cast<TExternalPayloadStorage*>(BuildExternalPayloadStorage_.AsBoxed().Get()) : nullptr; 

        for (auto& block: BuildData_) {
            if (IsBuildIndirected_) {
                auto [keyBlock, payloadBlock] = TExternalPayloadStorage::SplitBlock(
                                                    block, *payloadStorage, BuildKeyColumnsSet_);
                BuildConverter_->Pack(keyBlock.Columns, joinState.BuildPackedInput);
                payloadStorage->AddBlock(std::move(payloadBlock));
            } else {
                BuildConverter_->Pack(block.Columns, joinState.BuildPackedInput);
            }
        }
        BuildData_.clear(); // we don't need this data anymore, so don't waste memory

        auto& packed = joinState.BuildPackedInput;
        Table_.Build(packed.PackedTuples.data(), packed.Overflow.data(), packed.NTuples);
    }

    NUdf::EFetchStatus DoProbe() {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateStageSpentTime(JoinName_, "Probe", spent);
        };

        NUdf::EFetchStatus status{NUdf::EFetchStatus::Finish};
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        // If we have some output blocks from previous DoProbe call
        if (joinState.HasBlocks()) {
            return NUdf::EFetchStatus::Ok;
        }

        while (joinState.IsNotFull() && joinState.HasEnoughMemory()) {
            if (!IsFinished_) {
                status = ProbeStream_.WideFetch(ProbeInputs_.data(), ProbeInputs_.size());
            }

            // If we have some cached probe data in ProbeData_
            // handle it no matter what status we got from ProbeStream_
            if (status == NUdf::EFetchStatus::Yield && ProbeData_.empty()) {
                return NUdf::EFetchStatus::Yield;
            }
            if (status == NUdf::EFetchStatus::Finish) {
                IsFinished_ = true;
                if (ProbeData_.empty()) {
                    break;
                }
            }

            if (status == NUdf::EFetchStatus::Ok) {
                // Extract block and put it to cache
                TBlock fetchedBlock;
                TVector<arrow::Datum> blockColumns;
                for (size_t i = 0; i < ProbeInputs_.size() - 1; i++) {
                    auto& datum = TArrowBlock::From(ProbeInputs_[i]).GetDatum();
                    blockColumns.push_back(datum.array());
                }

                auto blockSize = ::GetBlockCount(ProbeInputs_[ProbeInputs_.size() - 1]);
                fetchedBlock.Size = blockSize;
                fetchedBlock.Columns = std::move(blockColumns);
                ProbeData_.emplace_back(std::move(fetchedBlock));
            }

            // Convert
            PackNextProbeBlock(joinState);

            // Do lookup, add result to state
            DoBatchLookup(joinState);

            // Clear probe's packed tuples
            // Overflow cant be cleared because output have pointers to it
            // Also payload block storage can't be cleared too for the same reason
            joinState.ProbePackedInput.PackedTuples.clear();
            joinState.ProbePackedInput.NTuples = 0;
        }

        // Nothing to do, all work was done
        if (joinState.OutputRows == 0) {
            Y_ENSURE(status == NUdf::EFetchStatus::Finish);
            joinState.ResetProbeInput();
            joinState.ResetOutput();
            return NUdf::EFetchStatus::Finish;
        }

        // Make output
        joinState.MakeBlocks(Ctx_.HolderFactory);
        joinState.ResetProbeInput();
        joinState.ResetOutput();
        return NUdf::EFetchStatus::Ok;
    }

    void FillOutput(NUdf::TUnboxedValue* output, ui32 width) {
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
        auto sliceSize = joinState.Slice();
        for (size_t i = 0; i < width; i++) {
            output[i] = joinState.Get(sliceSize, Ctx_.HolderFactory, i);
        }
    }

private:
    void PackNextProbeBlock(TJoinState& joinState) {
        const auto& block = ProbeData_.front();

        if (IsProbeIndirected_) {
            auto& payloadStorage = *static_cast<TExternalPayloadStorage*>(ProbeExternalPayloadStorage_.AsBoxed().Get());
            auto [keyBlock, payloadBlock] = TExternalPayloadStorage::SplitBlock(
                                                block, payloadStorage, ProbeKeyColumnsSet_);
            ProbeConverter_->Pack(keyBlock.Columns, joinState.ProbePackedInput);
            payloadStorage.AddBlock(std::move(payloadBlock));
        } else {
            ProbeConverter_->Pack(block.Columns, joinState.ProbePackedInput);
        }

        ProbeData_.pop_front();
    }

    void DoBatchLookup(TJoinState& joinState) {
        auto* buildLayout = BuildConverter_->GetTupleLayout();
        auto* probeLayout = ProbeConverter_->GetTupleLayout();
        auto  tuple = joinState.ProbePackedInput.PackedTuples.data();
        auto  nTuples = joinState.ProbePackedInput.NTuples;
        auto  overflow = joinState.ProbePackedInput.Overflow.data();

        constexpr auto batchSize = 16;
        
    if constexpr (false) { /// ignore batch api switch (test purpose)
        using TIterPair = std::pair<TTable::TIterator, const ui8*>;
        TVector<TIterPair> iterators(batchSize);

        for (size_t i = 0; i < nTuples; i += batchSize) {
            auto remaining = std::min<ui64>(batchSize, nTuples - i);
            for (size_t offset = 0; offset < remaining; ++offset, tuple += probeLayout->TotalRowSize) {
                iterators[offset] = {Table_.Find(tuple, overflow), tuple};
            }

            for (size_t offset = 0; offset < remaining; ++offset) {
                auto [it, inTuple] = iterators[offset];
                const ui8* foundTuple = nullptr;
                while ((foundTuple = Table_.NextMatch(it, overflow)) != nullptr) {
                    // Copy tuple from build part into output
                    auto prevSize = joinState.BuildPackedOutput.size();
                    joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                    std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                    // Copy tuple from probe part into output
                    prevSize = joinState.ProbePackedOutput.size();
                    joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                    std::copy(inTuple, inTuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                    // New row added
                    joinState.OutputRows++;
                }
            }
        }
    } else {
        for (size_t i = 0; i < nTuples; i += batchSize) {
            auto remaining = std::min<ui64>(batchSize, nTuples - i);

            std::array<const ui8*, batchSize> tuples = {};
            for (size_t offset = 0; offset < remaining; ++offset, tuple += probeLayout->TotalRowSize) {
                tuples[offset] = tuple;
            }
            auto iterators = Table_.FindBatch(tuples, overflow);

            for (size_t offset = 0; offset < remaining; ++offset) {
                auto inTuple = tuples[offset];
                auto it = iterators[offset];
                const ui8* foundTuple = nullptr;
                while ((foundTuple = Table_.NextMatch(it, overflow)) != nullptr) {
                    // Copy tuple from build part into output
                    auto prevSize = joinState.BuildPackedOutput.size();
                    joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                    std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                    // Copy tuple from probe part into output
                    prevSize = joinState.ProbePackedOutput.size();
                    joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                    std::copy(inTuple, inTuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                    // New row added
                    joinState.OutputRows++;
                }
            }
        }
    }

    }

private:
    TComputationContext&        Ctx_;
    const char *                JoinName_;
    const TVector<TType*>*      ResultItemTypes_;

    TDeque<TBlock>              BuildData_;
    const TVector<ui32>*        BuildKeyColumns_;
    THashSet<ui32>              BuildKeyColumnsSet_;
    IBlockLayoutConverter::TPtr BuildConverter_;
    NUdf::TUnboxedValue         BuildExternalPayloadStorage_;
    bool                        IsBuildIndirected_{false}; // was external payload storage used

    NUdf::TUnboxedValue         ProbeStream_;
    TUnboxedValueVector         ProbeInputs_;
    TDeque<TBlock>              ProbeData_;
    const TVector<ui32>*        ProbeKeyColumns_;
    THashSet<ui32>              ProbeKeyColumnsSet_;
    IBlockLayoutConverter::TPtr ProbeConverter_;
    NUdf::TUnboxedValue         ProbeExternalPayloadStorage_;
    bool                        IsProbeIndirected_{false}; // was external payload storage used

    NUdf::TUnboxedValue         JoinState_;
    TTable                      Table_;
    bool                        IsFinished_{false};
};

// -------------------------------------------------------------------
class TInMemoryGraceJoin : public TComputationValue<TInMemoryGraceJoin> {
private:
    using TBase = TComputationValue<TInMemoryGraceJoin>;
    using TBlock = TTempJoinStorage::TBlock;
    using TTable = TNeumannTable;

public:
    TInMemoryGraceJoin(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const char *            joinName,
        const TVector<TType*>*  resultItemTypes,
        const TVector<TType*>*  leftItemTypesArg,
        const TVector<ui32>*    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        const TVector<TType*>*  rightItemTypesArg,
        const TVector<ui32>*    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        IBlockGraceJoinPolicy*  policy,
        NUdf::TUnboxedValue     tempStorageValue
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , JoinName_(joinName)
        , ResultItemTypes_(resultItemTypes)
    {
        using EJoinAlgo = IBlockGraceJoinPolicy::EJoinAlgo;

        auto& tempStorage = *static_cast<TTempJoinStorage*>(tempStorageValue.AsBoxed().Get());
        auto [leftPSz, rightPSz] = tempStorage.GetPayloadSizes();
        auto [leftFetchedTuples, rightFetchedTuples] = tempStorage.GetFetchedTuples();
        auto maxFetchedTuples = std::max(leftFetchedTuples, rightFetchedTuples);
        auto cardinality = tempStorage.EstimateCardinality(); // bootstrap value, may be far from truth
        auto [leftData, rightData] = tempStorage.DetachData();
        
        size_t leftRowsNum = 0;
        for (const auto& block : leftData) {
            leftRowsNum += block.Size;
        }

        size_t rightRowsNum = 0;
        for (const auto& block : rightData) {
            rightRowsNum += block.Size;
        }

        THashSet<ui32> leftKeyColumnsSet(leftKeyColumns->begin(), leftKeyColumns->end());
        // Use or not external payload depends on the policy
        bool isLeftIndirected = policy->UseExternalPayload(
            EJoinAlgo::InMemoryGraceJoin, leftPSz, maxFetchedTuples / cardinality);

        THashSet<ui32> rightKeyColumnsSet(rightKeyColumns->begin(), rightKeyColumns->end());
        // Use or not external payload depends on the policy
        bool isRightIndirected = policy->UseExternalPayload(
            EJoinAlgo::InMemoryGraceJoin, rightPSz, maxFetchedTuples / cardinality);

        // Create converters
        TVector<TType*> leftItemTypes;
        MakeTupleConverter(ctx, isLeftIndirected, *leftItemTypesArg, leftKeyColumnsSet, &leftItemTypes, &LeftExternalPayloadStorage_, &LeftConverter_);

        TVector<TType*> rightItemTypes;
        MakeTupleConverter(ctx, isRightIndirected, *rightItemTypesArg, rightKeyColumnsSet, &rightItemTypes, &RightExternalPayloadStorage_, &RightConverter_);
        
        const size_t leftTupleSize = leftRowsNum * LeftConverter_->GetTupleLayout()->TotalRowSize;
        const size_t rightTupleSize = rightRowsNum * RightConverter_->GetTupleLayout()->TotalRowSize;
        const size_t minTupleSize = std::min(leftTupleSize, rightTupleSize);
        constexpr size_t bucketDesiredSize = 4 * L2_CACHE_SIZE;

        BucketsLogNum_ = minTupleSize ? sizeof(size_t) * 8 - std::countl_zero((minTupleSize - 1) / bucketDesiredSize) : 0;
        LeftBuckets_.resize(1u << BucketsLogNum_);
        RightBuckets_.resize(1u << BucketsLogNum_);

        // Reserve memory for buckets
        const size_t leftOverflowSizeEst = CalculateExpectedOverflowSize(LeftConverter_->GetTupleLayout(), leftRowsNum >> BucketsLogNum_);
        const size_t rightOverflowSizeEst = CalculateExpectedOverflowSize(RightConverter_->GetTupleLayout(), rightRowsNum >> BucketsLogNum_);
        for (ui32 bucket = 0; bucket < (1u << BucketsLogNum_); ++bucket) {
            LeftBuckets_[bucket].PackedTuples.reserve(bucketDesiredSize);
            RightBuckets_[bucket].PackedTuples.reserve(bucketDesiredSize);
            LeftBuckets_[bucket].Overflow.reserve(leftOverflowSizeEst);
            RightBuckets_[bucket].Overflow.reserve(rightOverflowSizeEst);
        }

        // Prepare pointers to external payload storage for Join state
        auto leftPayloadStorage = isLeftIndirected ? static_cast<TExternalPayloadStorage*>(LeftExternalPayloadStorage_.AsBoxed().Get()) : nullptr;
        auto rightPayloadStorage = isRightIndirected ? static_cast<TExternalPayloadStorage*>(RightExternalPayloadStorage_.AsBoxed().Get()) : nullptr;

        // Create inner hash join state
        JoinState_ = Ctx_.HolderFactory.Create<TJoinState>(
            ResultItemTypes_, LeftConverter_.get(), RightConverter_.get(), leftIOMap, rightIOMap,
            leftPayloadStorage, rightPayloadStorage, false);
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        // Pack and partition rows into buckets
        {
            const auto begin = std::chrono::steady_clock::now();
            Y_DEFER {
                const auto end = std::chrono::steady_clock::now();
                const auto spent =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
                globalResourceMeter.UpdateStageSpentTime(JoinName_, "Partition", spent);
            };

            for (auto &block : leftData) {
                if (isLeftIndirected) {
                    auto [keyBlock, payloadBlock] = TExternalPayloadStorage::SplitBlock(
                                                        block, *leftPayloadStorage, leftKeyColumnsSet);
                    LeftConverter_->BucketPack(keyBlock.Columns, LeftBuckets_.data(), BucketsLogNum_);
                    leftPayloadStorage->AddBlock(std::move(payloadBlock));
                } else {
                    LeftConverter_->BucketPack(block.Columns, LeftBuckets_.data(), BucketsLogNum_);
                }
            }
            leftData.clear();

            for (auto &block : rightData) {
                if (isRightIndirected) {
                    auto [keyBlock, payloadBlock] = TExternalPayloadStorage::SplitBlock(
                                                        block, *rightPayloadStorage, rightKeyColumnsSet);
                    RightConverter_->BucketPack(keyBlock.Columns, RightBuckets_.data(), BucketsLogNum_);
                    rightPayloadStorage->AddBlock(std::move(payloadBlock));
                } else {
                    RightConverter_->BucketPack(block.Columns, RightBuckets_.data(), BucketsLogNum_);
                }
            }
            rightData.clear();
        }
        
        // Reserve memory for output
        joinState.BuildPackedOutput.reserve(
            CalcMaxBlockLength(leftItemTypes, false) * LeftConverter_->GetTupleLayout()->TotalRowSize);
        joinState.ProbePackedOutput.reserve(
            CalcMaxBlockLength(rightItemTypes, false) * RightConverter_->GetTupleLayout()->TotalRowSize);
    }

    NUdf::EFetchStatus DoProbe() {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateStageSpentTime(JoinName_, "Probe", spent);
        };

        for (;;) {
            if (CurrBucket_ >> BucketsLogNum_) {
                return NUdf::EFetchStatus::Finish;
            }

            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

            // If we have some output blocks from previous DoProbe call
            if (joinState.HasBlocks()) {
                return NUdf::EFetchStatus::Ok;
            }

            if (NeedNextBucket_) {
                NeedNextBucket_ = false;
                BuildIndex(joinState);
            }

            // Fill output buffers and signal if next bucket is needed
            DoBatchLookup(joinState);

            if (joinState.OutputRows == 0) {
                continue;
            }

            // Make output
            joinState.MakeBlocks(Ctx_.HolderFactory);
            joinState.ResetOutput();
            return NUdf::EFetchStatus::Ok;
        }
    }

    void FillOutput(NUdf::TUnboxedValue* output, ui32 width) {
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
        auto sliceSize = joinState.Slice();
        for (size_t i = 0; i < width; i++) {
            output[i] = joinState.Get(sliceSize, Ctx_.HolderFactory, i);
        }
    }

private:
    void BuildIndex(TJoinState& joinState) {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateStageSpentTime(JoinName_, "Build", spent);
        };

        auto& leftPack = LeftBuckets_[CurrBucket_];
        auto& rightPack = RightBuckets_[CurrBucket_];

        if (leftPack.NTuples < rightPack.NTuples) {
            joinState.SetSwapped(false);
            joinState.BuildPackedInput = std::move(leftPack);
            joinState.ProbePackedInput = std::move(rightPack);
            Table_.SetTupleLayout(LeftConverter_->GetTupleLayout());
        } else {
            joinState.SetSwapped(true);
            joinState.BuildPackedInput = std::move(rightPack);
            joinState.ProbePackedInput = std::move(leftPack);
            Table_.SetTupleLayout(RightConverter_->GetTupleLayout());
        }
        
        auto& packed = joinState.BuildPackedInput;
        Table_.Build(packed.PackedTuples.data(), packed.Overflow.data(), packed.NTuples);
    }

    void DoBatchLookup(TJoinState& joinState) {
        const bool wasSwapped = joinState.GetSwapped();
        auto *const buildLayout = wasSwapped ? RightConverter_->GetTupleLayout() : LeftConverter_->GetTupleLayout();
        auto *const probeLayout = wasSwapped ? LeftConverter_->GetTupleLayout() : RightConverter_->GetTupleLayout();

        const auto nTuples = joinState.ProbePackedInput.NTuples;
        auto *const overflow = joinState.ProbePackedInput.Overflow.data();
        auto *tuple = joinState.ProbePackedInput.PackedTuples.data() + CurrProbeRow_ * probeLayout->TotalRowSize;

        using TIterPair = std::pair<TTable::TIterator, const ui8*>;
        constexpr auto batchSize = 16;
        TVector<TIterPair> iterators(batchSize);

        // TODO: interrupt this loop when joinState is full as in BlockMapJoin. So track current iterator and save iterators somewhere.
        // WARNING: we can not properly track the number of output rows due to uninterruptible for loop in DoBatchLookup,
        // so add joinState.IsNotFull() check to prevent overflow in AddMany builder's method.
        for (; CurrProbeRow_ < nTuples && joinState.IsNotFull(); CurrProbeRow_ += batchSize) {
            auto remaining = std::min<ui64>(batchSize, nTuples - CurrProbeRow_);
            for (size_t offset = 0; offset < remaining; ++offset, tuple += probeLayout->TotalRowSize) {
                iterators[offset] = {Table_.Find(tuple, overflow), tuple};
            }

            for (size_t offset = 0; offset < remaining; ++offset) {
                auto [it, inTuple] = iterators[offset];
                const ui8* foundTuple = nullptr;
                while ((foundTuple = Table_.NextMatch(it, overflow)) != nullptr) {
                    // Copy tuple from build part into output
                    auto prevSize = joinState.BuildPackedOutput.size();
                    joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                    std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                    // Copy tuple from probe part into output
                    prevSize = joinState.ProbePackedOutput.size();
                    joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                    std::copy(inTuple, inTuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                    // New row added
                    joinState.OutputRows++;
                }
            }
        }

        if (CurrProbeRow_ >= nTuples) { // >= because remaining can be less than batchSize
            NeedNextBucket_ = true;
            ++CurrBucket_;
            CurrProbeRow_ = 0;
        }
    }

private:
    TComputationContext&        Ctx_;
    const char *                JoinName_;
    const TVector<TType*>*      ResultItemTypes_;

    std::unique_ptr<IBlockLayoutConverter> LeftConverter_;
    std::unique_ptr<IBlockLayoutConverter> RightConverter_;

    ui32 BucketsLogNum_;
    TVector<IBlockLayoutConverter::TPackResult> LeftBuckets_;
    TVector<IBlockLayoutConverter::TPackResult> RightBuckets_;
    NUdf::TUnboxedValue JoinState_;
    TTable Table_;

    NUdf::TUnboxedValue LeftExternalPayloadStorage_;
    NUdf::TUnboxedValue RightExternalPayloadStorage_;

    ui32 CurrBucket_ = 0;
    ui32 CurrProbeRow_ = 0;
    bool NeedNextBucket_{true};  // if need to get to next bucket
};

// -------------------------------------------------------------------
class TGraceHashJoin : public TComputationValue<TGraceHashJoin> {
private:
    using TBase = TComputationValue<TGraceHashJoin>;
    using TBlock = TTempJoinStorage::TBlock;
    using TTable = TNeumannTable;

    using TPaddedPackResult = TPaddedPtr<IBlockLayoutConverter::TPackResult>;

    enum ESide {
        kLeftSide, kRightSide, kSides
    };

    enum class EState {
        Partition, Build, Probe
    };

    static constexpr size_t kSpillLowerLimit = 8 * KB;

    class TSpilled {
        using TSpilledVector = TVectorSpillerAdapter<ui8, TMKQLAllocator<ui8>>;
        using ESpilledState = typename TSpilledVector::EState;

        struct TSpilledPack {
            TSpilledPack(ISpiller::TPtr spiller, size_t spillLimit)
                : SpilledTuples(spiller, spillLimit)
                , SpilledOverflow(spiller, spillLimit)
            {}

        public:
            TSpilledVector SpilledTuples;
            TSpilledVector SpilledOverflow;
        };

    public:
        /// TODO: manage spill and buffer sizes

        explicit TSpilled(ISpiller::TPtr spiller, size_t spillLimit)
            : SpilledPack(MakeHolder<TSpilledPack>(spiller, spillLimit))
            , SpillLimit_(spillLimit)
        {}

        void ReserveBuffer() {
            Buffer.PackedTuples.reserve(SpillLimit_);
            Buffer.Overflow.reserve(SpillLimit_);
        }

        bool MaySpill() {
            Update();
            return SpilledPack->SpilledTuples.GetState() == ESpilledState::AcceptingData &&
                   SpilledPack->SpilledOverflow.GetState() == ESpilledState::AcceptingData;
        }

        bool MayRequest() {
            // Update(); /// pipeline is: ask MayExtract --on fail-> ask MayRequest
            return SpilledPack->SpilledTuples.GetState() == ESpilledState::AcceptingDataRequests &&
                   SpilledPack->SpilledOverflow.GetState() == ESpilledState::AcceptingDataRequests;
        }

        bool MayExtract() {
            Update();
            return SpilledPack->SpilledTuples.GetState() == ESpilledState::DataReady &&
                   SpilledPack->SpilledOverflow.GetState() == ESpilledState::DataReady;
        }

        auto GetSizes() const {
            return std::pair{SpilledTuplesSize_, SpilledOverflowSize_};
        }

        bool Empty() const {
            return SpilledNum_ == 0;
        }

        void SuggestToSpillBuffer() {
            /// TODO: manage spill and buffer sizes
            if (Buffer.PackedTuples.size() * 8 > SpillLimit_ * 7 ||
                Buffer.Overflow.size() * 8 > SpillLimit_ / 7) {
                SpillBuffer();
            }
        }

        void SpillBuffer() {
            SpilledNum_++;
            SpilledTuplesSize_ += Buffer.PackedTuples.size();
            SpilledOverflowSize_ += Buffer.Overflow.size();

            SpilledPack->SpilledTuples.AddData(std::move(Buffer.PackedTuples));
            SpilledPack->SpilledOverflow.AddData(std::move(Buffer.Overflow));

            Buffer.PackedTuples.clear();
            Buffer.Overflow.clear();
            Buffer.NTuples = 0;
        }
        
        void Finalize() {
            const bool maySpill = MaySpill();

            if (Buffer.PackedTuples.empty() && maySpill) {
                /// successful finalize call
                State_ = EState::Reading;
                SpilledPack->SpilledTuples.Finalize();
                SpilledPack->SpilledOverflow.Finalize();
                return;
            }
            
            State_ = EState::Finalizing;
            if (maySpill) {
                SpillBuffer();
            }
        }

        void RequestBuffer() {
            SpilledPack->SpilledTuples.RequestNextVector();
            SpilledPack->SpilledOverflow.RequestNextVector();
        }

        void ExtractBuffer(const NPackedTuple::TTupleLayout* layout) {
            SpilledNum_--;
            Buffer.PackedTuples = SpilledPack->SpilledTuples.ExtractVector();
            Buffer.Overflow = SpilledPack->SpilledOverflow.ExtractVector();
            Buffer.NTuples = Buffer.PackedTuples.size() / layout->TotalRowSize;
        }

    private:
        void Update() {
            SpilledPack->SpilledTuples.Update();
            SpilledPack->SpilledOverflow.Update();

            if (State_ == EState::Finalizing && 
                SpilledPack->SpilledTuples.GetState() == ESpilledState::AcceptingData &&
                SpilledPack->SpilledOverflow.GetState() == ESpilledState::AcceptingData) {
                if (!Buffer.PackedTuples.empty()) {
                    /// left to spill, still cant finalize
                    SpillBuffer();
                } else {
                    State_ = EState::Reading;
                    SpilledPack->SpilledTuples.Finalize();
                    SpilledPack->SpilledOverflow.Finalize();
                }
            }
        }

    public:
        IBlockLayoutConverter::TPackResult Buffer;

    private:
        THolder<TSpilledPack> SpilledPack;
        size_t SpillLimit_;

        size_t SpilledNum_ = 0;
        size_t SpilledTuplesSize_ = 0;
        size_t SpilledOverflowSize_ = 0;

        enum class EState {
            Writing, Finalizing, Reading
        };
        EState State_ = EState::Writing;
    };

    struct TBucket {
        TBucket(ISpiller::TPtr spiller, size_t spillLimit, ui32 prefixBitsLen)
            : Spilled({TSpilled(spiller, spillLimit), TSpilled(spiller, spillLimit)})
            , PrefixBitsLen(prefixBitsLen)
        {}

    public:
        std::array<TSpilled, kSides> Spilled;
        ui32 PrefixBitsLen;
    };

public:
    TGraceHashJoin(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const char *            joinName,
        const TVector<TType*>*  resultItemTypes,
        NUdf::TUnboxedValue*    leftStream,
        const TVector<TType*>*  leftItemTypesArg,
        const TVector<ui32>*    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        NUdf::TUnboxedValue*    rightStream,
        const TVector<TType*>*  rightItemTypesArg,
        const TVector<ui32>*    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        IBlockGraceJoinPolicy*  policy,
        NUdf::TUnboxedValue     tempStorageValue
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , JoinName_(joinName)
        , ResultItemTypes_(resultItemTypes)
    {
        using EJoinAlgo = IBlockGraceJoinPolicy::EJoinAlgo;

        MaxSize_ = policy->GetMaximumData();

        auto& tempStorage = *static_cast<TTempJoinStorage*>(tempStorageValue.AsBoxed().Get());
        auto [leftFetchedTuples, rightFetchedTuples] = tempStorage.GetFetchedTuples();
        auto [leftPSz, rightPSz] = tempStorage.GetPayloadSizes();
        auto cardinality = tempStorage.EstimateCardinality(); // bootstrap value, may be far from truth
        auto [leftData, rightData] = tempStorage.DetachData();
        
        const bool wasSwapped = false;

        auto& leftSide = JoinSides_[kLeftSide];
        leftSide.Stream = *leftStream;
        leftSide.Inputs.resize(leftItemTypesArg->size());
        auto leftKeyColumnsSet = THashSet<ui32>(leftKeyColumns->begin(), leftKeyColumns->end());
        // Use or not external payload depends on the policy
        /// TODO: support indexed payload, currently it is ignored
        leftSide.IsIndirected = policy->UseExternalPayload(
            EJoinAlgo::HashJoin, leftPSz, leftFetchedTuples / cardinality);

        auto& rightSide = JoinSides_[kRightSide];
        rightSide.Stream = *rightStream;
        rightSide.Inputs.resize(rightItemTypesArg->size());
        auto rightKeyColumnsSet = THashSet<ui32>(rightKeyColumns->begin(), rightKeyColumns->end());
        // Use or not external payload depends on the policy
        rightSide.IsIndirected = policy->UseExternalPayload(
            EJoinAlgo::HashJoin, rightPSz, rightFetchedTuples / cardinality);

        // Create converters
        TVector<TType*> leftItemTypes;
        MakeTupleConverter(ctx, false, *leftItemTypesArg, leftKeyColumnsSet, &leftItemTypes, nullptr, &leftSide.Converter);

        TVector<TType*> rightItemTypes;
        MakeTupleConverter(ctx, false, *rightItemTypesArg, rightKeyColumnsSet, &rightItemTypes, nullptr, &rightSide.Converter);

        // Prepare pointers to external payload storage for Join state
        auto leftPayloadStorage = nullptr;
        auto rightPayloadStorage = nullptr;

        // Create inner hash join state
        JoinState_ = Ctx_.HolderFactory.Create<TJoinState>(
            ResultItemTypes_, leftSide.Converter.get(), rightSide.Converter.get(), leftIOMap, rightIOMap,
            leftPayloadStorage, rightPayloadStorage, wasSwapped);
        // auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        /// TODO: reserve memory?
        // // Reserve memory for input
        // joinState.LeftPackedInput.PackedTuples.reserve(
        //     CalcMaxBlockLength(leftItemTypes, false) * leftSide.Converter->GetTupleLayout()->TotalRowSize);
        
        // joinState.RightPackedInput.PackedTuples.reserve(
        //     CalcMaxBlockLength(rightItemTypes, false) * rightSide.Converter->GetTupleLayout()->TotalRowSize);

        // // Reserve buffers for overflow
        // size_t nTuplesLeft = CalcMaxBlockLength(leftItemTypes, false) * 4; // Lets assume that average join selectivity eq 25%, so we have to fetch 4 blocks in general to fill output properly
        // joinState.LeftPackedInput.Overflow.reserve(
        //     CalculateExpectedOverflowSize(leftSide.Converter->GetTupleLayout(), nTuplesLeft));

        // size_t nTuplesRight = CalcMaxBlockLength(rightItemTypes, false) * 4; // Lets assume that average join selectivity eq 25%, so we have to fetch 4 blocks in general to fill output properly
        // joinState.RightPackedInput.Overflow.reserve(
        //     CalculateExpectedOverflowSize(rightSide.Converter->GetTupleLayout(), nTuplesRight));

        // // Reserve memory for output
        // joinState.LeftPackedOutput.reserve(
        //     CalcMaxBlockLength(leftItemTypes, false) * leftSide.Converter->GetTupleLayout()->TotalRowSize);
        // joinState.RightPackedOutput.reserve(
        //     CalcMaxBlockLength(rightItemTypes, false) * rightSide.Converter->GetTupleLayout()->TotalRowSize);

        // Initialize partitions
        if (!ctx.SpillerFactory) {
            throw yexception() << "Attempt to use grace join without spiller";
        }
        Spiller_ = ctx.SpillerFactory->CreateSpiller();

        const size_t initialBucketsLogNum = 5;
        const size_t maxBucketsNum = MaxSize_ / kSpillLowerLimit;
        if (maxBucketsNum < 2) {
            throw yexception() << "Max data limits are too low";
        }

        LogBucketsNum_ =
            std::min(initialBucketsLogNum,
                     sizeof(maxBucketsNum) * 8 - std::countl_zero(maxBucketsNum - 1));
        Buckets_.reserve(1u << LogBucketsNum_);
        for (size_t ind = 0; ind < (1u << LogBucketsNum_); ++ind) {
            Buckets_.emplace_back(Spiller_, SpillLimit(), LogBucketsNum_);
            Buckets_[Buckets_.size() - 1].Spilled[kLeftSide].ReserveBuffer();
            Buckets_[Buckets_.size() - 1].Spilled[kRightSide].ReserveBuffer();
        }

        auto leftBucketsPtr = TPaddedPackResult(&Buckets_[0].Spilled[kLeftSide].Buffer, sizeof(Buckets_[0]));
        for (auto &block : leftData) {
            leftSide.Converter->BucketPack(block.Columns, leftBucketsPtr, LogBucketsNum_);
        }

        auto rightBucketsPtr = TPaddedPackResult(&Buckets_[0].Spilled[kRightSide].Buffer, sizeof(Buckets_[0]));
        for (auto &block : rightData) {
            rightSide.Converter->BucketPack(block.Columns, rightBucketsPtr, LogBucketsNum_);
        }

        for (size_t ind = 0; ind < Buckets_.size(); ++ind) {
            Buckets_[ind].Spilled[kLeftSide].SuggestToSpillBuffer();
            Buckets_[ind].Spilled[kRightSide].SuggestToSpillBuffer();
        }
    }


    NUdf::EFetchStatus DoProbe() {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateStageSpentTime(JoinName_, "Probe", spent);
        };

        for (;;) {
            if (Buckets_.empty()) {
                return NUdf::EFetchStatus::Finish;
            }

            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
            // If we have some output blocks from previous DoProbe call
            if (joinState.HasBlocks()) {
                return NUdf::EFetchStatus::Ok;
            }

            if (State_ == EState::Partition) {
                UpdatePartition(joinState);
                /// may result in transition to Build state
            }

            if (State_ == EState::Build) {
                UpdateBuild(joinState);
                /// may result in transition to Probe state
            }

            /// if not in Probe state than there is data to wait
            if (State_ != EState::Probe) {
                return NUdf::EFetchStatus::Yield;
            }

            if (joinState.ProbePackedInput.NTuples == 0) {
                auto status = UpdateProbe(joinState);

                if (status == NUdf::EFetchStatus::Yield) {
                    return NUdf::EFetchStatus::Yield;
                } else if (status == NUdf::EFetchStatus::Finish) {
                    continue;
                }
            }

            // Fill output buffers and signal if next bucket is needed
            DoBatchLookup(joinState);

            if (joinState.OutputRows == 0) {
                continue;
            }

            // Make output
            joinState.MakeBlocks(Ctx_.HolderFactory);
            joinState.ResetOutput();
            return NUdf::EFetchStatus::Ok;
        }
    }

    void FillOutput(NUdf::TUnboxedValue* output, ui32 width) {
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
        auto sliceSize = joinState.Slice();
        for (size_t i = 0; i < width; i++) {
            output[i] = joinState.Get(sliceSize, Ctx_.HolderFactory, i);
        }
    }

private:
    void UpdatePartition(TJoinState& joinState) {
        for (;;) {
            auto leftStatus = NUdf::EFetchStatus::Ok;
            auto rightStatus = NUdf::EFetchStatus::Ok;

            while (leftStatus == NUdf::EFetchStatus::Ok ||
                   rightStatus == NUdf::EFetchStatus::Ok) {
                if (IsFirstPartition_) {
                    /// spill input streams into buckets
                    if (OrderedFirstPartition) {
                        /// read build first
                        leftStatus = PartitionStream(BuildSide());
                        rightStatus = NUdf::EFetchStatus::Yield;
                        /// then probe
                        if (leftStatus == NUdf::EFetchStatus::Finish) {
                            rightStatus = PartitionStream(ProbeSide());
                        }
                    } else {
                        leftStatus = PartitionStream(kLeftSide);
                        rightStatus = PartitionStream(kRightSide);
                    }
                } else {
                    /// recursive spill into buckets
                    leftStatus = PartitionSpilled(kLeftSide);
                    rightStatus = PartitionSpilled(kRightSide);
                }
            }

            if (leftStatus == NUdf::EFetchStatus::Finish &&
                rightStatus == NUdf::EFetchStatus::Finish) {
                MakeNextPartition(joinState);
                if (State_ == EState::Partition) {
                    /// need to update just created recursive partition
                    continue;
                }
            }
            break;
        }
    }

    void MakeNextPartition(TJoinState& joinState) {
        if (Buckets_.empty()) {
            /// reached last probe
            return;
        }

        IsFirstPartition_ = false;
        PartitionedSpilled_ = std::nullopt;
        Partitioners_[kLeftSide].Reset();
        Partitioners_[kRightSide].Reset();

        auto &bucket = Buckets_[Buckets_.size() - 1];

        const auto [leftTuplesSize, leftOverflowSize] = bucket.Spilled[kLeftSide].GetSizes();
        const auto [rightTuplesSize, rightOverflowSize] = bucket.Spilled[kRightSide].GetSizes();
        const auto leftConsumption = std::max(leftTuplesSize, leftOverflowSize);
        const auto rightConsumption = std::max(rightTuplesSize, rightOverflowSize);
        const auto minConsumption = std::min(leftConsumption, rightConsumption);

        if (minConsumption <= MaxSize_) {
            State_ = EState::Build;

            /// init joinState build
            WasSwapped_ = rightConsumption < leftConsumption;
            joinState.SetSwapped(WasSwapped_);
            joinState.ResetBuildInput();
        } else {
            State_ = EState::Partition;

            const size_t estBucketsNum =
                std::min(1 + (minConsumption - 1) / (MaxSize_ * 7 / 8),
                         MaxSize_ / kSpillLowerLimit);
            LogBucketsNum_ =
                sizeof(estBucketsNum) * 8 - std::countl_zero(estBucketsNum - 1);
            /// TODO: set upper buckets num limit?

            Partitioners_[kLeftSide] =
                NPackedTuple::TAccumulator::Create(
                    JoinSides_[kLeftSide].Converter->GetTupleLayout(),
                    bucket.PrefixBitsLen, LogBucketsNum_);
            Partitioners_[kRightSide] =
                NPackedTuple::TAccumulator::Create(
                    JoinSides_[kRightSide].Converter->GetTupleLayout(),
                    bucket.PrefixBitsLen, LogBucketsNum_);
            PartitionedSpilled_ = std::move(bucket);
            Buckets_.pop_back();

            /// init rec partitions
            const auto prefixBitsLen =
                PartitionedSpilled_->PrefixBitsLen + LogBucketsNum_;
            for (size_t ind = 0; ind < (1u << LogBucketsNum_); ++ind) {
                Buckets_.emplace_back(Spiller_, SpillLimit(), prefixBitsLen);
            }
        }
    }

    NUdf::EFetchStatus PartitionStream(ESide side) {
        TJoinData &joinData = JoinSides_[side];
        if (joinData.StreamFinished) {
            return NUdf::EFetchStatus::Finish;
        }

        /// TODO: maybe do smth better than barrier
        ///       needed to prevent exceeding memory consumption limit
        bool bucketsBusy = false;
        for (auto& bucket : Buckets_) {
            bucketsBusy = bucketsBusy || !bucket.Spilled[side].MaySpill();
        }
        if (bucketsBusy) {
            return NUdf::EFetchStatus::Yield;
        }

        auto status = joinData.Stream.WideFetch(joinData.Inputs.data(), joinData.Inputs.size());
        
        switch (status) {
        case NUdf::EFetchStatus::Yield:
            return status;

        case NUdf::EFetchStatus::Ok: {
            TVector<arrow::Datum> blockColumns;
            for (size_t i = 0; i < joinData.Inputs.size() - 1; i++) {
                auto& datum = TArrowBlock::From(joinData.Inputs[i]).GetDatum();
                blockColumns.push_back(datum.array());
            }
    
            for (auto& bucket : Buckets_) {
                /// attempt to decrease number of allocations
                bucket.Spilled[side].ReserveBuffer();
            }

            auto bucketsPtr = TPaddedPackResult(&Buckets_[0].Spilled[side].Buffer, sizeof(Buckets_[0]));
            joinData.Converter->BucketPack(blockColumns, bucketsPtr, LogBucketsNum_);
    
            for (auto& bucket : Buckets_) {
                bucket.Spilled[side].SuggestToSpillBuffer();
            }
            return status;
        }

        case NUdf::EFetchStatus::Finish:
            for (auto& bucket : Buckets_) {
                bucket.Spilled[side].Finalize();
            }
            joinData.StreamFinished = true;
            return status;
        }
    }

    NUdf::EFetchStatus PartitionSpilled(ESide side) {
        const ui32 bucketsNum = 1u << LogBucketsNum_;
        auto &spilled = PartitionedSpilled_->Spilled[side];
        auto buckets = TPaddedPtr(
            &Buckets_[Buckets_.size() - bucketsNum].Spilled[side],
            sizeof(Buckets_[0]));
        auto& partitioner = *Partitioners_[side];
        const auto *layout = JoinSides_[side].Converter->GetTupleLayout();

        if (spilled.Empty()) {
            return NUdf::EFetchStatus::Finish;
        }

        bool bucketsBusy = false;
        for (ui32 ind = 0; ind < bucketsNum; ind++) {
            bucketsBusy = bucketsBusy || !buckets[ind].MaySpill();
        }
        if (bucketsBusy) {
            return NUdf::EFetchStatus::Yield;
        }

        while (!spilled.MayExtract()) {
            if (spilled.MayRequest()) {
                spilled.RequestBuffer();
            } else {
                return NUdf::EFetchStatus::Yield;
            }
        }

        spilled.ExtractBuffer(layout);
        partitioner.AddData(spilled.Buffer.PackedTuples.data(),
                            spilled.Buffer.Overflow.data(),
                            spilled.Buffer.NTuples);
        /// TODO: change accumulator interface
        std::vector<NPackedTuple::TAccumulator::TBucket, TMKQLAllocator<NPackedTuple::TAccumulator::TBucket>> tmpBuckets;
        partitioner.Detach(tmpBuckets);

        for (ui32 ind = 0; ind < bucketsNum; ind++) {
            auto &bucket = buckets[ind];
            auto &tmpBucket = tmpBuckets[ind];
            /// attempt to decrease number of allocations
            bucket.ReserveBuffer();
            
            layout->Join(bucket.Buffer.PackedTuples,
                         bucket.Buffer.Overflow,
                         bucket.Buffer.NTuples,
                         tmpBucket.PackedTuples.data(),
                         tmpBucket.Overflow.data(),
                         tmpBucket.NTuples,
                         tmpBucket.Overflow.size());
            bucket.Buffer.NTuples += tmpBucket.NTuples;

            bucket.SuggestToSpillBuffer();
            if (spilled.Empty()) {
                bucket.Finalize();
            }
        }

        return NUdf::EFetchStatus::Ok;
    }

    void UpdateBuild(TJoinState& joinState) {
        auto &buildPacked = joinState.BuildPackedInput;
        auto &buildSpilled = Buckets_[Buckets_.size() - 1].Spilled[BuildSide()];
        auto buildLayout = JoinSides_[BuildSide()].Converter->GetTupleLayout();

        while (!buildSpilled.Empty()) { /// "if" AcTuAlLy :o
            while (!buildSpilled.MayExtract()) {
                if (buildSpilled.MayRequest()) {
                    buildSpilled.RequestBuffer();
                } else {
                    return;
                }
            }

            buildSpilled.ExtractBuffer(buildLayout);
            buildLayout->Join(buildPacked.PackedTuples,
                              buildPacked.Overflow,
                              buildPacked.NTuples,
                              buildSpilled.Buffer.PackedTuples.data(),
                              buildSpilled.Buffer.Overflow.data(),
                              buildSpilled.Buffer.NTuples,
                              buildSpilled.Buffer.Overflow.size());
            buildPacked.NTuples += buildSpilled.Buffer.NTuples;
        }

        Table_.SetTupleLayout(buildLayout);
        Table_.Build(buildPacked.PackedTuples.data(),
                     buildPacked.Overflow.data(),
                     buildPacked.NTuples); 
        State_ = EState::Probe;
    }

    NUdf::EFetchStatus UpdateProbe(TJoinState& joinState) {
        auto &probePacked = joinState.ProbePackedInput;
        auto &probeSpilled = Buckets_[Buckets_.size() - 1].Spilled[ProbeSide()];
        auto probeLayout = JoinSides_[ProbeSide()].Converter->GetTupleLayout();

        if (!probeSpilled.Empty()) {
            while (!probeSpilled.MayExtract()) {
                if (probeSpilled.MayRequest()) {
                    probeSpilled.RequestBuffer();
                } else {
                    return NUdf::EFetchStatus::Yield;
                }
            }
            
            probeSpilled.ExtractBuffer(probeLayout);
            probePacked = std::move(probeSpilled.Buffer);
            return NUdf::EFetchStatus::Ok;
        }

        State_ = EState::Partition;
        Buckets_.pop_back();
        MakeNextPartition(joinState);

        return NUdf::EFetchStatus::Finish;
    }

    void DoBatchLookup(TJoinState& joinState) {
        auto buildLayout = JoinSides_[BuildSide()].Converter->GetTupleLayout();
        auto probeLayout = JoinSides_[ProbeSide()].Converter->GetTupleLayout();

        const auto nTuples = joinState.ProbePackedInput.NTuples;
        auto *const overflow = joinState.ProbePackedInput.Overflow.data();
        auto *tuple = joinState.ProbePackedInput.PackedTuples.data() + CurrProbeRow_ * probeLayout->TotalRowSize;

        constexpr ui32 batchSize = 16;

        /// TODO: add check if HasEnoughMemory()
        for (; CurrProbeRow_ < nTuples && joinState.IsNotFull(); CurrProbeRow_ += batchSize) {
            const auto remaining = std::min(batchSize, nTuples - CurrProbeRow_);
            
            std::array<const ui8*, batchSize> tuples = {};
            for (size_t offset = 0; offset < remaining; ++offset, tuple += probeLayout->TotalRowSize) {
                tuples[offset] = tuple;
            }
            auto iterators = Table_.FindBatch(tuples, overflow);

            for (size_t offset = 0; offset < remaining; ++offset) {
                auto inTuple = tuples[offset];
                auto it = iterators[offset];
                
                const ui8* foundTuple = nullptr;
                while ((foundTuple = Table_.NextMatch(it, overflow)) != nullptr) {
                    // Copy tuple from build part into output
                    auto prevSize = joinState.BuildPackedOutput.size();
                    joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                    std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                    // Copy tuple from probe part into output
                    prevSize = joinState.ProbePackedOutput.size();
                    joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                    std::copy(inTuple, inTuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                    // New row added
                    joinState.OutputRows++;
                }
            }
        }

        if (CurrProbeRow_ >= nTuples) { // >= because remaining can be less than batchSize
            CurrProbeRow_ = 0;
            joinState.ResetProbeInput(); // signal need of next probe block
        }
    }

    size_t SpillLimit() const { return MaxSize_ >> LogBucketsNum_; };

    ESide BuildSide() const { return ESide(WasSwapped_); }
    ESide ProbeSide() const { return ESide(!WasSwapped_); }

  private:
    TComputationContext&        Ctx_;
    ISpiller::TPtr              Spiller_;
    const char *                JoinName_;
    const TVector<TType*>*      ResultItemTypes_;
    size_t                      MaxSize_;

    struct TJoinData {
        NUdf::TUnboxedValue Stream;
        TUnboxedValueVector Inputs;

        IBlockLayoutConverter::TPtr Converter;

        bool StreamFinished{false};
        /// TODO: support indirect for spilled, currently ignored
        bool IsIndirected{false}; // was external payload storage used    
    };
    TJoinData JoinSides_[kSides];

    NUdf::TUnboxedValue         JoinState_;
    TTable                      Table_;

    TVector<TBucket> Buckets_;

    EState State_{EState::Partition};
    bool IsFirstPartition_{true};
    bool OrderedFirstPartition{false};
    bool WasSwapped_{false};
    ui32 LogBucketsNum_{0};

    std::optional<TBucket>              PartitionedSpilled_;
    THolder<NPackedTuple::TAccumulator> Partitioners_[kSides];

    ui32 CurrProbeRow_{0};
};

// -------------------------------------------------------------------
class TStreamValue : public TComputationValue<TStreamValue> {
private:
    using TBase = TComputationValue<TStreamValue>;

    enum class EMode {
        Start,  // trying to decide what algorithm use: hash join or grace hash join
        HashJoin,
        InMemoryGraceJoin,
        GraceHashJoin,
    };
    
public:
    TStreamValue(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const TVector<TType*>&  resultItemTypes,
        NUdf::TUnboxedValue&&   leftStream,
        const TVector<TType*>&  leftItemTypes,
        const TVector<ui32>&    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        NUdf::TUnboxedValue&&   rightStream,
        const TVector<TType*>&  rightItemTypes,
        const TVector<ui32>&    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        IBlockGraceJoinPolicy*  policy
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , ResultItemTypes_(resultItemTypes)
        , LeftStream_(std::move(leftStream))
        , LeftItemTypes_(leftItemTypes)
        , LeftKeyColumns_(leftKeyColumns)
        , LeftIOMap_(leftIOMap)
        , RightStream_(std::move(rightStream))
        , RightItemTypes_(rightItemTypes)
        , RightKeyColumns_(rightKeyColumns)
        , RightIOMap_(rightIOMap)
        , Policy_(policy)
    {
        TempStorage_ = Ctx_.HolderFactory.Create<TTempJoinStorage>(
            leftItemTypes,
            leftKeyColumns,
            LeftStream_,
            rightItemTypes,
            rightKeyColumns,
            RightStream_,
            Policy_,
            &Ctx_.ArrowMemoryPool
        );
    }

private:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            globalResourceMeter.UpdateSpentTime(JoinName_, spent);
            globalResourceMeter.UpdateConsumptedMemory(JoinName_, TlsAllocState->GetUsed());
        };

    switch_mode:
        switch (GetMode()) {
        case EMode::Start: {
            using EJoinAlgo = IBlockGraceJoinPolicy::EJoinAlgo;

            auto& tempStorage = *GetTempState();
            while (!tempStorage.IsReady()) {
                if (tempStorage.FetchStreams() == NUdf::EFetchStatus::Yield) {
                    return NUdf::EFetchStatus::Yield;
                }
            }

            auto [lTuples, rTuples] = tempStorage.GetFetchedTuples();
            const auto [isLeftFinished, isRightFinished] = tempStorage.IsFinished();
            if (!isLeftFinished) {
                lTuples = IBlockGraceJoinPolicy::STREAM_NOT_FETCHED;
            } 
            if (!isRightFinished) {
                rTuples = IBlockGraceJoinPolicy::STREAM_NOT_FETCHED;
            }

            switch (Policy_->PickAlgorithm(lTuples, rTuples)) {
            case EJoinAlgo::HashJoin:
                MakeHashJoin();
                GetHashJoin()->BuildIndex();
                SwitchModeTo(EMode::HashJoin);
                break;

            case EJoinAlgo::InMemoryGraceJoin:
                MakeInMemoryGraceJoin();
                SwitchModeTo(EMode::InMemoryGraceJoin);
                break;

            case EJoinAlgo::SpillingGraceJoin:
                MakeGraceHashJoin();
                SwitchModeTo(EMode::GraceHashJoin);
                break;
            }

            goto switch_mode;
        }

        case EMode::HashJoin: {
            auto& hashJoin = *GetHashJoin();
            auto status = hashJoin.DoProbe();
            if (status == NUdf::EFetchStatus::Ok) {
                hashJoin.FillOutput(output, width);
            }
            return status;
        }
        case EMode::InMemoryGraceJoin: {
            auto& join = *GetInMemoryGraceJoin();
            auto status = join.DoProbe();
            if (status == NUdf::EFetchStatus::Ok) {
                join.FillOutput(output, width);
            }
            return status;
        }
        case EMode::GraceHashJoin: {
            auto& join = *GetGraceHashJoin();
            auto status = join.DoProbe();
            if (status == NUdf::EFetchStatus::Ok) {
                join.FillOutput(output, width);
            }
            return status;
        }
        }

        Y_UNREACHABLE();
    }

private:
    TTempJoinStorage* GetTempState() {
        return static_cast<TTempJoinStorage*>(TempStorage_.AsBoxed().Get());
    }

    void MakeHashJoin() {
        auto newJoinName = "BlockGraceJoin::HashJoin";
        Join_ = Ctx_.HolderFactory.Create<THashJoin>(
            Ctx_, newJoinName, &ResultItemTypes_,
            &LeftStream_, &LeftItemTypes_, &LeftKeyColumns_, LeftIOMap_,
            &RightStream_, &RightItemTypes_, &RightKeyColumns_, RightIOMap_,
            Policy_, std::move(TempStorage_));
        globalResourceMeter.MergeHistoryPages(JoinName_, newJoinName);
        JoinName_ = newJoinName;
    }

    THashJoin* GetHashJoin() {
        return static_cast<THashJoin*>(Join_.AsBoxed().Get());
    }

    void MakeInMemoryGraceJoin() {
        auto newJoinName = "BlockGraceJoin::InMemoryGraceJoin";
        Join_ = Ctx_.HolderFactory.Create<TInMemoryGraceJoin>(
            Ctx_, newJoinName, &ResultItemTypes_,
            &LeftItemTypes_, &LeftKeyColumns_, LeftIOMap_,
            &RightItemTypes_, &RightKeyColumns_, RightIOMap_,
            Policy_, std::move(TempStorage_));
        globalResourceMeter.MergeHistoryPages(JoinName_, newJoinName);
        JoinName_ = newJoinName;
    }

    TInMemoryGraceJoin* GetInMemoryGraceJoin() {
        return static_cast<TInMemoryGraceJoin*>(Join_.AsBoxed().Get());
    }

    void MakeGraceHashJoin() {
        auto newJoinName = "BlockGraceJoin::GraceHashJoin";
        Join_ = Ctx_.HolderFactory.Create<TGraceHashJoin>(
            Ctx_, newJoinName, &ResultItemTypes_,
            &LeftStream_, &LeftItemTypes_, &LeftKeyColumns_, LeftIOMap_,
            &RightStream_, &RightItemTypes_, &RightKeyColumns_, RightIOMap_,
            Policy_, std::move(TempStorage_));    
        globalResourceMeter.MergeHistoryPages(JoinName_, newJoinName);
        JoinName_ = newJoinName;
    }

    TGraceHashJoin* GetGraceHashJoin() {
        return static_cast<TGraceHashJoin*>(Join_.AsBoxed().Get());
    }

    EMode GetMode() const {
        return Mode_;
    }

    void SwitchModeTo(EMode other) {
        Mode_ = other;
    }

private:
    EMode                   Mode_{EMode::Start};
    TComputationContext&    Ctx_;
    const TVector<TType*>&  ResultItemTypes_;

    NUdf::TUnboxedValue     LeftStream_;
    const TVector<TType*>&  LeftItemTypes_;
    const TVector<ui32>&    LeftKeyColumns_;
    const TVector<ui32>&    LeftIOMap_;

    NUdf::TUnboxedValue     RightStream_;
    const TVector<TType*>&  RightItemTypes_;
    const TVector<ui32>&    RightKeyColumns_;
    const TVector<ui32>&    RightIOMap_;

    IBlockGraceJoinPolicy*  Policy_;

    NUdf::TUnboxedValue     TempStorage_;
    NUdf::TUnboxedValue     Join_;
    const char *            JoinName_ = "BlockGraceJoin";
};

// -------------------------------------------------------------------
class TBlockGraceJoinCoreWraper : public TMutableComputationNode<TBlockGraceJoinCoreWraper> {
private:
    using TBaseComputation = TMutableComputationNode<TBlockGraceJoinCoreWraper>;

public:
    TBlockGraceJoinCoreWraper(
        TComputationMutables&   mutables,
        const TVector<TType*>&& resultItemTypes,
        const TVector<TType*>&& leftItemTypes,
        const TVector<ui32>&&   leftKeyColumns,
        const TVector<ui32>&&   leftIOMap,
        const TVector<TType*>&& rightItemTypes,
        const TVector<ui32>&&   rightKeyColumns,
        const TVector<ui32>&&   rightIOMap,
        IComputationNode*       leftStream,
        IComputationNode*       rightStream,
        IBlockGraceJoinPolicy*  policy
    )
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftItemTypes_(std::move(leftItemTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , LeftIOMap_(std::move(leftIOMap))
        , RightItemTypes_(std::move(rightItemTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , RightIOMap_(std::move(rightIOMap))
        , LeftStream_(std::move(leftStream))
        , RightStream_(std::move(rightStream))
        , Policy_(policy)
        , KeyTupleCache_(mutables)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(
            ctx,
            ResultItemTypes_,
            std::move(LeftStream_->GetValue(ctx)),
            LeftItemTypes_,
            LeftKeyColumns_,
            LeftIOMap_,
            std::move(RightStream_->GetValue(ctx)),
            RightItemTypes_,
            RightKeyColumns_,
            RightIOMap_,
            Policy_
        );
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

private:
    const TVector<TType*>   ResultItemTypes_;

    const TVector<TType*>   LeftItemTypes_;
    const TVector<ui32>     LeftKeyColumns_;
    const TVector<ui32>     LeftIOMap_;

    const TVector<TType*>   RightItemTypes_;
    const TVector<ui32>     RightKeyColumns_;
    const TVector<ui32>     RightIOMap_;

    IComputationNode*       LeftStream_;
    IComputationNode*       RightStream_;

    IBlockGraceJoinPolicy*  Policy_;

    const TContainerCacheOnContext KeyTupleCache_;
};

} // namespace

IComputationNode* WrapBlockGraceJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 9, "Expected 9 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    MKQL_ENSURE(leftStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> leftStreamItems(leftStreamComponents.cbegin(), leftStreamComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    MKQL_ENSURE(rightStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> rightStreamItems(rightStreamComponents.cbegin(), rightStreamComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    MKQL_ENSURE(joinKind == EJoinKind::Inner,
                "Only inner join is supported in block grace hash join prototype");

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(leftKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    const THashSet<ui32> leftKeySet(leftKeyColumns.cbegin(), leftKeyColumns.cend());

    const auto leftKeyDropsLiteral = callable.GetInput(4);
    const auto leftKeyDropsTuple = AS_VALUE(TTupleLiteral, leftKeyDropsLiteral);
    THashSet<ui32> leftKeyDrops;
    leftKeyDrops.reserve(leftKeyDropsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyDropsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyDropsTuple->GetValue(i));
        leftKeyDrops.emplace(item->AsValue().Get<ui32>());
    }

    for (const auto& drop : leftKeyDrops) {
        MKQL_ENSURE(leftKeySet.contains(drop),
                    "Only key columns has to be specified in drop column set");
    }

    const auto rightKeyColumnsLiteral = callable.GetInput(5);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    TVector<ui32> rightKeyColumns;
    rightKeyColumns.reserve(rightKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        rightKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    const THashSet<ui32> rightKeySet(rightKeyColumns.cbegin(), rightKeyColumns.cend());

    const auto rightKeyDropsLiteral = callable.GetInput(6);
    const auto rightKeyDropsTuple = AS_VALUE(TTupleLiteral, rightKeyDropsLiteral);
    THashSet<ui32> rightKeyDrops;
    rightKeyDrops.reserve(rightKeyDropsTuple->GetValuesCount());
    for (ui32 i = 0; i < rightKeyDropsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyDropsTuple->GetValue(i));
        rightKeyDrops.emplace(item->AsValue().Get<ui32>());
    }

    for (const auto& drop : rightKeyDrops) {
        MKQL_ENSURE(rightKeySet.contains(drop),
                    "Only key columns has to be specified in drop column set");
    }

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    [[maybe_unused]] const auto rightAnyNode = callable.GetInput(7);

    const auto untypedPolicyNode = callable.GetInput(8);
    const auto untypedPolicy = AS_VALUE(TDataLiteral, untypedPolicyNode)->AsValue().Get<ui64>();
    const auto policy = static_cast<IBlockGraceJoinPolicy*>(reinterpret_cast<void*>(untypedPolicy));

    // XXX: Mind the last wide item, containing block length.
    TVector<ui32> leftIOMap;
    for (size_t i = 0; i < leftStreamItems.size() - 1; i++) {
        if (leftKeyDrops.contains(i)) {
            continue;
        }
        leftIOMap.push_back(i);
    }

    // XXX: Mind the last wide item, containing block length.
    TVector<ui32> rightIOMap;
    for (size_t i = 0; i < rightStreamItems.size() - 1; i++) {
        if (rightKeyDrops.contains(i)) {
            continue;
        }
        rightIOMap.push_back(i);
    }

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);

    return new TBlockGraceJoinCoreWraper(
        ctx.Mutables,
        std::move(joinItems),
        std::move(leftStreamItems),
        std::move(leftKeyColumns),
        std::move(leftIOMap),
        std::move(rightStreamItems),
        std::move(rightKeyColumns),
        std::move(rightIOMap),
        leftStream,
        rightStream,
        policy ? policy : &globalDefaultPolicy
    );
}

} // namespace NKikimr::NMiniKQL
