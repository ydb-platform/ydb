#include "mkql_block_grace_join.h"

#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/block_layout_converter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_resource_meter.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/robin_hood_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/neumann_hash_table.h>

#include <util/generic/serialized_enum.h>

#include <chrono>

namespace NKikimr::NMiniKQL {

namespace {

using namespace std::chrono_literals;

// -------------------------------------------------------------------
[[maybe_unused]] constexpr size_t KB = 1024;
[[maybe_unused]] constexpr size_t MB = KB * KB;
[[maybe_unused]] constexpr size_t L1_CACHE_SIZE = 256 * KB;
[[maybe_unused]] constexpr size_t L2_CACHE_SIZE =   2 * MB;
[[maybe_unused]] constexpr size_t L3_CACHE_SIZE =  16 * MB;

// -------------------------------------------------------------------
size_t CalcMaxBlockLength(const TVector<TType*>& items) {
    return CalcBlockLen(std::accumulate(items.cbegin(), items.cend(), 0ULL,
        [](size_t max, const TType* type) {
            const TType* itemType = AS_TYPE(TBlockType, type)->GetItemType();
            return std::max(max, CalcMaxBlockItemSize(itemType));
        }));
}

// -------------------------------------------------------------------

using TRobinHoodTable = NPackedTuple::TRobinHoodHashBase<>;
using TNeumannTable = NPackedTuple::TNeumannHashTable;

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
class TTempJoinStorage : public TComputationValue<TTempJoinStorage> {
private:
    using TBase = TComputationValue<TTempJoinStorage>;

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

    enum class TStatus {
        Unknown,
        OneStreamFinished, // Only one stream is finished
        BothStreamsFinished, // Both streams are finished
        MemoryLimitExceeded // We have to use Grace Hash Join algorithm
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
        arrow::MemoryPool*      pool
    )
        : TBase(memInfo)
        , LeftStream_(leftStream)
        , LeftInputs_(leftItemTypesArg.size())
        , RightStream_(rightStream)
        , RightInputs_(rightItemTypesArg.size())
    {
        TVector<TType*> leftItemTypes;
        for (size_t i = 0; i < leftItemTypesArg.size() - 1; i++) { // ignore last column, because this is block size
            leftItemTypes.push_back(AS_TYPE(TBlockType, leftItemTypesArg[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> leftRoles(LeftInputs_.size() - 1, NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: leftKeyColumns) {
            leftRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        LeftConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), leftItemTypes, leftRoles, pool);

        TVector<TType*> rightItemTypes;
        for (size_t i = 0; i < rightItemTypesArg.size() - 1; i++) { // ignore last column, because this is block size
            rightItemTypes.push_back(AS_TYPE(TBlockType, rightItemTypesArg[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> rightRoles(RightInputs_.size() - 1, NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: rightKeyColumns) {
            rightRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        RightConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), rightItemTypes, rightRoles, pool);
    }

    NUdf::EFetchStatus FetchStreams() {
        auto resultLeft = NUdf::EFetchStatus::Finish;
        if (!LeftIsFinished_ && LeftEstimatedSize_ < MEMORY_THRESHOLD) {
            resultLeft = LeftStream_.WideFetch(LeftInputs_.data(), LeftInputs_.size());
            if (resultLeft == NUdf::EFetchStatus::Ok) {
                TBlock leftBlock;
                ExtractBlock(LeftInputs_, leftBlock);
                LeftEstimatedSize_ += EstimateBlockSize(leftBlock, LeftConverter_->GetTupleLayout());
                LeftData_.push_back(std::move(leftBlock));
            } else if (resultLeft == NUdf::EFetchStatus::Finish) {
                LeftIsFinished_ = true;
            }
        }
        
        auto resultRight = NUdf::EFetchStatus::Finish;
        if (!RightIsFinished_ && RightEstimatedSize_ < MEMORY_THRESHOLD) {
            resultRight = RightStream_.WideFetch(RightInputs_.data(), RightInputs_.size());
            if (resultRight == NUdf::EFetchStatus::Ok) {
                TBlock rightBlock;
                ExtractBlock(RightInputs_, rightBlock);
                RightEstimatedSize_ += EstimateBlockSize(rightBlock, RightConverter_->GetTupleLayout());
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

    TStatus GetStatus() {
        if (LeftIsFinished_ && RightIsFinished_) {
            return TStatus::BothStreamsFinished;
        }
        if ((LeftIsFinished_ && RightEstimatedSize_ >= MEMORY_THRESHOLD) ||
            (LeftEstimatedSize_ >= MEMORY_THRESHOLD && RightIsFinished_)) {
          return TStatus::OneStreamFinished;
        }
        if (LeftEstimatedSize_ >= MEMORY_THRESHOLD && RightEstimatedSize_ >= MEMORY_THRESHOLD) {
            return TStatus::MemoryLimitExceeded;
        }
        return TStatus::Unknown;
    }

    // After the method is called FetchStreams cannot be called anymore
    std::pair<TDeque<TBlock>, TDeque<TBlock>> DetachData() {
        return {std::move(LeftData_), std::move(RightData_)};
    }

    std::pair<bool, bool> IsFinished() const {
        return {LeftIsFinished_, RightIsFinished_};
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

private:
    NUdf::TUnboxedValue LeftStream_;
    TUnboxedValueVector LeftInputs_;
    TDeque<TBlock>      LeftData_;
    size_t              LeftEstimatedSize_{0}; // size in tuple layout represenation
    bool                LeftIsFinished_{false};
    IBlockLayoutConverter::TPtr LeftConverter_; // Converters here are used only for size estimation via info in TupleLayout class

    NUdf::TUnboxedValue RightStream_;
    TUnboxedValueVector RightInputs_;
    TDeque<TBlock>      RightData_;
    size_t              RightEstimatedSize_{0}; // size in tuple layout represenation
    bool                RightIsFinished_{false};
    IBlockLayoutConverter::TPtr RightConverter_;

    static constexpr size_t MEMORY_THRESHOLD{L3_CACHE_SIZE / 2}; // heuristic value of maximum data size for hash table
};

// -------------------------------------------------------------------
struct TJoinState : public TBlockState {
public:
    TJoinState(
        TMemoryUsageInfo*       memInfo,
        const TVector<TType*>*  resultItemTypes,
        IBlockLayoutConverter*  buildConverter,
        IBlockLayoutConverter*  probeConverter,
        const TVector<ui32>&    leftIOMap,
        const TVector<ui32>&    rightIOMap,
        bool                    wasSwapped
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

        RightPackedTuple_ = &ProbePackedOutput;
        RightOverflow_ = &ProbePackedInput.Overflow;
        RightConverter_ = probeConverter;

        // Check if was swapped.
        // If was not swapped, left stream is build and right is probe
        if (wasSwapped) {
            using std::swap;
            swap(LeftPackedTuple_, RightPackedTuple_);
            swap(LeftOverflow_, RightOverflow_);
            swap(LeftConverter_, RightConverter_);
        }
    }

    bool GetSwapped() const { return WasSwapped_; }
    void SetSwapped(bool wasSwapped) {
        if (wasSwapped != WasSwapped_) {
            using std::swap;
            swap(LeftPackedTuple_, RightPackedTuple_);
            swap(LeftOverflow_, RightOverflow_);
            WasSwapped_ = wasSwapped;
        }
    }

    void MakeBlocks(const THolderFactory& holderFactory)
    {
        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(OutputRows)));

        size_t index = 0;
        IBlockLayoutConverter::PackResult leftPackResult{std::move(*LeftPackedTuple_), std::move(*LeftOverflow_), OutputRows};
        TVector<arrow::Datum> leftColumns;
        LeftConverter_->Unpack(leftPackResult, leftColumns);
        for (size_t i = 0; i < LeftIOMap_.size(); i++, index++) {
            Values[index] = holderFactory.CreateArrowBlock(std::move(leftColumns[LeftIOMap_[i]]));
        }

        IBlockLayoutConverter::PackResult rightPackResult{std::move(*RightPackedTuple_), std::move(*RightOverflow_), OutputRows};
        TVector<arrow::Datum> rightColumns;
        RightConverter_->Unpack(rightPackResult, rightColumns);
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
        return OutputRows < MaxLength_ && HasEnoughMemory();
    }

    void Reset() {
        OutputRows = 0;
        ProbePackedInput.PackedTuples.clear();
        ProbePackedInput.Overflow.clear();
        ProbePackedInput.NTuples = 0;
        // Do not clear build one, because it is constant for all DoProbe calls
        BuildPackedOutput.clear();
        ProbePackedOutput.clear();
    }

private:    
    bool HasEnoughMemory() const {
      return ProbePackedInput.Overflow.capacity() == 0 ||
             ProbePackedInput.Overflow.size() * 5 <
                 ProbePackedInput.Overflow.capacity() * 4;
    }

public:
    IBlockLayoutConverter::PackResult BuildPackedInput;   // converted data right after fetch
    IBlockLayoutConverter::PackResult ProbePackedInput;

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

    IBlockLayoutConverter*                  RightConverter_;
    IBlockLayoutConverter::TPackedTuple*    RightPackedTuple_;
    IBlockLayoutConverter::TOverflow*       RightOverflow_;
    const TVector<ui32>&                    RightIOMap_;

    NUdf::TUnboxedValue Table_; // Hash table for smaller stream
};


// -------------------------------------------------------------------
class THashJoin : public TComputationValue<THashJoin> {
private:
    using TBase = TComputationValue<THashJoin>;
    using TBlock = TTempJoinStorage::TBlock;
    using TTable = TRobinHoodTable;

public:
    THashJoin(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const TVector<TType*>*  resultItemTypes,
        NUdf::TUnboxedValue*    leftStream,
        const TVector<TType*>*  leftItemTypesArg,
        const TVector<ui32>*    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        NUdf::TUnboxedValue*    rightStream,
        const TVector<TType*>*  rightItemTypesArg,
        const TVector<ui32>*    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        NUdf::TUnboxedValue     tempStorageValue
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , ResultItemTypes_(resultItemTypes)
    {
        auto& tempStorage = *static_cast<TTempJoinStorage*>(tempStorageValue.AsBoxed().Get());
        auto [leftData, rightData] = tempStorage.DetachData();
        auto [isLeftFinished, isRightFinished] = tempStorage.IsFinished(); 
        bool wasSwapped = false;
        if (!isLeftFinished && isRightFinished) { // assume that finished stream has less size than unfinished
            using std::swap;
            swap(leftStream, rightStream); // so swap them
            swap(leftData, rightData);
            swap(leftItemTypesArg, rightItemTypesArg);
            swap(leftKeyColumns, rightKeyColumns);
            wasSwapped = true;
        }

        BuildData_ = std::move(leftData);
        BuildKeyColumns_ = leftKeyColumns;

        ProbeStream_ = *rightStream;
        ProbeData_ = std::move(rightData);
        ProbeKeyColumns_ = rightKeyColumns;
        ProbeInputs_.resize(rightItemTypesArg->size());

        // Create converters
        auto pool = &Ctx_.ArrowMemoryPool;

        TVector<TType*> leftItemTypes;
        for (size_t i = 0; i < leftItemTypesArg->size() - 1; i++) { // ignore last column, because this is block size
            leftItemTypes.push_back(AS_TYPE(TBlockType, (*leftItemTypesArg)[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> buildRoles(leftItemTypes.size(), NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: *BuildKeyColumns_) {
            buildRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        BuildConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), leftItemTypes, buildRoles, pool);

        TVector<TType*> rightItemTypes;
        for (size_t i = 0; i < rightItemTypesArg->size() - 1; i++) { // ignore last column, because this is block size
            rightItemTypes.push_back(AS_TYPE(TBlockType, (*rightItemTypesArg)[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> probeRoles(rightItemTypes.size(), NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: *ProbeKeyColumns_) {
            probeRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        ProbeConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), rightItemTypes, probeRoles, pool);

        Table_.SetTupleLayout(BuildConverter_->GetTupleLayout());

        // Create inner hash join state
        JoinState_ = Ctx_.HolderFactory.Create<TJoinState>(
            ResultItemTypes_, BuildConverter_.get(), ProbeConverter_.get(), leftIOMap, rightIOMap, wasSwapped);
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        // Reserve buffers for overflow
        size_t nTuplesBuild = 0;
        for (auto& block: BuildData_) {
            nTuplesBuild += block.Size;
        }
        joinState.BuildPackedInput.Overflow.reserve(
            CalculateExpectedOverflowSize(BuildConverter_->GetTupleLayout(), nTuplesBuild));

        size_t nTuplesProbe = CalcMaxBlockLength(*rightItemTypesArg) * 4; // Lets assume that average join selectivity eq 25%, so we have to fetch 4 blocks in general to fill output properly
        joinState.ProbePackedInput.Overflow.reserve(
            CalculateExpectedOverflowSize(ProbeConverter_->GetTupleLayout(), nTuplesProbe));

        // Reserve memory for probe input
        joinState.ProbePackedInput.PackedTuples.reserve(
            CalcMaxBlockLength(*rightItemTypesArg) * ProbeConverter_->GetTupleLayout()->TotalRowSize);

        // Reserve memory for output
        joinState.BuildPackedOutput.reserve(
            CalcMaxBlockLength(*leftItemTypesArg) * BuildConverter_->GetTupleLayout()->TotalRowSize);
        joinState.ProbePackedOutput.reserve(
            CalcMaxBlockLength(*rightItemTypesArg) * ProbeConverter_->GetTupleLayout()->TotalRowSize);
    }

    void BuildIndex() {
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        for (auto& block: BuildData_) {
            BuildConverter_->Pack(block.Columns, joinState.BuildPackedInput);
        }
        BuildData_.clear(); // we don't need this data anymore, so don't waste memory

        auto& packed = joinState.BuildPackedInput;
        Table_.Build(packed.PackedTuples.data(), packed.Overflow.data(), packed.NTuples);
    }

    NUdf::EFetchStatus DoProbe() {
        NUdf::EFetchStatus status{NUdf::EFetchStatus::Finish};
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        while (joinState.IsNotFull()) {
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
            const auto block = ProbeData_.front();
            ProbeConverter_->Pack(block.Columns, joinState.ProbePackedInput);
            ProbeData_.pop_front();

            // Do lookup, add result to state
            DoBatchLookup(joinState);

            // Clear probe's packed tuples
            // Overflow cant be cleared because output have pointers to it
            joinState.ProbePackedInput.PackedTuples.clear();
            joinState.ProbePackedInput.NTuples = 0;
        }

        // Nothing to do, all work was done
        if (joinState.OutputRows == 0) {
            Y_ENSURE(status == NUdf::EFetchStatus::Finish);
            joinState.Reset();
            return NUdf::EFetchStatus::Finish;
        }

        // Make output
        joinState.MakeBlocks(Ctx_.HolderFactory);
        joinState.Reset();
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
    void DoBatchLookup(TJoinState& joinState) {
        auto* buildLayout = BuildConverter_->GetTupleLayout();
        auto* probeLayout = ProbeConverter_->GetTupleLayout();
        auto  tuple = joinState.ProbePackedInput.PackedTuples.data();
        auto  nTuples = joinState.ProbePackedInput.NTuples;
        auto  overflow = joinState.ProbePackedInput.Overflow.data();

        for (size_t i = 0; i < nTuples; i++, tuple += probeLayout->TotalRowSize) {
            Table_.Apply(tuple, overflow, [&joinState, buildLayout, probeLayout, tuple](const ui8* foundTuple){
                // Copy tuple from build part into output
                auto prevSize = joinState.BuildPackedOutput.size();
                joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                // Copy tuple from probe part into output
                prevSize = joinState.ProbePackedOutput.size();
                joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                std::copy(tuple, tuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                // New row added
                joinState.OutputRows++;
            });
        }
    }

private:
    TComputationContext&        Ctx_;
    const TVector<TType*>*      ResultItemTypes_;

    TDeque<TBlock>              BuildData_;
    const TVector<ui32>*        BuildKeyColumns_;
    IBlockLayoutConverter::TPtr BuildConverter_;

    NUdf::TUnboxedValue         ProbeStream_;
    TUnboxedValueVector         ProbeInputs_;
    TDeque<TBlock>              ProbeData_;
    const TVector<ui32>*        ProbeKeyColumns_;
    IBlockLayoutConverter::TPtr ProbeConverter_;

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
        const TVector<TType*>*  resultItemTypes,
        const TVector<TType*>*  leftItemTypesArg,
        const TVector<ui32>*    leftKeyColumns,
        const TVector<ui32>&    leftIOMap,
        const TVector<TType*>*  rightItemTypesArg,
        const TVector<ui32>*    rightKeyColumns,
        const TVector<ui32>&    rightIOMap,
        NUdf::TUnboxedValue     tempStorageValue
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , ResultItemTypes_(resultItemTypes)
    {
        auto& tempStorage = *static_cast<TTempJoinStorage*>(tempStorageValue.AsBoxed().Get());
        auto [leftData, rightData] = tempStorage.DetachData();
        
        size_t leftRowsNum = 0;
        for (const auto& block : leftData) {
            leftRowsNum += block.Size;
        }

        size_t rightRowsNum = 0;
        for (const auto& block : rightData) {
            rightRowsNum += block.Size;
        }

        // Create converters
        auto pool = &Ctx_.ArrowMemoryPool;

        TVector<TType*> leftItemTypes;
        for (size_t i = 0; i < leftItemTypesArg->size() - 1; i++) { // ignore last column, because this is block size
            leftItemTypes.push_back(AS_TYPE(TBlockType, (*leftItemTypesArg)[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> leftRoles(leftItemTypes.size(), NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: *leftKeyColumns) {
            leftRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        LeftConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), leftItemTypes, leftRoles, pool);

        TVector<TType*> rightItemTypes;
        for (size_t i = 0; i < rightItemTypesArg->size() - 1; i++) { // ignore last column, because this is block size
            rightItemTypes.push_back(AS_TYPE(TBlockType, (*rightItemTypesArg)[i])->GetItemType());
        }
        TVector<NPackedTuple::EColumnRole> rightRoles(rightItemTypes.size(), NPackedTuple::EColumnRole::Payload);
        for (auto keyCol: *rightKeyColumns) {
            rightRoles[keyCol] = NPackedTuple::EColumnRole::Key;
        }
        RightConverter_ = MakeBlockLayoutConverter(TTypeInfoHelper(), rightItemTypes, rightRoles, pool);

        const size_t leftTupleSize = leftRowsNum * LeftConverter_->GetTupleLayout()->TotalRowSize;
        const size_t rightTupleSize = rightRowsNum * RightConverter_->GetTupleLayout()->TotalRowSize;
        const size_t minTupleSize = std::min(leftTupleSize, rightTupleSize);
        constexpr size_t bucketDesiredSize = L1_CACHE_SIZE;

        BucketsLogNum_ = minTupleSize ? sizeof(size_t) * 8 - std::countl_zero((minTupleSize - 1) / bucketDesiredSize) : 0;
        LeftBuckets_.resize(1u << BucketsLogNum_);
        RightBuckets_.resize(1u << BucketsLogNum_);

        const size_t leftOverflowSizeEst = CalculateExpectedOverflowSize(LeftConverter_->GetTupleLayout(), leftRowsNum >> BucketsLogNum_);
        const size_t rightOverflowSizeEst = CalculateExpectedOverflowSize(RightConverter_->GetTupleLayout(), rightRowsNum >> BucketsLogNum_);
        for (ui32 bucket = 0; bucket < (1u << BucketsLogNum_); ++bucket) {
            LeftBuckets_[bucket].Overflow.reserve(leftOverflowSizeEst);
            RightBuckets_[bucket].Overflow.reserve(rightOverflowSizeEst);
        }

        for (auto &block : leftData) {
            LeftConverter_->BucketPack(block.Columns, LeftBuckets_.data(), BucketsLogNum_);
        }
        leftData.clear();

        for (auto &block : rightData) {
            RightConverter_->BucketPack(block.Columns, RightBuckets_.data(), BucketsLogNum_);
        }
        rightData.clear();

        // Create inner hash join state
        JoinState_ = Ctx_.HolderFactory.Create<TJoinState>(
            ResultItemTypes_, LeftConverter_.get(), RightConverter_.get(), leftIOMap, rightIOMap, false);
        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
        
        // Reserve memory for output
        joinState.BuildPackedOutput.reserve(
            CalcMaxBlockLength(*leftItemTypesArg) * LeftConverter_->GetTupleLayout()->TotalRowSize);
        joinState.ProbePackedOutput.reserve(
            CalcMaxBlockLength(*rightItemTypesArg) * RightConverter_->GetTupleLayout()->TotalRowSize);
    }

    NUdf::EFetchStatus DoProbe() {
        if (CurrBucket_ >> BucketsLogNum_) {
            return NUdf::EFetchStatus::Finish;
        }

        auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

        if (NeedNextBucket_) {
            NeedNextBucket_ = false;
            BuildIndex(joinState);
        }

        /// Fill output buffers and signal if next bucket is needed
        DoBatchLookup(joinState);

        if (joinState.OutputRows == 0) {
            return DoProbe();
        }

        // Make output
        joinState.MakeBlocks(Ctx_.HolderFactory);
        joinState.Reset();
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
    void BuildIndex(TJoinState& joinState) {
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

        /// TODO: batching
        for (; CurrProbeRow_ < nTuples; CurrProbeRow_++, tuple += probeLayout->TotalRowSize) {
            Table_.Apply(tuple, overflow, [&joinState, buildLayout, probeLayout, tuple](const ui8* foundTuple){
                // Copy tuple from build part into output
                auto prevSize = joinState.BuildPackedOutput.size();
                joinState.BuildPackedOutput.resize(prevSize + buildLayout->TotalRowSize);
                std::copy(foundTuple, foundTuple + buildLayout->TotalRowSize, joinState.BuildPackedOutput.data() + prevSize);

                // Copy tuple from probe part into output
                prevSize = joinState.ProbePackedOutput.size();
                joinState.ProbePackedOutput.resize(prevSize + probeLayout->TotalRowSize);
                std::copy(tuple, tuple + probeLayout->TotalRowSize, joinState.ProbePackedOutput.data() + prevSize);

                // New row added
                joinState.OutputRows++;
            });
        }

        if (CurrProbeRow_ == nTuples) {
            NeedNextBucket_ = true;
            ++CurrBucket_;
            CurrProbeRow_ = 0;
        }
    }

private:
    TComputationContext&        Ctx_;
    const TVector<TType*>*      ResultItemTypes_;

    std::unique_ptr<IBlockLayoutConverter> LeftConverter_;
    std::unique_ptr<IBlockLayoutConverter> RightConverter_;

    ui32 BucketsLogNum_;
    TVector<IBlockLayoutConverter::PackResult> LeftBuckets_;
    TVector<IBlockLayoutConverter::PackResult> RightBuckets_;
    NUdf::TUnboxedValue JoinState_;
    TTable Table_;

    ui32 CurrBucket_ = 0;
    ui32 CurrProbeRow_ = 0;
    bool NeedNextBucket_{true};  // if need to get to next bucket
};


// -------------------------------------------------------------------
class TStreamValue : public TComputationValue<TStreamValue> {
private:
    using TBase = TComputationValue<TStreamValue>;

    enum class TMode {
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
        const TVector<ui32>&    rightIOMap
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
    {
        TempStorage_ = Ctx_.HolderFactory.Create<TTempJoinStorage>(
            leftItemTypes,
            leftKeyColumns,
            LeftStream_,
            rightItemTypes,
            rightKeyColumns,
            RightStream_,
            &Ctx_.ArrowMemoryPool
        );
    }

private:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
        const auto begin = std::chrono::steady_clock::now();
        Y_DEFER {
            const auto end = std::chrono::steady_clock::now();
            const auto spent =
                std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
                    .count();
            globalResourceMeter.UpdateSpentTime(JoinName_, spent);
            globalResourceMeter.UpdateConsumptedMemory(JoinName_,
                                                       TlsAllocState->GetUsed());
        };

    switch_mode:
        switch (GetMode()) {
        case TMode::Start:
        {
            using TStatus = TTempJoinStorage::TStatus;
            auto& tempStorage = *GetTempState();
            auto status = TStatus::Unknown;

            while (status == TStatus::Unknown) {
                if (tempStorage.FetchStreams() == NUdf::EFetchStatus::Yield) {
                    return NUdf::EFetchStatus::Yield;
                }
                status = tempStorage.GetStatus();
            }

            switch (status) {
            case TTempJoinStorage::TStatus::BothStreamsFinished: {
                MakeInMemoryGraceJoin();

                SwitchModeTo(TMode::InMemoryGraceJoin);
                goto switch_mode;
            }
            case TTempJoinStorage::TStatus::OneStreamFinished: {
                MakeHashJoin();
                auto& hashJoin = *GetHashJoin();
                hashJoin.BuildIndex();

                SwitchModeTo(TMode::HashJoin);
                goto switch_mode;
            }
            case TTempJoinStorage::TStatus::MemoryLimitExceeded: {
                /// TODO: not implemented
                Y_ASSERT(false); // Grace hash join not implemented yet

                SwitchModeTo(TMode::GraceHashJoin);
                goto switch_mode;
            }
            case TTempJoinStorage::TStatus::Unknown:
                Y_ASSERT(false);
            }

            Y_UNREACHABLE();
        }
        case TMode::HashJoin: {
            auto& hashJoin = *GetHashJoin();
            auto status = hashJoin.DoProbe();
            if (status == NUdf::EFetchStatus::Ok) {
                hashJoin.FillOutput(output, width);
            }
            return status;
        }
        case TMode::InMemoryGraceJoin: {
            auto& join = *GetInMemoryGraceJoin();
            auto status = join.DoProbe();
            if (status == NUdf::EFetchStatus::Ok) {
                join.FillOutput(output, width);
            }
            return status;
        }
        case TMode::GraceHashJoin: {
            /// TODO: not implemented
            Y_ASSERT(false); // Grace hash join not implemented yet
        }
        }

        Y_UNREACHABLE();
    }

private:
    TTempJoinStorage* GetTempState() {
        return static_cast<TTempJoinStorage*>(TempStorage_.AsBoxed().Get());
    }

    void MakeHashJoin() {
        Join_ = Ctx_.HolderFactory.Create<THashJoin>(
            Ctx_, &ResultItemTypes_,
            &LeftStream_, &LeftItemTypes_, &LeftKeyColumns_, LeftIOMap_,
            &RightStream_, &RightItemTypes_, &RightKeyColumns_, RightIOMap_,
            std::move(TempStorage_));
        JoinName_ = "BlockGraceJoin:HashJoin";
    }

    THashJoin* GetHashJoin() {
        return static_cast<THashJoin*>(Join_.AsBoxed().Get());
    }

    void MakeInMemoryGraceJoin() {
        Join_ = Ctx_.HolderFactory.Create<TInMemoryGraceJoin>(
            Ctx_, &ResultItemTypes_,
            &LeftItemTypes_, &LeftKeyColumns_, LeftIOMap_,
            &RightItemTypes_, &RightKeyColumns_, RightIOMap_,
            std::move(TempStorage_));
        JoinName_ = "BlockGraceJoin:InMemoryGraceJoin";
    }

    TInMemoryGraceJoin* GetInMemoryGraceJoin() {
        return static_cast<TInMemoryGraceJoin*>(Join_.AsBoxed().Get());
    }

    TMode GetMode() const {
        return Mode_;
    }

    void SwitchModeTo(TMode other) {
        Mode_ = other;
    }

private:
    TMode                   Mode_{TMode::Start};
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
        IComputationNode*       rightStream
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
            RightIOMap_
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

    const TContainerCacheOnContext KeyTupleCache_;
};

} // namespace

IComputationNode* WrapBlockGraceJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 8, "Expected 8 args");

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
        rightStream
    );
}

} // namespace NKikimr::NMiniKQL
