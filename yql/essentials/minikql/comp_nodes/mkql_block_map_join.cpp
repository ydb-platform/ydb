#include "mkql_block_map_join.h"

#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_block_trimmer.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_block_map_join_utils.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/generic/serialized_enum.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

size_t CalcMaxBlockLength(const TVector<TType*>& items) {
    return CalcBlockLen(std::accumulate(items.cbegin(), items.cend(), 0ULL,
        [](size_t max, const TType* type) {
            const TType* itemType = AS_TYPE(TBlockType, type)->GetItemType();
            return std::max(max, CalcMaxBlockItemSize(itemType));
        }));
}

ui64 CalculateTupleHash(const std::vector<ui64>& hashes) {
    ui64 hash = 0;
    for (size_t i = 0; i < hashes.size(); i++) {
        if (!hashes[i]) {
            return 0;
        }

        hash = CombineHashes(hash, hashes[i]);
    }

    return hash;
}

template <bool RightRequired>
class TBlockJoinState : public TBlockState {
public:
    TBlockJoinState(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
                    const TVector<TType*>& inputItems,
                    const TVector<ui32>& leftIOMap,
                    const TVector<TType*> outputItems)
        : TBlockState(memInfo, outputItems.size())
        , InputWidth_(inputItems.size() - 1)
        , OutputWidth_(outputItems.size() - 1)
        , Inputs_(inputItems.size())
        , LeftIOMap_(leftIOMap)
        , InputsDescr_(ToValueDescr(inputItems))
    {
        const auto& pgBuilder = ctx.Builder->GetPgBuilder();
        MaxLength_ = CalcMaxBlockLength(outputItems);
        TBlockTypeHelper helper;
        for (size_t i = 0; i < inputItems.size(); i++) {
            TType* blockItemType = AS_TYPE(TBlockType, inputItems[i])->GetItemType();
            Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
            Converters_.push_back(MakeBlockItemConverter(TTypeInfoHelper(), blockItemType, pgBuilder));
            Hashers_.push_back(helper.MakeHasher(blockItemType));
        }
        // The last output column (i.e. block length) doesn't require a block builder.
        for (size_t i = 0; i < OutputWidth_; i++) {
            const TType* blockItemType = AS_TYPE(TBlockType, outputItems[i])->GetItemType();
            Builders_.push_back(MakeArrayBuilder(TTypeInfoHelper(), blockItemType, ctx.ArrowMemoryPool, MaxLength_, &pgBuilder, &BuilderAllocatedSize_));
        }
        MaxBuilderAllocatedSize_ = MaxAllocatedFactor_ * BuilderAllocatedSize_;
    }

    void CopyRow() {
        // Copy items from the "left" stream.
        // Use the mapping from input fields to output ones to
        // produce a tight loop to copy row items.
        for (size_t i = 0; i < LeftIOMap_.size(); i++) {
            AddItem(GetItem(LeftIOMap_[i]), i);
        }
        OutputRows_++;
    }

    void MakeRow(const NUdf::TUnboxedValuePod& value) {
        size_t builderIndex = 0;
        // Copy items from the "left" stream.
        // Use the mapping from input fields to output ones to
        // produce a tight loop to copy row items.
        for (size_t i = 0; i < LeftIOMap_.size(); i++, builderIndex++) {
            AddItem(GetItem(LeftIOMap_[i]), i);
        }
        // Convert and append items from the "right" dict.
        // Since the keys are copied to the output only from the
        // "left" stream, process all values unconditionally.
        if constexpr (RightRequired) {
            for (size_t i = 0; builderIndex < OutputWidth_; i++) {
                AddValue(value.GetElement(i), builderIndex++);
            }
        } else {
            if (value) {
                for (size_t i = 0; builderIndex < OutputWidth_; i++) {
                    AddValue(value.GetElement(i), builderIndex++);
                }
            } else {
                while (builderIndex < OutputWidth_) {
                    AddValue(value, builderIndex++);
                }
            }
        }
        OutputRows_++;
    }

    void MakeRow(const std::vector<NYql::NUdf::TBlockItem>& rightColumns) {
        size_t builderIndex = 0;

        for (size_t i = 0; i < LeftIOMap_.size(); i++, builderIndex++) {
            AddItem(GetItem(LeftIOMap_[i]), builderIndex);
        }

        if (!rightColumns.empty()) {
            Y_ENSURE(LeftIOMap_.size() + rightColumns.size() == OutputWidth_);
            for (size_t i = 0; i < rightColumns.size(); i++) {
                AddItem(rightColumns[i], builderIndex++);
            }
        } else {
            while (builderIndex < OutputWidth_) {
                AddItem(TBlockItem(), builderIndex++);
            }
        }

        OutputRows_++;
    }

    void MakeBlocks(const THolderFactory& holderFactory) {
        Values.back() = holderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(OutputRows_)));
        OutputRows_ = 0;
        BuilderAllocatedSize_ = 0;

        for (size_t i = 0; i < Builders_.size(); i++) {
            Values[i] = holderFactory.CreateArrowBlock(Builders_[i]->Build(IsFinished_));
        }
        FillArrays();
    }

    TBlockItem GetItem(size_t idx, size_t offset = 0) const {
        Y_ENSURE(Current_ + offset < InputRows_);
        const auto& datum = TArrowBlock::From(Inputs_[idx]).GetDatum();
        ARROW_DEBUG_CHECK_DATUM_TYPES(InputsDescr_[idx], datum.descr());
        if (datum.is_scalar()) {
            return Readers_[idx]->GetScalarItem(*datum.scalar());
        }
        MKQL_ENSURE(datum.is_array(), "Expecting array");
        return Readers_[idx]->GetItem(*datum.array(), Current_ + offset);
    }

    std::pair<TBlockItem, ui64> GetItemWithHash(size_t idx, size_t offset) const {
        auto item = GetItem(idx, offset);
        ui64 hash = Hashers_[idx]->Hash(item);
        return std::make_pair(item, hash);
    }

    NUdf::TUnboxedValuePod GetValue(const THolderFactory& holderFactory, size_t idx) const {
        return Converters_[idx]->MakeValue(GetItem(idx), holderFactory);
    }

    void Reset() {
        Current_ = 0;
        InputRows_ = GetBlockCount(Inputs_.back());
    }

    void Finish() {
        IsFinished_ = true;
    }

    void NextRow() {
        Current_++;
    }

    bool HasBlocks() {
        return Count > 0;
    }

    bool IsNotFull() const {
        return OutputRows_ < MaxLength_
            && BuilderAllocatedSize_ <= MaxBuilderAllocatedSize_;
    }

    bool IsEmpty() const {
        return OutputRows_ == 0;
    }

    bool IsFinished() const {
        return IsFinished_;
    }

    size_t RemainingRowsCount() const {
        Y_ENSURE(InputRows_ >= Current_);
        return InputRows_ - Current_;
    }

    NUdf::TUnboxedValue* GetRawInputFields() {
        return Inputs_.data();
    }

    size_t GetInputWidth() const {
        // Mind the last block length column.
        return InputWidth_ + 1;
    }

    size_t GetOutputWidth() const {
        // Mind the last block length column.
        return OutputWidth_ + 1;
    }

private:
    void AddItem(const TBlockItem& item, size_t idx) {
        Builders_[idx]->Add(item);
    }

    void AddValue(const NUdf::TUnboxedValuePod& value, size_t idx) {
        Builders_[idx]->Add(value);
    }

    size_t Current_ = 0;
    bool IsFinished_ = false;
    size_t MaxLength_;
    size_t BuilderAllocatedSize_ = 0;
    size_t MaxBuilderAllocatedSize_ = 0;
    static const size_t MaxAllocatedFactor_ = 4;
    size_t InputRows_ = 0;
    size_t OutputRows_ = 0;
    size_t InputWidth_;
    size_t OutputWidth_;
    TUnboxedValueVector Inputs_;
    const TVector<ui32> LeftIOMap_;
    const std::vector<arrow::ValueDescr> InputsDescr_;
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<std::unique_ptr<IBlockItemConverter>> Converters_;
    TVector<std::unique_ptr<IArrayBuilder>> Builders_;
    TVector<NYql::NUdf::IBlockItemHasher::TPtr> Hashers_;
};

template <typename TDerived>
class TBlockStorageBase : public TComputationValue<TDerived> {
    using TSelf = TBlockStorageBase<TDerived>;
    using TBase = TComputationValue<TDerived>;

public:
    struct TBlock {
        size_t Size;
        std::vector<arrow::Datum> Columns;

        TBlock() = default;
        TBlock(size_t size, std::vector<arrow::Datum> columns)
            : Size(size)
            , Columns(std::move(columns))
        {}
    };

    struct TRowEntry {
        ui32 BlockOffset;
        ui32 ItemOffset;

        TRowEntry() = default;
        TRowEntry(ui32 blockOffset, ui32 itemOffset)
            : BlockOffset(blockOffset)
            , ItemOffset(itemOffset)
        {}
    };

    class TRowIterator {
        friend class TBlockStorageBase;

    public:
        TRowIterator() = default;

        TRowIterator(const TRowIterator&) = default;
        TRowIterator& operator=(const TRowIterator&) = default;

        TMaybe<TRowEntry> Next() {
            Y_ENSURE(IsValid());
            if (IsEmpty()) {
                return Nothing();
            }

            auto entry = TRowEntry(CurrentBlockOffset_, CurrentItemOffset_);

            auto& block = BlockStorage_->GetBlock(CurrentBlockOffset_);
            CurrentItemOffset_++;
            if (CurrentItemOffset_ == block.Size) {
                CurrentBlockOffset_++;
                CurrentItemOffset_ = 0;
            }

            return entry;
        }

        bool IsValid() const {
            return BlockStorage_;
        }

        bool IsEmpty() const {
            Y_ENSURE(IsValid());
            return CurrentBlockOffset_ >= BlockStorage_->GetBlockCount();
        }

    private:
        TRowIterator(const TSelf* blockStorage)
            : BlockStorage_(blockStorage)
        {}

    private:
        size_t CurrentBlockOffset_ = 0;
        size_t CurrentItemOffset_ = 0;

        const TSelf* BlockStorage_ = nullptr;
    };

    TBlockStorageBase(
        TMemoryUsageInfo* memInfo,
        const TVector<TType*>& itemTypes,
        NUdf::TUnboxedValue stream,
        arrow::MemoryPool* pool
    )
        : TBase(memInfo)
        , InputsDescr_(ToValueDescr(itemTypes))
        , Stream_(stream)
        , Inputs_(itemTypes.size())
    {
        TBlockTypeHelper helper;
        for (size_t i = 0; i < itemTypes.size(); i++) {
            TType* blockItemType = AS_TYPE(TBlockType, itemTypes[i])->GetItemType();
            Readers_.push_back(MakeBlockReader(TTypeInfoHelper(), blockItemType));
            Hashers_.push_back(helper.MakeHasher(blockItemType));
            Comparators_.push_back(helper.MakeComparator(blockItemType));
            Trimmers_.push_back(MakeBlockTrimmer(TTypeInfoHelper(), blockItemType, pool));
        }
    }

    NUdf::EFetchStatus FetchStream() {
        switch (Stream_.WideFetch(Inputs_.data(), Inputs_.size())) {
        case NUdf::EFetchStatus::Yield:
            return NUdf::EFetchStatus::Yield;
        case NUdf::EFetchStatus::Finish:
            return NUdf::EFetchStatus::Finish;
        case NUdf::EFetchStatus::Ok:
            break;
        }

        std::vector<arrow::Datum> blockColumns;
        for (size_t i = 0; i < Inputs_.size() - 1; i++) {
            auto& datum = TArrowBlock::From(Inputs_[i]).GetDatum();
            ARROW_DEBUG_CHECK_DATUM_TYPES(InputsDescr_[i], datum.descr());
            if (datum.is_scalar()) {
                blockColumns.push_back(datum);
            } else {
                MKQL_ENSURE(datum.is_array(), "Expecting array");
                blockColumns.push_back(Trimmers_[i]->Trim(datum.array()));
            }
        }

        auto blockSize = ::GetBlockCount(Inputs_[Inputs_.size() - 1]);
        Data_.emplace_back(blockSize, std::move(blockColumns));
        RowCount_ += blockSize;

        return NUdf::EFetchStatus::Ok;
    }

    const TBlock& GetBlock(size_t blockOffset) const {
        Y_ENSURE(blockOffset < GetBlockCount());
        return Data_[blockOffset];
    }

    size_t GetBlockCount() const {
        return Data_.size();
    }

    TRowEntry GetRowEntry(size_t blockOffset, size_t itemOffset) const {
        auto& block = GetBlock(blockOffset);
        Y_ENSURE(itemOffset < block.Size);
        return TRowEntry(blockOffset, itemOffset);
    }

    TRowIterator GetRowIterator() const {
        return TRowIterator(this);
    }

    TBlockItem GetItem(TRowEntry entry, ui32 columnIdx) const {
        Y_ENSURE(columnIdx < Inputs_.size() - 1);
        return GetItemFromBlock(GetBlock(entry.BlockOffset), columnIdx, entry.ItemOffset);
    }

    void GetRow(TRowEntry entry, const TVector<ui32>& ioMap, std::vector<NYql::NUdf::TBlockItem>& row) const {
        Y_ENSURE(row.size() == ioMap.size());
        for (size_t i = 0; i < row.size(); i++) {
            row[i] = GetItem(entry, ioMap[i]);
        }
    }

protected:
    TBlockItem GetItemFromBlock(const TBlock& block, ui32 columnIdx, size_t offset) const {
        Y_ENSURE(offset < block.Size);
        const auto& datum = block.Columns[columnIdx];
        if (datum.is_scalar()) {
            return Readers_[columnIdx]->GetScalarItem(*datum.scalar());
        } else {
            MKQL_ENSURE(datum.is_array(), "Expecting array");
            return Readers_[columnIdx]->GetItem(*datum.array(), offset);
        }
    }

protected:
    const std::vector<arrow::ValueDescr> InputsDescr_;

    TVector<std::unique_ptr<IBlockReader>> Readers_;
    TVector<NUdf::IBlockItemHasher::TPtr> Hashers_;
    TVector<NUdf::IBlockItemComparator::TPtr> Comparators_;
    TVector<IBlockTrimmer::TPtr> Trimmers_;

    std::vector<TBlock> Data_;
    size_t RowCount_ = 0;

    NUdf::TUnboxedValue Stream_;
    TUnboxedValueVector Inputs_;
};

class TBlockStorage: public TBlockStorageBase<TBlockStorage> {
private:
    using TBase = TBlockStorageBase<TBlockStorage>;
public:
    using TBase::TBase;
};

class TIndexedBlockStorage : public TBlockStorageBase<TIndexedBlockStorage> {
    using TBase = TBlockStorageBase<TIndexedBlockStorage>;

    struct TIndexNode {
        TRowEntry Entry;
        TIndexNode* Next;

        TIndexNode() = delete;
        TIndexNode(TRowEntry entry, TIndexNode* next = nullptr)
            : Entry(entry)
            , Next(next)
        {}
    };

    class TIndexMapValue {
    public:
        TIndexMapValue()
            : Raw(0)
        {}

        TIndexMapValue(TRowEntry entry) {
            TIndexEntryUnion un;
            un.Entry = entry;

            Y_ENSURE(((un.Raw << 1) >> 1) == un.Raw);
            Raw = (un.Raw << 1) | 1;
        }

        TIndexMapValue(TIndexNode* entryList)
            : EntryList(entryList)
        {}

        bool IsInplace() const {
            return Raw & 1;
        }

        TIndexNode* GetList() const {
            Y_ENSURE(!IsInplace());
            return EntryList;
        }

        TRowEntry GetEntry() const {
            Y_ENSURE(IsInplace());

            TIndexEntryUnion un;
            un.Raw = Raw >> 1;
            return un.Entry;
        }

    private:
        union TIndexEntryUnion {
            TRowEntry Entry;
            ui64 Raw;
        };

        union {
            TIndexNode* EntryList;
            ui64 Raw;
        };
    };

    using TIndexMap = TRobinHoodHashFixedMap<
        ui64,
        TIndexMapValue,
        std::equal_to<ui64>,
        std::hash<ui64>,
        TMKQLHugeAllocator<char>
    >;

    static_assert(sizeof(TIndexMapValue) == 8);
    static_assert(std::max(TIndexMap::GetCellSize(), static_cast<ui32>(sizeof(TIndexNode))) == BlockMapJoinIndexEntrySize);

public:
    class TIterator {
        friend class TIndexedBlockStorage;

        enum class EIteratorType {
            EMPTY,
            INPLACE,
            LIST
        };

    public:
        TIterator() = default;

        TIterator(const TIterator&) = delete;
        TIterator& operator=(const TIterator&) = delete;

        TIterator(TIterator&& other) {
            *this = std::move(other);
        }

        TIterator& operator=(TIterator&& other) {
            if (this != &other) {
                Type_ = other.Type_;
                BlockIndex_ = other.BlockIndex_;
                ItemsToLookup_ = std::move(other.ItemsToLookup_);

                switch (Type_) {
                case EIteratorType::EMPTY:
                    break;

                case EIteratorType::INPLACE:
                    Entry_ = other.Entry_;
                    EntryConsumed_ = other.EntryConsumed_;
                    break;

                case EIteratorType::LIST:
                    Node_ = other.Node_;
                    break;
                }

                other.BlockIndex_ = nullptr;
            }
            return *this;
        }

        TMaybe<TRowEntry> Next() {
            Y_ENSURE(IsValid());

            switch (Type_) {
            case EIteratorType::EMPTY:
                return Nothing();

            case EIteratorType::INPLACE:
                if (EntryConsumed_) {
                    return Nothing();
                }

                EntryConsumed_ = true;
                return BlockIndex_->IsKeyEquals(Entry_, ItemsToLookup_) ? TMaybe<TRowEntry>(Entry_) : Nothing();

            case EIteratorType::LIST:
                for (; Node_ != nullptr; Node_ = Node_->Next) {
                    if (BlockIndex_->IsKeyEquals(Node_->Entry, ItemsToLookup_)) {
                        auto entry = Node_->Entry;
                        Node_ = Node_->Next;
                        return entry;
                    }
                }

                return Nothing();
            }
        }

        bool IsValid() const {
            return BlockIndex_;
        }

        bool IsEmpty() const {
            Y_ENSURE(IsValid());

            switch (Type_) {
            case EIteratorType::EMPTY:
                return true;
            case EIteratorType::INPLACE:
                return EntryConsumed_;
            case EIteratorType::LIST:
                return Node_ == nullptr;
            }
        }

        void Reset() {
            *this = TIterator();
        }

    private:
        TIterator(const TIndexedBlockStorage* blockIndex)
            : Type_(EIteratorType::EMPTY)
            , BlockIndex_(blockIndex)
        {}

        TIterator(const TIndexedBlockStorage* blockIndex, TRowEntry entry, std::vector<NYql::NUdf::TBlockItem> itemsToLookup)
            : Type_(EIteratorType::INPLACE)
            , BlockIndex_(blockIndex)
            , Entry_(entry)
            , EntryConsumed_(false)
            , ItemsToLookup_(std::move(itemsToLookup))
        {}

        TIterator(const TIndexedBlockStorage* blockIndex, TIndexNode* node, std::vector<NYql::NUdf::TBlockItem> itemsToLookup)
            : Type_(EIteratorType::LIST)
            , BlockIndex_(blockIndex)
            , Node_(node)
            , ItemsToLookup_(std::move(itemsToLookup))
        {}

    private:
        EIteratorType Type_;
        const TIndexedBlockStorage* BlockIndex_ = nullptr;

        union {
            TIndexNode* Node_;
            struct {
                TRowEntry Entry_;
                bool EntryConsumed_;
            };
        };

        std::vector<NYql::NUdf::TBlockItem> ItemsToLookup_;
    };

public:
    TIndexedBlockStorage(
        TMemoryUsageInfo* memInfo,
        const TVector<TType*>& itemTypes,
        const TVector<ui32>& keyColumns,
        NUdf::TUnboxedValue stream,
        bool any,
        arrow::MemoryPool* pool
    )
        : TBase(memInfo, itemTypes, stream, pool)
        , KeyColumns_(keyColumns)
        , Any_(any)
    {}

    NUdf::EFetchStatus FetchStream() {
        Y_ENSURE(!Index_, "Data fetch shouldn't be done after the index has been built");
        return TBase::FetchStream();
    }

    void BuildIndex() {
        Index_ = std::make_unique<TIndexMap>(CalculateRHHashTableCapacity(RowCount_));
        for (size_t blockOffset = 0; blockOffset < Data_.size(); blockOffset++) {
            const auto& block = GetBlock(blockOffset);
            auto blockSize = block.Size;

            std::array<TRobinHoodBatchRequestItem<ui64>, PrefetchBatchSize> insertBatch;
            std::array<TRowEntry, PrefetchBatchSize> insertBatchEntries;
            std::array<std::vector<NYql::NUdf::TBlockItem>, PrefetchBatchSize> insertBatchKeys;
            ui32 insertBatchLen = 0;

            auto processInsertBatch = [&]() {
                Index_->BatchInsert({insertBatch.data(), insertBatchLen}, [&](size_t i, TIndexMap::iterator iter, bool isNew) {
                    auto value = static_cast<TIndexMapValue*>(Index_->GetMutablePayload(iter));
                    if (isNew) {
                        // Store single entry inplace
                        *value = TIndexMapValue(insertBatchEntries[i]);
                        Index_->CheckGrow();
                    } else {
                        if (Any_ && ContainsKey(value, insertBatchKeys[i])) {
                            return;
                        }

                        // Store as list
                        if (value->IsInplace()) {
                            *value = TIndexMapValue(InsertIndexNode(value->GetEntry()));
                        }

                        *value = TIndexMapValue(InsertIndexNode(insertBatchEntries[i], value->GetList()));
                    }
                });
            };

            Y_ENSURE(blockOffset <= std::numeric_limits<ui32>::max());
            Y_ENSURE(blockSize <= std::numeric_limits<ui32>::max());
            for (size_t itemOffset = 0; itemOffset < blockSize; itemOffset++) {
                ui64 keyHash = GetKey(block, itemOffset, insertBatchKeys[insertBatchLen]);
                if (!keyHash) {
                    continue;
                }

                insertBatchEntries[insertBatchLen] = TRowEntry(blockOffset, itemOffset);
                insertBatch[insertBatchLen].ConstructKey(keyHash);
                insertBatchLen++;

                if (insertBatchLen == PrefetchBatchSize) {
                    processInsertBatch();
                    insertBatchLen = 0;
                }
            }

            if (insertBatchLen > 0) {
                processInsertBatch();
            }
        }
    }

    template<typename TGetKey>
    void BatchLookup(size_t batchSize, std::array<TIndexedBlockStorage::TIterator, PrefetchBatchSize>& iterators, TGetKey&& getKey) {
        Y_ENSURE(batchSize <= PrefetchBatchSize);

        std::array<TRobinHoodBatchRequestItem<ui64>, PrefetchBatchSize> lookupBatch;
        std::array<std::vector<NYql::NUdf::TBlockItem>, PrefetchBatchSize> itemsBatch;

        for (size_t i = 0; i < batchSize; i++) {
            const auto& [items, keyHash] = getKey(i);
            lookupBatch[i].ConstructKey(keyHash);
            itemsBatch[i] = items;
        }

        Index_->BatchLookup({lookupBatch.data(), batchSize}, [&](size_t i, TIndexMap::iterator iter) {
            if (!iter) {
                // Empty iterator
                iterators[i] = TIterator(this);
                return;
            }

            auto value = static_cast<const TIndexMapValue*>(Index_->GetPayload(iter));
            if (value->IsInplace()) {
                iterators[i] = TIterator(this, value->GetEntry(), std::move(itemsBatch[i]));
            } else {
                iterators[i] = TIterator(this, value->GetList(), std::move(itemsBatch[i]));
            }
        });
    }

    bool IsKeyEquals(TRowEntry entry, const std::vector<NYql::NUdf::TBlockItem>& keyItems) const {
        Y_ENSURE(keyItems.size() == KeyColumns_.size());
        for (size_t i = 0; i < KeyColumns_.size(); i++) {
            auto indexItem = GetItem(entry, KeyColumns_[i]);
            if (Comparators_[KeyColumns_[i]]->Equals(indexItem, keyItems[i])) {
                return true;
            }
        }

        return false;
    }

private:
    ui64 GetKey(const TBlock& block, size_t offset, std::vector<NYql::NUdf::TBlockItem>& keyItems) const {
        ui64 keyHash = 0;
        keyItems.clear();
        for (ui32 keyColumn : KeyColumns_) {
            auto item = GetItemFromBlock(block, keyColumn, offset);
            if (!item) {
                keyItems.clear();
                return 0;
            }

            keyHash = CombineHashes(keyHash, Hashers_[keyColumn]->Hash(item));
            keyItems.push_back(std::move(item));
        }

        return keyHash;
    }

    TIndexNode* InsertIndexNode(TRowEntry entry, TIndexNode* currentHead = nullptr) {
        return &IndexNodes_.emplace_back(entry, currentHead);
    }

    bool ContainsKey(const TIndexMapValue* chain, const std::vector<NYql::NUdf::TBlockItem>& keyItems) const {
        if (chain->IsInplace()) {
            return IsKeyEquals(chain->GetEntry(), keyItems);
        } else {
            for (TIndexNode* node = chain->GetList(); node != nullptr; node = node->Next) {
                if (IsKeyEquals(node->Entry, keyItems)) {
                    return true;
                }

                node = node->Next;
            }

            return false;
        }
    }

private:
    const TVector<ui32>& KeyColumns_;

    std::unique_ptr<TIndexMap> Index_;
    std::deque<TIndexNode> IndexNodes_;

    const bool Any_;
};

template <bool WithoutRight, bool RightRequired, bool RightAny>
class TBlockMapJoinCoreWraper : public TMutableComputationNode<TBlockMapJoinCoreWraper<WithoutRight, RightRequired, RightAny>>
{
using TBaseComputation = TMutableComputationNode<TBlockMapJoinCoreWraper<WithoutRight, RightRequired, RightAny>>;
using TJoinState = TBlockJoinState<RightRequired>;
using TIndexState = TIndexedBlockStorage;
public:
    TBlockMapJoinCoreWraper(
        TComputationMutables& mutables,
        const TVector<TType*>&& resultItemTypes,
        const TVector<TType*>&& leftItemTypes,
        const TVector<ui32>&& leftKeyColumns,
        const TVector<ui32>&& leftIOMap,
        const TVector<TType*>&& rightItemTypes,
        const TVector<ui32>&& rightKeyColumns,
        const TVector<ui32>&& rightIOMap,
        IComputationNode* leftStream,
        IComputationNode* rightStream
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
        const auto joinState = ctx.HolderFactory.Create<TJoinState>(
            ctx,
            LeftItemTypes_,
            LeftIOMap_,
            ResultItemTypes_
        );
        const auto indexState = ctx.HolderFactory.Create<TIndexState>(
            RightItemTypes_,
            RightKeyColumns_,
            std::move(RightStream_->GetValue(ctx)),
            RightAny,
            &ctx.ArrowMemoryPool
        );

        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(joinState),
                                                      std::move(indexState),
                                                      std::move(LeftStream_->GetValue(ctx)),
                                                      LeftKeyColumns_,
                                                      std::move(RightStream_->GetValue(ctx)),
                                                      RightKeyColumns_,
                                                      RightIOMap_
        );
    }

private:
    class TStreamValue : public TComputationValue<TStreamValue> {
    using TBase = TComputationValue<TStreamValue>;
    public:
        TStreamValue(
            TMemoryUsageInfo* memInfo,
            const THolderFactory& holderFactory,
            NUdf::TUnboxedValue&& joinState,
            NUdf::TUnboxedValue&& indexState,
            NUdf::TUnboxedValue&& leftStream,
            const TVector<ui32>& leftKeyColumns,
            NUdf::TUnboxedValue&& rightStream,
            const TVector<ui32>& rightKeyColumns,
            const TVector<ui32>& rightIOMap
        )
            : TBase(memInfo)
            , JoinState_(joinState)
            , IndexState_(indexState)
            , LeftStream_(leftStream)
            , LeftKeyColumns_(leftKeyColumns)
            , RightStream_(rightStream)
            , RightKeyColumns_(rightKeyColumns)
            , RightIOMap_(rightIOMap)
            , HolderFactory_(holderFactory)
        {}

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
            auto& indexState = *static_cast<TIndexState*>(IndexState_.AsBoxed().Get());

            if (!RightStreamConsumed_) {
                auto fetchStatus = NUdf::EFetchStatus::Ok;
                while (fetchStatus != NUdf::EFetchStatus::Finish) {
                    fetchStatus = indexState.FetchStream();
                    if (fetchStatus == NUdf::EFetchStatus::Yield) {
                        return NUdf::EFetchStatus::Yield;
                    }
                }

                indexState.BuildIndex();
                RightStreamConsumed_ = true;
            }

            auto* inputFields = joinState.GetRawInputFields();
            const size_t inputWidth = joinState.GetInputWidth();
            const size_t outputWidth = joinState.GetOutputWidth();

            MKQL_ENSURE(width == outputWidth,
                        "The given width doesn't equal to the result type size");

            std::vector<NYql::NUdf::TBlockItem> leftKeyColumns(LeftKeyColumns_.size());
            std::vector<ui64> leftKeyColumnHashes(LeftKeyColumns_.size());
            std::vector<NYql::NUdf::TBlockItem> rightRow(RightIOMap_.size());

            while (!joinState.HasBlocks()) {
                while (joinState.IsNotFull() && LookupBatchCurrent_ < LookupBatchSize_) {
                    auto& iter = LookupBatchIterators_[LookupBatchCurrent_];
                    if constexpr (WithoutRight) {
                        if (bool(iter.IsEmpty()) != RightRequired) {
                            joinState.CopyRow();
                        }

                        joinState.NextRow();
                        LookupBatchCurrent_++;
                        continue;
                    } else if constexpr (!RightRequired) {
                        if (iter.IsEmpty()) {
                            joinState.MakeRow(std::vector<NYql::NUdf::TBlockItem>());
                            joinState.NextRow();
                            LookupBatchCurrent_++;
                            continue;
                        }
                    }

                    while (joinState.IsNotFull() && !iter.IsEmpty()) {
                        auto key = iter.Next();
                        indexState.GetRow(*key, RightIOMap_, rightRow);
                        joinState.MakeRow(rightRow);
                    }

                    if (iter.IsEmpty()) {
                        joinState.NextRow();
                        LookupBatchCurrent_++;
                    }
                }

                if (joinState.IsNotFull() && joinState.RemainingRowsCount() > 0) {
                    LookupBatchSize_ = std::min(PrefetchBatchSize, static_cast<ui32>(joinState.RemainingRowsCount()));
                    indexState.BatchLookup(LookupBatchSize_, LookupBatchIterators_, [&](size_t i) {
                        MakeLeftKeys(leftKeyColumns, leftKeyColumnHashes, i);
                        ui64 keyHash = CalculateTupleHash(leftKeyColumnHashes);
                        return std::make_pair(std::ref(leftKeyColumns), keyHash);
                    });

                    LookupBatchCurrent_ = 0;
                    continue;
                }

                if (joinState.IsNotFull() && !joinState.IsFinished()) {
                    switch (LeftStream_.WideFetch(inputFields, inputWidth)) {
                    case NUdf::EFetchStatus::Yield:
                        return NUdf::EFetchStatus::Yield;
                    case NUdf::EFetchStatus::Ok:
                        joinState.Reset();
                        continue;
                    case NUdf::EFetchStatus::Finish:
                        joinState.Finish();
                        break;
                    }
                    // Leave the loop, if no values left in the stream.
                    Y_DEBUG_ABORT_UNLESS(joinState.IsFinished());
                }
                if (joinState.IsEmpty()) {
                    return NUdf::EFetchStatus::Finish;
                }
                joinState.MakeBlocks(HolderFactory_);
            }

            const auto sliceSize = joinState.Slice();

            for (size_t i = 0; i < outputWidth; i++) {
                output[i] = joinState.Get(sliceSize, HolderFactory_, i);
            }

            return NUdf::EFetchStatus::Ok;
        }

        void MakeLeftKeys(std::vector<NYql::NUdf::TBlockItem>& items, std::vector<ui64>& hashes, size_t offset) const {
            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());

            Y_ENSURE(items.size() == LeftKeyColumns_.size());
            Y_ENSURE(hashes.size() == LeftKeyColumns_.size());
            for (size_t i = 0; i < LeftKeyColumns_.size(); i++) {
                std::tie(items[i], hashes[i]) = joinState.GetItemWithHash(LeftKeyColumns_[i], offset);
            }
        }

        NUdf::TUnboxedValue JoinState_;
        NUdf::TUnboxedValue IndexState_;

        NUdf::TUnboxedValue LeftStream_;
        const TVector<ui32>& LeftKeyColumns_;

        NUdf::TUnboxedValue RightStream_;
        const TVector<ui32>& RightKeyColumns_;
        const TVector<ui32>& RightIOMap_;
        bool RightStreamConsumed_ = false;

        std::array<typename TIndexState::TIterator, PrefetchBatchSize> LookupBatchIterators_;
        ui32 LookupBatchCurrent_ = 0;
        ui32 LookupBatchSize_ = 0;

        const THolderFactory& HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

private:
    const TVector<TType*> ResultItemTypes_;

    const TVector<TType*> LeftItemTypes_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<ui32> LeftIOMap_;

    const TVector<TType*> RightItemTypes_;
    const TVector<ui32> RightKeyColumns_;
    const TVector<ui32> RightIOMap_;

    IComputationNode* const LeftStream_;
    IComputationNode* const RightStream_;

    const TContainerCacheOnContext KeyTupleCache_;
};

class TBlockCrossJoinCoreWraper : public TMutableComputationNode<TBlockCrossJoinCoreWraper>
{
using TBaseComputation = TMutableComputationNode<TBlockCrossJoinCoreWraper>;
using TJoinState = TBlockJoinState<true>;
using TStorageState = TBlockStorage;
public:
    TBlockCrossJoinCoreWraper(
        TComputationMutables& mutables,
        const TVector<TType*>&& resultItemTypes,
        const TVector<TType*>&& leftItemTypes,
        const TVector<ui32>&& leftKeyColumns,
        const TVector<ui32>&& leftIOMap,
        const TVector<TType*>&& rightItemTypes,
        const TVector<ui32>&& rightKeyColumns,
        const TVector<ui32>&& rightIOMap,
        IComputationNode* leftStream,
        IComputationNode* rightStream
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
        const auto joinState = ctx.HolderFactory.Create<TJoinState>(
            ctx,
            LeftItemTypes_,
            LeftIOMap_,
            ResultItemTypes_
        );
        const auto indexState = ctx.HolderFactory.Create<TStorageState>(
            RightItemTypes_,
            std::move(RightStream_->GetValue(ctx)),
            &ctx.ArrowMemoryPool
        );

        return ctx.HolderFactory.Create<TStreamValue>(ctx.HolderFactory,
                                                      std::move(joinState),
                                                      std::move(indexState),
                                                      std::move(LeftStream_->GetValue(ctx)),
                                                      std::move(RightStream_->GetValue(ctx)),
                                                      RightIOMap_
        );
    }

private:
    class TStreamValue : public TComputationValue<TStreamValue> {
    using TBase = TComputationValue<TStreamValue>;
    public:
        TStreamValue(
            TMemoryUsageInfo* memInfo,
            const THolderFactory& holderFactory,
            NUdf::TUnboxedValue&& joinState,
            NUdf::TUnboxedValue&& storageState,
            NUdf::TUnboxedValue&& leftStream,
            NUdf::TUnboxedValue&& rightStream,
            const TVector<ui32>& rightIOMap
        )
            : TBase(memInfo)
            , JoinState_(joinState)
            , StorageState_(storageState)
            , LeftStream_(leftStream)
            , RightStream_(rightStream)
            , RightIOMap_(rightIOMap)
            , HolderFactory_(holderFactory)
        {}

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
            auto& joinState = *static_cast<TJoinState*>(JoinState_.AsBoxed().Get());
            auto& storageState = *static_cast<TStorageState*>(StorageState_.AsBoxed().Get());

            if (!RightStreamConsumed_) {
                auto fetchStatus = NUdf::EFetchStatus::Ok;
                while (fetchStatus != NUdf::EFetchStatus::Finish) {
                    fetchStatus = storageState.FetchStream();
                    if (fetchStatus == NUdf::EFetchStatus::Yield) {
                        return NUdf::EFetchStatus::Yield;
                    }
                }

                RightStreamConsumed_ = true;
                RightRowIterator_ = storageState.GetRowIterator();
            }

            auto* inputFields = joinState.GetRawInputFields();
            const size_t inputWidth = joinState.GetInputWidth();
            const size_t outputWidth = joinState.GetOutputWidth();

            MKQL_ENSURE(width == outputWidth,
                        "The given width doesn't equal to the result type size");

            std::vector<NYql::NUdf::TBlockItem> rightRow(RightIOMap_.size());
            while (!joinState.HasBlocks()) {
                while (!RightRowIterator_.IsEmpty() && joinState.RemainingRowsCount() > 0 && joinState.IsNotFull()) {
                    auto rowEntry = *RightRowIterator_.Next();
                    storageState.GetRow(rowEntry, RightIOMap_, rightRow);
                    joinState.MakeRow(rightRow);
                }

                if (joinState.IsNotFull() && joinState.RemainingRowsCount() > 0) {
                    joinState.NextRow();
                    RightRowIterator_ = storageState.GetRowIterator();
                    continue;
                }

                if (joinState.IsNotFull() && !joinState.IsFinished()) {
                    switch (LeftStream_.WideFetch(inputFields, inputWidth)) {
                    case NUdf::EFetchStatus::Yield:
                        return NUdf::EFetchStatus::Yield;
                    case NUdf::EFetchStatus::Ok:
                        joinState.Reset();
                        continue;
                    case NUdf::EFetchStatus::Finish:
                        joinState.Finish();
                        break;
                    }
                    // Leave the loop, if no values left in the stream.
                    Y_DEBUG_ABORT_UNLESS(joinState.IsFinished());
                }
                if (joinState.IsEmpty()) {
                    return NUdf::EFetchStatus::Finish;
                }
                joinState.MakeBlocks(HolderFactory_);
            }

            const auto sliceSize = joinState.Slice();

            for (size_t i = 0; i < outputWidth; i++) {
                output[i] = joinState.Get(sliceSize, HolderFactory_, i);
            }

            return NUdf::EFetchStatus::Ok;
        }

        NUdf::TUnboxedValue JoinState_;
        NUdf::TUnboxedValue StorageState_;

        NUdf::TUnboxedValue LeftStream_;

        NUdf::TUnboxedValue RightStream_;
        const TVector<ui32>& RightIOMap_;
        bool RightStreamConsumed_ = false;

        TStorageState::TRowIterator RightRowIterator_;

        const THolderFactory& HolderFactory_;
    };

    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

private:
    const TVector<TType*> ResultItemTypes_;

    const TVector<TType*> LeftItemTypes_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<ui32> LeftIOMap_;

    const TVector<TType*> RightItemTypes_;
    const TVector<ui32> RightKeyColumns_;
    const TVector<ui32> RightIOMap_;

    IComputationNode* const LeftStream_;
    IComputationNode* const RightStream_;

    const TContainerCacheOnContext KeyTupleCache_;
};

} // namespace

IComputationNode* WrapBlockMapJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
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
    Y_ENSURE(joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left ||
             joinKind == EJoinKind::LeftSemi || joinKind == EJoinKind::LeftOnly || joinKind == EJoinKind::Cross);

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

    if (joinKind == EJoinKind::Cross) {
        MKQL_ENSURE(leftKeyColumns.empty() && leftKeyDrops.empty() && rightKeyColumns.empty() && rightKeyDrops.empty(),
                    "Specifying key columns is not allowed for cross join");
    }
    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    const auto rightAnyNode = callable.GetInput(7);
    const auto rightAny = AS_VALUE(TDataLiteral, rightAnyNode)->AsValue().Get<bool>();

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
    if (joinKind == EJoinKind::Inner || joinKind == EJoinKind::Left || joinKind == EJoinKind::Cross) {
        for (size_t i = 0; i < rightStreamItems.size() - 1; i++) {
            if (rightKeyDrops.contains(i)) {
                continue;
            }
            rightIOMap.push_back(i);
        }
    } else {
        MKQL_ENSURE(rightKeyDrops.empty(), "Right key drops are not allowed for semi/only join");
    }

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);

#define JOIN_WRAPPER(WITHOUT_RIGHT, RIGHT_REQUIRED, RIGHT_ANY)                      \
    return new TBlockMapJoinCoreWraper<WITHOUT_RIGHT, RIGHT_REQUIRED, RIGHT_ANY>( \
        ctx.Mutables,                                                               \
        std::move(joinItems),                                                       \
        std::move(leftStreamItems),                                                 \
        std::move(leftKeyColumns),                                                  \
        std::move(leftIOMap),                                                       \
        std::move(rightStreamItems),                                                \
        std::move(rightKeyColumns),                                                 \
        std::move(rightIOMap),                                                      \
        leftStream,                                                                 \
        rightStream                                                                 \
    )

    switch (joinKind) {
    case EJoinKind::Inner:
        if (rightAny) {
            JOIN_WRAPPER(false, true, true);
        } else {
            JOIN_WRAPPER(false, true, false);
        }
    case EJoinKind::Left:
        if (rightAny) {
            JOIN_WRAPPER(false, false, true);
        } else {
            JOIN_WRAPPER(false, false, false);
        }
    case EJoinKind::LeftSemi:
        MKQL_ENSURE(rightIOMap.empty(), "Can't access right table on left semi join");
        if (rightAny) {
            JOIN_WRAPPER(true, true, true);
        } else {
            JOIN_WRAPPER(true, true, false);
        }
    case EJoinKind::LeftOnly:
        MKQL_ENSURE(rightIOMap.empty(), "Can't access right table on left only join");
        if (rightAny) {
            JOIN_WRAPPER(true, false, true);
        } else {
            JOIN_WRAPPER(true, false, false);
        }
    case EJoinKind::Cross:
        MKQL_ENSURE(!rightAny, "rightAny can't be used with cross join");
        return new TBlockCrossJoinCoreWraper(
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
    default:
        /* TODO: Display the human-readable join kind name. */
        MKQL_ENSURE(false, "BlockMapJoinCore doesn't support join type #"
                    << static_cast<ui32>(joinKind));
    }

#undef JOIN_WRAPPER
}

} // namespace NMiniKQL
} // namespace NKikimr
