#pragma once
#include "data_extractor.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/chunk_data.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

namespace NKikimr::NArrow::NAccessor {

class TDictStats {
private:
    std::shared_ptr<arrow::RecordBatch> Original;
    std::shared_ptr<arrow::StringArray> DataNames;
    std::shared_ptr<arrow::UInt32Array> DataRecordsCount;
    std::shared_ptr<arrow::UInt32Array> DataSize;

public:
    class TRTStats {
    private:
        YDB_READONLY_DEF(TString, KeyName);
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui32, DataSize, 0);

    public:
        void Add(const TDictStats& stats, const ui32 idx) {
            RecordsCount += stats.GetColumnRecordsCount(idx);
            DataSize += stats.GetColumnSize(idx);
        }
    };

    static TDictStats Merge(const std::vector<const TDictStats*>& stats) {
        std::map<TString, TRTStats> resultMap;
        for (auto&& i : stats) {
            for (ui32 idx = 0; idx < i->GetColumnsCount(); ++idx) {
                resultMap[i->GetColumnName(idx)].Add(*i, idx);
            }
        }
        auto builder = MakeBuilder();
        for (auto&& i : resultMap) {
            builder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize());
        }
        return builder.Finish();
    }

    class TSplittedColumns {
    private:
        TDictStats Columns;
        TDictStats Others;

    public:
        TDictStats ExtractColumns() {
            return std::move(Columns);
        }
        TDictStats ExtractOthers() {
            return std::move(Others);
        }
        TSplittedColumns(TDictStats&& columns, TDictStats&& others)
            : Column(std::move(columns))
            , Others(std::move(others)) {
        }
    };
    TSplittedColumns SplitByVolume(const ui32 columnsLimit) const {
        std::map<ui64, std::vector<TRTStats>> bySize;
        for (ui32 i = 0; i < GetColumnsCount(); ++i) {
            bySize[GetColumnSize(i)].emplace_back(GetRTStats(i));
        }
        std::vector<TRTStats> columnStats;
        std::vector<TRTStats> otherStats;
        for (auto it = bySize.rbegin(); it != bySize.rend(); ++it) {
            for (auto&& i : it->second) {
                if (columnStats.size() < columnsLimit) {
                    columnStats.emplace_back(std::move(i));
                } else {
                    otherStats.emplace_back(std::move(i));
                }
            }
        }
        std::sort(columnStats.begin(), columnStats.end());
        std::sort(otherStats.begin(), otherStats.end());
        auto columnsBuilder = MakeBuilder();
        auto othersBuilder = MakeBuilder();
        for (auto&& i : columnStats) {
            columnsBuilder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize());
        }
        for (auto&& i : otherStats) {
            othersBuilder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize());
        }
        return TSplittedColumns(columnsBuilder.Finish(), othersBuilder.Finish());
    }

    class TBuilder: TNonCopyable {
    private:
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        arrow::StringBuilder* Names;
        arrow::UInt32Builder* Records;
        arrow::UInt32Builder* DataSize;
        std::optional<TString> LastKeyName;
        ui32 RecordsCount = 0;

    public:
        TBuilder() {
            Builders = NArrow::MakeBuilders(GetSchema());
            AFL_VERIFY(Builders.size() == 3);
            AFL_VERIFY(Builders[0]->type()->id() == arrow::utf8()->id());
            AFL_VERIFY(Builders[1]->type()->id() == arrow::uint32()->id());
            AFL_VERIFY(Builders[2]->type()->id() == arrow::uint32()->id());
            Names = static_cast<arrow::StringBuilder*>(Builders[0].get());
            Records = static_cast<arrow::UInt32Builder*>(Builders[1].get());
            DataSize = static_cast<arrow::UInt32Builder*>(Builders[2].get());
        }

        void Add(const TString& name, const ui32 recordsCount, const ui32 dataSize) {
            AFL_VERIFY(Builders.size());
            if (!LastKeyName) {
                LastKeyName = name;
            } else {
                AFL_VERIFY(*LastKeyName < name);
            }
            Names->Append(name.data(), name.size());
            Records->Append(recordsCount);
            DataSize->Append(dataSize);
            ++RecordsCount;
        }

        TDictStats Finish() {
            AFL_VERIFY(Builders.size());
            auto arrays = NArrow::Finish(std::move(Builders));
            return TDictStats(arrow::RecordBatch::Make(GetSchema(), RecordsCount, arrays));
        }
    };

    TBuilder MakeBuilder() const {
        return TBuilder();
    }

    std::shared_ptr<arrow::Schema> BuildSchema() const {
        arrow::FieldVector fields;
        for (ui32 i = 0; i < DataNames->length(); ++i) {
            fields.emplace_back(std::make_shared<arrow::Field>(DataNames->GetView(i), arrow::utf8()));
        }
        return std::make_shared<arrow::Schema>(fields);
    }

    TRTStats GetRTStats(const ui32 index) const {
        return TRTStats(GetColumnName(index), GetColumnRecordsCount(index), GetColumnSize(index));
    }

    ui32 GetColumnsCount() const {
        return Original->num_rows();
    }

    std::string_view GetColumnName(const ui32 index) const {
        return DataNames->GetView(index);
    }

    ui32 GetColumnRecordsCount(const ui32 index) const {
        return DataRecordsCount->Value(index);
    }

    ui32 GetColumnSize(const ui32 index) const {
        return DataSize->Value(index);
    }

    static std::shared_ptr<arrow::Schema> GetSchema() {
        static arrow::FieldVector fields = { std::make_shared<arrow::Field>("name", arrow::utf8()),
            std::make_shared<arrow::Field>("count", arrow::uint32()), std::make_shared<arrow::Field>("size", arrow::uint32()) };
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    TDictStats(const std::shared_ptr<arrow::RecordBatch>& original)
        : Original(original) {
        AFL_VERIFY(Original->num_columns() == 3)("count", Original->num_columns());
        AFL_VERIFY(Original->column(0)->type()->id() == arrow::utf8()->id());
        AFL_VERIFY(Original->column(1)->type()->id() == arrow::uint32()->id());
        AFL_VERIFY(Original->column(2)->type()->id() == arrow::uint32()->id());
        DataNames = std::static_pointer_cast<arrow::StringArray>(Original->column(0));
        DataRecordsCount = std::static_pointer_cast<arrow::UInt32Array>(Original->column(1));
        DataSize = std::static_pointer_cast<arrow::UInt32Array>(Original->column(2));
    }
};

class TOthersData {
private:
    TDictStats Stats;
    std::shared_ptr<TGeneralContainer> Records;

public:
    class TIterator {
    private:
        const ui32 RecordsCount;
        ui32 CurrentIndex = 0;

        IChunkedArray::TReader RecordIndexReader;
        IChunkedArray::TAddress RecordIndexChunk;
        std::shared_ptr<arrow::UInt32Array> RecordIndex;

        IChunkedArray::TReader KeyIndexReader;
        IChunkedArray::TAddress KeyIndexChunk;
        std::shared_ptr<arrow::UInt32Array> KeyIndex;

        IChunkedArray::TReader ValuesReader;
        IChunkedArray::TAddress ValuesChunk;
        std::shared_ptr<arrow::StringArray> Values;

    public:
        TIterator(const std::shared_ptr<TGeneralContainer>& records)
            : RecordsCount(records->GetRecordsCount())
            , RecordIndexReader(records->GetColumnVerified(0))
            , KeyIndexReader(records->GetColumnVerified(1))
            , ValuesReader(records->GetColumnVerified(2)) {
            RecordIndexChunk = RecordIndexReader.GetReadChunk(0);
            AFL_VERIFY(RecordIndexChunk.GetArray()->length() == RecordsCount);
            KeyIndexChunk = KeyIndexReader.GetReadChunk(0);
            AFL_VERIFY(KeyIndexChunk.GetArray()->length() == RecordsCount);
            ValuesChunk = ValuesReader.GetReadChunk(0);
            AFL_VERIFY(ValuesChunk.GetArray()->length() == RecordsCount);
            RecordIndex = std::static_pointer_cast<arrow::UInt32Array>(RecordIndexChunk.GetArray());
            CAKeyIndex = std::static_pointer_cast<arrow::UInt32Array>(KeyIndexChunk.GetArray());
            Values = std::static_pointer_cast<arrow::StringArray>(ValuesChunk.GetArray());
            CurrentIndex = 0;
        }

        ui32 GetRecordIndex() const {
            return RecordIndex->Value(CurrentIndex);
        }

        ui32 GetKeyIndex() const {
            return KeyIndex->Value(CurrentIndex);
        }

        std::string_view GetKeyIndex() const {
            return Values->GetView(CurrentIndex);
        }

        bool Next() {
            return ++CurrentIndex < RecordsCount;
        }

        bool IsValid() const {
            return CurrentIndex < RecordsCount;
        }
    };

    TIterator BuildIterator() const {
        return TIterator(Records);
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    ui32 GetColumnsCount() const {
        return Records->num_rows();
    }

    static std::shared_ptr<arrow::Schema> GetSchema() {
        static arrow::FieldVector fields = { std::make_shared<arrow::Field>("record_idx", arrow::uint32()),
            std::make_shared<arrow::Field>("key", arrow::uint32()), std::make_shared<arrow::Field>("value", arrow::utf8()) };
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    TOthersData(const TDictStats& stats, const std::shared_ptr<TGeneralContainer>& records)
        : Stats(stats)
        , Records(records) {
        AFL_VERIFY(Records->num_columns() == 3)("count", Original->num_columns());
        AFL_VERIFY(Records->GetColumnVerified(0)->GetDataType()->id() == arrow::uint32()->id());
        AFL_VERIFY(Records->GetColumnVerified(1)->GetDataType()->id() == arrow::utf8()->id());
        AFL_VERIFY(Records->GetColumnVerified(2)->GetDataType()->id() == arrow::uint32()->id());
    }

    class TBuilderWithStats: TNonCopyable {
    private:
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        arrow::UInt32Builder* RecordIndex;
        arrow::UInt32Builder* KeyIndex;
        std::vector<ui32> RTKeyIndexes;
        arrow::StringBuilder* Values;
        std::optional<ui32> LastRecordIndex;
        std::optional<ui32> LastKeyIndex;
        ui32 RecordsCount = 0;
        YDB_READONLY_DEF(std::vector<ui32>, RecordsCountByKeyIndex);
        YDB_READONLY_DEF(std::vector<ui32>, BytesByKeyIndex);

    public:
        TBuilderWithStats() {
            Builders = NArrow::MakeBuilders(GetSchema());
            AFL_VERIFY(Builders.size() == 3);
            AFL_VERIFY(Builders[0]->type()->id() == arrow::uint32()->id());
            AFL_VERIFY(Builders[1]->type()->id() == arrow::uint32()->id());
            AFL_VERIFY(Builders[2]->type()->id() == arrow::utf8()->id());
            RecordIndex = static_cast<arrow::UInt32Builder*>(Builders[0].get());
            KeyIndex = static_cast<arrow::UInt32Builder*>(Builders[1].get());
            Values = static_cast<arrow::StringBuilder*>(Builders[2].get());
        }

        void Add(const ui32 recordIndex, const ui32 keyIndex, const std::string_view value) {
            AFL_VERIFY(Builders.size());
            if (RecordsCountByKeyIndex.size() <= keyIndex) {
                RecordsCountByKeyIndex.resize(RecordsCountByKeyIndex.size() * 2);
                BytesByKeyIndex.resize(BytesByKeyIndex.size() * 2);
            }
            ++RecordsCountByKeyIndex[keyIndex];
            BytesByKeyIndex[keyIndex] += value.size();
            if (!LastRecordIndex) {
                LastRecordIndex = recordIndex;
                LastKeyIndex = keyIndex;
            } else {
                AFL_VERIFY(*LastRecordIndex < recordIndex || (*LastRecordIndex == recordIndex && *LastKeyIndex < keyIndex));
            }
            RecordIndex->Append(recordIndex);
            RTKeyIndexes.emplace_back(keyIndex);
            Values->Append(value.data(), value.size());
            ++RecordsCount;
        }

        TOthersData Finish(const TDictStats& stats) {
            AFL_VERIFY(Builders.size());
            std::vector<ui32> toRemove;
            for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                if (!RecordsCountByKeyIndex[i]) {
                    toRemove.emplace_back(i);
                }
            }
            std::optional<TDictStats> resultStats;
            if (toRemove.size()) {
                auto dictBuilder = TDictStats::MakeBuilder();
                std::vector<ui32> remap;
                ui32 correctIdx = 0;
                for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                    if (!RecordsCountByKeyIndex[i]) {
                        remap.emplace_back(Max<ui32>());
                    } else {
                        remap.emplace_back(correctIdx++);
                        dictBuilder.Add(stats.GetColumnName(i), RecordsCountByKeyIndex[i], BytesByKeyIndex[i]);
                    }
                }
                for (ui32 idx = 0; RTKeyIndexes.size(); ++idx) {
                    AFL_VERIFY(RTKeyIndexes[idx] < remap.size());
                    RTKeyIndexes[idx] = remap[RTKeyIndexes[idx]];
                }
                resultStats = dictBuilder.Finish();
            } else {
                resultStats = Stats;
            }
            
            for (auto&& i : RTKeyIndexes) {
                KeyIndex->Append(i);
            }
            auto arrays = NArrow::Finish(std::move(Builders));
            return TOthersData(*resultStats, std::make_shared<TGeneralContainer>(arrow::RecordBatch::Make(GetSchema(), RecordsCount, arrays)));
        }
    };

    static TBuilderMerged MakeMergedBuilder() const {
        return TBuilderMerged();
    }
};

class TColumnsData {
private:
    TDictStats Stats;
    std::shared_ptr<TGeneralContainer> Records;

public:
    class TIterator {
    private:
        const ui32 KeyIndex;
        std::shared_ptr<IChunkedArray> ChunkedArray;
        std::shared_ptr<arrow::StringArray> CurrentArray;
        IChunkedArray::TAddress CurrentAddress;
        IChunkedArray::TReader Reader;
        ui32 CurrentIndex = 0;

        void InitArrays() {
            while (CurrentIndex < ChunkedArray->GetRecordsCount()) {
                CurrentAddress = Reader.GetReadChunk(CurrentIndex);
                AFL_VERIFY(CurrentAddress.GetPosition() == 0);
                AFL_VERIFY(CurrentAddress.GetArray()->type()->id() == arrow::utf8()->id());
                CurrentArray = std::static_pointer_cast<arrow::StringArray>(CurrentAddress.GetArray());
                if (ChunkedArray->GetType() != IChunkedArray::EType::SparsedArray || !CurrentArray->IsNull(0)) {
                    break;
                } else {
                    CurrentIndex += CurrentAddress.GetArray()->length();
                }
            }
        }

    public:
        TIterator(const ui32 keyIndex, const std::shared_ptr<IChunkedArray>& chunkedArray)
            : KeyIndex(keyIndex)
            , ChunkedArray(chunkedArray)
            , Reader(ChunkedArray) {
            InitArrays();
        }

        ui32 GetCurrentRecordIndex() const {
            return CurrentIndex;
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        std::string_view GetValue() const {
            return CurrentArray->GetView(CurrentAddress.GetPosition());
        }

        bool IsValid() const {
            return CurrentIndex < ChunkedArray->GetRecordsCount();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            while (true) {
                if (CurrentAddress.NextPosition()) {
                    AFL_VERIFY(++CurrentIndex < ChunkedArray->GetRecordsCount());
                    if (CurrentArray->IsNull(CurrentIndex)) {
                        continue;
                    }
                    return true;
                } else if (++CurrentIndex == ChunkedArray->GetRecordsCount()) {
                    return false;
                } else {
                    InitArrays();
                    return IsValid();
                }
            }
        }
    };

    TIterator BuildIterator(const ui32 keyIndex) const {
        return TIterator(KeyIndex, Records->GetColumnVerified(keyIndex));
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    TColumnsData(const std::shared_ptr<arrow::RecordBatch>& dict, const std::shared_ptr<TGeneralContainer>& data)
        : Stats(dict)
        , Records(data) {
        AFL_VERIFY(Records->num_columns() == Stats.GetColumnsCount());
        for (auto&& i : Records->GetColumns()) {
            AFL_VERIFY(i->GetDataType()->id() == arrow::utf8()->id());
        }
    }
};

class TGeneralIterator {
private:
    std::variant<TColumnsData::TIterator, TOthersData::TIterator> Iterator;
    std::optional<ui32> RemappedKey;
    std::vector<ui32> RemapKeys;

public:
    TGeneralIterator(TColumnsData::TIterator&& iterator, const std::optional<ui32> remappedKey)
        : Iterator(iterator)
        , RemappedKey(remappedKey) {
    }
    TGeneralIterator(TOthersData::TIterator&& iterator, const std::vector<ui32>& remapKeys)
        : Iterator(iterator)
        , RemapKeys(remapKeys) {
    }
    bool IsColumnKey() {
        struct TVisitor {
            bool operator(TOthersData::TIterator& iterator) {
                return false;
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return true;
            }
        };
        return Iterator.visit(TVisitor());
    }
    bool Next() {
        struct TVisitor {
            bool operator(TOthersData::TIterator& iterator) {
                return iterator.Next();
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return iterator.Next();
            }
        };
        return Iterator.visit(TVisitor());
    }
    bool IsValid() {
        struct TVisitor {
            bool operator(TOthersData::TIterator& iterator) {
                return iterator.IsValid();
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return iterator.IsValid();
            }
        };
        return Iterator.visit(TVisitor());
    }
    ui32 GetRecordIndex() {
        struct TVisitor {
            bool operator(TOthersData::TIterator& iterator) {
                return iterator.GetRecordIndex();
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return iterator.GetRecordIndex();
            }
        };
        return Iterator.visit(TVisitor());
    }
    ui32 GetKeyIndex() {
        struct TVisitor {
        private:
            const TGeneralIterator& Owner;

        public:
            TVisitor(const TGeneralIterator& owner)
                : Owner(owner) {
            }
            bool operator(TOthersData::TIterator& iterator) {
                return Owner.RemapKeys.size() ? Owner.RemapKeys[iterator.GetKeyIndex()] : iterator.GetKeyIndex();
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return Owner.RemappedKey.value_or(iterator.GetKeyIndex());
            }
        };
        return Iterator.visit(TVisitor());
    }
    std::string_view GetValue() {
        struct TVisitor {
            bool operator(TOthersData::TIterator& iterator) {
                return iterator.GetValue();
            }
            bool operator(TColumnsData::TIterator& iterator) {
                return iterator.GetValue();
            }
        };
        return Iterator.visit(TVisitor());
    }

    bool operator<(const TGeneralIterator& item) const {
        return std::tie(GetRecordIndex(), GetKeyIndex()) < std::tie(item.GetRecordIndex(), item.GetKeyIndex());
    }
};

class TReadIteratorUnorderedKeys {
private:
    TColumnsData ColumnsData;
    TOthersData OthersData;
    std::vector<TGeneralIterator> SortedIterators;

public:
    bool IsValid() const {
        return SortedIterators.size();
    }

    TReadIteratorUnorderedKeys(const TColumnsData& columnsData, const TOthersData& othersData)
        : ColumnsData(columnsData)
        , OthersData(othersData) {
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            SortedIterators.emplace_back(ColumnsData.BuildIterator(i));
        }
        SortedIterators.emplace_back(OthersData.BuildIterator());
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecords(const ui32 recordsCount, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        for (ui32 i = 0; i < recordsCount; ++i) {
            startRecordActor(i);
            for (ui32 iIter = 0; iIter < SortedIterators.size();) {
                auto& itColumn = SortedIterators[iIter];
                while (itColumn.GetCurrentRecordIndex() == i) {
                    kvActor(itColumn.GetKeyIndex(), itColumn.GetValue(), itColumn.IsColumn());
                    if (!itColumn.Next()) {
                        std::swap(SortedIterators[iIter], SortedIterators[SortedIterators.size() - 1]);
                        SortedIterators.pop_back();
                        break;
                    } else if (itColumn.GetCurrentRecordIndex() != i) {
                        ++iIter;
                        break;
                    }
                }
            }
            finishRecordActor();
        }
    }
};

class TReadIteratorOrderedKeys {
private:
    TColumnsData ColumnsData;
    TOthersData OthersData;
    std::vector<TGeneralIterator> SortedIterators;

public:
    bool IsValid() const {
        return SortedIterators.size();
    }

    const TDictStats& GetResultStats() const {
        return ResultStats;
    }

    TReadIteratorOrderedKeys(const TColumnsData& columnsData, const TOthersData& othersData)
        : ColumnsData(columnsData)
        , OthersData(othersData) {
        class TKeyAddress {
        private:
            const TString KeyName;
            const ui32 OriginalIndex = 0;
            YDB_READONLY(bool, IsColumn, false);

        public:
            ui32 GetOriginalIndex() const {
                return OriginalIndex;
            }

            TKeyAddress(const TString& keyName, const ui32 keyIndex, const bool isColumn)
                : KeyName(keyName)
                , KeyIndex(keyIndex)
                , IsColumn(isColumn) {
            }

            bool operator<(const TKeyAddress& item) const {
                return KeyName < item.KeyName;
            }
        };

        std::vector<TKeyAddress> addresses;
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount()) {
            addresses.emplace_back(ColumnsData.GetStats().GetColumnName(i), i, true);
        }
        for (ui32 i = 0; i < OthersData.GetStats().GetColumnsCount()) {
            addresses.emplace_back(OthersData.GetStats().GetColumnName(i), i, false);
        }
        std::sort(addresses.begin(), addresses.end());
        std::vector<ui32> remapColumns;
        remapColumns.resize(ColumnsData.GetStats().GetColumnsCount());
        std::vector<ui32> remapOthers;
        remapOthers.resize(OthersData.GetStats().GetColumnsCount());
        for (ui32 i = 0; i < addresses.size(); ++i) {
            if (i) {
                AFL_VERIFY(addresses[i].GetName() != addresses[i - 1].GetName());
            }
            if (addresses[i].GetIsColumn()) {
                remapColumns[addresses[i].GetOriginalIndex()] = i;
            } else {
                remapOthers[addresses[i].GetOriginalIndex()] = i;
            }
        }
        for (ui32 i = 0; i < ColumnsData.GetStats().GetColumnsCount(); ++i) {
            SortedIterators.emplace_back(ColumnsData.BuildIterator(i), remapColumns[i]);
        }
        SortedIterators.emplace_back(OthersData.BuildIterator(), remapOthers);
        std::make_heap(SortedIterators.begin(), SortedIterators.end());
    }

    template <class TStartRecordActor, class TKVActor, class TFinishRecordActor>
    void ReadRecords(const ui32 recordsCount, const TStartRecordActor& startRecordActor, const TKVActor& kvActor,
        const TFinishRecordActor& finishRecordActor) {
        for (ui32 i = 0; i < recordsCount; ++i) {
            startRecordActor(i);
            while (SortedIterators.size() && SortedIterators.front().GetRecordIndex() == i) {
                std::pop_heap(SortedIterators.begin(), SortedIterators.end());
                auto& itColumn = SortedIterators.back();
                kvActor(itColumn.GetKeyIndex(), itColumn.GetValue(), itColumn.IsColumnKey());
                if (!itColumn.Next()) {
                    SortedIterators.pop_back();
                } else {
                    std::push_heap(SortedIterators.begin(), SortedIterators.end());
                }
            }
            finishRecordActor();
        }
    }
};

class TSubColumnsArray: public IChunkedArray {
private:
    using TBase = IChunkedArray;
    TColumnsData ColumnsData;
    TOthersData OthersData;

protected:
    virtual std::shared_ptr<arrow::Scalar> DoGetMaxScalar() const override {
        return nullptr;
    }

    virtual std::vector<TChunkedArraySerialized> DoSplitBySizes(
        const TColumnLoader& loader, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) override;

    virtual TLocalDataAddress DoGetLocalData(const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const override {
        auto it = BuildUnorderedIterator();
        ui32 recordsCount = 0;
        auto builder = NArrow::MakeBuilder(arrow::binary());
        auto* binaryBuilder = static_cast<arrow::BinaryBuilder>(builder.get());
        while (it.IsValid()) {
            NJson::TJsonValue value;
            auto onStartRecord = [](const ui32 index) {
                AFL_VERIFY(recordsCount++ == index);
            };
            auto onFinishRecord = []() {
                auto bJson = NBinaryJson::SerializeToBinaryJson(value.GetStringRobust());
                if (const TString* val = std::get_if<TString>(&bJson)) {
                    AFL_VERIFY(false);
                } else if (const NBinaryJson::TBinaryJson* val = std::get_if<NBinaryJson::TBinaryJson>(&bJson)) {
                    binaryBuilder->Append(val->data(), val->size());
                } else {
                    AFL_VERIFY(false);
                }
            };
            auto onRecordKV = [&](const ui32 index, const std::string_view value, const bool isColumn) {
                if (isColumn) {
                    value.InsertValue(ColumnsData.GetStats().GetColumnName(index), value);
                } else {
                    value.InsertValue(OthersData.GetStats().GetColumnName(index), value);
                }
            };
            it.ReadRecords(1, onStartRecord, onRecordKV, onFinishRecord);
        }
        AFL_VERIFY(recordsCount == chunkCurrent.GetLength())("real", recordsCount)("expected", chunkCurrent.GetLength());
        return TLocalDataAddress(NArrow::FinishBuilder(builder), 0, 0);
    }
    virtual std::optional<ui64> DoGetRawSize() const override {
        return ColumnsData.GetRawSize() + OthersData.GetRawSize();
    }

public:
    TReadIteratorOrderedKeys BuildOrderedIterator() const {
        return TReadIteratorOrderedKeys(ColumnsData, OthersData);
    }

    TReadIteratorOrderedKeys BuildUnorderedIterator() const {
        return TReadIteratorUnorderedKeys(ColumnsData, OthersData);
    }

    const TColumnsData& GetColumnsData() const {
        return ColumnsData;
    }
    const TOthersData& GetOthersData() const {
        return OthersData;
    }

    TString SerializeToString(const TChunkConstructionData& externalInfo) const;

    TSubColumnsArray(const std::shared_ptr<arrow::RecordBatch>& batch);

    TSubColumnsArray(const std::shared_ptr<IChunkedArray>& sourceArray, const std::shared_ptr<IDataAdapter>& adapter);

    TSubColumnsArray(
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<TString>& columns, const TChunkConstructionData& externalInfo);

    TSubColumnsArray(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);

    virtual std::shared_ptr<arrow::Scalar> DoGetScalar(const ui32 /*index*/) const override {
        return nullptr;
    }
};

}   // namespace NKikimr::NArrow::NAccessor
