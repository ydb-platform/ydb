#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor {

class IDataAdapter {
private:
    virtual TConclusion<std::shared_ptr<arrow::Schema>> DoBuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const = 0;
    virtual TConclusionStatus DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const = 0;

public:
    virtual ~IDataAdapter() = default;

    TConclusion<std::shared_ptr<arrow::Schema>> BuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const {
        return DoBuildSchemaForData(sourceArray);
    }
    TConclusionStatus AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const;
};

class TColumnElements {
private:
    YDB_READONLY_DEF(TString, Key);
    YDB_READONLY_DEF(std::vector<TStringBuf>, Values);
    std::vector<TString> ValuesStorage;
    YDB_READONLY_DEF(std::vector<ui32>, RecordIndexes);
    YDB_READONLY(ui32, DataSize, 0);
    std::shared_ptr<IChunkedArray> Accessor;

public:
    const std::shared_ptr<IChunkedArray>& GetAccessorVerified() const {
        AFL_VERIFY(!!Accessor);
        return Accessor;
    }

    void BuildSparsedAccessor(const ui32 recordsCount) {
        AFL_VERIFY(!Accessor);
        auto recordsBuilder = TSparsedArray::TBuilder::MakeRecordBuilders(arrow::utf8());
        for (ui32 idx = 0; idx < RecordIndexes.size(); ++idx) {
            recordsBuilder.Add(RecordIndexes[idx], Values[idx]);
        }
        TSparsedArray::TBuilder sparsedBuilder(nullptr, arrow::utf8());
        builder.AddChunk(recordsCount, recordsBuilder.FinishVerified());
        Accessor = sparsedBuilder.Finish();
    }

    void BuildPlainAccessor(const ui32 recordsCount) {
        AFL_VERIFY(!Accessor);
        auto builder = NArrow::MakeBuilder(arrow::utf8());
        auto* valueBuilder = std::static_cast<arrow::StringBuilder*>(builder.get());
        auto it = RecordIndexes.begin();
        for (ui32 idx = 0; idx < recordsCount; ++idx) {
            if (it == RecordIndexes.end() || idx < *it) {
                valueBuilder->AppendNull();
            } else if (idx == *it) {
                valueBuilder->Append(Values[it - RecordIndexes.begin()]);
                ++it;
            } else {
                AFL_VERIFY(false);
            }
        }
        Accessor = std::make_shared<TTrivialArray>(NArrow::Finish({ std::move(builder) }).front());
    }

    TColumnElements(const TString& key)
        : Key(key) {
    }

    void AddData(const TStringBuf sb, const ui32 index) {
        Values.emplace_back(sb);
        RecordIndexes.emplace_back(index);
        DataSize += sb.size();
    }

    void AddDataToOwn(const TString& value, const ui32 index) {
        ValuesStorage.emplace_back(value);
        AddData(TStringBuf(value.data(), value.size()), index);
    }
};

class TDataMerger {
private:
    std::vector<std::shared_ptr<TGeneralContainer>> ColumnContainers;
    std::vector<TOtherData> OtherDataContainers;
    std::vector<TDataStats> MergeStats;

public:
    void Add(const std::shared_ptr<TGeneralContainer>& columns, const TDataStats& columnStats, const TOtherData& other,
        const TDataStats& otherStats) {
        MergeStats.emplace_back(columnStats);
        MergeStats.emplace_back(otherStats);
        ColumnContainers.emplace_back(columns);
        OtherDataContainers.emplace_back(other);
    }
};

class TDataBuilder {
private:
    ui32 CurrentRecordIndex = 0;
    THashMap<TString, TColumnElements> Elements;
    std::deque<TString> Storage;
    static const uint32 ColumnAccessorsCountLimit = 1024;

public:
    TDataBuilder() {
    }

    void StartNextRecord() {
        ++CurrentRecordIndex;
    }

    void AddKV(const TStringBuf key, const TStringBuf value) {
        auto itElements = Elements.find(key.GetString());
        if (itElements == Elements.end()) {
            itElements = itElements.emplace(key.GetString(), key.GetString()).first;
        }
        itElements->second.AddData(value, CurrentRecordIndex);
    }

    void AddKVOwn(const TStringBuf key, const TString& value) {
        Storage.emplace_back(value);
        auto itElements = Elements.find(key.GetString());
        if (itElements == Elements.end()) {
            itElements = itElements.emplace(key.GetString(), key.GetString()).first;
        }
        itElements->second.AddData(Storage.back(), CurrentRecordIndex);
    }

    class THeapElements {
    private:
        const TColumnElements* Elements;
        ui32 Index = 0;
        const ui32 KeyIndex = 0;

    public:
        THeapElements(const TColumnElements* elements, const ui32 keyIndex)
            : Elements(elements)
            , KeyIndex(keyIndex) {
            AFL_VERIFY(Elements);
        }

        ui32 GetRecordIndex() const {
            return Elements->GetRecordIndexes()[Index];
        }

        const TString& GetKey() const {
            return Elements->GetKey();
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        TStringBuf GetValue() const {
            return Elements->GetValues()[Index];
        }

        bool operator<(const THeapElements& item) const {
            if (Elements->GetRecordIndexes()[Index] == item.Elements->GetRecordIndexes()[item.Index]) {
                return Elements->GetKey() < item.Elements->GetKey();
            } else {
                return item.Elements->GetRecordIndexes()[item.Index] < Elements->GetRecordIndexes()[Index];
            }
        }

        bool IsValid() const {
            return Index < Elements->GetRecordIndexes().size();
        }

        bool Next() {
            return ++Index < Elements->GetRecordIndexes().size();
        }
    };

    TDictStats BuildStats(const std::vector<TColumnElements*>& keys) const {
        auto builder = TDictStats::MakeBuilder();
        for (auto&& i : keys) {
            builder.Add(i->GetKey(), i->GetRecordIndexes().size(), i->GetDataSize());
        }
        return builder.Finish();
    }

    TOthersData MergeOthers(const std::vector<TColumnElements*>& otherKeys) const {
        std::vector<THeapElements> heap;
        ui32 idx = 0;
        for (auto&& i : otherKeys) {
            heap.emplace_back(i, idx);
            AFL_VERIFY(heap.back().IsValid());
            ++idx;
        }
        std::make_heap(heap.begin(), heap.end());
        auto othersBuilder = TOthersData::MakeBuilder();
        while (heap.size()) {
            std::pop_heap(heap.begin(), heap.end());
            othersBuilder.Add(heap.back().GetRecordIndex(), heap.back().GetKeyIndex(), heap.back().GetValue());
            if (!heap.back().Next()) {
                heap.pop_back();
            } else {
                std::push_heap(heap.begin(), heap.end());
            }
        }
        return othersBuilder.Finish(BuildStats(otherKeys));
    }

    std::shared_ptr<TSubColumnsArray> Finish() {
        ui64 sumDataSize = 0;
        std::map<ui64, std::vector<TColumnElements*>> elementsBySize;
        for (auto&& i : Elements) {
            sumDataSize += i.GetDataSize();
            elementsBySize[i.GetDataSize()].emplace_back(&i);
        }
        ui32 columnAccessorsCount = 0;
        ui64 columnsDataSize = 0;
        std::vector<TColumnElements*> columnElements;
        std::vector<TColumnElements*> otherElements;
        for (auto rIt = elementsBySize.rbegin(); rIt != elementsBySize.end(); ++rIt) {
            for (auto&& i : rIt->second) {
                if (columnAccessorsCount < ColumnAccessorsCountLimit) {
                    columnElements.emplace_back(i);
                    if (i->GetRecordIndexes().size() * 20 < CurrentRecordIndex) {
                        i->BuildSparsedAccessor(CurrentRecordIndex);
                    } else {
                        i->BuildPlainAccessor(CurrentRecordIndex);
                    }
                    columnsDataSize += i->GetDataSize();
                    ++columnAccessorsCount;
                } else {
                    otherElements.emplace_back(i);
                }
            }
        }
        const auto predSortElements = [](const TColumnElements* l, const TColumnElements* r) {
            return l->GetColumnName() < r->GetColumnName();
        };
        std::sort(columnElements.begin(), columnElements.end(), predSortElements);
        std::sort(otherElements.begin(), otherElements.end(), predSortElements);

        TOthersData rbOthers = MergeOthers(otherElements);

        TDictStats columnStats = BuildStats(columnElements);
        auto records = std::make_shared<TGeneralContainer>(CurrentRecordIndex);
        for (auto&& i : columnElements) {
            records->AddField(std::make_shared<arrow::Field>(i->GetColumnName(), arrow::utf8()), i->GetAccessorVerified());
        }
        TColumnsData cData(std::move(columnStats), std::move(records));
        return std::make_shared<TSubColumnsArray>(rbOthers, cData);
    }
};

class TFirstLevelSchemaData: public IDataAdapter {
private:
    virtual TConclusion<std::shared_ptr<arrow::Schema>> DoBuildSchemaForData(const std::shared_ptr<IChunkedArray>& sourceArray) const override;

    virtual TConclusionStatus DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const override;

public:
};

class TFirstLevelKVData: public IDataAdapter {
private:
    virtual TConclusion<std::shared_ptr<arrow::Schema>> DoBuildSchemaForData(
        const std::shared_ptr<IChunkedArray>& /*sourceArray*/) const override {
        return std::shared_ptr<arrow::Schema>();
    }

public:
};

}   // namespace NKikimr::NArrow::NAccessor
