#pragma once
#include "settings.h"

#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TSplittedColumns;

class TDictStats {
private:
    std::shared_ptr<arrow::RecordBatch> Original;
    std::shared_ptr<arrow::StringArray> DataNames;
    std::shared_ptr<arrow::UInt32Array> DataRecordsCount;
    std::shared_ptr<arrow::UInt32Array> DataSize;
    std::shared_ptr<arrow::UInt8Array> AccessorType;

public:
    ui32 GetFilledValuesCount() const {
        ui32 result = 0;
        for (ui32 i = 0; i < (ui32)DataRecordsCount->length(); ++i) {
            result += DataRecordsCount->Value(i);
        }
        return result;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("key_names", NArrow::DebugJson(DataNames, 1000000, 1000000)["data"]);
        result.InsertValue("records", NArrow::DebugJson(DataRecordsCount, 1000000, 1000000)["data"]);
        result.InsertValue("size", NArrow::DebugJson(DataSize, 1000000, 1000000)["data"]);
        result.InsertValue("accessor", NArrow::DebugJson(AccessorType, 1000000, 1000000)["data"]);
        return result;
    }
    static TDictStats BuildEmpty();
    TString SerializeAsString(const std::shared_ptr<NSerialization::ISerializer>& serializer) const;

    std::optional<ui32> GetKeyIndexOptional(const std::string_view keyName) const {
        for (ui32 i = 0; i < DataNames->length(); ++i) {
            const auto arrView = DataNames->GetView(i);
            if (std::string_view(arrView.data(), arrView.size()) == keyName) {
                return i;
            }
        }
        return std::nullopt;
    }

    ui32 GetKeyIndexVerified(const std::string_view keyName) const {
        for (ui32 i = 0; i < DataNames->length(); ++i) {
            const auto arrView = DataNames->GetView(i);
            if (std::string_view(arrView.data(), arrView.size()) == keyName) {
                return i;
            }
        }
        AFL_VERIFY(false);
        return 0;
    }

    class TRTStatsValue {
    private:
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui32, DataSize, 0);

    public:
        TRTStatsValue() = default;
        TRTStatsValue(const ui32 recordsCount, const ui32 dataSize)
            : RecordsCount(recordsCount)
            , DataSize(dataSize) {
        }

        void AddValue(const std::string_view str) {
            ++RecordsCount;
            DataSize += str.size();
        }

        void Add(const TDictStats& stats, const ui32 idx) {
            RecordsCount += stats.GetColumnRecordsCount(idx);
            DataSize += stats.GetColumnSize(idx);
        }

        IChunkedArray::EType GetAccessorType(const TSettings& settings, const ui32 recordsCount) const {
            return settings.IsSparsed(RecordsCount, recordsCount) ? IChunkedArray::EType::SparsedArray : IChunkedArray::EType::Array;
        }
    };

    class TRTStats: public TRTStatsValue {
    private:
        using TBase = TRTStatsValue;
        YDB_READONLY_DEF(TString, KeyName);

    public:
        TRTStats(const TString& keyName)
            : KeyName(keyName) {
        }
        TRTStats(const TString& keyName, const ui32 recordsCount, const ui32 dataSize)
            : TBase(recordsCount, dataSize)
            , KeyName(keyName) {
        }

        TRTStats(const std::string_view keyName)
            : KeyName(keyName.data(), keyName.size()) {
        }
        TRTStats(const std::string_view keyName, const ui32 recordsCount, const ui32 dataSize)
            : TBase(recordsCount, dataSize)
            , KeyName(keyName.data(), keyName.size()) {
        }

        bool operator<(const TRTStats& item) const {
            return KeyName < item.KeyName;
        }
    };

    static TDictStats Merge(const std::vector<const TDictStats*>& stats, const TSettings& settings, const ui32 recordsCount);

    TSplittedColumns SplitByVolume(const TSettings& settings, const ui32 recordsCount) const;

    class TBuilder: TNonCopyable {
    private:
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        arrow::StringBuilder* Names;
        arrow::UInt32Builder* Records;
        arrow::UInt32Builder* DataSize;
        arrow::UInt8Builder* AccessorType;

        std::optional<TString> LastKeyName;
        ui32 RecordsCount = 0;

    public:
        TBuilder();
        void Add(const TString& name, const ui32 recordsCount, const ui32 dataSize, const IChunkedArray::EType accessorType);
        void Add(const std::string_view name, const ui32 recordsCount, const ui32 dataSize, const IChunkedArray::EType accessorType);
        TDictStats Finish();
    };

    static TBuilder MakeBuilder() {
        return TBuilder();
    }

    std::shared_ptr<arrow::Schema> BuildColumnsSchema() const {
        arrow::FieldVector fields;
        for (ui32 i = 0; i < DataNames->length(); ++i) {
            const auto view = DataNames->GetView(i);
            fields.emplace_back(std::make_shared<arrow::Field>(std::string(view.data(), view.size()), arrow::utf8()));
        }
        return std::make_shared<arrow::Schema>(fields);
    }

    std::shared_ptr<arrow::Field> GetField(const ui32 index) const {
        AFL_VERIFY(index < DataNames->length());
        auto name = DataNames->GetView(index);
        return std::make_shared<arrow::Field>(std::string(name.data(), name.size()), arrow::utf8());
    }

    TRTStats GetRTStats(const ui32 index) const {
        auto view = GetColumnName(index);
        return TRTStats(TString(view.data(), view.size()), GetColumnRecordsCount(index), GetColumnSize(index));
    }

    ui32 GetColumnsCount() const {
        return Original->num_rows();
    }

    TConstructorContainer GetAccessorConstructor(const ui32 columnIndex) const;
    IChunkedArray::EType GetAccessorType(const ui32 columnIndex) const;

    std::string_view GetColumnName(const ui32 index) const;
    TString GetColumnNameString(const ui32 index) const {
        auto view = GetColumnName(index);
        return TString(view.data(), view.size());
    }
    ui32 GetColumnRecordsCount(const ui32 index) const;
    ui32 GetColumnSize(const ui32 index) const;

    static std::shared_ptr<arrow::Schema> GetStatsSchema() {
        static arrow::FieldVector fields = { std::make_shared<arrow::Field>("name", arrow::utf8()),
            std::make_shared<arrow::Field>("count", arrow::uint32()), std::make_shared<arrow::Field>("size", arrow::uint32()),
            std::make_shared<arrow::Field>("accessor_type", arrow::uint8()) };
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    bool IsSparsed(const ui32 columnIndex, const ui32 recordsCount, const TSettings& settings) const;
    TDictStats(const std::shared_ptr<arrow::RecordBatch>& original);
};

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
        : Columns(std::move(columns))
        , Others(std::move(others)) {
    }
};
}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
