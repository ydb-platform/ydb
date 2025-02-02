#pragma once
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

public:
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

    class TRTStats {
    private:
        YDB_READONLY_DEF(TString, KeyName);
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui32, DataSize, 0);

    public:
        TRTStats(const TString& keyName)
            : KeyName(keyName) {
        }
        TRTStats(const TString& keyName, const ui32 recordsCount, const ui32 dataSize)
            : KeyName(keyName)
            , RecordsCount(recordsCount)
            , DataSize(dataSize) {
        }

        TRTStats(const std::string_view keyName)
            : KeyName(keyName.data(), keyName.size()) {
        }
        TRTStats(const std::string_view keyName, const ui32 recordsCount, const ui32 dataSize)
            : KeyName(keyName.data(), keyName.size())
            , RecordsCount(recordsCount)
            , DataSize(dataSize) {
        }

        void Add(const TDictStats& stats, const ui32 idx) {
            RecordsCount += stats.GetColumnRecordsCount(idx);
            DataSize += stats.GetColumnSize(idx);
        }

        bool operator<(const TRTStats& item) const {
            return KeyName < item.KeyName;
        }
    };

    static TDictStats Merge(const std::vector<const TDictStats*>& stats);

    TSplittedColumns SplitByVolume(const ui32 columnsLimit) const;

    class TBuilder: TNonCopyable {
    private:
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
        arrow::StringBuilder* Names;
        arrow::UInt32Builder* Records;
        arrow::UInt32Builder* DataSize;
        std::optional<TString> LastKeyName;
        ui32 RecordsCount = 0;

    public:
        TBuilder();
        void Add(const TString& name, const ui32 recordsCount, const ui32 dataSize);
        void Add(const std::string_view name, const ui32 recordsCount, const ui32 dataSize);
        TDictStats Finish();
    };

    static TBuilder MakeBuilder() {
        return TBuilder();
    }

    std::shared_ptr<arrow::Schema> BuildSchema() const {
        arrow::FieldVector fields;
        for (ui32 i = 0; i < DataNames->length(); ++i) {
            const auto view = DataNames->GetView(i);
            fields.emplace_back(std::make_shared<arrow::Field>(std::string(view.data(), view.size()), arrow::utf8()));
        }
        return std::make_shared<arrow::Schema>(fields);
    }

    TRTStats GetRTStats(const ui32 index) const {
        auto view = GetColumnName(index);
        return TRTStats(TString(view.data(), view.size()), GetColumnRecordsCount(index), GetColumnSize(index));
    }

    ui32 GetColumnsCount() const {
        return Original->num_rows();
    }

    std::string_view GetColumnName(const ui32 index) const;
    TString GetColumnNameString(const ui32 index) const {
        auto view = GetColumnName(index);
        return TString(view.data(), view.size());
    }
    ui32 GetColumnRecordsCount(const ui32 index) const;
    ui32 GetColumnSize(const ui32 index) const;

    static std::shared_ptr<arrow::Schema> GetSchema() {
        static arrow::FieldVector fields = { std::make_shared<arrow::Field>("name", arrow::utf8()),
            std::make_shared<arrow::Field>("count", arrow::uint32()), std::make_shared<arrow::Field>("size", arrow::uint32()) };
        static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(fields);
        return result;
    }

    TConstructorContainer GetAccessorConstructor(const ui32 columnIndex, const ui32 recordsCount) const;
    bool IsSparsed(const ui32 columnIndex, const ui32 recordsCount) const;
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
