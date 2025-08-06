#pragma once
#include "view_v0.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TSimpleRow;

class TSimpleRowView {
private:
    const TStringBuf Data;
    const arrow::Schema* Schema = nullptr;

public:
    TSimpleRowView(const TStringBuf data, const arrow::Schema& schema)
        : Data(data)
        , Schema(&schema) {
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(schema));
    }

    ui32 GetColumnsCount() const {
        return Schema->num_fields();
    }

    TString DebugString() const {
        return TSimpleRowViewV0(Data).DebugString(*Schema).GetResult();
    }

    std::partial_ordering ComparePartNotNull(const TSimpleRowView& item, const ui32 columnsCount) const;

    std::partial_ordering CompareNotNull(const TSimpleRowView& item) const;

    std::partial_ordering operator<=>(const TSimpleRowView& item) const;
    bool operator==(const TSimpleRowView& item) const;
};

class TSimpleRowContent {
private:
    YDB_READONLY_DEF(TString, Data);

public:
    TSimpleRowContent(TString&& data)
        : Data(std::move(data)) {
    }

    TSimpleRowContent(const TString& data)
        : Data(data) {
    }

    TSimpleRow Build(const std::shared_ptr<arrow::Schema>& schema) const;
    ui64 GetMemorySize() const {
        return Data.capacity();
    }
    ui64 GetDataSize() const {
        return Data.size();
    }
    TSimpleRowView GetView(const arrow::Schema& schema) const {
        return TSimpleRowView(Data, schema);
    }
};
namespace NMerger {
class TSortableBatchPosition;
}
class TRowsCollection;

class TSimpleRow {
private:
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);

    TSimpleRow(const TString& data, const std::shared_ptr<arrow::Schema>& schema)
        : Data(data)
        , Schema(schema) {
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(*Schema));
    }

    friend class TRowsCollection;
    friend class TSimpleRowContent;

public:
    static TSimpleRow BuildWithCopy(const TString& data, const std::shared_ptr<arrow::Schema>& schema) {
        return TSimpleRow(data, schema);
    }

    TSimpleRowView GetView() const {
        return TSimpleRowView(Data, *Schema);
    }

    TSimpleRowContent GetContent() const {
        return TSimpleRowContent(Data);
    }

    ui32 GetMemorySize() const {
        return Data.capacity();
    }

    ui32 GetDataSize() const {
        return Data.size();
    }

    NMerger::TSortableBatchPosition BuildSortablePosition(const bool reverse = false) const;

    TSimpleRow(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex) {
        AFL_VERIFY(batch);
        Schema = batch->schema();
        Data = TSimpleRowViewV0::BuildString(batch, recordIndex);
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(*Schema));
    }

    std::shared_ptr<arrow::RecordBatch> ToBatch() const;
    [[nodiscard]] TConclusionStatus AddToBuilders(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
        return TSimpleRowViewV0(Data).AddToBuilders(builders, Schema);
    }

    ui32 GetColumnsCount() const {
        return Schema->num_fields();
    }

    template <class T>
    std::optional<T> GetValue(const TString& columnName) const {
        const int fIndex = Schema->GetFieldIndex(columnName);
        AFL_VERIFY(fIndex >= 0);
        return GetValue<T>(fIndex);
    }

    std::shared_ptr<arrow::Scalar> GetScalar(const ui32 columnIndex) const {
        return TSimpleRowViewV0(Data).GetScalar(columnIndex, Schema).DetachResult();
    }

    template <class T>
    T GetValueVerified(const TString& columnName) const {
        const int fIndex = Schema->GetFieldIndex(columnName);
        AFL_VERIFY(fIndex >= 0);
        return GetValueVerified<T>(fIndex);
    }

    template <class T>
    std::optional<T> GetValue(const ui32 columnIndex) const {
        return TSimpleRowViewV0(Data).GetValue<T>(columnIndex, Schema).DetachResult();
    }

    template <class T>
    T GetValueVerified(const ui32 columnIndex) const {
        const auto result = TSimpleRowViewV0(Data).GetValue<T>(columnIndex, Schema).DetachResult();
        AFL_VERIFY(!!result);
        return *result;
    }

    TSimpleRow(TString&& data, const std::shared_ptr<arrow::Schema>& schema)
        : Data(std::move(data))
        , Schema(schema) {
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(*Schema));
    }

    TString DebugString() const {
        return TSimpleRowViewV0(Data).DebugString(Schema).GetResult();
    }

    std::partial_ordering ComparePartNotNull(const TSimpleRow& item, const ui32 columnsCount) const;

    std::partial_ordering CompareNotNull(const TSimpleRow& item) const;

    std::partial_ordering operator<=>(const TSimpleRow& item) const;
    bool operator==(const TSimpleRow& item) const;
};

}   // namespace NKikimr::NArrow
