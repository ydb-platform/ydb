#pragma once
#include "view_v0.h"

#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TSimpleRow;

class TSimpleRowContent {
private:
    YDB_READONLY_DEF(TString, Data);

public:
    TSimpleRowContent(const TString& data)
        : Data(data) {
    }

    TSimpleRow Build(const std::shared_ptr<arrow::Schema>& schema, const std::vector<NScheme::TTypeInfo>& logicalTypes) const;
    ui64 GetMemorySize() const {
        return Data.capacity();
    }
    ui64 GetDataSize() const {
        return Data.size();
    }
};
namespace NMerger {
class TSortableBatchPosition;
}

class TSimpleRow {
private:
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, Schema);
    YDB_READONLY_DEF(std::vector<NScheme::TTypeInfo>, LogicalTypes);

public:
    TSimpleRowContent GetContent() const {
        return TSimpleRowContent(Data);
    }

    ui32 GetMemorySize() const {
        return Data.capacity() + LogicalTypes.capacity() * sizeof(NScheme::TTypeInfo);
    }

    ui32 GetDataSize() const {
        return Data.size();
    }

    NMerger::TSortableBatchPosition BuildSortablePosition(const bool reverse = false) const;

    explicit TSimpleRow(const NMerger::TSortableBatchPosition& position);

    TSimpleRow(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 recordIndex, const std::vector<NScheme::TTypeInfo>& logicalTypes)
        : LogicalTypes(logicalTypes) {
        AFL_VERIFY(batch);
        AFL_VERIFY(logicalTypes.size() == (size_t)batch->num_columns());
        Schema = batch->schema();
        Data = TSimpleRowViewV0::BuildString(batch, recordIndex);
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(Schema));
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

    TSimpleRow(const TString& data, const std::shared_ptr<arrow::Schema>& schema, const std::vector<NScheme::TTypeInfo>& logicalTypes)
        : Data(data)
        , Schema(schema)
        , LogicalTypes(logicalTypes) {
        AFL_VERIFY(logicalTypes.size() == (size_t)schema->num_fields());
        AFL_VERIFY_DEBUG(TSimpleRowViewV0(Data).DoValidate(Schema));
    }

    TString DebugString() const {
        return TSimpleRowViewV0(Data).DebugString(Schema).GetResult();
    }

    std::partial_ordering ComparePartNotNull(const TSimpleRow& item, const ui32 columnsCount) const;

    std::partial_ordering CompareNotNull(const TSimpleRow& item) const;

    std::partial_ordering operator<=>(const TSimpleRow& item) const;
    bool operator==(const TSimpleRow& item) const;
};

// Does not own: the bytes, schema, and logical types must outlive this view.
class TSimpleRowRef {
private:
    TSimpleRowViewV0 Row;
    const arrow::Schema& Schema;
    const std::vector<NScheme::TTypeInfo>& LogicalTypes;

public:
    TSimpleRowRef(const TSimpleRowViewV0 row, const arrow::Schema& schema, const std::vector<NScheme::TTypeInfo>& logicalTypes)
        : Row(row)
        , Schema(schema)
        , LogicalTypes(logicalTypes) {
        AFL_VERIFY(logicalTypes.size() == (size_t)schema.num_fields());
    }

    TSimpleRowRef(TSimpleRowViewV0, const arrow::Schema&, const std::vector<NScheme::TTypeInfo>&&) = delete;

    std::partial_ordering operator<=>(const TSimpleRowRef& item) const {
        AFL_VERIFY_DEBUG(Schema.Equals(item.Schema));
        AFL_VERIFY_DEBUG(LogicalTypes == item.LogicalTypes);
        return Row.Compare(item.Row, Schema, LogicalTypes).GetResult();
    }

    bool operator==(const TSimpleRowRef& item) const {
        return (*this <=> item) == std::partial_ordering::equivalent;
    }
};

}   // namespace NKikimr::NArrow
