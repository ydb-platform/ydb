#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NArrow {

// Interface to accept rows that are read form arrow batch
class IRowWriter {
public:
    virtual ~IRowWriter() = default;

    // NOTE: This method must copy cells data to its own strorage
    virtual void AddRow(const TConstArrayRef<TCell>& cells) = 0;
};

// Converts an arrow batch into YDB rows feeding them IRowWriter one by one
class TArrowToYdbConverter {
private:
    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema_; // Destination schema (allow shrink and reorder)
    IRowWriter& RowWriter_;

    template <typename TArray>
    TCell MakeCellFromValue(const std::shared_ptr<arrow::Array>& column, i64 row) {
        auto array = std::static_pointer_cast<TArray>(column);
        return TCell::Make(array->Value(row));
    }

    template <typename TArray>
    TCell MakeCellFromView(const std::shared_ptr<arrow::Array>& column, i64 row) {
        auto array = std::static_pointer_cast<TArray>(column);
        auto data = array->GetView(row);
        return TCell(data.data(), data.size());
    }

    template <typename TArrayType>
    TCell MakeCell(const std::shared_ptr<arrow::Array>& column, i64 row) {
        return MakeCellFromValue<TArrayType>(column, row);
    }

    template <>
    TCell MakeCell<arrow::BinaryArray>(const std::shared_ptr<arrow::Array>& column, i64 row) {
        return MakeCellFromView<arrow::BinaryArray>(column, row);
    }

    template <>
    TCell MakeCell<arrow::StringArray>(const std::shared_ptr<arrow::Array>& column, i64 row) {
        return MakeCellFromView<arrow::StringArray>(column, row);
    }

    template <>
    TCell MakeCell<arrow::Decimal128Array>(const std::shared_ptr<arrow::Array>& column, i64 row) {
        return MakeCellFromView<arrow::Decimal128Array>(column, row);
    }

public:
    static bool NeedDataConversion(const NScheme::TTypeInfo& colType);

    static bool NeedInplaceConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType);

    static bool NeedConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType);

    TArrowToYdbConverter(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema, IRowWriter& rowWriter)
        : YdbSchema_(ydbSchema)
        , RowWriter_(rowWriter)
    {}

    bool Process(const arrow::RecordBatch& batch, TString& errorMessage);
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                  const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert);
arrow::Result<std::shared_ptr<arrow::RecordBatch>> InplaceConvertColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                         const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert);

} // namespace NKikimr::NArrow
