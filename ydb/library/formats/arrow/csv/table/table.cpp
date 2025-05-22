#include "table.h"
#include <util/string/join.h>

namespace NKikimr::NFormats {

arrow::Result<TArrowCSV> TArrowCSVTable::Create(const std::vector<NYdb::NTable::TTableColumn>& columns, bool header) {
    TVector<TString> errors;
    TColummns convertedColumns;
    convertedColumns.reserve(columns.size());
    std::set<std::string> notNullColumns;
    for (auto& column : columns) {
        const auto arrowType = GetArrowType(column.Type);
        if (!arrowType.ok()) {
            errors.emplace_back("column " + column.Name + ": " + arrowType.status().ToString());
            continue;
        }
        const auto csvArrowType = GetCSVArrowType(column.Type);
        if (!csvArrowType.ok()) {
            errors.emplace_back("column " + column.Name + ": " + csvArrowType.status().ToString());
            continue;
        }
        convertedColumns.emplace_back(TColumnInfo{TString{column.Name}, *arrowType, *csvArrowType});
        if (NYdb::TTypeParser(column.Type).GetKind() != NYdb::TTypeParser::ETypeKind::Optional || column.NotNull.value_or(false)) {
            notNullColumns.emplace(column.Name);
        }
    }
    if (!errors.empty()) {
        return arrow::Status::TypeError(ErrorPrefix() + "columns errors: " + JoinSeq("; ", errors));
    }
    return TArrowCSVTable(convertedColumns, header, notNullColumns);
}

NYdb::TTypeParser TArrowCSVTable::ExtractType(const NYdb::TType& type) {
    NYdb::TTypeParser tp(type);
    if (tp.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        tp.OpenOptional();
    }
    return std::move(tp);
}

arrow::Result<std::shared_ptr<arrow::DataType>> TArrowCSVTable::GetArrowType(const NYdb::TType& type) {
    auto tp = ExtractType(type);
    switch (tp.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Decimal:
        return arrow::decimal(tp.GetDecimal().Precision, tp.GetDecimal().Scale);
    case NYdb::TTypeParser::ETypeKind::Primitive:
        switch (tp.GetPrimitive()) {
            case NYdb::EPrimitiveType::Bool:
                return arrow::boolean();
            case NYdb::EPrimitiveType::Int8:
                return arrow::int8();
            case NYdb::EPrimitiveType::Uint8:
                return arrow::uint8();
            case NYdb::EPrimitiveType::Int16:
                return arrow::int16();
            case NYdb::EPrimitiveType::Date:
                return arrow::uint16();
            case NYdb::EPrimitiveType::Date32:
                return arrow::int32();
            case NYdb::EPrimitiveType::Datetime:
                return arrow::uint32();
            case NYdb::EPrimitiveType::Uint16:
                return arrow::uint16();
            case NYdb::EPrimitiveType::Int32:
                return arrow::int32();
            case NYdb::EPrimitiveType::Uint32:
                return arrow::uint32();
            case NYdb::EPrimitiveType::Int64:
                return arrow::int64();
            case NYdb::EPrimitiveType::Uint64:
                return arrow::uint64();
            case NYdb::EPrimitiveType::Float:
                return arrow::float32();
            case NYdb::EPrimitiveType::Double:
                return arrow::float64();
            case NYdb::EPrimitiveType::Utf8:
            case NYdb::EPrimitiveType::Json:
                return arrow::utf8();
            case NYdb::EPrimitiveType::String:
            case NYdb::EPrimitiveType::Yson:
            case NYdb::EPrimitiveType::DyNumber:
            case NYdb::EPrimitiveType::JsonDocument:
                return arrow::binary();
            case NYdb::EPrimitiveType::Timestamp:
                return arrow::timestamp(arrow::TimeUnit::MICRO);
            case NYdb::EPrimitiveType::Interval:
                return arrow::duration(arrow::TimeUnit::MILLI);
            case NYdb::EPrimitiveType::Datetime64:
            case NYdb::EPrimitiveType::Interval64:
            case NYdb::EPrimitiveType::Timestamp64:
                return arrow::int64();
            default:
                return arrow::Status::TypeError(ErrorPrefix() + "Not supported type " + ToString(tp.GetPrimitive()));
        }
    default:
        return arrow::Status::TypeError(ErrorPrefix() + "Not supported type kind " + ToString(tp.GetKind()));
    }
}

arrow::Result<std::shared_ptr<arrow::DataType>> TArrowCSVTable::GetCSVArrowType(const NYdb::TType& type) {
    auto tp = ExtractType(type);
    if (tp.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive) {
        switch (tp.GetPrimitive()) {
        case NYdb::EPrimitiveType::Datetime:
        case NYdb::EPrimitiveType::Datetime64:
            return arrow::timestamp(arrow::TimeUnit::SECOND);
        case NYdb::EPrimitiveType::Timestamp:
        case NYdb::EPrimitiveType::Timestamp64:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case NYdb::EPrimitiveType::Date:
        case NYdb::EPrimitiveType::Date32:
            return arrow::timestamp(arrow::TimeUnit::SECOND);
        default:
            break;
        }
    }
    return GetArrowType(type);
}

}