#include "table.h"
#include <util/string/join.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr::NFormats {

arrow20::Result<TArrowCSV> TArrowCSVTable::Create(const std::vector<NYdb::NTable::TTableColumn>& columns, bool header) {
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

        auto tp = ExtractType(column.Type);
        TColumnInfo columnInfo{TString{column.Name}, *arrowType, *csvArrowType};
        if (tp.GetKind() == NYdb::TTypeParser::ETypeKind::Decimal) {
            columnInfo.Precision = tp.GetDecimal().Precision;
            columnInfo.Scale = tp.GetDecimal().Scale;
        }

        convertedColumns.emplace_back(columnInfo);
        if (NYdb::TTypeParser(column.Type).GetKind() != NYdb::TTypeParser::ETypeKind::Optional || column.NotNull.value_or(false)) {
            notNullColumns.emplace(column.Name);
        }
    }

    if (!errors.empty()) {
        return arrow20::Status::TypeError(ErrorPrefix() + "columns errors: " + JoinSeq("; ", errors));
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

arrow20::Result<std::shared_ptr<arrow20::DataType>> TArrowCSVTable::GetArrowType(const NYdb::TType& type) {
    auto tp = ExtractType(type);
    switch (tp.GetKind()) {
    case NYdb::TTypeParser::ETypeKind::Decimal:
        return std::make_shared<arrow20::FixedSizeBinaryType>(NScheme::FSB_SIZE);
    case NYdb::TTypeParser::ETypeKind::Primitive:
        switch (tp.GetPrimitive()) {
            case NYdb::EPrimitiveType::Bool:
                return arrow20::boolean();
            case NYdb::EPrimitiveType::Int8:
                return arrow20::int8();
            case NYdb::EPrimitiveType::Uint8:
                return arrow20::uint8();
            case NYdb::EPrimitiveType::Int16:
                return arrow20::int16();
            case NYdb::EPrimitiveType::Date:
                return arrow20::uint16();
            case NYdb::EPrimitiveType::Date32:
                return arrow20::int32();
            case NYdb::EPrimitiveType::Datetime:
                return arrow20::uint32();
            case NYdb::EPrimitiveType::Uint16:
                return arrow20::uint16();
            case NYdb::EPrimitiveType::Int32:
                return arrow20::int32();
            case NYdb::EPrimitiveType::Uint32:
                return arrow20::uint32();
            case NYdb::EPrimitiveType::Int64:
                return arrow20::int64();
            case NYdb::EPrimitiveType::Uint64:
                return arrow20::uint64();
            case NYdb::EPrimitiveType::Float:
                return arrow20::float32();
            case NYdb::EPrimitiveType::Double:
                return arrow20::float64();
            case NYdb::EPrimitiveType::Utf8:
            case NYdb::EPrimitiveType::Json:
                return arrow20::utf8();
            case NYdb::EPrimitiveType::String:
            case NYdb::EPrimitiveType::Yson:
            case NYdb::EPrimitiveType::DyNumber:
            case NYdb::EPrimitiveType::JsonDocument:
                return arrow20::binary();
            case NYdb::EPrimitiveType::Timestamp:
                return arrow20::timestamp(arrow20::TimeUnit::MICRO);
            case NYdb::EPrimitiveType::Interval:
                return arrow20::duration(arrow20::TimeUnit::MILLI);
            case NYdb::EPrimitiveType::Datetime64:
            case NYdb::EPrimitiveType::Interval64:
            case NYdb::EPrimitiveType::Timestamp64:
                return arrow20::int64();
            default:
                return arrow20::Status::TypeError(ErrorPrefix() + "Not supported type " + ToString(tp.GetPrimitive()));
        }
    default:
        return arrow20::Status::TypeError(ErrorPrefix() + "Not supported type kind " + ToString(tp.GetKind()));
    }
}

arrow20::Result<std::shared_ptr<arrow20::DataType>> TArrowCSVTable::GetCSVArrowType(const NYdb::TType& type) {
    auto tp = ExtractType(type);
    if (tp.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive) {
        switch (tp.GetPrimitive()) {
        case NYdb::EPrimitiveType::Datetime:
        case NYdb::EPrimitiveType::Datetime64:
            return arrow20::timestamp(arrow20::TimeUnit::SECOND);
        case NYdb::EPrimitiveType::Timestamp:
        case NYdb::EPrimitiveType::Timestamp64:
            return arrow20::timestamp(arrow20::TimeUnit::MICRO);
        case NYdb::EPrimitiveType::Date:
        case NYdb::EPrimitiveType::Date32:
            return arrow20::timestamp(arrow20::TimeUnit::SECOND);
        default:
            break;
        }
    }

    return GetArrowType(type);
}

}