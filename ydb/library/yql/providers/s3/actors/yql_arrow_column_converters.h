#pragma once

#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>

namespace NYql::NDq {

TColumnConverter BuildColumnConverter(
    const std::string& columnName,
    const std::shared_ptr<arrow::DataType>& originalType,
    const std::shared_ptr<arrow::DataType>& targetType,
    NKikimr::NMiniKQL::TType* yqlType,
    const NDB::FormatSettings& formatSettings);

void BuildColumnConverters(
    std::shared_ptr<arrow::Schema> outputSchema,
    std::shared_ptr<arrow::Schema> dataSchema,
    std::vector<int>& columnIndices,
    std::vector<TColumnConverter>& columnConverters,
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> rowTypes,
    const NDB::FormatSettings& settings);

std::shared_ptr<arrow::RecordBatch> ConvertArrowColumns(
    std::shared_ptr<arrow::RecordBatch> batch,
    std::vector<TColumnConverter>& columnConverters);

} // namespace NYql::NDq
