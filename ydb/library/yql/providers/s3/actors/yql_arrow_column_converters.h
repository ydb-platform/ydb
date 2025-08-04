#pragma once

#include <yql/essentials/parser/pg_wrapper/interface/arrow.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>

namespace NDB {

// forward declaration for <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatSettings.h>
struct FormatSettings;

} // namespace NDB

namespace NYql::NDq {

TColumnConverter BuildColumnConverter(
    const std::string& columnName,
    const std::shared_ptr<arrow::DataType>& originalType,
    const std::shared_ptr<arrow::DataType>& targetType,
    NKikimr::NMiniKQL::TType* yqlType,
    const NDB::FormatSettings& formatSettings);

TColumnConverter BuildOutputColumnConverter(
    const std::string& columnName,
    NKikimr::NMiniKQL::TType* columnType);

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

bool S3ConvertArrowOutputType(NUdf::EDataSlot slot, std::shared_ptr<arrow::DataType>& type);
bool S3ConvertArrowOutputType(NKikimr::NMiniKQL::TType* itemType, std::shared_ptr<arrow::DataType>& type);

void BuildOutputColumnConverters(
    const NKikimr::NMiniKQL::TStructType* outputStructType,
    std::vector<TColumnConverter>& columnConverters);

} // namespace NYql::NDq
