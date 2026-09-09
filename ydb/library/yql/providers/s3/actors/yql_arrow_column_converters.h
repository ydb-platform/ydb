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

// Optional column from the output schema that is absent in the file.
// Such columns are not read from the file and are filled with nulls
// at position OutputIndex of the resulting record batch.
struct TMissingColumn {
    int OutputIndex = -1;
    std::shared_ptr<arrow::Field> Field;
};

// Matches output schema columns with file (data schema) columns.
// columnIndices - indices of file columns to read, in output schema order (missing columns are skipped)
// columnConverters - converters for read columns, parallel to columnIndices
// missingColumns - optional output columns absent in the file, sorted by OutputIndex
// Throws if a non optional column is absent in the file.
void BuildColumnConverters(
    std::shared_ptr<arrow::Schema> outputSchema,
    std::shared_ptr<arrow::Schema> dataSchema,
    std::vector<int>& columnIndices,
    std::vector<TColumnConverter>& columnConverters,
    std::vector<TMissingColumn>& missingColumns,
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> rowTypes,
    const NDB::FormatSettings& settings);

// Applies converters to batch columns and inserts null columns for missingColumns.
std::shared_ptr<arrow::RecordBatch> ConvertArrowColumns(
    std::shared_ptr<arrow::RecordBatch> batch,
    std::vector<TColumnConverter>& columnConverters,
    const std::vector<TMissingColumn>& missingColumns = {});

bool S3ConvertArrowOutputType(NUdf::EDataSlot slot, std::shared_ptr<arrow::DataType>& type, NKikimr::NMiniKQL::TType* itemType = nullptr);
bool S3ConvertArrowOutputType(NKikimr::NMiniKQL::TType* itemType, std::shared_ptr<arrow::DataType>& type);

void BuildOutputColumnConverters(
    const NKikimr::NMiniKQL::TStructType* outputStructType,
    std::vector<TColumnConverter>& columnConverters);

} // namespace NYql::NDq
