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

// Optional output columns absent in the file, filled with nulls at their positions in the record batch
struct TMissingColumns {
    struct TColumn {
        size_t OutputIndex = 0;
        std::shared_ptr<arrow::Field> Field;
    };

    std::vector<TColumn> Columns; // sorted by OutputIndex
    std::shared_ptr<arrow::Schema> Schema; // schema of the record batch with the missing columns inserted
};

void BuildColumnConverters(
    std::shared_ptr<arrow::Schema> outputSchema,
    std::shared_ptr<arrow::Schema> dataSchema,
    std::vector<int>& columnIndices,
    std::vector<TColumnConverter>& columnConverters,
    TMissingColumns& missingColumns,
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> rowTypes,
    const NDB::FormatSettings& settings);

std::shared_ptr<arrow::RecordBatch> ConvertArrowColumns(
    std::shared_ptr<arrow::RecordBatch> batch,
    std::vector<TColumnConverter>& columnConverters,
    const TMissingColumns& missingColumns = {});

bool S3ConvertArrowOutputType(NUdf::EDataSlot slot, std::shared_ptr<arrow::DataType>& type, NKikimr::NMiniKQL::TType* itemType = nullptr);
bool S3ConvertArrowOutputType(NKikimr::NMiniKQL::TType* itemType, std::shared_ptr<arrow::DataType>& type);

void BuildOutputColumnConverters(
    const NKikimr::NMiniKQL::TStructType* outputStructType,
    std::vector<TColumnConverter>& columnConverters);

} // namespace NYql::NDq
