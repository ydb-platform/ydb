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

} // namespace NYql::NDq
