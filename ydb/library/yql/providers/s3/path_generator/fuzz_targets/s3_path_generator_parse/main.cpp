// Fuzzer for NYql::NPathGenerator::CreatePathGenerator.
// This function parses a JSON/text "projection" specification that controls
// how S3 object paths are generated for partitioned external tables.
// It is reachable through the YQL S3 external table DDL (CREATE EXTERNAL TABLE
// ... WITH PARTITION BY) which is accepted over gRPC YQL service.
// The parser iterates over JSON keys, parses date/integer/enum types, and
// performs format string expansion - rich ground for parser bugs.
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <util/generic/string.h>
#include <util/generic/map.h>
#include <yql/essentials/public/udf/udf_data_type.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 64 * 1024) return 0;
    TString projection(reinterpret_cast<const char*>(data), size);
    try {
        TMap<TString, NYql::NUdf::EDataSlot> columns;
        columns["year"]  = NYql::NUdf::EDataSlot::Uint32;
        columns["month"] = NYql::NUdf::EDataSlot::Uint32;
        columns["day"]   = NYql::NUdf::EDataSlot::String;
        auto gen = NYql::NPathGenerator::CreatePathGenerator(
            projection,
            {"year", "month", "day"},
            columns,
            /*pathsLimit=*/100);
        (void)gen;
    } catch (...) {}
    return 0;
}
