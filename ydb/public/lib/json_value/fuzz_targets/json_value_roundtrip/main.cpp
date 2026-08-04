#include <ydb/public/lib/value/fuzzing/value_fuzz.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>

#include <library/cpp/json/json_reader.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

namespace {

void FuzzJsonRoundTrip(FuzzedDataProvider& fdp) {
    TVector<NFuzzing::NValueFuzz::TGeneratedColumn> columns;
    const size_t columnCount = fdp.ConsumeIntegralInRange<size_t>(1, 3);
    columns.reserve(columnCount);
    for (size_t i = 0; i < columnCount; ++i) {
        columns.push_back(NFuzzing::NValueFuzz::GenerateColumn(fdp, i));
    }

    const auto resultSet = NFuzzing::NValueFuzz::BuildResultSet(columns);

    for (auto encoding : {
             NYdb::EBinaryStringEncoding::Unicode,
             NYdb::EBinaryStringEncoding::Base64,
         })
    {
        for (const auto& column : columns) {
            const TString json = NYdb::FormatValueJson(column.Value, encoding);

            NJson::TJsonValue tree;
            if (NJson::ReadJsonTree(json, &tree)) {
                auto fromTree = NYdb::JsonToYdbValue(tree, column.Type, encoding);
                (void)NYdb::FormatValueJson(fromTree, encoding);
            }

            auto fromString = NYdb::JsonToYdbValue(json, column.Type, encoding);
            (void)NYdb::FormatValueJson(fromString, encoding);
        }

        const TString resultSetJson = NYdb::FormatResultSetJson(resultSet, encoding);
        NJson::TJsonValue tree;
        (void)NJson::ReadJsonTree(resultSetJson, &tree);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzJsonRoundTrip(fdp);
    } catch (...) {
    }

    return 0;
}
