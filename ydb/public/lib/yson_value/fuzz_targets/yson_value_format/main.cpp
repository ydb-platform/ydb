#include <ydb/public/lib/value/fuzzing/value_fuzz.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/yson/node/node_io.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

namespace {

void ParseFormattedYson(const TString& yson) {
    auto node = NYT::NodeFromYsonString(yson, ::NYson::EYsonType::Node);
    (void)NYT::NodeToCanonicalYsonString(node, ::NYson::EYsonFormat::Text);
}

void FuzzYsonFormatting(FuzzedDataProvider& fdp) {
    TVector<NFuzzing::NValueFuzz::TGeneratedColumn> columns;
    const size_t columnCount = fdp.ConsumeIntegralInRange<size_t>(1, 3);
    columns.reserve(columnCount);
    for (size_t i = 0; i < columnCount; ++i) {
        columns.push_back(NFuzzing::NValueFuzz::GenerateColumn(fdp, i));
    }

    const auto resultSet = NFuzzing::NValueFuzz::BuildResultSet(columns);

    for (auto format : {
             ::NYson::EYsonFormat::Text,
             ::NYson::EYsonFormat::Binary,
             ::NYson::EYsonFormat::Pretty,
         })
    {
        for (const auto& column : columns) {
            ParseFormattedYson(NYdb::FormatValueYson(column.Value, format));
        }

        ParseFormattedYson(NYdb::FormatResultSetYson(resultSet, format));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzYsonFormatting(fdp);
    } catch (...) {
    }

    return 0;
}
