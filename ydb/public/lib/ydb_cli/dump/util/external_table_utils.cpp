#include "external_table_utils.h"
#include "query_utils.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/join.h>

#include <format>
#include <ranges>

namespace NYdb::NDump {

namespace {

std::string PropertyToString(const std::pair<TString, TString>& property) {
    const auto& [key, json] = property;
    const auto items = NJson::ReadJsonFastTree(json).GetArray();
    Y_ENSURE(!items.empty(), "Empty items for an external table property: " << key);
    if (items.size() == 1) {
        return KeyValueToString(key, items.front().GetString());
    } else {
        return KeyValueToString(key, std::format("[{}]", JoinSeq(", ", items).c_str()));
    }
}

std::string ColumnToString(const Ydb::Table::ColumnMeta& column) {
    const auto& type = column.type();
    const bool notNull = !type.has_optional_type() || (type.has_pg_type() && column.not_null());
    return std::format(
        "    {} {}{}",
        column.name().c_str(),
        TType(type).ToString(),
        notNull ? " NOT NULL" : ""
    );
}

}

TString BuildCreateExternalTableQuery(const Ydb::Table::DescribeExternalTableResult& description) {
    return std::format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `{}` (\n"
        "  {}\n"
        ") WITH (\n"
        "{},\n"
        "{}"
        "{}\n"
        ");",
        description.self().name().c_str(),
        JoinSeq(",\n", std::views::transform(description.columns(), ColumnToString)).c_str(),
        KeyValueToString("DATA_SOURCE", description.data_source_path()),
        KeyValueToString("LOCATION", description.location()),
        description.content().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.content(), PropertyToString)).c_str()
    );
}

bool RewriteCreateExternalTableQuery(
    TString& query,
    const TString& dbPath,
    NYql::TIssues& issues)
{

    return RewriteCreateQuery(query, "CREATE EXTERNAL TABLE IF NOT EXISTS `{}`", dbPath, issues);
}

}
