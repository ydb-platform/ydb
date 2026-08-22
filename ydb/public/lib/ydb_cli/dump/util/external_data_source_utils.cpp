#include "external_data_source_utils.h"
#include "query_utils.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/string/join.h>

#include <format>
#include <ranges>

namespace NYdb::NDump {

namespace {

std::string PropertyToString(const std::pair<TString, TString>& property) {
    const auto& [key, value] = property;
    return KeyValueToString(key, value);
}

}

TString BuildCreateExternalDataSourceQuery(
    const Ydb::Table::DescribeExternalDataSourceResult& description,
    const TString& db)
{
    return std::format(
        "-- database: \"{}\"\n"
        "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS `{}`\n"
        "WITH (\n"
        "{},\n"
        "{}"
        "{}\n"
        ");",
        db.c_str(),
        description.self().name().c_str(),
        KeyValueToString("SOURCE_TYPE", description.source_type()),
        KeyValueToString("LOCATION", description.location()),
        description.properties().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.properties(), PropertyToString)).c_str()
    );
}

bool RewriteCreateExternalDataSourceQueryNoSecrets(
    TString& query,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    return RewriteCreateQuery(query, "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS `{}`", dbPath, issues);
}

bool RewriteCreateExternalDataSourceQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (!RewriteQuerySecretsNoCheck(query, dbRestoreRoot, issues)) {
        return false;
    }
    return RewriteCreateExternalDataSourceQueryNoSecrets(query, dbPath, issues);
}

}
