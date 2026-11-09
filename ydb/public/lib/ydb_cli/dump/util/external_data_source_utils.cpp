#include "external_data_source_utils.h"
#include "query_utils.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/hash_set.h>
#include <util/string/join.h>
#include <google/protobuf/unknown_field_set.h>

#include <cctype>
#include <format>
#include <ranges>

namespace NYdb::NDump {

namespace {

std::string PropertyToString(const std::pair<TString, TString>& property) {
    const auto& [key, value] = property;
    return KeyValueToString(key, value);
}

}

bool ValidateDirectExternalDataSourcePath(const TString& path, NYql::TIssues& issues) {
    // Old restore embeds the complete destination in backticks without escaping. Validate every
    // original component, not just the basename, so an in-place restore has a representable path.
    for (const unsigned char c : path) {
        if (c == '`' || c == '\\' || c == '\0' || std::isspace(c)) {
            issues.AddIssue("Direct external data source dump supports only source paths without whitespace, "
                "backticks, backslashes or NUL; this includes every parent component");
            return false;
        }
    }
    return true;
}

bool ValidateDirectExternalDataSource(
    const Ydb::Table::DescribeExternalDataSourceResult& description,
    NYql::TIssues& issues)
{
    const auto reject = [&issues]() {
        issues.AddIssue("Direct external data source dump supports only Ydb/YdbExternal sources with known properties "
            "and AUTH_METHOD NONE; credential-bearing sources, unknown formats "
            "and values requiring SQL escaping are unsupported");
        return false;
    };
    const bool ydb = description.source_type() == "Ydb";
    if ((!ydb && description.source_type() != "YdbExternal")
        || description.GetReflection()->GetUnknownFields(description).field_count()) {
        return reject();
    }

    // Provider properties are defined in core/external_sources/external_source_factory.cpp.
    // Fail closed on additions: Describe flattens provider properties and auth into one map.
    static const THashSet<TString> commonProperties = {"AUTH_METHOD", "DATABASE_NAME", "USE_TLS"};
    static const THashSet<TString> ydbProperties = {"DATABASE_ID", "SHARED_READING", "SHARED_READING_GROUP"};
    const auto isLiteral = [](std::string_view value) {
        return value.find_first_of("'\\\r\n") == std::string_view::npos && value.find('\0') == std::string_view::npos;
    };
    // The existing serializer embeds the source name in backticks without escaping.
    const auto& name = description.self().name();
    if (name.find_first_of("`\\\r\n") != TString::npos || name.find('\0') != TString::npos
        || !isLiteral(description.location())) {
        return reject();
    }
    // Old restore matches the archived identifier as a single non-whitespace token.
    for (const unsigned char c : name) {
        if (std::isspace(c)) {
            return reject();
        }
    }
    const auto& properties = description.properties();
    for (const auto& [key, value] : properties) {
        if ((!commonProperties.contains(key) && !(ydb && ydbProperties.contains(key))) || !isLiteral(value)) {
            return reject();
        }
    }
    const auto auth = properties.find("AUTH_METHOD");
    // Named-secret restore requires ownership, while EDS use also permits a shared-secret grantee.
    // Reject every credential mode: dump cannot establish the privileges of a future restoring principal.
    if (auth == properties.end() || auth->second != "NONE") {
        return reject();
    }
    return true;
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
