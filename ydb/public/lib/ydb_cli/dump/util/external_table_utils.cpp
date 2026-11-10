#include "external_table_utils.h"
#include "query_utils.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/json/json_reader.h>

#include <util/folder/pathsplit.h>
#include <util/string/join.h>
#include <util/string/strip.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <ranges>
#include <utility>

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

TString BuildCreateExternalTableQuery(
    const TString& db,
    const TString& backupRoot,
    const Ydb::Table::DescribeExternalTableResult& description)
{
    return std::format(
        "-- database: \"{}\"\n"
        "-- backup root: \"{}\"\n"
        "CREATE EXTERNAL TABLE IF NOT EXISTS `{}` (\n"
        "  {}\n"
        ") WITH (\n"
        "{},\n"
        "{}"
        "{}\n"
        ");",
        db.c_str(),
        backupRoot.c_str(),
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

namespace {

TString GetDataSourcePath(TStringInput query) {
    TString line;
    TString pattern = R"(DATA_SOURCE = ')";
    while (query.ReadLine(line)) {
        StripInPlace(line);
        if (line.StartsWith(pattern)) {
            TStringBuf result = TStringBuf(line).Skip(pattern.size());
            if (result.EndsWith(',')) {
                result.Chop(2); // Last "',"
            } else {
                result.Chop(1); // Last "'"
            }
            return TString(result);
        }
    }

    return "";
}

bool ResolveBackupRootDataSourcePath(
    TString& dataSourcePath,
    const TString& database,
    const TString& backupRoot,
    const TString& restoreRoot,
    NYql::TIssues& issues)
{
    const auto unsafeSqlPath = [](const TString& path) {
        return std::any_of(path.begin(), path.end(), [](unsigned char ch) {
            return ch == 0 || ch == '\\' || ch == '\'' || std::isspace(ch);
        });
    };
    if (unsafeSqlPath(dataSourcePath) || unsafeSqlPath(restoreRoot)) {
        issues.AddIssue("Cannot map external table data source containing unsupported SQL path characters");
        return false;
    }
    const TPathSplitUnix source(dataSourcePath);
    const TPathSplitUnix sourceDatabase(database);
    const TPathSplitUnix backup(backupRoot);
    TPathSplitUnix restore(restoreRoot);
    if (!source.IsAbsolute || !sourceDatabase.IsAbsolute || !backup.IsAbsolute || !restore.IsAbsolute
        || sourceDatabase.empty() || backup.size() < sourceDatabase.size()
        || !std::equal(sourceDatabase.begin(), sourceDatabase.end(), backup.begin())) {
        issues.AddIssue("Cannot map external table data source: the backup has no valid source database and backup root");
        return false;
    }

    if (source.size() >= backup.size() && std::equal(backup.begin(), backup.end(), source.begin())) {
        dataSourcePath = restore.AppendMany(source.begin() + backup.size(), source.end()).Reconstruct();
        return true;
    }

    if (backup == restore) {
        // An external dependency outside the selection stays at its original path only
        // when the backup is restored to the same root.
        return true;
    }

    issues.AddIssue("Cannot map external table data source outside a moved backup root");
    return false;
}

bool RewriteDataSourcePath(
    TString& query,
    const TString& dbRestoreRoot,
    NYql::TIssues& issues,
    EExternalTableSourceMapping mapping)
{
    TString dataSourcePath = GetDataSourcePath(query);
    if (dataSourcePath.empty()) {
        return false;
    }

    switch (mapping) {
        case EExternalTableSourceMapping::DatabaseRoot:
            dataSourcePath = RewriteAbsolutePath(dataSourcePath, GetDatabase(query), dbRestoreRoot);
            break;
        case EExternalTableSourceMapping::BackupRoot:
            if (!ResolveBackupRootDataSourcePath(dataSourcePath, GetDatabase(query), GetBackupRoot(query), dbRestoreRoot, issues)) {
                return false;
            }
            break;
        default:
            issues.AddIssue("Unknown external table data-source mapping");
            return false;
    }
    return RewriteCreateQuery(query, "DATA_SOURCE = '{}'", dataSourcePath, issues);
}

}

bool RewriteCreateExternalTableQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues,
    EExternalTableSourceMapping mapping)
{
    if (mapping == EExternalTableSourceMapping::BackupRoot) {
        if (std::any_of(dbPath.begin(), dbPath.end(), [](unsigned char ch) {
                return ch == 0 || ch == '`' || ch == '\\' || std::isspace(ch);
            })) {
            issues.AddIssue("Cannot restore external table at a path with unsupported SQL identifier characters");
            return false;
        }
        // Leave the caller's query unchanged when a mapping or destination rewrite fails.
        TString rewritten = query;
        if (!RewriteDataSourcePath(rewritten, dbRestoreRoot, issues, mapping)
            || !RewriteCreateQuery(rewritten, "CREATE EXTERNAL TABLE IF NOT EXISTS `{}`", dbPath, issues)) {
            return false;
        }
        query = std::move(rewritten);
        return true;
    }
    if (!RewriteDataSourcePath(query, dbRestoreRoot, issues, mapping)) {
        return false;
    }
    return RewriteCreateQuery(query, "CREATE EXTERNAL TABLE IF NOT EXISTS `{}`", dbPath, issues);
}

bool RewriteCreateExternalTableQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    return RewriteCreateExternalTableQuery(query, dbRestoreRoot, dbPath, issues, EExternalTableSourceMapping::DatabaseRoot);
}

}
