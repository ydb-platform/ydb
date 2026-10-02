#include "create_external_data_source_formatter.h"

#include <ydb/core/ydb_convert/external_data_source_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <util/string/join.h>

#include <format>
#include <ranges>

namespace NKikimr::NSysView {

namespace {

std::string PropertyToString(const std::pair<TString, TString>& property) {
    const auto& [key, value] = property;
    return NYdb::NDump::KeyValueToString(key, value);
}

// Canonical SHOW CREATE EXTERNAL DATA SOURCE form: no `-- database:` header
// and no `IF NOT EXISTS`. Backup emits its own variant with those extras in
// ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.cpp.
TString BuildCreateExternalDataSourceQuery(const Ydb::Table::DescribeExternalDataSourceResult& description) {
    return std::format(
        "CREATE EXTERNAL DATA SOURCE `{}`\n"
        "WITH (\n"
        "{},\n"
        "{}"
        "{}\n"
        ");",
        description.self().name().c_str(),
        NYdb::NDump::KeyValueToString("SOURCE_TYPE", description.source_type()),
        NYdb::NDump::KeyValueToString("LOCATION", description.location()),
        description.properties().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.properties(), PropertyToString)).c_str()
    );
}

} // anonymous namespace

TFormatResult TCreateExternalDataSourceFormatter::Format(
    const TString& dataSourcePath,
    const NKikimrSchemeOp::TExternalDataSourceDescription& dataSourceDesc,
    const NKikimrSchemeOp::TDirEntry& dirEntry)
{
    Ydb::Table::DescribeExternalDataSourceResult description;
    FillExternalDataSourceDescription(description, dataSourceDesc, dirEntry);

    // REFERENCES is an internal schemeshard dependency-tracking property,
    // not part of the user-facing CREATE statement.
    description.mutable_properties()->erase("REFERENCES");

    description.mutable_self()->set_name(dataSourcePath);

    TString query = BuildCreateExternalDataSourceQuery(description);

    NYql::TIssues issues;
    TString formattedQuery;
    if (!NYdb::NDump::Format(query, formattedQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    return TFormatResult(formattedQuery);
}

}
