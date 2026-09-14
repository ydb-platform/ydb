#include "create_external_table_formatter.h"

#include <ydb/core/ydb_convert/external_table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/join.h>

#include <algorithm>
#include <format>
#include <ranges>

namespace NKikimr::NSysView {

namespace {

// External table properties are stored packed in the description's Content and
// unpacked by FillExternalTableDescription into a map of JSON arrays, because
// IExternalSource::GetParameters is list-shaped for every property. Arity alone
// therefore cannot tell a list setting from a scalar one: a single-column
// PARTITIONED_BY is a one-element array that still has to be emitted as a JSON
// list to be accepted back, while FORMAT must never be wrapped in one. The
// list-valued settings are spelled out here.
constexpr TStringBuf ListValuedProperties[] = {
    "PARTITIONED_BY",
};

bool IsListValuedProperty(TStringBuf key) {
    return std::find(std::begin(ListValuedProperties), std::end(ListValuedProperties), key)
        != std::end(ListValuedProperties);
}

std::string PropertyToString(const std::pair<TString, TString>& property) {
    const auto& [key, json] = property;
    const auto items = NJson::ReadJsonFastTree(json).GetArray();
    Y_ENSURE(!items.empty(), "Empty items for an external table property: " << key);
    if (items.size() == 1 && !IsListValuedProperty(key)) {
        return NYdb::NDump::KeyValueToString(key, items.front().GetString());
    }
    return NYdb::NDump::KeyValueToString(key, std::format("[{}]", JoinSeq(", ", items).c_str()));
}

// Columns are taken from the scheme description rather than from the converted
// Ydb one: it already carries the YQL type name, so no type conversion is
// needed, and names are escaped the same way as in SHOW CREATE TABLE.
std::string ColumnToString(const NKikimrSchemeOp::TColumnDescription& column) {
    TStringStream stream;
    stream << "  ";
    EscapeName(column.GetName(), stream);
    stream << " " << column.GetType();
    if (column.GetNotNull()) {
        stream << " NOT NULL";
    }
    return stream.Str().c_str();
}

// Canonical SHOW CREATE EXTERNAL TABLE form: no `-- database:` header and no
// `IF NOT EXISTS`. Backup emits its own variant with those extras in
// ydb/public/lib/ydb_cli/dump/util/external_table_utils.cpp.
TString BuildCreateExternalTableQuery(
    const NKikimrSchemeOp::TExternalTableDescription& tableDesc,
    const Ydb::Table::DescribeExternalTableResult& description)
{
    return std::format(
        "CREATE EXTERNAL TABLE `{}` (\n"
        "{}\n"
        ") WITH (\n"
        "{},\n"
        "{}"
        "{}\n"
        ");",
        description.self().name().c_str(),
        JoinSeq(",\n", std::views::transform(tableDesc.GetColumns(), ColumnToString)).c_str(),
        NYdb::NDump::KeyValueToString("DATA_SOURCE", description.data_source_path()),
        NYdb::NDump::KeyValueToString("LOCATION", description.location()),
        description.content().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.content(), PropertyToString)).c_str()
    );
}

} // anonymous namespace

TFormatResult TCreateExternalTableFormatter::Format(
    const TString& tablePath,
    const NKikimrSchemeOp::TExternalTableDescription& tableDesc,
    const NKikimrSchemeOp::TDirEntry& dirEntry)
{
    Ydb::Table::DescribeExternalTableResult description;
    {
        auto status = Ydb::StatusIds::SUCCESS;
        TString error;
        if (!FillExternalTableDescription(description, tableDesc, dirEntry, status, error)) {
            return TFormatResult(status, error);
        }
    }

    description.mutable_self()->set_name(tablePath);

    TString query = BuildCreateExternalTableQuery(tableDesc, description);

    NYql::TIssues issues;
    TString formattedQuery;
    if (!NYdb::NDump::Format(query, formattedQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    return TFormatResult(formattedQuery);
}

}
