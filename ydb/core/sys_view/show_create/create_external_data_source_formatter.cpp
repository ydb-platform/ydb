#include "create_external_data_source_formatter.h"

#include <ydb/core/ydb_convert/external_data_source_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

namespace NKikimr::NSysView {

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

    TString query = NYdb::NDump::BuildCreateExternalDataSourceQuery(
        description, /* db */ {}, /* ifNotExists */ false);

    NYql::TIssues issues;
    TString formattedQuery;
    if (!NYdb::NDump::Format(query, formattedQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::SCHEME_ERROR, issues);
    }

    return TFormatResult(formattedQuery);
}

}
