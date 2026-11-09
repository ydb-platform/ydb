#pragma once

#include <util/generic/fwd.h>

namespace Ydb::Table {
    class DescribeExternalDataSourceResult;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

// Direct credential-bearing archives cannot be safely restored by older clients in all supported cases.
bool ValidateDirectExternalDataSource(
    const Ydb::Table::DescribeExternalDataSourceResult& description,
    NYql::TIssues& issues);

TString BuildCreateExternalDataSourceQuery(
    const Ydb::Table::DescribeExternalDataSourceResult& description,
    const TString& db);

bool RewriteCreateExternalDataSourceQueryNoSecrets(
    TString& query,
    const TString& dbPath,
    NYql::TIssues& issues);

bool RewriteCreateExternalDataSourceQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

} // namespace NYdb::NDump
