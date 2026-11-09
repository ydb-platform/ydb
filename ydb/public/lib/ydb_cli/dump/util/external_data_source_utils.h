#pragma once

#include <util/generic/fwd.h>

namespace Ydb::Table {
    class DescribeExternalDataSourceResult;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

// Direct archives cannot safely relocate schema-secret references with older restore clients.
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
