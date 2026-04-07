#pragma once

#include <util/generic/fwd.h>

namespace Ydb::Table {
    class DescribeExternalDataSourceResult;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

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
