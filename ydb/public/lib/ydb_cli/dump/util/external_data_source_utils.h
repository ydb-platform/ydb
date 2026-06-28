#pragma once

#include <util/generic/fwd.h>

namespace Ydb::Table {
    class DescribeExternalDataSourceResult;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

// When `db` is empty, the leading `-- database:` comment is omitted.
// When `ifNotExists` is false, the `IF NOT EXISTS` modifier is omitted.
// Dump/restore callers should leave the defaults; SHOW CREATE callers
// pass an empty db and ifNotExists=false so the output matches the
// canonical SHOW CREATE style used for tables and views.
TString BuildCreateExternalDataSourceQuery(
    const Ydb::Table::DescribeExternalDataSourceResult& description,
    const TString& db,
    bool ifNotExists = true);

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
