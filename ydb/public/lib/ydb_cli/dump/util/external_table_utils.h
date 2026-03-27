#pragma once

#include <util/generic/fwd.h>

namespace Ydb::Table {
    class DescribeExternalTableResult;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

TString BuildCreateExternalTableQuery(
    const TString& db,
    const TString& backupRoot,
    const Ydb::Table::DescribeExternalTableResult& description);

bool RewriteCreateExternalTableQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues);

}
