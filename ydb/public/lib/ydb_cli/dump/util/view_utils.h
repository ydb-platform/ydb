#pragma once

#include <util/generic/string.h>

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& backupRoot,
    NYql::TIssues& issues
);

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, NYql::TIssues& issues
);

} // NYdb::NDump
