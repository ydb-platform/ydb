#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYdb::NDump {

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& database, const TString& backupRoot,
    NYql::TIssues& issues
);

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, const TString& source, NYql::TIssues& issues
);

}
