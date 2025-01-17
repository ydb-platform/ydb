#include <yql/essentials/public/issue/yql_issue.h>

namespace NYdb::NDump {

bool ValidateViewQuery(const TString& query, NYql::TIssues& issues);

TString RewriteAbsolutePath(TStringBuf path, TStringBuf backupRoot, TStringBuf restoreRoot);

bool RewriteTableRefs(TString& scheme, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues);

}
