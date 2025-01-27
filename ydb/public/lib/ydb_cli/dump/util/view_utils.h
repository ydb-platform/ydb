#include <yql/essentials/public/issue/yql_issue.h>

namespace NYdb::NDump {

TString RewriteAbsolutePath(TStringBuf path, TStringBuf backupRoot, TStringBuf restoreRoot);

bool RewriteTableRefs(TString& scheme, TStringBuf backupRoot, TStringBuf restoreRoot, NYql::TIssues& issues);

struct TViewQuerySplit {
    TString ContextRecreation;
    TString Select;
};

TViewQuerySplit SplitViewQuery(TStringInput query);

// returns void, because the validation is non-blocking
void ValidateViewQuery(const TString& query, const TString& dbPath, NYql::TIssues& issues);

bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues);

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& backupRoot,
    NYql::TIssues& issues
);

}
