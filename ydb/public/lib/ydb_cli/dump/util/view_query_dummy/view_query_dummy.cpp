#include <ydb/public/lib/ydb_cli/dump/util/view_query_iface/view_query.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NYdb::NDump {

namespace {

constexpr TStringBuf NotSupported =
    "views are not supported: this binary is built without the SQL parser";

} // namespace

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& backupRoot,
    NYql::TIssues& issues)
{
    Y_UNUSED(name, dbPath, viewQuery, backupRoot);
    issues.AddIssue(NYql::TIssue(TString(NotSupported)));
    return {};
}

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, NYql::TIssues& issues)
{
    Y_UNUSED(query, restoreRoot, restoreRootIsDatabase, dbPath);
    issues.AddIssue(NYql::TIssue(TString(NotSupported)));
    return false;
}

} // namespace NYdb::NDump
