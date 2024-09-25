#include "helpers.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>


namespace NKikimr::NKqp::NWorkload {

TString CreateDatabaseId(const TString& database, bool serverless, TPathId pathId) {
    TString result = CanonizePath(database ? database : AppData()->TenantName);
    if (serverless) {
        result = TStringBuilder() << pathId.OwnerId << ":" << pathId.LocalPathId << ":" << result;
    }
    return result;
}

TString DatabaseIdToDatabase(TStringBuf databaseId) {
    if (databaseId.empty()) {
        return {};
    }

    TStringBuf id;
    TStringBuf database;
    Y_ENSURE(databaseId.TrySplit("/", id, database), "Invalid database id: " << databaseId);
    return CanonizePath(TString(databaseId));
}

NYql::TIssues GroupIssues(const NYql::TIssues& issues, const TString& message) {
    NYql::TIssue rootIssue(message);
    for (const NYql::TIssue& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    return {rootIssue};
}

void ParsePoolSettings(const NKikimrSchemeOp::TResourcePoolDescription& description, NResourcePool::TPoolSettings& poolConfig) {
    poolConfig = NResourcePool::TPoolSettings(description.GetProperties().GetProperties());
}

ui64 SaturationSub(ui64 x, ui64 y) {
    return (x > y) ? x - y : 0;
}

}  // NKikimr::NKqp::NWorkload
