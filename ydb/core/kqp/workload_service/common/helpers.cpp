#include "helpers.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>


namespace NKikimr::NKqp::NWorkload {

TString CreateDatabaseId(const TString& database, bool serverless, TPathId pathId) {
    TString databasePath = CanonizePath(database);
    TString tennantPath = CanonizePath(AppData()->TenantName);
    if (databasePath.empty() || databasePath == tennantPath) {
        return tennantPath;
    }

    if (serverless) {
        databasePath = TStringBuilder() << pathId.OwnerId << ":" << pathId.LocalPathId << ":" << databasePath;
    }
    return databasePath;
}

TString DatabaseIdToDatabase(TStringBuf databaseId) {
    TStringBuf id;
    TStringBuf database;
    return databaseId.TrySplit("/", id, database)
        ? CanonizePath(TString(database))     // Serverless
        : CanonizePath(TString(databaseId));  // Dedicated
}

TString CreatePoolKey(const TString& databaseId, const TString& poolId) {
    return TStringBuilder() << databaseId << "/" << poolId;
}

std::pair<TString, TString> ParsePoolKey(const TString& poolKey, TString& error) {
    const auto pos = poolKey.rfind('/');
    if (pos == TString::npos || pos == poolKey.size()) {
        error = "Invalid pool key";
        return {};
    }
    return {poolKey.substr(0, pos), poolKey.substr(pos + 1)};
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
