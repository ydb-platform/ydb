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

NResourcePool::TPoolSettings PoolSettingsFromConfig(const NKikimrConfig::TWorkloadManagerConfig& workloadManagerConfig) {
    NResourcePool::TPoolSettings poolSettings;
    if (workloadManagerConfig.HasResourceWeight()) {
        poolSettings.ResourceWeight = workloadManagerConfig.GetResourceWeight();
    }

    if (workloadManagerConfig.HasConcurrentQueryLimit()) {
        poolSettings.ConcurrentQueryLimit = workloadManagerConfig.GetConcurrentQueryLimit();
    }

    if (workloadManagerConfig.HasQueueSize()) {
        poolSettings.QueueSize = workloadManagerConfig.GetQueueSize();
    }

    if (workloadManagerConfig.HasQueryCpuLimitPercentPerNode()) {
        poolSettings.QueryCpuLimitPercentPerNode = workloadManagerConfig.GetQueryCpuLimitPercentPerNode();
    }

    if (workloadManagerConfig.HasQueryMemoryLimitPercentPerNode()) {
        poolSettings.QueryMemoryLimitPercentPerNode = workloadManagerConfig.GetQueryMemoryLimitPercentPerNode();
    }

    if (workloadManagerConfig.HasTotalCpuLimitPercentPerNode()) {
        poolSettings.TotalCpuLimitPercentPerNode = workloadManagerConfig.GetTotalCpuLimitPercentPerNode();
    }

    if (workloadManagerConfig.HasDatabaseLoadCpuThreshold()) {
        poolSettings.DatabaseLoadCpuThreshold = workloadManagerConfig.GetDatabaseLoadCpuThreshold();
    }
    return poolSettings;
}

}  // NKikimr::NKqp::NWorkload
