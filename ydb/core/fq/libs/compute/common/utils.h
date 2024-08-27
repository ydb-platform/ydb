#pragma once

#include <memory>

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {

inline std::shared_ptr<NYdb::NTable::TTableClient> CreateNewTableClient(const TString& scope,
                                                                 const ::NFq::TComputeConfig& computeConfig,
                                                                 const ::NFq::NConfig::TYdbStorageConfig& connection,
                                                                 const TYqSharedResources::TPtr& yqSharedResources,
                                                                 const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    ::NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetSchemeConnection(scope);
    computeConnection.set_endpoint(connection.endpoint());
    computeConnection.set_database(connection.database());
    computeConnection.set_usessl(connection.usessl());

    auto tableSettings = GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection,
                                                                            credentialsProviderFactory);
    return std::make_shared<NYdb::NTable::TTableClient>(yqSharedResources->UserSpaceYdbDriver,
                                                        tableSettings);
}

TString GetV1StatFromV2Plan(const TString& plan, double* cpuUsage = nullptr);
TString GetV1StatFromV2PlanV2(const TString& plan);
TString GetPrettyStatistics(const TString& statistics);
THashMap<TString, i64> AggregateStats(TStringBuf plan);

TString FormatDurationMs(ui64 durationMs);
TString FormatDurationUs(ui64 durationUs);
TString FormatInstant(TInstant instant);
TDuration ParseDuration(TStringBuf str);

struct TPublicStat {
    std::optional<int> MemoryUsageBytes = 0;
    std::optional<int> CpuUsageUs = 0;
    std::optional<int> InputBytes = 0;
    std::optional<int> OutputBytes = 0;
    std::optional<int> SourceInputRecords = 0;
    std::optional<int> SinkOutputRecords = 0;
    std::optional<int> RunningTasks = 0;
};

TPublicStat GetPublicStat(const TString& statistics);

struct IPlanStatProcessor {
    virtual ~IPlanStatProcessor() = default;
    virtual Ydb::Query::StatsMode GetStatsMode() = 0;
    virtual TString ConvertPlan(const TString& plan) = 0;
    virtual TString GetPlanVisualization(const TString& plan) = 0;
    virtual TString GetQueryStat(const TString& plan, double& cpuUsage) = 0;
    virtual TPublicStat GetPublicStat(const TString& stat) = 0;
    virtual THashMap<TString, i64> GetFlatStat(TStringBuf plan) = 0;
};

std::unique_ptr<IPlanStatProcessor> CreateStatProcessor(const TString& statViewName);

class PingTaskRequestBuilder {
public:
    PingTaskRequestBuilder(const NConfig::TCommonConfig& commonConfig, std::unique_ptr<IPlanStatProcessor>&& processor);
    Fq::Private::PingTaskRequest Build(
        const Ydb::TableStats::QueryStats& queryStats, 
        const NYql::TIssues& issues, 
        std::optional<FederatedQuery::QueryMeta::ComputeStatus> computeStatus = std::nullopt,
        std::optional<NYql::NDqProto::StatusIds::StatusCode> pendingStatusCode = std::nullopt
    );
    Fq::Private::PingTaskRequest Build(const Ydb::TableStats::QueryStats& queryStats);
    Fq::Private::PingTaskRequest Build(const TString& queryPlan, const TString& queryAst, int64_t compilationTimeUs, int64_t computeTimeUs);
    NYql::TIssues Issues;
    double CpuUsage = 0.0;
    TPublicStat PublicStat;
private:
    const TCompressor Compressor;
    std::unique_ptr<IPlanStatProcessor> Processor;
};

TString GetStatViewName(const ::NFq::TRunActorParams& params);

} // namespace NFq
