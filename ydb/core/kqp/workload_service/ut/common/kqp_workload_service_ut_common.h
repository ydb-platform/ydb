#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/kqp/workload_service/common/events.h>

#include <ydb/core/testlib/actors/test_runtime.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>


namespace NKikimr::NKqp::NWorkload {

inline constexpr TDuration FUTURE_WAIT_TIMEOUT = TDuration::Seconds(60);


// Query runner

struct TQueryRunnerSettings {
    using TSelf = TQueryRunnerSettings;

    // Query settings
    FLUENT_SETTING_DEFAULT(ui32, NodeIndex, 0);
    FLUENT_SETTING_DEFAULT(std::optional<TString>, PoolId, std::nullopt);
    FLUENT_SETTING_DEFAULT(TString, UserSID, "user@" BUILTIN_SYSTEM_DOMAIN);

    // Runner settings
    FLUENT_SETTING_DEFAULT(bool, HangUpDuringExecution, false);
    FLUENT_SETTING(std::optional<NActors::TActorId>, InFlightCoordinatorActorId);

    // Runner validations
    FLUENT_SETTING_DEFAULT(bool, ExecutionExpected, true);
};

struct TQueryRunnerResult {
    NKikimrKqp::TEvQueryResponse Response;
    std::vector<Ydb::ResultSet> ResultSets;

    NYdb::EStatus GetStatus() const;
    NYql::TIssues GetIssues() const;

    const Ydb::ResultSet& GetResultSet(size_t resultIndex) const;
    const std::vector<Ydb::ResultSet>& GetResultSets() const;
};

struct TQueryRunnerResultAsync {
    NThreading::TFuture<TQueryRunnerResult> AsyncResult;
    NActors::TActorId QueryRunnerActor;
    NActors::TActorId EdgeActor;  // Receives events about progress from QueryRunnerActor

    TQueryRunnerResult GetResult(TDuration timeout = FUTURE_WAIT_TIMEOUT) const;
    NThreading::TFuture<void> GetFuture() const;
    bool HasValue() const;
};

// Ydb setup

class IYdbSetup;

struct TYdbSetupSettings {
    using TSelf = TYdbSetupSettings;

    // Cluster settings
    FLUENT_SETTING_DEFAULT(ui32, NodeCount, 1);
    FLUENT_SETTING_DEFAULT(TString, DomainName, "Root");
    FLUENT_SETTING_DEFAULT(bool, EnableResourcePools, true);

    // Default pool settings
    FLUENT_SETTING_DEFAULT(TString, PoolId, "sample_pool_id");
    FLUENT_SETTING_DEFAULT(i32, ConcurrentQueryLimit, -1);
    FLUENT_SETTING_DEFAULT(i32, QueueSize, -1);
    FLUENT_SETTING_DEFAULT(TDuration, QueryCancelAfter, FUTURE_WAIT_TIMEOUT);
    FLUENT_SETTING_DEFAULT(double, QueryMemoryLimitPercentPerNode, -1);
    FLUENT_SETTING_DEFAULT(double, DatabaseLoadCpuThreshold, -1);

    NResourcePool::TPoolSettings GetDefaultPoolSettings() const;
    TIntrusivePtr<IYdbSetup> Create() const;
};

class IYdbSetup : public TThrRefBase {
public:
    // Cluster helpers
    virtual void UpdateNodeCpuInfo(double usage, ui32 threads, ui64 nodeIndex = 0) = 0;

    // Scheme queries helpers
    virtual NYdb::NScheme::TSchemeClient GetSchemeClient() const = 0;
    virtual void ExecuteSchemeQuery(const TString& query, NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS, const TString& expectedMessage = "") const = 0;
    virtual THolder<NKikimr::NSchemeCache::TSchemeCacheNavigate> Navigate(const TString& path, NKikimr::NSchemeCache::TSchemeCacheNavigate::EOp operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown) const = 0;
    virtual void WaitPoolAccess(const TString& userSID, ui32 access, const TString& poolId = "") const = 0;

    // Generic query helpers
    virtual TQueryRunnerResult ExecuteQuery(const TString& query, TQueryRunnerSettings settings = TQueryRunnerSettings()) const = 0;
    virtual TQueryRunnerResultAsync ExecuteQueryAsync(const TString& query, TQueryRunnerSettings settings = TQueryRunnerSettings()) const = 0;

    // Async query execution actions
    virtual void WaitQueryExecution(const TQueryRunnerResultAsync& query, TDuration timeout = FUTURE_WAIT_TIMEOUT) const = 0;
    virtual void ContinueQueryExecution(const TQueryRunnerResultAsync& query) const = 0;
    virtual NActors::TActorId CreateInFlightCoordinator(ui32 numberRequests, ui32 expectedInFlight) const = 0;

    // Pools actions
    virtual TPoolStateDescription GetPoolDescription(TDuration leaseDuration = FUTURE_WAIT_TIMEOUT, const TString& poolId = "") const = 0;
    virtual void WaitPoolState(const TPoolStateDescription& state, const TString& poolId = "") const = 0;
    virtual void WaitPoolHandlersCount(i64 finalCount, std::optional<i64> initialCount = std::nullopt, TDuration timeout = FUTURE_WAIT_TIMEOUT) const = 0;
    virtual void StopWorkloadService(ui64 nodeIndex = 0) const = 0;
    virtual void ValidateWorkloadServiceCounters(bool checkTableCounters = true, const TString& poolId = "") const = 0;

    // Coomon helpers
    virtual TTestActorRuntime* GetRuntime() const = 0;
    virtual const TYdbSetupSettings& GetSettings() const = 0;
    static void WaitFor(TDuration timeout, TString description, std::function<bool(TString&)> callback);
};

// Test queries

struct TSampleQueries {
    template <typename TResult>
    static void CheckSuccess(const TResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    template <typename TResult>
    static void CheckOverloaded(const TResult& result, const TString& poolId) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::OVERLOADED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), TStringBuilder() << "Too many pending requests for pool " << poolId);
    }

    template <typename TResult>
    static void CheckCancelled(const TResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Request timeout exceeded, cancelling after");
    }

    struct TSelect42 {
        static constexpr char Query[] = "SELECT 42;";

        template <typename TResult>
        static void CheckResult(const TResult& result) {
            CheckSuccess(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, "Unexpected result set size");
            CompareYson("[[42]]", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    };

private:
    static void CompareYson(const TString& expected, const TString& actual);
};

}  // namespace NKikimr::NKqp::NWorkload
