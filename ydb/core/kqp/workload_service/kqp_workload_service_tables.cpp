#include "kqp_workload_service_tables.h"
#include "kqp_workload_service_tables_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/table_creator/table_creator.h>


namespace NKikimr::NKqp::NWorkload {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadServiceTables] " << LogPrefix() << stream)

using namespace NActors;


class TQueryBase : public NKikimr::TQueryBase {
    using TBase = NKikimr::TQueryBase;

public:
    TQueryBase(const TString& operationName, const TString& traceId, NMonitoring::TDynamicCounterPtr counters)
        : TBase(NKikimrServices::KQP_WORKLOAD_SERVICE)
    {
        SetOperationInfo(operationName, traceId, counters);
    }

    TQueryBase(const TString& operationName, const TString& traceId, const TString& sessionId, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(operationName, ComposeTraceId(traceId, sessionId), counters)
    {}

    void UpdateLogInfo(const TString& traceId, const TString& sessionId) {
        SetOperationInfo(OperationName, ComposeTraceId(traceId, sessionId), nullptr);
    }

private:
    TString ComposeTraceId(const TString& traceId, const TString& sessionId) {
        return TStringBuilder() << traceId << ", RequestSessionId: " << sessionId;
    }
};


class TTablesCreator : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

    static constexpr TDuration DEADLINE_OFFSET = TDuration::Minutes(20);
    static constexpr TDuration BRO_RUN_INTERVAL = TDuration::Minutes(60);

    inline static const TVector<TString> PathPrefix = { ".metadata", "workload_manager" };
    static constexpr char DelayedRequests[] = "delayed_requests";
    static constexpr char RunningRequests[] = "running_requests";

public:
    explicit TTablesCreator()
        : TBase({
            GetDelayedRequestsCreator(),
            GetRunningRequestsCreator()
        })
    {}

    static TVector<TVector<TString>> GetTablePahs() {
        return {GetTablePath(DelayedRequests), GetTablePath(RunningRequests)};
    }

    static TString GetDelayedRequestsPath() {
        return JoinPath(GetTablePath(DelayedRequests));
    }

    static TString GetRunningRequestsPath() {
        return JoinPath(GetTablePath(RunningRequests));
    }

private:
    static TVector<TString> GetTablePath(const char* tableName) {
        auto path = PathPrefix;
        path.emplace_back(tableName);
        return path;
    }

    static IActor* GetDelayedRequestsCreator() {
        return CreateTableCreator(
            GetTablePath(DelayedRequests),
            {
                Col("database", NScheme::NTypeIds::Text),
                Col("pool_id", NScheme::NTypeIds::Text),
                Col("session_id", NScheme::NTypeIds::Text),
                Col("node_id", NScheme::NTypeIds::Uint32),
                Col("wait_deadline", NScheme::NTypeIds::Timestamp),
            },
            { "database", "pool_id", "wait_deadline", "session_id" },
            NKikimrServices::KQP_WORKLOAD_SERVICE,
            TtlCol("wait_deadline", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
        );
    }

    static IActor* GetRunningRequestsCreator() {
        return CreateTableCreator(
            GetTablePath(RunningRequests),
            {
                Col("database", NScheme::NTypeIds::Text),
                Col("pool_id", NScheme::NTypeIds::Text),
                Col("session_id", NScheme::NTypeIds::Text),
                Col("node_id", NScheme::NTypeIds::Uint32),
                Col("lease_deadline", NScheme::NTypeIds::Timestamp),
            },
            { "database", "pool_id", "node_id", "session_id" },
            NKikimrServices::KQP_WORKLOAD_SERVICE,
            TtlCol("lease_deadline", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
        );
    }

    void OnTablesCreated(bool success, NYql::TIssues issues) override  {
        Send(Owner, new TEvPrivate::TEvTablesCreationFinished(success, std::move(issues)));
    }
};


class TCleanupTablesQuery : public TQueryBase {
public:
    explicit TCleanupTablesQuery(const TString& path)
        : TQueryBase(__func__, path, nullptr)
        , Path(path)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TCleanupTablesQuery::OnRunQuery
            DECLARE $node_id AS Uint32;

            DELETE FROM `)" << Path << R"(`
            WHERE node_id = $node_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvCleanupTableResponse(status, Path, std::move(issues)));
    }

private:
    const TString Path;
};

class TCleanupTablesActor : public TActorBootstrapped<TCleanupTablesActor> {
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

    using TRetryPolicy = IRetryPolicy<>;
    using TCleanupTablesRetryQuery = TQueryRetryActor<TCleanupTablesQuery, TEvPrivate::TEvCleanupTableResponse, TString>;

public:
    TCleanupTablesActor()
        : TablePathsToCheck(TTablesCreator::GetTablePahs())
    {}

    void Bootstrap() {
        CheckTablesExistance();
        Become(&TCleanupTablesActor::StateFunc);
    }

    void CheckTablesExistance() const {
        LOG_D("Start check tables existence, number paths: " << TablePathsToCheck.size());
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(NTableCreator::BuildSchemeCacheNavigateRequest(
            TablePathsToCheck
        ).Release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown && ScheduleRetry("Scheme cache service not found")) {
            LOG_D("Scheduled retry for TEvNavigateKeySet to scheme cache");
            return;
        }

        TablesExists = false;
        AddError("Scheme cache is unavailable");
        TablePathsToCheck.clear();
        TryFinish();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TablePathsToCheck.clear();
        for (const auto& result : ev->Get()->Request->ResultSet) {
            const TString& path = JoinPath(result.Path);
            LOG_D("Describe table '" << path << "' status " << result.Status);

            switch (result.Status) {
                case EStatus::Unknown:
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::AccessDenied:
                case EStatus::RedirectLookupError:
                    TablesExists = false;
                    AddError(TStringBuilder() << "Failed to describe table path '" << path << "', " << result.Status);
                    break;
                case EStatus::LookupError:
                    RetryPathCheck(result.Path, result.Status);
                    break;
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                case EStatus::TableCreationNotComplete:
                    TablesExists = false;
                    break;
                case EStatus::Ok:
                    LOG_D("Start cleanup for table '" << path << "'");
                    CleanupQueriesInFlight++;
                    Register(new TCleanupTablesRetryQuery(SelfId(), path));
                    break;
            }
        }
        TryFinish();
    }

    void Handle(TEvPrivate::TEvCleanupTableResponse::TPtr& ev) {
        Y_ABORT_UNLESS(CleanupQueriesInFlight);
        CleanupQueriesInFlight--;

        LOG_D("Cleanup finished for table '" << ev->Get()->Path << "' " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssue rootIssue(TStringBuilder() << "Cleanup table '" << ev->Get()->Path << "' failed, " << ev->Get()->Status);
            for (const NYql::TIssue& issue : ev->Get()->Issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
            AddError(rootIssue);
        }

        TryFinish();
    }

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvWakeup, CheckTablesExistance);
        hFunc(TEvents::TEvUndelivered, Handle)
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(TEvPrivate::TEvCleanupTableResponse, Handle);
    )

private:
    bool ScheduleRetry(const TString& message) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay()) {
            Issues.AddIssue(message);
            Schedule(*delay, new TEvents::TEvWakeup());
            return true;
        }

        return false;
    }

    void RetryPathCheck(const TVector<TString>& path, EStatus status) {
        if (TablePathsToCheck.empty() && !ScheduleRetry(TStringBuilder() << "Retry " << status << " for table '" << CanonizePath(path) << "'")) {
            TablesExists = false;
            AddError(TStringBuilder() << "Retry limit exceeded for table '" << CanonizePath(path) << "', " << status);
            return;
        }

        TablePathsToCheck.emplace_back(path);
    }

    template <typename TMessage>
    void AddError(const TMessage& message) {
        Success = false;
        Issues.AddIssue(message);
    }

    void TryFinish() {
        if (TablePathsToCheck.empty() && !CleanupQueriesInFlight) {
            Reply();
        }
    }

    void Reply() {
        if (Success) {
            LOG_D("Successfully finished");
        } else {
            LOG_E("Failed with issues: " << Issues.ToOneLineString());
        }

        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new TEvPrivate::TEvCleanupTablesFinished(Success, TablesExists, std::move(Issues)));
        PassAway();
    }

private:
    static TRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return TRetryPolicy::GetFixedIntervalPolicy(
                  [](){ return ERetryErrorClass::ShortRetry; }
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(300)
                , 100
            )->CreateRetryState();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[TCleanupTablesActor] ActorId: " << SelfId() << ", ";
    }

private:
    TVector<TVector<TString>> TablePathsToCheck;

    size_t CleanupQueriesInFlight = 0;
    TRetryPolicy::IRetryState::TPtr RetryState;

    bool Success = true;
    bool TablesExists = true;
    NYql::TIssues Issues;
};


class TRefreshPoolStateQuery : public TQueryBase {
public:
    TRefreshPoolStateQuery(const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, counters)
        , PoolId(poolId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TRefreshPoolStateQuery::OnRunQuery
            DECLARE $pool_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $lease_duration AS Interval;

            UPDATE `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            SET lease_deadline = CurrentUtcTimestamp() + $lease_duration
            WHERE pool_id = $pool_id
              AND node_id = $node_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TRefreshPoolStateQuery::OnLeaseUpdated, "Update lease");
    }

    void OnLeaseUpdated() {
        TString sql = TStringBuilder() << R"(
            -- TRefreshPoolStateQuery::OnLeaseUpdated
            DECLARE $pool_id AS Text;

            SELECT COUNT(*) AS delayed_requests
            FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND wait_deadline >= CurrentUtcTimestamp();

            SELECT COUNT(*) AS running_requests
            FROM `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND lease_deadline >= CurrentUtcTimestamp();
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TRefreshPoolStateQuery::OnQueryResult, "Describe pool");
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 2) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        {  // Requests queue
            NYdb::TResultSetParser result(ResultSets[0]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            PoolState.DelayedRequests = result.ColumnParser("delayed_requests").GetUint64();
        }

        {  // Running requests
            NYdb::TResultSetParser result(ResultSets[1]);
            if (!result.TryNextRow()) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
                return;
            }

            PoolState.RunningRequests = result.ColumnParser("running_requests").GetUint64();
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvRefreshPoolStateResponse(status, PoolId, PoolState, std::move(issues)));
    }

private:
    const TString PoolId;
    const TDuration LeaseDuration;

    TPoolStateDescription PoolState;
};


class TDelayRequestQuery : public TQueryBase {
public:
    TDelayRequestQuery(const TString& poolId, const TString& sessionId, TInstant waitDeadline, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, sessionId, counters)
        , PoolId(poolId)
        , SessionId(sessionId)
        , WaitDeadline(waitDeadline)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TDelayRequestQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $session_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $wait_deadline AS Timestamp;

            UPSERT INTO `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
                (database, pool_id, session_id, node_id, wait_deadline)
            VALUES (
                $database, $pool_id, $session_id, $node_id, $wait_deadline
            );
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(GetDefaultDatabase())
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$session_id")
                .Utf8(SessionId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$wait_deadline")
                .Timestamp(WaitDeadline)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvDelayRequestResponse(status, PoolId, SessionId, std::move(issues)));
    }

private:
    const TString PoolId;
    const TString SessionId;
    const TInstant WaitDeadline;
};


class TStartFirstDelayedRequestQuery : public TQueryBase {
public:
    TStartFirstDelayedRequestQuery(const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, counters)
        , PoolId(poolId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TStartFirstDelayedRequestQuery::OnRunQuery
            DECLARE $pool_id AS Text;

            SELECT pool_id, wait_deadline, session_id, node_id
            FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND wait_deadline >= CurrentUtcTimestamp()
            ORDER BY pool_id, wait_deadline
            LIMIT 1;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TStartFirstDelayedRequestQuery::OnGetPoolInfo, "Describe pool");
    }

    void OnGetPoolInfo() {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish();
            return;
        }

        TMaybe<ui32> nodeId = result.ColumnParser("node_id").GetOptionalUint32();
        if (!nodeId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Node id is not specified for delayed request");
            return;
        }

        RequestNodeId = *nodeId;
        if (RequestNodeId != SelfId().NodeId()) {
            Finish();
            return;
        }

        TMaybe<TString> sessionId = result.ColumnParser("session_id").GetOptionalUtf8();
        if (!sessionId) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Session id is not specified for delayed request");
            return;
        }

        RequestSessionId = *sessionId;
        UpdateLogInfo(PoolId, RequestSessionId);

        TMaybe<TInstant> waitDeadline = result.ColumnParser("wait_deadline").GetOptionalTimestamp();
        if (!waitDeadline) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Wait deadline is not specified for delayed request");
            return;
        }

        RequestWaitDeadline = *waitDeadline;
        StartQueuedRequest();
    }

    void StartQueuedRequest() {
        TString sql = TStringBuilder() << R"(
            -- TStartFirstDelayedRequestQuery::StartQueuedRequest
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $session_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $wait_deadline AS Timestamp;
            DECLARE $lease_duration AS Interval;

            DELETE FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND wait_deadline = $wait_deadline
              AND session_id = $session_id;

            UPSERT INTO `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
                (database, pool_id, session_id, node_id, lease_deadline)
            VALUES (
                $database, $pool_id, $session_id, $node_id,
                CurrentUtcTimestamp() + $lease_duration
            );
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(GetDefaultDatabase())
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$session_id")
                .Utf8(RequestSessionId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$wait_deadline")
                .Timestamp(RequestWaitDeadline)
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build();

        RunDataQuery(sql, &params);
        SetQueryResultHandler(&TStartFirstDelayedRequestQuery::OnQueryResult, "Start request");
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvStartRequestResponse(status, PoolId, RequestNodeId, RequestSessionId, std::move(issues)));
    }

private:
    const TString PoolId;
    const TDuration LeaseDuration;

    ui32 RequestNodeId = 0;
    TString RequestSessionId;
    TInstant RequestWaitDeadline;
};

class TStartRequestQuery : public TQueryBase {
public:
    TStartRequestQuery(const TString& poolId, const TString& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, sessionId, counters)
        , PoolId(poolId)
        , SessionId(sessionId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TStartRequestQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $session_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $lease_duration AS Interval;

            UPSERT INTO `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
                (database, pool_id, session_id, node_id, lease_deadline)
            VALUES (
                $database, $pool_id, $session_id, $node_id,
                CurrentUtcTimestamp() + $lease_duration
            );
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(GetDefaultDatabase())
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$session_id")
                .Utf8(SessionId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$lease_duration")
                .Interval(static_cast<i64>(LeaseDuration.MicroSeconds()))
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvStartRequestResponse(status, PoolId, SelfId().NodeId(), SessionId, std::move(issues)));
    }

private:
    const TString PoolId;
    const TString SessionId;
    const TDuration LeaseDuration;
};

class TStartRequestActor : public TActorBootstrapped<TStartRequestActor> {
    using TStartFirstDelayedRequestRetryQuery = TQueryRetryActor<TStartFirstDelayedRequestQuery, TEvPrivate::TEvStartRequestResponse, TString, TDuration, NMonitoring::TDynamicCounterPtr>;
    using TStartRequestRetryQuery = TQueryRetryActor<TStartRequestQuery, TEvPrivate::TEvStartRequestResponse, TString, TString, TDuration, NMonitoring::TDynamicCounterPtr>;

public:
    TStartRequestActor(const TString& poolId, const std::optional<TString>& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : PoolId(poolId)
        , SessionId(sessionId)
        , LeaseDuration(leaseDuration)
        , Counters(counters)
    {}

    void Bootstrap() {
        Become(&TStartRequestActor::StateFunc);

        if (!SessionId) {
            Register(new TStartFirstDelayedRequestRetryQuery(SelfId(), PoolId, LeaseDuration, Counters));
        } else {
            Register(new TStartRequestRetryQuery(SelfId(), PoolId, *SessionId, LeaseDuration, Counters));
        }
    }

    void Handle(TEvPrivate::TEvStartRequestResponse::TPtr& ev) {
        Send(ev->Forward(MakeKqpWorkloadServiceId(SelfId().NodeId())));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvStartRequestResponse, Handle);
    )

private:
    const TString PoolId;
    const std::optional<TString> SessionId;
    const TDuration LeaseDuration;
    const NMonitoring::TDynamicCounterPtr Counters;
};


class TCleanupRequestsQuery : public TQueryBase {
public:
    TCleanupRequestsQuery(const TString& poolId, const std::vector<TString>& sessionIds, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, counters)
        , PoolId(poolId)
        , SessionIds(sessionIds)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TCleanupRequestsQuery::OnRunQuery
            PRAGMA AnsiInForEmptyOrNullableItemsCollections;

            DECLARE $pool_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $session_ids AS List<Text>;

            DELETE FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND session_id IN $session_ids;

            DELETE FROM `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            WHERE pool_id = $pool_id
              AND node_id = $node_id
              AND session_id IN $session_ids;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build();

        auto& param = params.AddParam("$session_ids").BeginList();
        for (const TString& sessionId : SessionIds) {
            LOG_T("cleanup request with session id: " << sessionId);
            param.AddListItem().Utf8(sessionId);
        }
        param.EndList().Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvCleanupRequestsResponse(status, PoolId, SessionIds, std::move(issues)));
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TCleanupRequestsQuery] ActorId: " << SelfId() << ", PoolId: " << PoolId << ", ";
    }

private:
    const TString PoolId;
    const std::vector<TString> SessionIds;
};

}  // anonymous namespace

IActor* CreateTablesCreator() {
    return new TTablesCreator();
}

IActor* CreateCleanupTablesActor() {
    return new TCleanupTablesActor();
}

IActor* CreateRefreshPoolStateActor(const TActorId& replyActorId, const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TRefreshPoolStateQuery, TEvPrivate::TEvRefreshPoolStateResponse, TString, TDuration, NMonitoring::TDynamicCounterPtr>(replyActorId, poolId, leaseDuration, counters);
}

IActor* CreateDelayRequestActor(const TActorId& replyActorId, const TString& poolId, const TString& sessionId, TInstant waitDeadline, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TDelayRequestQuery, TEvPrivate::TEvDelayRequestResponse, TString, TString, TInstant, NMonitoring::TDynamicCounterPtr>(replyActorId, poolId, sessionId, waitDeadline, counters);
}

IActor* CreateStartRequestActor(const TString& poolId, const std::optional<TString>& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters) {
    return new TStartRequestActor(poolId, sessionId, leaseDuration, counters);
}

IActor* CreateCleanupRequestsActor(const TActorId& replyActorId, const TString& poolId, const std::vector<TString>& sessionIds, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TCleanupRequestsQuery, TEvPrivate::TEvCleanupRequestsResponse, TString, std::vector<TString>, NMonitoring::TDynamicCounterPtr>(replyActorId, poolId, sessionIds, counters);
}

}  // NKikimr::NKqp::NWorkload
