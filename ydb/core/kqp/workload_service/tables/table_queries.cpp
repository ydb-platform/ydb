#include "table_queries.h"

#include <ydb/core/base/path.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/workload_service/common/events.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>

#include <ydb/library/query_actor/query_actor.h>

#include <ydb/library/table_creator/table_creator.h>


namespace NKikimr::NKqp::NWorkload {

namespace {

using namespace NActors;


class TQueryBase : public NKikimr::TQueryBase {
    using TBase = NKikimr::TQueryBase;

public:
    TQueryBase(const TString& operationName, const TString& traceId, NMonitoring::TDynamicCounterPtr counters)
        : TBase(NKikimrServices::KQP_WORKLOAD_SERVICE)
    {
        SetOperationInfo(operationName, traceId, counters);
    }

    TQueryBase(const TString& operationName, const TString& traceId, const TString& database, const TString& sessionId, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(operationName, ComposeTraceId(traceId, database, sessionId), counters)
    {}

    void UpdateLogInfo(const TString& traceId, const TString& database, const TString& sessionId) {
        SetOperationInfo(OperationName, ComposeTraceId(traceId, database, sessionId), nullptr);
    }

private:
    static TString ComposeTraceId(const TString& traceId, const TString& database, const TString& sessionId) {
        return TStringBuilder() << traceId << ", RequestDatabase: " << database << ", RequestSessionId: " << sessionId;
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
                Col("start_time", NScheme::NTypeIds::Timestamp),
                Col("session_id", NScheme::NTypeIds::Text),
                Col("node_id", NScheme::NTypeIds::Uint32),
                Col("wait_deadline", NScheme::NTypeIds::Timestamp),
                Col("lease_deadline", NScheme::NTypeIds::Timestamp),
            },
            { "database", "pool_id", "node_id", "start_time", "session_id" },
            NKikimrServices::KQP_WORKLOAD_SERVICE,
            TtlCol("lease_deadline", DEADLINE_OFFSET, BRO_RUN_INTERVAL)
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

class TCleanupTablesActor : public TSchemeActorBase<TCleanupTablesActor> {
    using TCleanupTablesRetryQuery = TQueryRetryActor<TCleanupTablesQuery, TEvPrivate::TEvCleanupTableResponse, TString>;

public:
    TCleanupTablesActor()
        : TablePathsToCheck(TTablesCreator::GetTablePahs())
    {}

    void DoBootstrap() {
        Become(&TCleanupTablesActor::StateFunc);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != TablePathsToCheck.size()) {
            OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, NYql::TIssue("Unexpected scheme cache response"));
            return;
        }

        TablePathsToCheck.clear();
        for (const auto& result : results) {
            const TString& fullPath = CanonizePath(result.Path);
            LOG_D("Describe table " << fullPath << " status " << result.Status);

            std::pair<TString, TString> pathPair;
            if (TString error; !TrySplitPathByDb(fullPath, AppData()->TenantName, pathPair, error)) {
                TablesExists = false;
                AddError(TStringBuilder() << "Failed to describe table path " << fullPath << ", " << error);
                continue;
            }

            switch (result.Status) {
                case EStatus::Unknown:
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::AccessDenied:
                case EStatus::RedirectLookupError:
                    TablesExists = false;
                    AddError(TStringBuilder() << "Failed to describe table path " << fullPath << ", " << result.Status);
                    break;
                case EStatus::LookupError:
                    RetryPathCheck(pathPair.second, result.Status);
                    break;
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                case EStatus::TableCreationNotComplete:
                    TablesExists = false;
                    break;
                case EStatus::Ok:
                    LOG_D("Start cleanup for table " << fullPath);
                    CleanupQueriesInFlight++;
                    Register(new TCleanupTablesRetryQuery(SelfId(), fullPath));
                    break;
            }
        }
        TryFinish();
    }

    void Handle(TEvPrivate::TEvCleanupTableResponse::TPtr& ev) {
        Y_ABORT_UNLESS(CleanupQueriesInFlight);
        CleanupQueriesInFlight--;

        LOG_D("Cleanup finished for table " << ev->Get()->Path << ", " << ev->Get()->Status << ", issues: " << ev->Get()->Issues.ToOneLineString());

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            AddError(GroupIssues(ev->Get()->Issues, TStringBuilder() << "Cleanup table " << ev->Get()->Path << " failed, " << ev->Get()->Status));
        }
        TryFinish();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPrivate::TEvCleanupTableResponse, Handle);
            default:
                StateFuncBase(ev);
        }
    }

protected:
    void StartRequest() override {
        LOG_D("Start check tables existence, number paths: " << TablePathsToCheck.size());
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(NTableCreator::BuildSchemeCacheNavigateRequest(
            TablePathsToCheck
        ).Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssue issue) override {
        Y_UNUSED(status);

        TablesExists = false;
        AddError(issue);
        TablePathsToCheck.clear();
        TryFinish();
    }

    TString LogPrefix() const override {
        return TStringBuilder() << "[TCleanupTablesActor] ActorId: " << SelfId() << ", ";
    }

private:
    void RetryPathCheck(const TString& path, EStatus status) {
        if (TablePathsToCheck.empty() && !ScheduleRetry(TStringBuilder() << "Retry " << status << " for table " << path)) {
            TablesExists = false;
            AddError(TStringBuilder() << "Retry limit exceeded for table " << path << ", " << status);
            return;
        }

        TablePathsToCheck.emplace_back(SplitPath(path));
    }

    template <typename TMessage>
    void AddError(const TMessage& message) {
        Success = false;
        Issues.AddIssue(message);
    }

    template <>
    void AddError(const NYql::TIssues& issues) {
        Success = false;
        Issues.AddIssues(issues);
    }

    void TryFinish() {
        if (!TablePathsToCheck.empty() || CleanupQueriesInFlight) {
            return;
        }

        if (Success) {
            LOG_D("Successfully finished");
        } else {
            LOG_E("Failed with issues: " << Issues.ToOneLineString());
        }

        Send(MakeKqpWorkloadServiceId(SelfId().NodeId()), new TEvPrivate::TEvCleanupTablesFinished(Success, TablesExists, std::move(Issues)));
        PassAway();
    }

private:
    TVector<TVector<TString>> TablePathsToCheck;
    size_t CleanupQueriesInFlight = 0;

    bool Success = true;
    bool TablesExists = true;
};


class TRefreshPoolStateQuery : public TQueryBase {
public:
    TRefreshPoolStateQuery(const TString& database, const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, database, "", counters)
        , Database(database)
        , PoolId(poolId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        if (!LeaseDuration) {
            OnLeaseUpdated();
            return;
        }

        TString sql = TStringBuilder() << R"(
            -- TRefreshPoolStateQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $lease_duration AS Interval;

            UPDATE `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            SET lease_deadline = CurrentUtcTimestamp() + $lease_duration
            WHERE database = $database
              AND pool_id = $pool_id
              AND node_id = $node_id
              AND (wait_deadline IS NULL OR wait_deadline >= CurrentUtcTimestamp())
              AND lease_deadline >= CurrentUtcTimestamp();

            UPDATE `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            SET lease_deadline = CurrentUtcTimestamp() + $lease_duration
            WHERE database = $database
              AND pool_id = $pool_id
              AND node_id = $node_id
              AND lease_deadline >= CurrentUtcTimestamp();
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
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
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;

            SELECT COUNT(*) AS delayed_requests
            FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND (wait_deadline IS NULL OR wait_deadline >= CurrentUtcTimestamp())
              AND lease_deadline >= CurrentUtcTimestamp();

            SELECT COUNT(*) AS running_requests
            FROM `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND lease_deadline >= CurrentUtcTimestamp();
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginAndCommitTx(true));
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
        Send(Owner, new TEvPrivate::TEvRefreshPoolStateResponse(status, PoolState, std::move(issues)));
    }

private:
    const TString Database;
    const TString PoolId;
    const TDuration LeaseDuration;

    TPoolStateDescription PoolState;
};


class TDelayRequestQuery : public TQueryBase {
public:
    TDelayRequestQuery(const TString& database, const TString& poolId, const TString& sessionId, TInstant startTime, TMaybe<TInstant> waitDeadline, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, database, sessionId, counters)
        , Database(database)
        , PoolId(poolId)
        , SessionId(sessionId)
        , StartTime(startTime)
        , WaitDeadline(waitDeadline)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TDelayRequestQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $start_time AS Timestamp;
            DECLARE $session_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $wait_deadline AS Optional<Timestamp>;
            DECLARE $lease_duration AS Interval;

            UPSERT INTO `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
                (database, pool_id, start_time, session_id, node_id, wait_deadline, lease_deadline)
            VALUES (
                $database, $pool_id, $start_time, $session_id, $node_id, $wait_deadline,
                CurrentUtcTimestamp() + $lease_duration
            );
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$start_time")
                .Timestamp(StartTime)
                .Build()
            .AddParam("$session_id")
                .Utf8(SessionId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$wait_deadline")
                .OptionalTimestamp(WaitDeadline)
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
        Send(Owner, new TEvPrivate::TEvDelayRequestResponse(status, SessionId, std::move(issues)));
    }

private:
    const TString Database;
    const TString PoolId;
    const TString SessionId;
    const TInstant StartTime;
    const TMaybe<TInstant> WaitDeadline;
    const TDuration LeaseDuration;
};


class TStartFirstDelayedRequestQuery : public TQueryBase {
public:
    TStartFirstDelayedRequestQuery(const TString& database, const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, database, "", counters)
        , Database(database)
        , PoolId(poolId)
        , LeaseDuration(leaseDuration)
    {}

    void OnRunQuery() override {
        RequestNodeId = SelfId().NodeId();

        TString sql = TStringBuilder() << R"(
            -- TStartFirstDelayedRequestQuery::OnRunQuery
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;

            SELECT database, pool_id, start_time, session_id, node_id
            FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND (wait_deadline IS NULL OR wait_deadline >= CurrentUtcTimestamp())
              AND lease_deadline >= CurrentUtcTimestamp()
            ORDER BY database, pool_id, start_time
            LIMIT 1;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build();

        RunDataQuery(sql, &params, TTxControl::BeginAndCommitTx(true));
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
        UpdateLogInfo(PoolId, Database, RequestSessionId);

        TMaybe<TInstant> startTime = result.ColumnParser("start_time").GetOptionalTimestamp();
        if (!startTime) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Start time is not specified for delayed request");
            return;
        }

        RequestStartTime = *startTime;
        StartQueuedRequest();
    }

    void StartQueuedRequest() {
        TString sql = TStringBuilder() << R"(
            -- TStartFirstDelayedRequestQuery::StartQueuedRequest
            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $start_time AS Timestamp;
            DECLARE $session_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $lease_duration AS Interval;

            DELETE FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND node_id = $node_id
              AND start_time = $start_time
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
                .Utf8(Database)
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$start_time")
                .Timestamp(RequestStartTime)
                .Build()
            .AddParam("$session_id")
                .Utf8(RequestSessionId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
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
        Send(Owner, new TEvPrivate::TEvStartRequestResponse(status, RequestNodeId, RequestSessionId, std::move(issues)));
    }

private:
    const TString Database;
    const TString PoolId;
    const TDuration LeaseDuration;

    ui32 RequestNodeId = 0;
    TString RequestSessionId;
    TInstant RequestStartTime;
};

class TStartRequestQuery : public TQueryBase {
public:
    TStartRequestQuery(const TString& database, const TString& poolId, const TString& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, database, sessionId, counters)
        , Database(database)
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
                .Utf8(Database)
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
        Send(Owner, new TEvPrivate::TEvStartRequestResponse(status, SelfId().NodeId(), SessionId, std::move(issues)));
    }

private:
    const TString Database;
    const TString PoolId;
    const TString SessionId;
    const TDuration LeaseDuration;
};

class TStartRequestActor : public TActorBootstrapped<TStartRequestActor> {
    using TStartFirstDelayedRequestRetryQuery = TQueryRetryActor<TStartFirstDelayedRequestQuery, TEvPrivate::TEvStartRequestResponse, TString, TString, TDuration, NMonitoring::TDynamicCounterPtr>;
    using TStartRequestRetryQuery = TQueryRetryActor<TStartRequestQuery, TEvPrivate::TEvStartRequestResponse, TString, TString, TString, TDuration, NMonitoring::TDynamicCounterPtr>;

public:
    TStartRequestActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const std::optional<TString>& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters)
        : ReplyActorId(replyActorId)
        , Database(database)
        , PoolId(poolId)
        , SessionId(sessionId)
        , LeaseDuration(leaseDuration)
        , Counters(counters)
    {}

    void Bootstrap() {
        Become(&TStartRequestActor::StateFunc);

        if (!SessionId) {
            Register(new TStartFirstDelayedRequestRetryQuery(SelfId(), Database, PoolId, LeaseDuration, Counters));
        } else {
            Register(new TStartRequestRetryQuery(SelfId(), Database, PoolId, *SessionId, LeaseDuration, Counters));
        }
    }

    void Handle(TEvPrivate::TEvStartRequestResponse::TPtr& ev) {
        Send(ev->Forward(ReplyActorId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvStartRequestResponse, Handle);
    )

private:
    const TActorId ReplyActorId;
    const TString Database;
    const TString PoolId;
    const std::optional<TString> SessionId;
    const TDuration LeaseDuration;
    const NMonitoring::TDynamicCounterPtr Counters;
};


class TCleanupRequestsQuery : public TQueryBase {
public:
    TCleanupRequestsQuery(const TString& database, const TString& poolId, const std::vector<TString>& sessionIds, NMonitoring::TDynamicCounterPtr counters)
        : TQueryBase(__func__, poolId, database, "", counters)
        , Database(database)
        , PoolId(poolId)
        , SessionIds(sessionIds)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TCleanupRequestsQuery::OnRunQuery
            PRAGMA AnsiInForEmptyOrNullableItemsCollections;

            DECLARE $database AS Text;
            DECLARE $pool_id AS Text;
            DECLARE $node_id AS Uint32;
            DECLARE $session_ids AS List<Text>;

            DELETE FROM `)" << TTablesCreator::GetDelayedRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND node_id = $node_id
              AND session_id IN $session_ids;

            DELETE FROM `)" << TTablesCreator::GetRunningRequestsPath() << R"(`
            WHERE database = $database
              AND pool_id = $pool_id
              AND node_id = $node_id
              AND session_id IN $session_ids;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database")
                .Utf8(Database)
                .Build()
            .AddParam("$pool_id")
                .Utf8(PoolId)
                .Build()
            .AddParam("$node_id")
                .Uint32(SelfId().NodeId())
                .Build();

        auto& param = params.AddParam("$session_ids").BeginList();
        for (const TString& sessionId : SessionIds) {
            LOG_T("Cleanup request with session id: " << sessionId);
            param.AddListItem().Utf8(sessionId);
        }
        param.EndList().Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvCleanupRequestsResponse(status, SessionIds, std::move(issues)));
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[TCleanupRequestsQuery] ActorId: " << SelfId() << ", Database: " << Database << ", PoolId: " << PoolId << ", ";
    }

private:
    const TString Database;
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

IActor* CreateRefreshPoolStateActor(const TActorId& replyActorId, const TString& database, const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TRefreshPoolStateQuery, TEvPrivate::TEvRefreshPoolStateResponse, TString, TString, TDuration, NMonitoring::TDynamicCounterPtr>(replyActorId, database, poolId, leaseDuration, counters);
}

IActor* CreateDelayRequestActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const TString& sessionId, TInstant startTime, TMaybe<TInstant> waitDeadline, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TDelayRequestQuery, TEvPrivate::TEvDelayRequestResponse, TString, TString, TString, TInstant, TMaybe<TInstant>, TDuration, NMonitoring::TDynamicCounterPtr>(replyActorId, database, poolId, sessionId, startTime, waitDeadline, leaseDuration, counters);
}

IActor* CreateStartRequestActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const std::optional<TString>& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters) {
    return new TStartRequestActor(replyActorId, database, poolId, sessionId, leaseDuration, counters);
}

IActor* CreateCleanupRequestsActor(const TActorId& replyActorId, const TString& database, const TString& poolId, const std::vector<TString>& sessionIds, NMonitoring::TDynamicCounterPtr counters) {
    return new TQueryRetryActor<TCleanupRequestsQuery, TEvPrivate::TEvCleanupRequestsResponse, TString, TString, std::vector<TString>, NMonitoring::TDynamicCounterPtr>(replyActorId, database, poolId, sessionIds, counters);
}

}  // NKikimr::NKqp::NWorkload
