#include "schema.h"
#include "validators.h"
#include "ydb_control_plane_storage_impl.h"

#include <ydb/core/yq/libs/ydb/create_schema.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

namespace NYq {

namespace {

void CollectDebugInfo(const TString& query, const TParams& params, TSession session, TDebugInfoPtr debugInfo) {
    if (debugInfo) {
        try {
            auto explainResult = session.ExplainDataQuery(query).GetValue(TDuration::Minutes(1));
            debugInfo->push_back({query, params, explainResult.GetPlan(), explainResult.GetAst(), {}});
        } catch (...) {
            debugInfo->push_back({query, params, {}, {}, CurrentExceptionMessage()});
        }
    }
}

ERetryErrorClass RetryFunc(const NYdb::TStatus& status) {
    return status.GetStatus() == NYdb::EStatus::OVERLOADED ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
}

TYdbSdkRetryPolicy::TPtr MakeCreateSchemaRetryPolicy() {
    return TYdbSdkRetryPolicy::GetExponentialBackoffPolicy(RetryFunc, TDuration::MilliSeconds(10), TDuration::Seconds(1), TDuration::Seconds(5));
}

} // namespace

void TYdbControlPlaneStorageActor::Bootstrap() {
    CPS_LOG_I("Starting ydb control plane storage service. Actor id: " << SelfId());
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YQ_CONTROL_PLANE_STORAGE_PROVIDER));

    YdbConnection = NewYdbConnection(Config.Proto.GetStorage(), CredProviderFactory, YqSharedResources->CoreYdbDriver);
    DbPool = YqSharedResources->DbPoolHolder->GetOrCreate(EDbPoolId::MAIN, 10, YdbConnection->TablePathPrefix);
    CreateDirectory();
    CreateQueriesTable();
    CreatePendingSmallTable();
    CreateConnectionsTable();
    CreateBindingsTable();
    CreateIdempotencyKeysTable();
    CreateResultSetsTable();
    CreateJobsTable();
    CreateNodesTable();
    // CreateQuotasTable(); // not yet
    Become(&TThis::StateFunc);
}

/*
* Creating tables
*/
void TYdbControlPlaneStorageActor::RunCreateTableActor(const TString& path, NYdb::NTable::TTableDescription desc) {
    Register(MakeCreateTableActor({}, NKikimrServices::YQ_CONTROL_PLANE_STORAGE, YdbConnection, path, std::move(desc), MakeCreateSchemaRetryPolicy()));
}

void TYdbControlPlaneStorageActor::CreateQueriesTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, QUERIES_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(QUERY_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(RESULT_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(GENERATION_COLUMN_NAME, EPrimitiveType::Uint64)
        .AddNullableColumn(NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(USER_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(VISIBILITY_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(AUTOMATIC_COLUMN_NAME, EPrimitiveType::Bool)
        .AddNullableColumn(STATUS_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(QUERY_TYPE_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(EXECUTE_MODE_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(QUERY_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(REVISION_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(INTERNAL_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(LAST_JOB_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(RESULT_SETS_EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(META_REVISION_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(TENANT_COLUMN_NAME, EPrimitiveType::String)
        .SetPrimaryKeyColumns({SCOPE_COLUMN_NAME, QUERY_ID_COLUMN_NAME})
        .SetTtlSettings(EXPIRE_AT_COLUMN_NAME)
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreatePendingSmallTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, PENDING_SMALL_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(QUERY_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(LAST_SEEN_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(RETRY_COUNTER_COLUMN_NAME, EPrimitiveType::Uint64)
        .AddNullableColumn(RETRY_COUNTER_UPDATE_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(QUERY_TYPE_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(IS_RESIGN_QUERY_COLUMN_NAME, EPrimitiveType::Bool)
        .AddNullableColumn(HOST_NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(OWNER_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(TENANT_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(ASSIGNED_UNTIL_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(RETRY_RATE_COLUMN_NAME, EPrimitiveType::Double)
        .SetPrimaryKeyColumns({TENANT_COLUMN_NAME, SCOPE_COLUMN_NAME, QUERY_ID_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateConnectionsTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, CONNECTIONS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(CONNECTION_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(CONNECTION_TYPE_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(USER_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(VISIBILITY_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(CONNECTION_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(REVISION_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(INTERNAL_COLUMN_NAME, EPrimitiveType::String)
        .SetPrimaryKeyColumns({SCOPE_COLUMN_NAME, CONNECTION_ID_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateDirectory()
{
    Register(MakeCreateDirectoryActor({}, NKikimrServices::YQ_CONTROL_PLANE_STORAGE, YdbConnection, YdbConnection->TablePathPrefix, MakeCreateSchemaRetryPolicy()));
}

void TYdbControlPlaneStorageActor::CreateJobsTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, JOBS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(JOB_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(QUERY_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(JOB_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(USER_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(VISIBILITY_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .SetPrimaryKeyColumns({SCOPE_COLUMN_NAME, QUERY_ID_COLUMN_NAME, JOB_ID_COLUMN_NAME})
        .SetTtlSettings(EXPIRE_AT_COLUMN_NAME)
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateNodesTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, NODES_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(TENANT_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(INSTANCE_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(NODE_ID_COLUMN_NAME, EPrimitiveType::Uint32)
        .AddNullableColumn(HOST_NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(ACTIVE_WORKERS_COLUMN_NAME, EPrimitiveType::Uint64)
        .AddNullableColumn(MEMORY_LIMIT_COLUMN_NAME, EPrimitiveType::Uint64)
        .AddNullableColumn(MEMORY_ALLOCATED_COLUMN_NAME, EPrimitiveType::Uint64)
        .AddNullableColumn(EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .AddNullableColumn(INTERCONNECT_PORT_COLUMN_NAME, EPrimitiveType::Uint32)
        .AddNullableColumn(NODE_ADDRESS_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(DATA_CENTER_COLUMN_NAME, EPrimitiveType::String)
        .SetTtlSettings(EXPIRE_AT_COLUMN_NAME)
        .SetPrimaryKeyColumns({TENANT_COLUMN_NAME, NODE_ID_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateBindingsTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, BINDINGS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(BINDING_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(CONNECTION_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(USER_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(VISIBILITY_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(BINDING_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(REVISION_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(INTERNAL_COLUMN_NAME, EPrimitiveType::String)
        .SetPrimaryKeyColumns({SCOPE_COLUMN_NAME, BINDING_ID_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateIdempotencyKeysTable()
{

    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, IDEMPOTENCY_KEYS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SCOPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(IDEMPOTENCY_KEY_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(RESPONSE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .SetTtlSettings(EXPIRE_AT_COLUMN_NAME)
        .SetPrimaryKeyColumns({SCOPE_COLUMN_NAME, IDEMPOTENCY_KEY_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateResultSetsTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, RESULT_SETS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(RESULT_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(RESULT_SET_ID_COLUMN_NAME, EPrimitiveType::Int32)
        .AddNullableColumn(ROW_ID_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(RESULT_SET_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(EXPIRE_AT_COLUMN_NAME, EPrimitiveType::Timestamp)
        .SetTtlSettings(EXPIRE_AT_COLUMN_NAME)
        .SetPrimaryKeyColumns({RESULT_ID_COLUMN_NAME, RESULT_SET_ID_COLUMN_NAME, ROW_ID_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

void TYdbControlPlaneStorageActor::CreateQuotasTable()
{
    auto tablePath = JoinPath(YdbConnection->TablePathPrefix, QUOTAS_TABLE_NAME);

    auto description = TTableBuilder()
        .AddNullableColumn(SUBJECT_TYPE_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(SUBJECT_ID_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(METRIC_NAME_COLUMN_NAME, EPrimitiveType::String)
        .AddNullableColumn(METRIC_LIMIT_COLUMN_NAME, EPrimitiveType::Int64)
        .AddNullableColumn(METRIC_USAGE_COLUMN_NAME, EPrimitiveType::Int64)
        .SetPrimaryKeyColumns({SUBJECT_TYPE_COLUMN_NAME, SUBJECT_ID_COLUMN_NAME, METRIC_NAME_COLUMN_NAME})
        .Build();

    RunCreateTableActor(tablePath, TTableDescription(description));
}

bool TYdbControlPlaneStorageActor::IsSuperUser(const TString& user)
{
    return AnyOf(Config.Proto.GetSuperUsers(), [&user](const auto& superUser) {
        return superUser == user;
    });
}

void InsertIdempotencyKey(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey, const TString& response, const TInstant& expireAt) {
    if (idempotencyKey) {
        builder.AddString("scope", scope);
        builder.AddString("idempotency_key", idempotencyKey);
        builder.AddString("response", response);
        builder.AddTimestamp("expire_at", expireAt);
        builder.AddText(
            "INSERT INTO `" IDEMPOTENCY_KEYS_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" IDEMPOTENCY_KEY_COLUMN_NAME "`, `" RESPONSE_COLUMN_NAME "`, `" EXPIRE_AT_COLUMN_NAME "`)\n"
            "VALUES ($scope, $idempotency_key, $response, $expire_at);\n"
        );
    }
}

void ReadIdempotencyKeyQuery(TSqlQueryBuilder& builder, const TString& scope, const TString& idempotencyKey) {
    if (idempotencyKey) {
        builder.AddString("scope", scope);
        builder.AddString("idempotency_key", idempotencyKey);
        builder.AddText(
            "SELECT `" RESPONSE_COLUMN_NAME "` FROM `" IDEMPOTENCY_KEYS_TABLE_NAME "`\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope AND `" IDEMPOTENCY_KEY_COLUMN_NAME "` = $idempotency_key;\n"
        );
    }
}

class TDbRequest: public NActors::TActorBootstrapped<TDbRequest> {
    using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
    TDbPool::TPtr DbPool;
    TPromise<NYdb::TStatus> Promise;
    TFunction Handler;

public:
    TDbRequest(const TDbPool::TPtr& dbPool, const TPromise<NYdb::TStatus>& promise, const TFunction& handler)
        : DbPool(dbPool)
        , Promise(promise)
        , Handler(handler)
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_STORAGE_DB_REQUEST";

    void Bootstrap() {
        CPS_LOG_T("DbRequest actor request. Actor id: " << SelfId());
        Become(&TDbRequest::StateFunc);
        Send(DbPool->GetNextActor(), new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvDbFunctionResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
    )

    void Handle(TEvents::TEvDbFunctionResponse::TPtr& ev) {
        CPS_LOG_T("DbRequest actor response. Actor id: " << SelfId());
        Promise.SetValue(ev->Get()->Status);
        PassAway();
    }

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&) {
        CPS_LOG_E("On delivered. Actor id: " << SelfId());
        Send(DbPool->GetNextActor(), new TEvents::TEvDbFunctionRequest(Handler), IEventHandle::FlagTrackDelivery);
    }
};

TAsyncStatus ExecDbRequest(TDbPool::TPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler) {
    TPromise<NYdb::TStatus> promise = NewPromise<NYdb::TStatus>();
    TActivationContext::Register(new TDbRequest(dbPool, promise, handler));
    return promise.GetFuture();
}

std::pair<TAsyncStatus, std::shared_ptr<TVector<NYdb::TResultSet>>> TYdbControlPlaneStorageActor::Read(
    const TString& query,
    const NYdb::TParams& params,
    const TRequestCountersPtr& requestCounters,
    TDebugInfoPtr debugInfo,
    TTxSettings transactionMode,
    bool retryOnTli)
{
    auto resultSet = std::make_shared<TVector<NYdb::TResultSet>>();

    std::shared_ptr<int> retryCount = std::make_shared<int>();
    auto handler = [=](TSession& session) {
        if (*retryCount != 0) {
            requestCounters->Retry->Inc();
        }
        ++(*retryCount);
        CollectDebugInfo(query, params, session, debugInfo);
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(transactionMode).CommitTx(), params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true));
        return result.Apply([retryOnTli, resultSet] (const TFuture<TDataQueryResult>& future) {
            NYdb::NTable::TDataQueryResult result = future.GetValue();
            *resultSet = result.GetResultSets();
            auto status = static_cast<TStatus>(result);
            if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                return TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}};
            }
            if (!retryOnTli && status.GetStatus() == EStatus::ABORTED) {
                return TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{status.GetIssues()}};
            }
            return status;
        });
    };

    TPromise<NYdb::TStatus> promise = NewPromise<NYdb::TStatus>();
    Register(new TDbRequest(DbPool, promise, handler));
    return {promise.GetFuture(), resultSet};
}

TAsyncStatus TYdbControlPlaneStorageActor::Validate(
    std::shared_ptr<TMaybe<TTransaction>> transaction,
    size_t item,
    const TVector<TValidationQuery>& validators,
    TSession session,
    std::shared_ptr<bool> successFinish,
    TDebugInfoPtr debugInfo,
    TTxSettings transactionMode)
{
    if (item >= validators.size()) {
        return MakeFuture(TStatus{EStatus::SUCCESS, NYql::TIssues{}});
    }

    const TValidationQuery& validatonItem = validators[item];
    CollectDebugInfo(validatonItem.Query, validatonItem.Params, session, debugInfo);
    auto result = session.ExecuteDataQuery(validatonItem.Query, item == 0 ? TTxControl::BeginTx(transactionMode) : TTxControl::Tx(**transaction), validatonItem.Params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true));
    return result.Apply([=, validator=validatonItem.Validator] (const TFuture<TDataQueryResult>& future) {
        NYdb::NTable::TDataQueryResult result = future.GetValue();
        *transaction = result.GetTransaction();
        auto status = static_cast<TStatus>(result);
        if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
            return MakeFuture(TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}});
        }
        if (!status.IsSuccess()) {
            return MakeFuture(status);
        }
        *successFinish = validator(result);
        if (*successFinish) {
            return MakeFuture(TStatus{EStatus::SUCCESS, NYql::TIssues{}});
        }
        return Validate(transaction, item + 1, validators, session, successFinish, debugInfo);
    });
}

TAsyncStatus TYdbControlPlaneStorageActor::Write(
    NActors::TActorSystem* actorSystem,
    const TString& query,
    const NYdb::TParams& params,
    const TRequestCountersPtr& requestCounters,
    TDebugInfoPtr debugInfo,
    const TVector<TValidationQuery>& validators,
    TTxSettings transactionMode,
    bool retryOnTli)
{
    std::shared_ptr<int> retryCount = std::make_shared<int>();
    auto transaction = std::make_shared<TMaybe<TTransaction>>();
    auto writeHandler = [=, retryOnTli=retryOnTli] (TSession session) {
        CollectDebugInfo(query, params, session, debugInfo);
        auto result = session.ExecuteDataQuery(query, validators ? TTxControl::Tx(**transaction).CommitTx() : TTxControl::BeginTx(transactionMode).CommitTx(), params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true));
        return result.Apply([=] (const TFuture<TDataQueryResult>& future) {
            NYdb::NTable::TDataQueryResult result = future.GetValue();
            auto status = static_cast<TStatus>(result);
            if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                return TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}};
            }
            if (!retryOnTli && status.GetStatus() == EStatus::ABORTED) {
                return TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{status.GetIssues()}};
            }
            return status;
        });
    };

    auto handler = [=] (TSession session) {
        if (*retryCount != 0) {
            requestCounters->Retry->Inc();
        }
        ++(*retryCount);
        std::shared_ptr<bool> successFinish = std::make_shared<bool>();
        return Validate(transaction, 0, validators, session, successFinish, debugInfo).Apply([=](const auto& future) {
            try {
                auto status = future.GetValue();
                if (!status.IsSuccess()) {
                    return future;
                }
                if (*successFinish) {
                    return future;
                }
                return writeHandler(session);
            } catch (const TControlPlaneStorageException& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{MakeErrorIssue(exception.Code, exception.GetRawMessage())}});
            } catch (const std::exception& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{exception.what()}}});
            } catch (...) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}});
            }
        });
    };
    TPromise<NYdb::TStatus> promise = NewPromise<NYdb::TStatus>();
    Register(new TDbRequest(DbPool, promise, handler));
    return promise.GetFuture();
}

NThreading::TFuture<void> TYdbControlPlaneStorageActor::PickTask(
    const TPickTaskParams& taskParams,
    const TRequestCountersPtr& requestCounters,
    TDebugInfoPtr debugInfo,
    std::shared_ptr<TResponseTasks> responseTasks,
    const TVector<TValidationQuery>& validators,
    TTxSettings transactionMode)
{
    return ReadModifyWrite(NActors::TActivationContext::ActorSystem(), taskParams.ReadQuery, taskParams.ReadParams,
        taskParams.PrepareParams, requestCounters, debugInfo, validators, transactionMode, taskParams.RetryOnTli)
            .Apply([=, responseTasks=responseTasks, queryId = taskParams.QueryId](const auto& future) {
                const auto status = future.GetValue();
                if (responseTasks && status.GetStatus() == EStatus::GENERIC_ERROR) {
                    responseTasks->SafeEraseTaskBlocking(queryId);
                }
            });
}

TAsyncStatus TYdbControlPlaneStorageActor::ReadModifyWrite(
    NActors::TActorSystem* actorSystem,
    const TString& readQuery,
    const NYdb::TParams& readParams,
    const std::function<std::pair<TString, NYdb::TParams>(const TVector<NYdb::TResultSet>&)>& prepare,
    const TRequestCountersPtr& requestCounters,
    TDebugInfoPtr debugInfo,
    const TVector<TValidationQuery>& validators,
    TTxSettings transactionMode,
    bool retryOnTli)
{
    std::shared_ptr<int> retryCount = std::make_shared<int>();
    auto resultSets = std::make_shared<TVector<NYdb::TResultSet>>();
    auto transaction = std::make_shared<TMaybe<TTransaction>>();

    auto readModifyWriteHandler = [=](TSession session) {
        CollectDebugInfo(readQuery, readParams, session, debugInfo);
        auto readResult = session.ExecuteDataQuery(readQuery, validators ? TTxControl::Tx(**transaction) : TTxControl::BeginTx(transactionMode), readParams, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true));
        auto readResultStatus = readResult.Apply([resultSets, transaction] (const TFuture<TDataQueryResult>& future) {
            NYdb::NTable::TDataQueryResult result = future.GetValue();
            *resultSets = result.GetResultSets();
            *transaction = result.GetTransaction();
            auto status = static_cast<TStatus>(result);
            if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                return TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}};
            }
            return status;
        });

        TFuture<std::pair<TString, NYdb::TParams>> resultPrepare = readResultStatus.Apply([=](const auto& future) {
            return future.GetValue().IsSuccess() ? prepare(*resultSets) : make_pair(TString(""), NYdb::TParamsBuilder{}.Build());
        });

        return resultPrepare.Apply([=](const auto& future) mutable {
            if (!readResultStatus.GetValue().IsSuccess()) {
                return readResultStatus;
            }

            try {
                auto [writeQuery, params] = future.GetValue();
                if (!writeQuery) {
                    return transaction->Get()->Commit().Apply([] (const auto& future) {
                        auto result = future.GetValue();
                        auto status = static_cast<TStatus>(result);
                        if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                            return TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}};
                        }
                        return status;
                    });
                }
                CollectDebugInfo(writeQuery, params, session, debugInfo);
                auto writeResult = session.ExecuteDataQuery(writeQuery, TTxControl::Tx(**transaction).CommitTx(), params, NYdb::NTable::TExecDataQuerySettings().KeepInQueryCache(true));
                return writeResult.Apply([retryOnTli] (const TFuture<TDataQueryResult>& future) {
                    NYdb::NTable::TDataQueryResult result = future.GetValue();
                    auto status = static_cast<TStatus>(result);
                    if (status.GetStatus() == EStatus::SCHEME_ERROR) { // retry if table does not exist
                        return TStatus{EStatus::UNAVAILABLE, NYql::TIssues{status.GetIssues()}};
                    }
                    if (!retryOnTli && status.GetStatus() == EStatus::ABORTED) {
                        return TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{status.GetIssues()}};
                    }
                    return status;
                });
            } catch (const TControlPlaneStorageException& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{MakeErrorIssue(exception.Code, exception.GetRawMessage())}});
            } catch (const std::exception& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{exception.what()}}});
            } catch (...) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}});
            }
        });
    };

    auto handler = [=] (TSession session) {
        if (*retryCount != 0) {
            requestCounters->Retry->Inc();
        }
        ++(*retryCount);

        std::shared_ptr<bool> successFinish = std::make_shared<bool>();
        return Validate(transaction, 0, validators, session, successFinish, debugInfo).Apply([=](const auto& future) {
            try {
                auto status = future.GetValue();
                if (!status.IsSuccess()) {
                    return future;
                }
                if (*successFinish) {
                    return future;
                }
                return readModifyWriteHandler(session);
            } catch (const TControlPlaneStorageException& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{MakeErrorIssue(exception.Code, exception.GetRawMessage())}});
            } catch (const std::exception& exception) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{exception.what()}}});
            } catch (...) {
                CPS_LOG_AS_D(*actorSystem, "Validation: " << CurrentExceptionMessage());
                return MakeFuture(TStatus{EStatus::GENERIC_ERROR, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}});
            }
        });
    };
    TPromise<NYdb::TStatus> promise = NewPromise<NYdb::TStatus>();
    Register(new TDbRequest(DbPool, promise, handler));
    return promise.GetFuture();
}

NActors::IActor* CreateYdbControlPlaneStorageServiceActor(
    const NConfig::TControlPlaneStorageConfig& config,
    const NConfig::TCommonConfig& common,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TString& tenantName) {
    return new TYdbControlPlaneStorageActor(config, common, counters, yqSharedResources, credentialsProviderFactory, tenantName);
}

} // NYq
