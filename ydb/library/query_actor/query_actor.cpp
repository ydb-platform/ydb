#include "query_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/system/yassert.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, LogComponent, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, LogComponent, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, LogComponent, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, LogComponent, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, LogComponent, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, LogComponent, stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, LogComponent, stream)

namespace NKikimr {

TQueryBase::TTxControl TQueryBase::TTxControl::CommitTx() {
    TTxControl control;
    control.Commit = true;
    return control;
}

TQueryBase::TTxControl TQueryBase::TTxControl::BeginTx() {
    TTxControl control;
    control.Begin = true;
    return control;
}

TQueryBase::TTxControl TQueryBase::TTxControl::BeginAndCommitTx() {
    TTxControl control;
    control.Begin = true;
    control.Commit = true;
    return control;
}

TQueryBase::TTxControl TQueryBase::TTxControl::ContinueTx() {
    TTxControl control;
    control.Continue = true;
    return control;
}

TQueryBase::TTxControl TQueryBase::TTxControl::ContinueAndCommitTx() {
    TTxControl control;
    control.Continue = true;
    control.Commit = true;
    return control;
}

NYql::TIssues TQueryBase::TEvQueryBasePrivate::IssuesFromOperation(const Ydb::Operations::Operation& operation) {
    NYql::TIssues issues;
    NYql::IssuesFromMessage(operation.issues(), issues);
    return issues;
}

TQueryBase::TEvQueryBasePrivate::TEvDataQueryResult::TEvDataQueryResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
    : Status(status)
    , Issues(std::move(issues))
{
}

TQueryBase::TEvQueryBasePrivate::TEvDataQueryResult::TEvDataQueryResult(const Ydb::Table::ExecuteDataQueryResponse& resp)
    : TEvDataQueryResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
{
    resp.operation().result().UnpackTo(&Result);
}

TQueryBase::TEvQueryBasePrivate::TEvCreateSessionResult::TEvCreateSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
    : Status(status)
    , Issues(std::move(issues))
{
}

TQueryBase::TEvQueryBasePrivate::TEvCreateSessionResult::TEvCreateSessionResult(const Ydb::Table::CreateSessionResponse& resp)
    : TEvCreateSessionResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
{
    Ydb::Table::CreateSessionResult result;
    resp.operation().result().UnpackTo(&result);
    SessionId = result.session_id();
}

TQueryBase::TEvQueryBasePrivate::TEvDeleteSessionResult::TEvDeleteSessionResult(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
    : Status(status)
    , Issues(std::move(issues))
{
}

TQueryBase::TEvQueryBasePrivate::TEvDeleteSessionResult::TEvDeleteSessionResult(const Ydb::Table::DeleteSessionResponse& resp)
    : TEvDeleteSessionResult(resp.operation().status(), IssuesFromOperation(resp.operation()))
{
}

TQueryBase::TEvQueryBasePrivate::TEvRollbackTransactionResponse::TEvRollbackTransactionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
    : Status(status)
    , Issues(std::move(issues))
{
}

TQueryBase::TEvQueryBasePrivate::TEvRollbackTransactionResponse::TEvRollbackTransactionResponse(const Ydb::Table::RollbackTransactionResponse& resp)
    : TEvRollbackTransactionResponse(resp.operation().status(), IssuesFromOperation(resp.operation()))
{
}

TQueryBase::TEvQueryBasePrivate::TEvCommitTransactionResponse::TEvCommitTransactionResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues)
    : Status(status)
    , Issues(std::move(issues))
{
}

TQueryBase::TEvQueryBasePrivate::TEvCommitTransactionResponse::TEvCommitTransactionResponse(const Ydb::Table::CommitTransactionResponse& resp)
    : TEvCommitTransactionResponse(resp.operation().status(), IssuesFromOperation(resp.operation()))
{
}

TQueryBase::TQueryBase(ui64 logComponent, TString sessionId, TString database)
    : LogComponent(logComponent)
    , Database(std::move(database))
    , SessionId(std::move(sessionId))
{
}

void TQueryBase::Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) {
    NActors::TActorBootstrapped<TQueryBase>::Registered(sys, owner);
    Owner = owner;
}

STRICT_STFUNC(TQueryBase::StateFunc,
    hFunc(TEvQueryBasePrivate::TEvDataQueryResult, Handle);
    hFunc(TEvQueryBasePrivate::TEvCreateSessionResult, Handle);
    hFunc(TEvQueryBasePrivate::TEvDeleteSessionResult, Handle);
    hFunc(TEvQueryBasePrivate::TEvRollbackTransactionResponse, Handle);
    hFunc(TEvQueryBasePrivate::TEvCommitTransactionResponse, Handle);
);

void TQueryBase::Bootstrap() {
    Become(&TQueryBase::StateFunc);

    if (!Database) {
        Database = GetDefaultDatabase();
    }

    if (SessionId) {
        RunQuery();
    } else {
        RunCreateSession();
    }
}

TString TQueryBase::GetDefaultDatabase() {
    return CanonizePath(AppData()->TenantName);
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvCreateSessionResult::TPtr& ev) {
    if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
        SessionId = ev->Get()->SessionId;
        DeleteSession = true;
        RunQuery();
        Y_ABORT_UNLESS(Finished || RunningQuery);
    } else {
        LOG_W("Failed to create session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvDeleteSessionResult::TPtr& ev) {
    if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
        LOG_W("Failed to delete session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
    }
    PassAway();
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvDataQueryResult::TPtr& ev) {
    Y_ABORT_UNLESS(RunningQuery);
    NumberRequests++;
    AmountRequestsTime += TInstant::Now() - RequestStartTime;
    RunningQuery = false;
    TxId = ev->Get()->Result.tx_meta().id();
    LOG_D("TQueryBase. TEvDataQueryResult " << ev->Get()->Status << ", Issues: \"" << ev->Get()->Issues.ToOneLineString() << "\", SessionId: " << SessionId << ", TxId: " << TxId);
    if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
        ResultSets.clear();
        ResultSets.reserve(ev->Get()->Result.result_sets_size());
        for (auto& resultSet : *ev->Get()->Result.mutable_result_sets()) {
            ResultSets.emplace_back(std::move(resultSet));
        }
        try {
            (this->*QueryResultHandler)();
        } catch (const std::exception& ex) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, ex.what());
        }
        Y_ABORT_UNLESS(Finished || RunningQuery || RunningCommit);
    } else {
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvRollbackTransactionResponse::TPtr& ev) {
    LOG_D("RollbackTransactionResult: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());

    // Continue finish
    if (DeleteSession) {
        RunDeleteSession();
    } else {
        PassAway();
    }
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvCommitTransactionResponse::TPtr& ev) {
    LOG_D("CommitTransactionResult: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());

    OnFinish(ev->Get()->Status, std::move(ev->Get()->Issues));

    if (DeleteSession) {
        RunDeleteSession();
    } else {
        PassAway();
    }
}

void TQueryBase::Finish(Ydb::StatusIds::StatusCode status, const TString& message, bool rollbackOnError) {
    NYql::TIssues issues;
    issues.AddIssue(message);
    Finish(status, std::move(issues), rollbackOnError);
}

void TQueryBase::Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues, bool rollbackOnError) {
    if (status == Ydb::StatusIds::SUCCESS) {
        LOG_D("TQueryBase. Finish with SUCCESS, SessionId: " << SessionId << ", TxId: " << TxId);
    } else {
        LOG_W("TQueryBase. Finish with " << status << ", Issues: " << issues.ToOneLineString() << ", SessionId: " << SessionId << ", TxId: " << TxId);
    }
    Finished = true;
    OnFinish(status, std::move(issues));
    if (rollbackOnError && !CommitRequested && TxId && status != Ydb::StatusIds::SUCCESS) {
        RollbackTransaction();
    } else if (DeleteSession) {
        RunDeleteSession();
    } else {
        PassAway();
    }
}

void TQueryBase::Finish() {
    Finish(Ydb::StatusIds::SUCCESS, NYql::TIssues());
}

void TQueryBase::RunQuery() {
    try {
        OnRunQuery();
    } catch (const std::exception& ex) {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, ex.what());
    }
}

void TQueryBase::RunCreateSession() {
    using TEvCreateSessionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
        Ydb::Table::CreateSessionResponse>;
    Ydb::Table::CreateSessionRequest req;
    Subscribe<Ydb::Table::CreateSessionResponse, TEvQueryBasePrivate::TEvCreateSessionResult>(NRpcService::DoLocalRpc<TEvCreateSessionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::RunDeleteSession() {
    using TEvDeleteSessionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
        Ydb::Table::DeleteSessionResponse>;
    Ydb::Table::DeleteSessionRequest req;
    req.set_session_id(SessionId);
    Subscribe<Ydb::Table::DeleteSessionResponse, TEvQueryBasePrivate::TEvDeleteSessionResult>(NRpcService::DoLocalRpc<TEvDeleteSessionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::RunDataQuery(const TString& sql, NYdb::TParamsBuilder* params, TTxControl txControl) {
    Y_ABORT_UNLESS(!RunningQuery);
    RequestStartTime = TInstant::Now();
    RunningQuery = true;
    LOG_D("RunDataQuery: " << sql);
    using TEvExecuteDataQueryRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest,
        Ydb::Table::ExecuteDataQueryResponse>;
    Ydb::Table::ExecuteDataQueryRequest req;
    req.set_session_id(SessionId);
    auto* txControlProto = req.mutable_tx_control();
    if (txControl.Begin) {
        txControlProto->mutable_begin_tx()->mutable_serializable_read_write();
    } else if (txControl.Continue) {
        Y_ABORT_UNLESS(TxId);
        txControlProto->set_tx_id(TxId);
    }
    if (txControl.Commit) {
        CommitRequested = true;
        txControlProto->set_commit_tx(true);
    }
    req.mutable_query()->set_yql_text(sql);
    req.mutable_query_cache_policy()->set_keep_in_cache(true);
    if (params) {
        auto p = params->Build();
        *req.mutable_parameters() = NYdb::TProtoAccessor::GetProtoMap(p);
    }
    Subscribe<Ydb::Table::ExecuteDataQueryResponse, TEvQueryBasePrivate::TEvDataQueryResult>(NRpcService::DoLocalRpc<TEvExecuteDataQueryRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::RollbackTransaction() {
    Y_ABORT_UNLESS(SessionId);
    Y_ABORT_UNLESS(TxId);
    LOG_D("Rollback transaction: " << TxId);
    using TEvRollbackTransactionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::RollbackTransactionRequest,
        Ydb::Table::RollbackTransactionResponse>;
    Ydb::Table::RollbackTransactionRequest req;
    req.set_session_id(SessionId);
    req.set_tx_id(TxId);
    Subscribe<Ydb::Table::RollbackTransactionResponse, TEvQueryBasePrivate::TEvRollbackTransactionResponse>(NRpcService::DoLocalRpc<TEvRollbackTransactionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::CommitTransaction() {
    RunningCommit = true;
    Y_ABORT_UNLESS(SessionId);
    Y_ABORT_UNLESS(TxId);
    LOG_D("Commit transaction: " << TxId);
    using TEvCommitTransactionRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::CommitTransactionRequest,
        Ydb::Table::CommitTransactionResponse>;
    Ydb::Table::CommitTransactionRequest req;
    req.set_session_id(SessionId);
    req.set_tx_id(TxId);
    Subscribe<Ydb::Table::CommitTransactionResponse, TEvQueryBasePrivate::TEvCommitTransactionResponse>(NRpcService::DoLocalRpc<TEvCommitTransactionRequest>(std::move(req), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::CallOnQueryResult() {
    OnQueryResult();
}

void TQueryBase::ClearTimeInfo() {
    AmountRequestsTime = TDuration::Zero();
    NumberRequests = 0;
}

TDuration TQueryBase::GetAverageTime() {
    Y_ABORT_UNLESS(NumberRequests);
    return AmountRequestsTime / NumberRequests;
}

} // namespace NKikimr
