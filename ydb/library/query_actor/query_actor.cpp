#include "query_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/service_table.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>


#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, LogComponent, LogPrefix() << stream)


namespace NKikimr {

using namespace NGRpcService;
using namespace NRpcService;
using namespace NYql;
using namespace Ydb;

namespace {

template <class TProto>
TIssues IssuesFromProtoMessage(const TProto& message) {
    TIssues issues;
    IssuesFromMessage(message.issues(), issues);
    return issues;
}

} // anonymous namespace

//// TTxControl

TQueryBase::TTxControl TQueryBase::TTxControl::CommitTx() {
    return TTxControl().Commit(true);
}

TQueryBase::TTxControl TQueryBase::TTxControl::BeginTx(bool snapshotRead) {
    return TTxControl().Begin(true).SnapshotRead(snapshotRead);
}

TQueryBase::TTxControl TQueryBase::TTxControl::BeginAndCommitTx(bool snapshotRead) {
    return BeginTx(snapshotRead).Commit(true);
}

TQueryBase::TTxControl TQueryBase::TTxControl::ContinueTx() {
    return TTxControl().Continue(true);
}

TQueryBase::TTxControl TQueryBase::TTxControl::ContinueAndCommitTx() {
    return ContinueTx().Commit(true);
}

//// Private events

TQueryBase::TEvQueryBasePrivate::TEvDataQueryResult::TEvDataQueryResult(Table::ExecuteDataQueryResponse&& response)
    : Status(response.operation().status())
    , Issues(IssuesFromProtoMessage(response.operation()))
{
    response.operation().result().UnpackTo(&Result);
}

TQueryBase::TEvQueryBasePrivate::TEvStreamQueryResultPart::TEvStreamQueryResultPart(Table::ExecuteScanQueryPartialResponse&& response)
    : Status(response.status())
    , Issues(IssuesFromProtoMessage(response))
    , ResultSet(std::move(*response.mutable_result()->mutable_result_set()))
{}

TQueryBase::TEvQueryBasePrivate::TEvCreateSessionResult::TEvCreateSessionResult(Table::CreateSessionResponse&& response)
    : Status(response.operation().status())
    , Issues(IssuesFromProtoMessage(response.operation()))
{
    Table::CreateSessionResult result;
    response.operation().result().UnpackTo(&result);
    SessionId = std::move(*result.mutable_session_id());
}

TQueryBase::TEvQueryBasePrivate::TEvDeleteSessionResponse::TEvDeleteSessionResponse(Table::DeleteSessionResponse&& response)
    : Status(response.operation().status())
    , Issues(IssuesFromProtoMessage(response.operation()))
{}

TQueryBase::TEvQueryBasePrivate::TEvRollbackTransactionResponse::TEvRollbackTransactionResponse(Table::RollbackTransactionResponse&& response)
    : Status(response.operation().status())
    , Issues(IssuesFromProtoMessage(response.operation()))
{}

TQueryBase::TEvQueryBasePrivate::TEvCommitTransactionResponse::TEvCommitTransactionResponse(Table::CommitTransactionResponse&& response)
    : Status(response.operation().status())
    , Issues(IssuesFromProtoMessage(response.operation()))
{}

//// TQueryBase

TQueryBase::TQueryBase(ui64 logComponent, TString sessionId, TString database, bool isSystemUser)
    : LogComponent(logComponent)
    , Database(std::move(database))
    , SessionId(std::move(sessionId))
    , IsSystemUser(isSystemUser)
{}

void TQueryBase::Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) {
    TBase::Registered(sys, owner);
    Owner = owner;
}

STRICT_STFUNC(TQueryBase::StateFunc,
    hFunc(TEvQueryBasePrivate::TEvDataQueryResult, Handle);
    hFunc(TEvQueryBasePrivate::TEvStreamQueryResultPart, Handle);
    hFunc(TEvQueryBasePrivate::TEvCreateSessionResult, Handle);
    hFunc(TEvQueryBasePrivate::TEvDeleteSessionResponse, Handle);
    hFunc(TEvQueryBasePrivate::TEvRollbackTransactionResponse, Handle);
    hFunc(TEvQueryBasePrivate::TEvCommitTransactionResponse, Handle);
);

void TQueryBase::Bootstrap() {
    Become(&TQueryBase::StateFunc);

    if (!Database) {
        Database = GetDefaultDatabase();
    }

    if (SessionId) {
        LOG_D("Bootstrap. Database: " << Database << ", SessionId: " << SessionId);
        RunQuery();
    } else {
        LOG_D("Bootstrap. Database: " << Database);
        RunCreateSession();
    }
}

TString TQueryBase::GetDefaultDatabase() {
    return CanonizePath(AppData()->TenantName);
}

//// TQueryBase session operations

void TQueryBase::RunCreateSession() const {
    using TCreateSessionRequest = TGrpcRequestOperationCall<Table::CreateSessionRequest, Table::CreateSessionResponse>;

    Table::CreateSessionRequest request;
    Subscribe<Table::CreateSessionResponse, TEvQueryBasePrivate::TEvCreateSessionResult>(DoLocalRpc<TCreateSessionRequest>(std::move(request), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvCreateSessionResult::TPtr& ev) {
    if (ev->Get()->Status == StatusIds::SUCCESS) {
        SessionId = ev->Get()->SessionId;
        DeleteSession = true;
        RunQuery();
        Y_ABORT_UNLESS(Finished || RunningQuery);
    } else {
        LOG_W("Failed to create session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }
}

void TQueryBase::RunDeleteSession() const {
    using TDeleteSessionRequest = TGrpcRequestOperationCall<Table::DeleteSessionRequest, Table::DeleteSessionResponse>;

    Y_ABORT_UNLESS(SessionId);

    Table::DeleteSessionRequest request;
    request.set_session_id(SessionId);
    Subscribe<Table::DeleteSessionResponse, TEvQueryBasePrivate::TEvDeleteSessionResponse>(DoLocalRpc<TDeleteSessionRequest>(std::move(request), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvDeleteSessionResponse::TPtr& ev) {
    if (ev->Get()->Status != StatusIds::SUCCESS) {
        LOG_W("Failed to delete session: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());
    }
    PassAway();
}

//// TQueryBase data query operations

void TQueryBase::RunQuery() {
    try {
        OnRunQuery();
    } catch (const std::exception& ex) {
        Finish(StatusIds::INTERNAL_ERROR, ex.what());
    }
}

void TQueryBase::RunDataQuery(const TString& sql, NYdb::TParamsBuilder* params, TTxControl txControl) {
    using TExecuteDataQueryRequest = TGrpcRequestOperationCall<Table::ExecuteDataQueryRequest, Table::ExecuteDataQueryResponse>;

    Y_ABORT_UNLESS(!RunningQuery);
    RequestStartTime = TInstant::Now();
    RunningQuery = true;
    LOG_D("RunDataQuery: " << sql);

    Table::ExecuteDataQueryRequest request;
    request.set_session_id(SessionId);
    request.mutable_query()->set_yql_text(sql);
    request.mutable_query_cache_policy()->set_keep_in_cache(true);

    if (params) {
        *request.mutable_parameters() = NYdb::TProtoAccessor::GetProtoMap(params->Build());
    }

    auto txControlProto = request.mutable_tx_control();
    if (txControl.Begin_) {
        auto& beginTx = *txControlProto->mutable_begin_tx();
        if (txControl.SnapshotRead_) {
            beginTx.mutable_snapshot_read_only();
        } else {
            beginTx.mutable_serializable_read_write();
        }
    } else if (txControl.Continue_) {
        Y_ABORT_UNLESS(TxId);
        txControlProto->set_tx_id(TxId);
    }
    if (txControl.Commit_) {
        CommitRequested = true;
        txControlProto->set_commit_tx(true);
    }

    TMaybe<TString> token = Nothing();
    if (IsSystemUser) {
        token = NACLib::TSystemUsers::Metadata().SerializeAsString();
    }

    Subscribe<Table::ExecuteDataQueryResponse, TEvQueryBasePrivate::TEvDataQueryResult>(
        DoLocalRpc<TExecuteDataQueryRequest>(std::move(request), Database, token, TActivationContext::ActorSystem(), true));
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvDataQueryResult::TPtr& ev) {
    Y_ABORT_UNLESS(RunningQuery);
    NumberRequests++;
    AmountRequestsTime += TInstant::Now() - RequestStartTime;
    RunningQuery = false;
    TxId = ev->Get()->Result.tx_meta().id();
    LOG_D("TEvDataQueryResult " << ev->Get()->Status << ", Issues: " << ev->Get()->Issues.ToOneLineString() << ", SessionId: " << SessionId << ", TxId: " << TxId);

    if (ev->Get()->Status == StatusIds::SUCCESS) {
        ResultSets.clear();
        ResultSets.reserve(ev->Get()->Result.result_sets_size());
        for (auto& resultSet : *ev->Get()->Result.mutable_result_sets()) {
            ResultSets.emplace_back(std::move(resultSet));
        }
        try {
            (this->*QueryResultHandler)();
        } catch (const std::exception& ex) {
            Finish(StatusIds::INTERNAL_ERROR, ex.what());
        }
        Y_ABORT_UNLESS(Finished || RunningQuery || RunningCommit);
    } else {
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
    }
}

void TQueryBase::CallOnQueryResult() {
    OnQueryResult();
}

//// TQueryBase stream query operations

void TQueryBase::RunStreamQuery(const TString& sql, NYdb::TParamsBuilder* params, ui64 channelBufferSize) {
    using TExecuteStreamQueryRequest = TGrpcRequestNoOperationCall<Table::ExecuteScanQueryRequest, Table::ExecuteScanQueryPartialResponse>;

    Y_ABORT_UNLESS(!RunningQuery);
    LOG_D("RunStreamQuery: " << sql);

    Table::ExecuteScanQueryRequest request;
    request.set_mode(Table::ExecuteScanQueryRequest::MODE_EXEC);
    request.mutable_query()->set_yql_text(sql);

    if (params) {
        *request.mutable_parameters() = NYdb::TProtoAccessor::GetProtoMap(params->Build());
    }

    auto facilityProvider = CreateFacilityProviderSameMailbox(ActorContext(), channelBufferSize);
    StreamQueryProcessor = DoLocalRpcStreamSameMailbox<TExecuteStreamQueryRequest>(std::move(request), Database, Nothing(), facilityProvider, &DoExecuteScanQueryRequest, true);
    ReadNextStreamPart();
}

void TQueryBase::ReadNextStreamPart() {
    Y_ABORT_UNLESS(!RunningQuery);
    Y_ABORT_UNLESS(StreamQueryProcessor && StreamQueryProcessor->HasData());
    RequestStartTime = TInstant::Now();
    RunningQuery = true;
    LOG_D("Start read next stream part");

    StreamQueryProcessor->Read(GetOperationCallback<Table::ExecuteScanQueryPartialResponse, TEvQueryBasePrivate::TEvStreamQueryResultPart>());
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvStreamQueryResultPart::TPtr& ev) {
    Y_ABORT_UNLESS(RunningQuery);
    Y_ABORT_UNLESS(StreamQueryProcessor);
    NumberRequests++;
    AmountRequestsTime += TInstant::Now() - RequestStartTime;
    RunningQuery = false;
    LOG_D("TEvStreamQueryResultPart " << ev->Get()->Status << ", Issues: " << ev->Get()->Issues.ToOneLineString());

    if (ev->Get()->Status != StatusIds::SUCCESS) {
        Finish(ev->Get()->Status, std::move(ev->Get()->Issues));
        return;
    }

    if (ev->Get()->ResultSet.rows_size()) {
        try {
            (this->*StreamResultHandler)(std::move(ev->Get()->ResultSet));
        } catch (const std::exception& ex) {
            Finish(StatusIds::INTERNAL_ERROR, ex.what());
            return;
        }
    }

    if (StreamQueryProcessor) {
        if (StreamQueryProcessor->HasData()) {
            ReadNextStreamPart();
        } else {
            FinishStreamRequest();
        }
    }
}

void TQueryBase::CallOnStreamResult(NYdb::TResultSet&& resultSet) {
    OnStreamResult(std::move(resultSet));
}

void TQueryBase::CancelStreamQuery() {
    LOG_D("Cancel stream request");
    Y_ABORT_UNLESS(StreamQueryProcessor);

    if (!StreamQueryProcessor->IsFinished()) {
        StreamQueryProcessor->Cancel();
    }
    FinishStreamRequest();
}

void TQueryBase::FinishStreamRequest() {
    Y_ABORT_UNLESS(StreamQueryProcessor && StreamQueryProcessor->IsFinished());

    StreamQueryProcessor = nullptr;
    try {
        (this->*QueryResultHandler)();
    } catch (const std::exception& ex) {
        Finish(StatusIds::INTERNAL_ERROR, ex.what());
    }
    Y_ABORT_UNLESS(Finished || RunningQuery || RunningCommit);
}

//// TQueryBase finish operations

void TQueryBase::Finish() {
    Finish(StatusIds::SUCCESS, TIssues());
}

void TQueryBase::Finish(StatusIds::StatusCode status, const TString& message, bool rollbackOnError) {
    TIssues issues;
    issues.AddIssue(message);
    Finish(status, std::move(issues), rollbackOnError);
}

void TQueryBase::Finish(StatusIds::StatusCode status, TIssues&& issues, bool rollbackOnError) {
    if (status == StatusIds::SUCCESS) {
        if (FinishOk) {
            FinishOk->Inc();
        }
        LOG_D("Finish with SUCCESS, SessionId: " << SessionId << ", TxId: " << TxId);
    } else {
        if (FinishError) {
            FinishError->Inc();
        }
        LOG_W("Finish with " << status << ", Issues: " << issues.ToOneLineString() << ", SessionId: " << SessionId << ", TxId: " << TxId);
    }

    Finished = true;
    OnFinish(status, std::move(issues));

    if (StreamQueryProcessor) {
        if (!StreamQueryProcessor->IsFinished()) {
            StreamQueryProcessor->Cancel();
        }
        StreamQueryProcessor = nullptr;
    }

    if (rollbackOnError && !CommitRequested && TxId && status != StatusIds::SUCCESS) {
        RollbackTransaction();
    } else if (DeleteSession) {
        RunDeleteSession();
    } else {
        PassAway();
    }
}

//// TQueryBase transactions operations

void TQueryBase::CommitTransaction() {
    using TCommitTransactionRequest = TGrpcRequestOperationCall<Table::CommitTransactionRequest, Table::CommitTransactionResponse>;

    Y_ABORT_UNLESS(SessionId);
    Y_ABORT_UNLESS(TxId);
    RunningCommit = true;
    LOG_D("Commit transaction: " << TxId);

    Table::CommitTransactionRequest request;
    request.set_session_id(SessionId);
    request.set_tx_id(TxId);
    Subscribe<Table::CommitTransactionResponse, TEvQueryBasePrivate::TEvCommitTransactionResponse>(DoLocalRpc<TCommitTransactionRequest>(std::move(request), Database, Nothing(), TActivationContext::ActorSystem(), true));
}

void TQueryBase::Handle(TEvQueryBasePrivate::TEvCommitTransactionResponse::TPtr& ev) {
    LOG_D("CommitTransactionResult: " << ev->Get()->Status << ". Issues: " << ev->Get()->Issues.ToOneLineString());

    OnFinish(ev->Get()->Status, std::move(ev->Get()->Issues));

    // Continue finish
    if (DeleteSession) {
        RunDeleteSession();
    } else {
        PassAway();
    }
}

void TQueryBase::RollbackTransaction() const {
    using TRollbackTransactionRequest = TGrpcRequestOperationCall<Table::RollbackTransactionRequest, Table::RollbackTransactionResponse>;

    Y_ABORT_UNLESS(SessionId);
    Y_ABORT_UNLESS(TxId);
    LOG_D("Rollback transaction: " << TxId);

    Table::RollbackTransactionRequest request;
    request.set_session_id(SessionId);
    request.set_tx_id(TxId);
    Subscribe<Table::RollbackTransactionResponse, TEvQueryBasePrivate::TEvRollbackTransactionResponse>(DoLocalRpc<TRollbackTransactionRequest>(std::move(request), Database, Nothing(), TActivationContext::ActorSystem(), true));
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

//// TQueryBase log methods

void TQueryBase::SetOperationInfo(const TString& operationName, const TString& traceId, NMonitoring::TDynamicCounterPtr counters) {
    OperationName = operationName;
    TraceId = traceId;

    if (counters) {
        auto subgroup = counters->GetSubgroup("operation", OperationName);
        FinishOk = subgroup->GetCounter("FinishOk", true);
        FinishError = subgroup->GetCounter("FinishError", true);
    }
}

TString TQueryBase::LogPrefix() const {
    TStringBuilder result = TStringBuilder() << "[TQueryBase] ";
    if (Y_LIKELY(OperationName)) {
        result << "[" << OperationName << "] ";
    }
    if (Y_LIKELY(TraceId)) {
        result << "TraceId: " << TraceId << ", ";
    }
    if (StateDescription) {
        result << "State: " << StateDescription << ", ";
    }
    return result;
}

void TQueryBase::ClearTimeInfo() {
    AmountRequestsTime = TDuration::Zero();
    NumberRequests = 0;
}

TDuration TQueryBase::GetAverageTime() const {
    Y_ABORT_UNLESS(NumberRequests);
    return AmountRequestsTime / NumberRequests;
}

} // namespace NKikimr
