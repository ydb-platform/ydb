#include "queries.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/query_actor/query_actor.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, "[StreamingQueries] " << LogPrefix() << stream)

using namespace fmt::literals;
using TExternalContext = NMetadata::NModifications::IOperationsManager::TExternalModificationContext;
using TStatus = NKikimr::TYQLConclusionSpecialStatus<Ydb::StatusIds::StatusCode, Ydb::StatusIds::SUCCESS, Ydb::StatusIds::INTERNAL_ERROR>;

template <typename TValue>
using TValueStatus = TConclusionImpl<TStatus, TValue>;

//// Events

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvStart = EventSpaceBegin(TEvents::ES_PRIVATE),

        // Scheme operations
        EvDescribeStreamingQueryResult = EvStart,
        EvExecuteSchemeTransactionResult,

        // Common query operations
        EvUpdateStreamingQueryResult,

        // Query locking
        EvLockStreamingQueryResult,
        EvUnlockStreamingQueryResult,
        EvCheckAliveRequest,
        EvCheckAliveResponse,

        // Drop streaming query
        EvCleanupStreamingQueryResult,
        EvRemoveStreamingQueryResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    template <typename TEv, ui32 EventType>
    struct TEvResultBase : public TEventLocal<TEv, EventType> {
        explicit TEvResultBase(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
            : Status(status)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
    };

    struct TEvDescribeStreamingQueryResult : public TEvResultBase<TEvDescribeStreamingQueryResult, EvDescribeStreamingQueryResult> {
        TEvDescribeStreamingQueryResult(Ydb::StatusIds::StatusCode status, std::optional<NKikimrSchemeOp::TStreamingQueryProperties> properties, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , Properties(std::move(properties))
        {}

        const std::optional<NKikimrSchemeOp::TStreamingQueryProperties> Properties;
    };

    struct TEvExecuteSchemeTransactionResult : public TEvResultBase<TEvExecuteSchemeTransactionResult, EvExecuteSchemeTransactionResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvUpdateStreamingQueryResult : public TEvResultBase<TEvUpdateStreamingQueryResult, EvUpdateStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvLockStreamingQueryResult : public TEvResultBase<TEvLockStreamingQueryResult, EvLockStreamingQueryResult> {
        struct TInfo {
            NKikimrKqp::TStreamingQueryState State;
            TActorId PreviousOwner;
            TInstant PreviousOperationStartedAt;
            TString PreviousOperationName;
            bool QueryExists = false;
            bool LockCreated = false;
            bool CheckLockOwner = false;
        };

        TEvLockStreamingQueryResult(Ydb::StatusIds::StatusCode status, const TInfo& info, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , Info(info)
        {}

        const TInfo Info;
    };

    struct TEvUnlockStreamingQueryResult : public TEvResultBase<TEvUnlockStreamingQueryResult, EvUnlockStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvCheckAliveRequest : public TEventPB<TEvCheckAliveRequest, google::protobuf::Empty, EvCheckAliveRequest> {
    };

    struct TEvCheckAliveResponse : public TEventPB<TEvCheckAliveResponse, google::protobuf::Empty, EvCheckAliveResponse> {
    };

    struct TEvCleanupStreamingQueryResult : public TEvResultBase<TEvCleanupStreamingQueryResult, EvCleanupStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvRemoveStreamingQueryResult : public TEvResultBase<TEvRemoveStreamingQueryResult, EvRemoveStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };
};

//// Common

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, bool addEmptyRoot = true) {
    if (!issues && !addEmptyRoot) {
        return {};
    }

    NYql::TIssue rootIssue(message);
    for (const auto& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    return {rootIssue};
}

NOperationId::TOperationId OperationIdFromExecutionId(const TString& executionId) {
    return NOperationId::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));
}

template <typename TDerived>
class TActionActorBase : public TActorBootstrapped<TDerived> {
    using TBase = TActorBootstrapped<TDerived>;

public:
    TActionActorBase(const TString& operationName, const TString& queryPath)
        : OperationName(operationName)
        , QueryPath(queryPath)
    {}

    TActionActorBase(const TString& operationName, const TString& workingDir, const TString& queryName)
        : TActionActorBase(operationName, JoinPath({workingDir, queryName}))
    {}

protected:
    template <typename TEvent>
    void SendToKqpProxy(std::unique_ptr<TEvent> event, ui64 cookie = 0) const {
        const auto& kqpProxy = MakeKqpProxyID(TBase::SelfId().NodeId());
        TBase::Send(kqpProxy, std::move(event), 0, cookie);
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << OperationName << "] OwnerId: " << Owner << " ActorId: " << TBase::SelfId() << " QueryPath: " << QueryPath << ". ";
    }

private:
    void Registered(TActorSystem* sys, const TActorId& owner) override {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

protected:
    const TString OperationName;
    const TString QueryPath;
    TActorId Owner;
};

//// Scheme actions

template <typename TDerived>
class TSchemeActorBase : public TActionActorBase<TDerived> {
    using TBase = TActionActorBase<TDerived>;
    using TRetryPolicy = IRetryPolicy<bool>;

public:
    using TBase::LogPrefix;

    TSchemeActorBase(const TString& operationName, const TString& queryPath, const TExternalContext& context)
        : TBase(operationName, queryPath)
        , Context(context)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap");
        StartRequest();

        Become(&TDerived::StateFunc);
    }

    STRICT_STFUNC(StateFuncBase,
        sFunc(TEvents::TEvWakeup, StartRequest);
        hFunc(TEvents::TEvUndelivered, Handle);
    )

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == TEvents::TEvUndelivered::ReasonActorUnknown && ScheduleRetry("Scheme service not found")) {
            return;
        }

        LOG_E("Scheme service is unavailable");
        OnFatalError(Ydb::StatusIds::UNAVAILABLE, "Scheme service is unavailable");
    }

protected:
    virtual void StartRequest() = 0;

    virtual void Finish(Ydb::StatusIds::StatusCode status) = 0;

protected:
    void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        Issues.AddIssues(std::move(issues));
        Finish(status);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode status, const TString& message) {
        OnFatalError(status, {NYql::TIssue(message)});
    }

    bool ScheduleRetry(NYql::TIssues issues, bool longDelay = false) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay(longDelay)) {
            LOG_W("Schedule retry for error: " << issues.ToOneLineString() << " in " << *delay);
            Issues.AddIssues(std::move(issues));
            this->Schedule(*delay, new TEvents::TEvWakeup());
            return true;
        }

        return false;
    }

    bool ScheduleRetry(const TString& message, bool longDelay = false) {
        return ScheduleRetry({NYql::TIssue(message)}, longDelay);
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
        const auto& userToken = Context.GetUserToken();
        if (!userToken) {
            return nullptr;
        }

        auto result = MakeIntrusive<NACLib::TUserToken>(userToken);
        if (result->GetSerializedToken().empty()) {
            result->SaveSerializationInfo();
        }

        return result;
    }

private:
    static TRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return TRetryPolicy::GetExponentialBackoffPolicy(
                  [](bool longDelay){return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;}
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(500)
                , TDuration::Seconds(1)
                , std::numeric_limits<size_t>::max()
                , TDuration::Seconds(10)
            )->CreateRetryState();
    }

protected:
    NYql::TIssues Issues;
    const TExternalContext Context;

private:
    TRetryPolicy::IRetryState::TPtr RetryState;
};

class TDescribeStreamingQuerySchemeActor : public TSchemeActorBase<TDescribeStreamingQuerySchemeActor> {
    using TBase = TSchemeActorBase<TDescribeStreamingQuerySchemeActor>;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

public:
    using TBase::LogPrefix;

    TDescribeStreamingQuerySchemeActor(const TString& queryPath, const TExternalContext& context, ui32 access)
        : TBase(__func__, queryPath, context)
        , Access(access)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != 1) {
            OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        const auto& result = results[0];
        LOG_D("Got scheme cache response: " << result.Status);

        switch (result.Status) {
            case EStatus::Unknown:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
            case EStatus::RedirectLookupError:
                OnFatalError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid streaming query path " << QueryPath);
                return;
            case EStatus::AccessDenied:
                OnFatalError(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for streaming query " << QueryPath);
                return;
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                Finish(Ydb::StatusIds::SUCCESS);
                return;
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    OnFatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                }
                return;
            case EStatus::Ok:
                if (result.Kind != NSchemeCache::TSchemeCacheNavigate::KindStreamingQuery) {
                    OnFatalError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Path " << QueryPath << " is not a streaming query");
                } else if (!result.StreamingQueryInfo) {
                    OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response for ok status");
                } else {
                    Properties = result.StreamingQueryInfo->Description.GetProperties();
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                return;
        }
    }

protected:
    void StartRequest() override {
        const auto& database = Context.GetDatabase();
        LOG_D("Describe streaming query in database: " << database << ", with access: " << Access);

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = CanonizePath(database);
        request->UserToken = GetUserToken();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = SplitPath(QueryPath);
        entry.Access = Access;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), IEventHandle::FlagTrackDelivery);
    }

    void Finish(Ydb::StatusIds::StatusCode status) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Streaming query info successfully fetched, query found: " << Properties.has_value());
        } else {
            LOG_W("Failed to fetch streaming query info " << status << ", issues: " << Issues.ToOneLineString());
        }

        Send(Owner, new TEvPrivate::TEvDescribeStreamingQueryResult(status, std::move(Properties), std::move(Issues)));
        PassAway();
    }

private:
    std::optional<NKikimrSchemeOp::TStreamingQueryProperties> Properties;
    const ui32 Access;
};

class TExecuteTransactionSchemeActor : public TSchemeActorBase<TExecuteTransactionSchemeActor> {
    using TBase = TSchemeActorBase<TExecuteTransactionSchemeActor>;

public:
    using TBase::LogPrefix;

    TExecuteTransactionSchemeActor(const TString& queryPath, const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context)
        : TBase(__func__, queryPath, context)
        , SchemeTx(schemeTx)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            IgnoreFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const auto ssStatus = response.GetSchemeShardStatus();
        const auto status = ev->Get()->Status();
        const auto& txId = response.GetTxId();

        LOG_D("Got propose transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " response"
            << ", Status: " << status
            << ", SchemeShardStatus: " << NKikimrScheme::EStatus_Name(ssStatus)
            << ", TxId: " << txId);

        switch (status) {
            case NTxProxy::TResultStatus::ExecInProgress: {
                if (txId == 0) {
                    OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, ExtractIssues(response, ssStatus, "unable to subscribe on creation transaction"));
                    return;
                }

                ClosePipeClient();
                SchemePipeActorId = Register(NTabletPipe::CreateClient(SelfId(), response.GetSchemeShardTabletId()));
                NTabletPipe::SendData(SelfId(), SchemePipeActorId, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(txId));

                LOG_D("Subscribe on scheme tx: " << txId);
                break;
            }
            case NTxProxy::TResultStatus::ExecAlready:
            case NTxProxy::TResultStatus::ExecComplete: {
                if (ssStatus == NKikimrScheme::EStatus::StatusSuccess) {
                    Finish(Ydb::StatusIds::SUCCESS);
                } else if (ssStatus == NKikimrScheme::EStatus::StatusAlreadyExists) {
                    OnFatalError(Ydb::StatusIds::ALREADY_EXISTS, ExtractIssues(response, ssStatus, TStringBuilder() << "execution completed, streaming query " << QueryPath << " already exists"));
                } else {
                    OnFatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "unexpected final execution status"));
                }
                break;
            }
            case NTxProxy::TResultStatus::ProxyNotReady:
            case NTxProxy::TResultStatus::ProxyShardTryLater:
            case NTxProxy::TResultStatus::ProxyShardNotAvailable: {
                ScheduleRetry(response, TStringBuilder() << "proxy shard not available " << status);
                break;
            }
            case NTxProxy::TResultStatus::AccessDenied: {
                OnFatalError(Ydb::StatusIds::UNAUTHORIZED, ExtractIssues(response, ssStatus, TStringBuilder() << "you don't have access permissions for operation on streaming query " << QueryPath));
                break;
            }
            case NTxProxy::TResultStatus::ResolveError: {
                if (ssStatus == NKikimrScheme::EStatus::StatusPathDoesNotExist) {
                    OnFatalError(Ydb::StatusIds::NOT_FOUND, ExtractIssues(response, ssStatus, TStringBuilder() << "streaming query " << QueryPath << " not found or you don't have access permissions"));
                } else {
                    OnFatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "resolve error"));
                }
                break;
            }
            case NTxProxy::TResultStatus::NotImplemented: {
                OnFatalError(Ydb::StatusIds::UNSUPPORTED, ExtractIssues(response, ssStatus, "operation not implemented"));
                break;
            }
            case NTxProxy::TResultStatus::ProxyShardOverloaded:  {
                OnFatalError(Ydb::StatusIds::OVERLOADED, ExtractIssues(response, ssStatus, "tx proxy is overloaded"));
                break;
            }
            case NTxProxy::TResultStatus::ExecAborted: {
                OnFatalError(Ydb::StatusIds::ABORTED, ExtractIssues(response, ssStatus, "execution aborted"));
                break;
            }
            case NTxProxy::TResultStatus::ExecTimeout: {
                OnFatalError(Ydb::StatusIds::TIMEOUT, ExtractIssues(response, ssStatus, "execution timeout"));
                break;
            }
            case NTxProxy::TResultStatus::ExecCancelled: {
                OnFatalError(Ydb::StatusIds::CANCELLED, ExtractIssues(response, ssStatus, "execution canceled"));
                break;
            }
            case NTxProxy::TResultStatus::ExecError: {
                switch (static_cast<NKikimrScheme::EStatus>(ssStatus)) {
                    case NKikimrScheme::StatusPathDoesNotExist: {
                        OnFatalError(Ydb::StatusIds::NOT_FOUND, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, streaming query " << QueryPath << " not found or you don't have access permissions"));
                        break;
                    }
                    case NKikimrScheme::StatusAlreadyExists: {
                        OnFatalError(Ydb::StatusIds::ALREADY_EXISTS, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, streaming query " << QueryPath << " already exists"));
                        break;
                    }
                    case NKikimrScheme::StatusAccessDenied: {
                        OnFatalError(Ydb::StatusIds::UNAUTHORIZED, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, you don't have access permissions for operation on streaming query " << QueryPath));
                        break;
                    }
                    case NKikimrScheme::StatusNotAvailable: {
                        OnFatalError(Ydb::StatusIds::UNAVAILABLE, ExtractIssues(response, ssStatus, "execution error, scheme shard is not available"));
                        break;
                    }
                    case NKikimrScheme::StatusPreconditionFailed: {
                        OnFatalError(Ydb::StatusIds::PRECONDITION_FAILED, ExtractIssues(response, ssStatus, "execution error, precondition failed"));
                        break;
                    }
                    case NKikimrScheme::StatusQuotaExceeded:
                    case NKikimrScheme::StatusResourceExhausted: {
                        OnFatalError(Ydb::StatusIds::OVERLOADED, ExtractIssues(response, ssStatus, "execution error, resource exhausted"));
                        break;
                    }
                    default: {
                        OnFatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "transaction execution failed"));
                        break;
                    }
                }
            }
            default: {
                OnFatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, TStringBuilder() << "unexpected transaction status " << status));
                break;
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto status = ev->Get()->Status;
        if (status == NKikimrProto::OK) {
            LOG_T("Tablet pipe successfully connected");
            return;
        }

        ClosePipeClient();
        OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Tablet to pipe not connected: " << NKikimrProto::EReplyStatus_Name(status));
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;
        if (!ClosedSchemePipeActors.contains(clientId)) {
            ClosePipeClient();
            OnFatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Tablet to pipe unexpectedly destroyed");
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_D("Scheme transaction " << ev->Get()->Record.GetTxId() << " successfully executed");
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void PassAway() override {
        ClosePipeClient();
        TBase::PassAway();
    }

protected:
    void StartRequest() override {
        const auto& database = Context.GetDatabase();
        LOG_D("Start scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " in database: " << database);

        auto event = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(database);
        if (const auto token = GetUserToken()) {
            event->Record.SetUserToken(token->GetSerializedToken());
        }

        Send(MakeTxProxyID(), std::move(event));
    }

    void Finish(Ydb::StatusIds::StatusCode status) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " successfully executed");
        } else {
            LOG_W("Scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " failed " << status << ", issues: " << Issues.ToOneLineString());
        }

        Send(Owner, new TEvPrivate::TEvExecuteSchemeTransactionResult(status, std::move(Issues)));
        PassAway();
    }

private:
    void ClosePipeClient() {
        if (SchemePipeActorId) {
            ClosedSchemePipeActors.emplace(SchemePipeActorId);
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
        }
    }

    void ScheduleRetry(const NKikimrTxUserProxy::TEvProposeTransactionStatus& response, const TString& message, bool longDelay = false) {
        ClosePipeClient();

        const auto ssStatus = response.GetSchemeShardStatus();
        if (!TBase::ScheduleRetry(ExtractIssues(response, ssStatus, message), longDelay)) {
            OnFatalError(Ydb::StatusIds::UNAVAILABLE, ExtractIssues(response, ssStatus, TStringBuilder() << "Retry limit exceeded on error: " << message));
        }
    }

    NYql::TIssues ExtractIssues(const NKikimrTxUserProxy::TEvProposeTransactionStatus& response, ui32 ssStatus, const TString& message) const {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(response.GetIssues(), issues);
        return AddRootIssue(
            TStringBuilder() << "Scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType())
                << " failed " << NKikimrScheme::EStatus_Name(ssStatus)
                << ": " << message
                << " (reason: " << response.GetSchemeShardReason() << ")",
            issues
        );
    }

private:
    const NKikimrSchemeOp::TModifyScheme SchemeTx;
    std::unordered_set<TActorId> ClosedSchemePipeActors;
    TActorId SchemePipeActorId;
};

//// Table actions

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(const TString& operationName, const TString& databaseId, const TString& queryPath, const TString& sessionId = {})
        : NKikimr::TQueryBase(NKikimrServices::KQP_PROXY, sessionId)
        , DatabaseId(databaseId)
        , QueryPath(queryPath)
        , TablePath(TStreamingQueryConfig::GetBehaviour()->GetStorageTablePath())
    {
        SetOperationInfo(operationName, queryPath);
    }

protected:
    void ReadQueryInfo(const TTxControl& txControl) {
        const TString sql = fmt::format(R"(
                DECLARE $database_id AS Text;
                DECLARE $query_path AS Text;

                SELECT
                    *
                FROM `{table}`
                WHERE database_id = $database_id
                  AND query_path = $query_path;
            )",
            "table"_a = TablePath
        );

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database_id")
                .Utf8(DatabaseId)
                .Build()
            .AddParam("$query_path")
                .Utf8(QueryPath)
                .Build();

        ExecuteQuery(__func__, sql, &params, txControl);
    }

    void PersistQueryInfo(const NKikimrKqp::TStreamingQueryState& state, const TTxControl& txControl) {
        const TString sql = fmt::format(R"(
                DECLARE $database_id AS Text;
                DECLARE $query_path AS Text;
                DECLARE $state AS JsonDocument;

                UPSERT INTO `{table}` (
                    database_id, query_path, state
                ) VALUES (
                    $database_id, $query_path, $state
                );
            )",
            "table"_a = TablePath
        );

        NJson::TJsonValue stateJson;
        NProtobufJson::Proto2Json(state, stateJson, NProtobufJson::TProto2JsonConfig());
        NJsonWriter::TBuf stateWriter;
        stateWriter.WriteJsonValue(&stateJson);

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database_id")
                .Utf8(DatabaseId)
                .Build()
            .AddParam("$query_path")
                .Utf8(QueryPath)
                .Build()
            .AddParam("$state")
                .JsonDocument(stateWriter.Str())
                .Build();

        ExecuteQuery(__func__, sql, &params, txControl);
    }

    void ExecuteQuery(const TString& func, const TString& sql, NYdb::TParamsBuilder* params, const TTxControl& txControl) {
        RunDataQuery(
            TStringBuilder() << "-- " << OperationName << "::" << func << "\n" << sql,
            params,
            txControl
        );
    }

protected:
    TValueStatus<NKikimrKqp::TStreamingQueryState> ParseQueryInfo() const {
        if (ResultSets.size() != 1) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            return TStatus::Fail(Ydb::StatusIds::NOT_FOUND, "No such steaming query");
        }

        const std::optional<TString>& stateJsonString = result.ColumnParser(TStreamingQueryConfig::StateColumn).GetOptionalJsonDocument();
        if (!stateJsonString) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query state not found");
        }

        NJson::TJsonValue stateJson;
        if (!NJson::ReadJsonTree(*stateJsonString, &stateJson)) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query state is corrupted");
        }

        NKikimrKqp::TStreamingQueryState state;
        NProtobufJson::Json2Proto(stateJson, state, NProtobufJson::TJson2ProtoConfig());

        return std::move(state);
    }

    void FinishWithStatus(const TStatus& status) {
        Finish(status.GetStatus(), NYql::TIssues(status.GetErrorDescription()));
    }

protected:
    const TString DatabaseId;
    const TString QueryPath;
    const TString TablePath;
};

class TUpdateStreamingQueryStateRequestActor : public TQueryBase {
public:
    using TRetry = TQueryRetryActor<TUpdateStreamingQueryStateRequestActor, TEvPrivate::TEvUpdateStreamingQueryResult, TString, TString, NKikimrKqp::TStreamingQueryState>;

    TUpdateStreamingQueryStateRequestActor(const TString& databaseId, const TString& queryPath, const NKikimrKqp::TStreamingQueryState& state)
        : TQueryBase(__func__, databaseId, queryPath)
        , State(state)
    {}

    void OnRunQuery() override {
        PersistQueryInfo(State, TTxControl::BeginAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvUpdateStreamingQueryResult(status, std::move(issues)));
    }

private:
    const NKikimrKqp::TStreamingQueryState State;
};

class TRemoveStreamingQueryRequestActor : public TQueryBase {
public:
    using TRetry = TQueryRetryActor<TRemoveStreamingQueryRequestActor, TEvPrivate::TEvRemoveStreamingQueryResult, TString, TString>;

    TRemoveStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath)
        : TQueryBase(__func__, databaseId, queryPath)
    {}

    void OnRunQuery() override {
        const TString sql = fmt::format(R"(
                DECLARE $database_id AS Text;
                DECLARE $query_path AS Text;

                DELETE FROM `{table}`
                WHERE database_id = $database_id
                  AND query_path = $query_path;
            )",
            "table"_a = TablePath
        );

        NYdb::TParamsBuilder params;
        params
            .AddParam("$database_id")
                .Utf8(DatabaseId)
                .Build()
            .AddParam("$query_path")
                .Utf8(QueryPath)
                .Build();

        ExecuteQuery(__func__, sql, &params, TTxControl::BeginAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvRemoveStreamingQueryResult(status, std::move(issues)));
    }
};

class TLockStreamingQueryRequestActor : public TQueryBase {
    static constexpr TDuration LOCK_TIMEOUT = TDuration::Seconds(10);

public:
    struct TSettings {
        TString Name;
        TInstant StartedAt;
        TActorId Owner;
        std::optional<TActorId> PreviousOwner;
        bool CreateIfNotExists = false;
        NKikimrKqp::TStreamingQueryState::EStatus DefaultStatus = NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED;
    };

    using TRetry = TQueryRetryActor<TLockStreamingQueryRequestActor, TEvPrivate::TEvLockStreamingQueryResult, TString, TString, TSettings>;

    TLockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TSettings& settings)
        : TQueryBase(__func__, databaseId, queryPath)
        , Settings(settings)
    {}

    void OnRunQuery() override {
        LOG_D("Locking streaming query"
            << ", OperationName: " << Settings.Name
            << ", OperationStartedAt: " << Settings.StartedAt
            << ", Owner: " << Settings.Owner
            << ", CreateIfNotExists: " << Settings.CreateIfNotExists
            << ", PreviousOwner: " << Settings.PreviousOwner.value_or(TActorId()));

        SetQueryResultHandler(&TLockStreamingQueryRequestActor::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        auto result = ParseQueryInfo();

        if (result.GetStatus() == Ydb::StatusIds::NOT_FOUND) {
            LOG_D("Streaming query not found, CreateIfNotExists: " << Settings.CreateIfNotExists);
            if (Settings.CreateIfNotExists) {
                State.SetStatus(Settings.DefaultStatus);
                LockQuery();
            } else {
                Finish();
            }
            return;
        }

        if (result.IsFail()) {
            FinishWithStatus(result);
            return;
        }

        QueryExists = true;
        State = result.DetachResult();
        if (!State.HasOperationActorId()) {
            LOG_D("Streaming query has no locks, creating new lock");
            LockQuery();
            return;
        }

        PreviousOwner = ActorIdFromProto(State.GetOperationActorId());
        PreviousOperationStartedAt = NProtoInterop::CastFromProto(State.GetOperationStartedAt());
        PreviousOperationName = State.GetOperationName();
        LOG_D("Streaming query under lock from " << PreviousOwner << " started at " << PreviousOperationStartedAt << ", with name " << PreviousOperationName);

        if (!Settings.PreviousOwner) {
            if (Settings.StartedAt - PreviousOperationStartedAt <= LOCK_TIMEOUT) {
                FinishUnderOperation();
            } else {
                LOG_I("Streaming query lock " << PreviousOwner << " expired, start check");
                CheckLockOwner = true;
                Finish();
            }
            return;
        }

        if (PreviousOwner != *Settings.PreviousOwner) {
            LOG_I("Streaming query was locked by " << PreviousOwner << " during lock check");
            FinishUnderOperation();
            return;
        }

        LOG_I("Remove expired lock from " << PreviousOwner);
        LockQuery();
    }

    void LockQuery() {
        State.SetOperationName(Settings.Name);
        *State.MutableOperationStartedAt() = NProtoInterop::CastToProto(Settings.StartedAt);
        ActorIdToProto(Settings.Owner, State.MutableOperationActorId());

        SetQueryResultHandler(&TLockStreamingQueryRequestActor::OnQueryResult, "Lock query");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        LockCreated = true;
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, {
            .State = std::move(State),
            .PreviousOwner = PreviousOwner,
            .PreviousOperationStartedAt = PreviousOperationStartedAt,
            .PreviousOperationName = std::move(PreviousOperationName),
            .QueryExists = QueryExists,
            .LockCreated = LockCreated,
            .CheckLockOwner = CheckLockOwner,
        }, std::move(issues)));
    }

private:
    void FinishUnderOperation() {
        Finish(Ydb::StatusIds::ABORTED, TStringBuilder() << "Streaming query already under operation " << PreviousOperationName << " started at " << PreviousOperationStartedAt << ", try repeat request later");
    }

private:
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    TActorId PreviousOwner;
    TInstant PreviousOperationStartedAt;
    TString PreviousOperationName;
    bool QueryExists = false;
    bool LockCreated = false;
    bool CheckLockOwner = false;
};

class TLockStreamingQueryTableActor : public TActionActorBase<TLockStreamingQueryTableActor> {
    using TBase = TActionActorBase<TLockStreamingQueryTableActor>;
    using TRetryPolicy = IRetryPolicy<bool>;

    inline static const TDuration CHECK_ALIVE_REQUEST_TIMEOUT = TDuration::Seconds(60);
    inline static const ui64 MAX_CHECK_ALIVE_RETRIES = 50;

    enum class EWakeup {
        RetryCheckAlive,
        CheckAliveTimeout,
    };

public:
    using TBase::LogPrefix;

    struct TSettings {
        TString Name;
        TInstant StartedAt;
        TActorId Owner;
        bool CreateIfNotExists = false;
        NKikimrKqp::TStreamingQueryState::EStatus DefaultStatus = NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED;
    };

    TLockStreamingQueryTableActor(const TString& databaseId, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , DatabaseId(databaseId)
        , Settings(settings)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap");
        StartLockStreamingQueryRequestActor();

        Become(&TLockStreamingQueryTableActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        WaitLock = false;
        Info = ev->Get()->Info;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Lock streaming query " << ev->Sender << " failed " << status << ", issues: " << ev->Get()->Issues);

            Finish(status, issues);
            return;
        }

        LOG_D("Lock streaming query " << ev->Sender << " finished"
            << ", PreviousOwner: " << Info.PreviousOwner
            << ", PreviousOperationStartedAt: " << Info.PreviousOperationStartedAt
            << ", PreviousOperationName: " << Info.PreviousOperationName
            << ", CheckLockOwner: " << Info.CheckLockOwner);

        if (!Info.CheckLockOwner) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        CheckAliveFlags = IEventHandle::FlagTrackDelivery;
        if (Info.PreviousOwner.NodeId() != SelfId().NodeId()) {
            CheckAliveFlags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = Info.PreviousOwner.NodeId();
        }

        LOG_D("Start check alive for " << Info.PreviousOwner);
        Send(Info.PreviousOwner, new TEvPrivate::TEvCheckAliveRequest(), CheckAliveFlags);
        Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveTimeout)));
    }

    void Handle(TEvPrivate::TEvCheckAliveResponse::TPtr& ev) {
        if (WaitLock) {
            LOG_W("Streaming query " << ev->Sender << " owner was verified after started lock");
        } else {
            LOG_I("Previous query owner " << ev->Sender << " is alive");
            Finish(Ydb::StatusIds::ABORTED, {NYql::TIssue(TStringBuilder() << "Streaming query already under operation " << Info.PreviousOperationName << " started at " << Info.PreviousOperationStartedAt << ", try repeat request later")});
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeup>(ev->Get()->Tag)) {
            case EWakeup::RetryCheckAlive:
                WaitRetryCheckAlive = false;
                LOG_D("Retry check alive request for " << Info.PreviousOwner);
                Send(Info.PreviousOwner, new TEvPrivate::TEvCheckAliveRequest(), CheckAliveFlags);
                Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveTimeout)));
                break;
            case EWakeup::CheckAliveTimeout:
                LOG_W("Deliver streaming query owner " << Info.PreviousOwner << " check alive request timeout, retry check alive");
                RetryCheckAlive(/* longDelay */ false);
                break;
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto reason = ev->Get()->Reason;
        if (reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
            LOG_W("Streaming query operation owner " << ev->Sender << " not found, start lock");
            StartLockStreamingQueryRequestActor(Info.PreviousOwner);
        } else {
            LOG_W("Got delivery problem to " << ev->Sender << ", node with owner unavailable, reason: " << reason);
            RetryCheckAlive(/* longDelay */ true);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        LOG_W("Node " << ev->Get()->NodeId << " with streaming query operation owner was disconnected, retry check alive");
        RetryCheckAlive(/* longDelay */ true);
    }

    void PassAway() override {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

private:
    void StartLockStreamingQueryRequestActor(std::optional<TActorId> previousOwner = std::nullopt) {
        if (WaitLock) {
            return;
        }

        WaitLock = true;

        const auto& lockActorId = Register(new TLockStreamingQueryRequestActor::TRetry(SelfId(), DatabaseId, QueryPath, {
            .Name = Settings.Name,
            .StartedAt = Settings.StartedAt,
            .Owner = Settings.Owner,
            .PreviousOwner = previousOwner,
            .CreateIfNotExists = Settings.CreateIfNotExists,
        }));
        LOG_D("Start TLockStreamingQueryRequestActor " << lockActorId);
    }

    void RetryCheckAlive(bool longDelay) {
        if (WaitLock || WaitRetryCheckAlive) {
            return;
        }

        if (!CheckAliveRetryState) {
            CheckAliveRetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [](bool longDelay) {
                    return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(300),
                TDuration::Seconds(1),
                MAX_CHECK_ALIVE_RETRIES
            )->CreateRetryState();
        }

        if (const auto delay = CheckAliveRetryState->GetNextRetryDelay(longDelay)) {
            LOG_D("Schedule retry check alive in " << *delay);
            Schedule(*delay, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RetryCheckAlive)));
            WaitRetryCheckAlive = true;
        } else {
            LOG_W("Retry limit " << MAX_CHECK_ALIVE_RETRIES << " exceeded for streaming query operation owner check alive, start lock");
            StartLockStreamingQueryRequestActor(Info.PreviousOwner);
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        LOG_D("Lock streaming query finished  " << status << ", issues: " << issues);
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, Info, std::move(issues)));
        PassAway();
    }

private:
    const TString DatabaseId;
    const TSettings Settings;
    TEvPrivate::TEvLockStreamingQueryResult::TInfo Info;
    std::optional<ui32> SubscribedOnSession;
    ui64 CheckAliveFlags = 0;
    TRetryPolicy::IRetryState::TPtr CheckAliveRetryState;
    bool WaitRetryCheckAlive = false;
    bool WaitLock = false;
};

class TUnlockStreamingQueryRequestActor : public TQueryBase {
public:
    using TRetry = TQueryRetryActor<TUnlockStreamingQueryRequestActor, TEvPrivate::TEvUnlockStreamingQueryResult, TString, TString, TActorId>;

    TUnlockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TActorId& owner)
        : TQueryBase(__func__, databaseId, queryPath)
        , Owner(owner)
    {}

    void OnRunQuery() override {
        LOG_D("Unlocking streaming query, Owner: " << Owner);
        SetQueryResultHandler(&TUnlockStreamingQueryRequestActor::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        auto result = ParseQueryInfo();

        if (result.IsFail()) {
            FinishWithStatus(result);
            return;
        }

        State = result.DetachResult();

        const auto previousOwner = ActorIdFromProto(State.GetOperationActorId());
        if (previousOwner != Owner) {
            LOG_E("Streaming query was locked by " << previousOwner << " during operation");
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query was changed during operation");
            return;
        }

        LOG_I("Remove streaming query lock " << Owner);
        UnlockQuery();
    }

    void UnlockQuery() {
        State.ClearOperationName();
        State.ClearOperationStartedAt();
        State.ClearOperationActorId();

        SetQueryResultHandler(&TUnlockStreamingQueryRequestActor::OnQueryResult, "Unlock query");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvUnlockStreamingQueryResult(status, std::move(issues)));
    }

private:
    const TActorId Owner;
    NKikimrKqp::TStreamingQueryState State;
};

class TCleanupStreamingQueryStateActor : public TActionActorBase<TCleanupStreamingQueryStateActor> {
    using TBase = TActionActorBase<TCleanupStreamingQueryStateActor>;

public:
    using TBase::LogPrefix;

    TCleanupStreamingQueryStateActor(const TExternalContext& context, const TString& queryPath, const NKikimrKqp::TStreamingQueryState& state)
        : TBase(__func__, queryPath)
        , Context(context)
        , State(state)
    {
        State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_DELETING);
    }

    void Bootstrap() {
        LOG_D("Bootstrap");
        StartUpdateState();

        Become(&TCleanupStreamingQueryStateActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, Handle);
        hFunc(TEvCancelScriptExecutionOperationResponse, Handle);
        hFunc(TEvForgetScriptExecutionOperationResponse, Handle);
    )

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Update streaming query state " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Update streaming query state", issues));
            return;
        }

        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            LOG_D("Cancel streaming query execution " << executionId);

            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)));
            return;
        }

        ClearStreamingQueryExecutions();
    }

    void Handle(TEvCancelScriptExecutionOperationResponse::TPtr& ev) {
        const auto& executionId = State.GetCurrentExecutionId();

        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::NOT_FOUND) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Cancel streaming query execution " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue(TStringBuilder() << "Cancel query execution (execution id: " << executionId << ")", issues));
            return;
        }

        LOG_D("Cancel streaming query execution " << ev->Sender << " finished " << status << ", execution id: " << executionId);
        State.AddPreviousExecutionIds(executionId);
        State.ClearCurrentExecutionId();

        StartUpdateState();
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < State.PreviousExecutionIdsSize());
        const auto& executionId = State.GetPreviousExecutionIds(ev->Cookie);

        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::SUCCESS && status != Ydb::StatusIds::NOT_FOUND) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Forget streaming query execution #" << ev->Cookie << " " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue(TStringBuilder() << "Forget query execution (execution id: " << executionId << ")", issues));
            return;
        }

        --OperationsToForget;
        LOG_D("Forget streaming query execution #" << ev->Cookie << " " << ev->Sender << " finished " << status << ", execution id: " << executionId << ", remains: " << OperationsToForget);

        if (OperationsToForget == 0) {
            State.ClearPreviousExecutionIds();
            StartUpdateState();
        }
    }

private:
    void StartUpdateState() const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId);
    }

    void ClearStreamingQueryExecutions() {
        if (State.PreviousExecutionIdsSize() == 0) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        LOG_D("Cleanup #" << State.PreviousExecutionIdsSize() << " previous executions");

        for (const auto& executionId : State.GetPreviousExecutionIds()) {
            SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)), OperationsToForget++);
            LOG_D("Forget streaming query execution #" << OperationsToForget << " " << executionId);
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        LOG_D("Cleanup streaming query finished  " << status << ", issues: " << issues);
        Send(Owner, new TEvPrivate::TEvCleanupStreamingQueryResult(status, std::move(issues)));
        PassAway();
    }

private:
    const TExternalContext Context;
    NKikimrKqp::TStreamingQueryState State;
    ui64 OperationsToForget = 0;
};

//// Request handlers

template <typename TDerived>
class TRequestHandlerBase : public TActionActorBase<TDerived> {
    using TBase = TActionActorBase<TDerived>;

public:
    using TBase::LogPrefix;

    TRequestHandlerBase(const TString& operationName, const NKikimrSchemeOp::TModifyScheme& schemeTx, const TString& name, const TExternalContext& context, NThreading::TPromise<TStreamingQueryConfig::TStatus> promise, ui32 access)
        : TBase(operationName, schemeTx.GetWorkingDir(), name)
        , Access(access)
        , StartedAt(TInstant::Now())
        , Context(context)
        , SchemeTx(schemeTx)
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(TBase::QueryPath, Context, Access));
        LOG_D("Bootstrap. Start TDescribeStreamingQuerySchemeActor " << describerId);

        Become(&TDerived::StateFunc);
    }

    STRICT_STFUNC(StateFuncBase,
        hFunc(TEvPrivate::TEvCheckAliveRequest, Handle);
        hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, Handle);
    )

    void Handle(TEvPrivate::TEvCheckAliveRequest::TPtr& ev) {
        LOG_N("Got check alive request from " << ev->Sender);
        TBase::Send(ev->Sender, new TEvPrivate::TEvCheckAliveResponse());
    }

    void Handle(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Describe streaming query " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Describe streaming query failed", issues));
            return;
        }

        const auto& properties = ev->Get()->Properties;
        LOG_D("Describe streaming query " << ev->Sender << " success, query exists: " << properties.has_value());

        OnQueryDescribed(properties);
    }

    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Lock streaming query " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Lock streaming query failed", issues));
            return;
        }

        const auto& info = ev->Get()->Info;
        IsLockCreated = info.LockCreated;
        const bool queryExists = info.QueryExists;
        LOG_D("Lock streaming query " << ev->Sender << " success, lock created: " << IsLockCreated << ", query exists: " << queryExists);

        OnQueryLocked(info.State, queryExists);
    }

    void Handle(TEvPrivate::TEvUnlockStreamingQueryResult::TPtr& ev) {
        IsLockCreated = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Unlock streaming query " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            if (FinalStatus == Ydb::StatusIds::SUCCESS || FinalStatus == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
                FinalStatus = status;
            }
            FinalIssues.AddIssues(AddRootIssue("Unlock streaming query failed", issues));
        } else {
            LOG_D("Unlock streaming query " << ev->Sender << " success");
        }

        Finish(FinalStatus, std::move(FinalIssues));
    }

protected:
    virtual void OnQueryDescribed(const std::optional<NKikimrSchemeOp::TStreamingQueryProperties>& properties) = 0;

    virtual void OnQueryLocked(const NKikimrKqp::TStreamingQueryState& state, bool queryExists) = 0;

protected:
    void LockQuery(const TString& name, bool createIfNotExists, NKikimrKqp::TStreamingQueryState::EStatus defaultStatus) const {
        const auto& lockActorId = TBase::Register(new TLockStreamingQueryTableActor(Context.GetDatabaseId(), TBase::QueryPath, {
            .Name = name,
            .StartedAt = StartedAt,
            .Owner = TBase::SelfId(),
            .CreateIfNotExists = createIfNotExists,
            .DefaultStatus = defaultStatus,
        }));
        LOG_D("Start TLockStreamingQueryTableActor " << lockActorId);
    }

    void UnlockQuery() const {
        const auto& unlockActorId = TBase::Register(new TUnlockStreamingQueryRequestActor::TRetry(TBase::SelfId(), Context.GetDatabaseId(), TBase::QueryPath, TBase::SelfId()));
        LOG_D("Start TUnlockStreamingQueryRequestActor " << unlockActorId);
    }

    void Finish(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (IsLockCreated) {
            FinalStatus = status;
            FinalIssues = std::move(issues);
            UnlockQuery();
            return;
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Finish streaming query operation successfully, issues: " << issues.ToOneLineString());
            Promise.SetValue(TStreamingQueryConfig::TStatus::Success());
        } else {
            LOG_W("Streaming query operation failed " << status << ", issues: " << issues.ToOneLineString());
            Promise.SetValue(TStreamingQueryConfig::TStatus::Fail(NYql::YqlStatusFromYdbStatus(status), std::move(issues)));
        }

        TBase::PassAway();
    }

    void Finish(Ydb::StatusIds::StatusCode status, const TString& message) {
        Finish(status, {NYql::TIssue(message)});
    }

private:
    const ui32 Access;
    Ydb::StatusIds::StatusCode FinalStatus;
    NYql::TIssues FinalIssues;

protected:
    const TInstant StartedAt;
    const TExternalContext Context;
    const NKikimrSchemeOp::TModifyScheme SchemeTx;
    bool IsLockCreated = false;
    NThreading::TPromise<TStreamingQueryConfig::TStatus> Promise;
};

class TDropStreamingQueryActor : public TRequestHandlerBase<TDropStreamingQueryActor> {
    using TBase = TRequestHandlerBase<TDropStreamingQueryActor>;

public:
    using TBase::LogPrefix;

    TDropStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, NThreading::TPromise<TStreamingQueryConfig::TStatus> promise)
        : TBase(__func__, schemeTx, schemeTx.GetDrop().GetName(), context, promise, NACLib::RemoveSchema)
        , SuccessOnNotExist(schemeTx.GetSuccessOnNotExist())
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCleanupStreamingQueryResult, Handle);
            hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, Handle);
            hFunc(TEvPrivate::TEvRemoveStreamingQueryResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Cleanup streaming query " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Cleanup streaming query failed", issues));
            return;
        }

        LOG_D("Cleanup streaming query " << ev->Sender << " success");
        QueryExistsInTable = false;

        if (!CleanupQuery()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Execute drop scheme operation failed " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Execute drop scheme operation failed", issues));
            return;
        }

        LOG_D("Drop query in scheme shard " << ev->Sender << " success");
        QueryExistsInSS = false;

        if (!CleanupQuery()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvRemoveStreamingQueryResult::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_W("Remove streaming query " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

            Finish(status, AddRootIssue("Remove streaming query failed", issues));
            return;
        }

        LOG_D("Remove streaming query " << ev->Sender << " success");
        IsLockCreated = false;

        if (!CleanupQuery()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

protected:
    void OnQueryDescribed(const std::optional<NKikimrSchemeOp::TStreamingQueryProperties>& properties) override {
        QueryExistsInSS = properties.has_value();
        LockQuery(
            TStringBuilder() << "DROP STREAMING QUERY" << (SuccessOnNotExist ? " IF EXISTS" : ""),
            QueryExistsInSS,
            NKikimrKqp::TStreamingQueryState::STATUS_DELETING
        );
    }

    void OnQueryLocked(const NKikimrKqp::TStreamingQueryState& state, bool queryExists) override {
        Y_UNUSED(state);

        State = state;
        QueryExistsInTable = queryExists;

        if (!CleanupQuery()) {
            if (SuccessOnNotExist) {
                Finish(Ydb::StatusIds::SUCCESS);
            } else {
                Finish(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            }
        }
    }

private:
    bool CleanupQuery() const {
        if (QueryExistsInTable) {
            // Clear query state
            const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateActor(Context, QueryPath, State));
            LOG_D("Start TCleanupStreamingQueryStateActor " << cleanupActorId);
            return true;
        }

        if (QueryExistsInSS) {
            // Remove query from SS
            const auto& executerId = Register(new TExecuteTransactionSchemeActor(QueryPath, SchemeTx, Context));
            LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
            return true;
        }

        if (IsLockCreated) {
            // Remove query row
            const auto& removeActorId = Register(new TRemoveStreamingQueryRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath));
            LOG_D("Start TRemoveStreamingQueryRequestActor " << removeActorId);
            return true;
        }

        return false;
    }

private:
    const bool SuccessOnNotExist = false;
    bool QueryExistsInSS = false;
    bool QueryExistsInTable = false;
    NKikimrKqp::TStreamingQueryState State;
};

}  // anonymous namespace

TStreamingQueryConfig::TAsyncStatus DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context) {
    const auto promise = NThreading::NewPromise<TStreamingQueryConfig::TStatus>();
    context.GetActorSystem()->Register(new TDropStreamingQueryActor(schemeTx, context, promise));

    return promise.GetFuture();
}

}  // namespace NKikimr::NKqp
