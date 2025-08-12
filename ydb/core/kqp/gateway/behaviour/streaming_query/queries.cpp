#include "queries.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
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

struct TSchemeInfo {
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
    ui64 Version = 0;
    TPathId PathId;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvStart = EventSpaceBegin(TEvents::ES_PRIVATE),

        // Scheme operations
        EvDescribeStreamingQueryResult = EvStart,
        EvExecuteSchemeTransactionResult,

        // Common query operations
        EvUpdateStreamingQueryResult,
        EvCleanupStreamingQueryResult,
        EvStartStreamingQueryResult,
        EvSynchronizeStreamingQueryResult,

        // Query locking
        EvLockStreamingQueryResult,
        EvUnlockStreamingQueryResult,
        EvCheckAliveRequest,
        EvCheckAliveResponse,

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
        TEvDescribeStreamingQueryResult(Ydb::StatusIds::StatusCode status, std::optional<TSchemeInfo> info, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , Info(std::move(info))
        {}

        const std::optional<TSchemeInfo> Info;
    };

    struct TEvExecuteSchemeTransactionResult : public TEvResultBase<TEvExecuteSchemeTransactionResult, EvExecuteSchemeTransactionResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvUpdateStreamingQueryResult : public TEvResultBase<TEvUpdateStreamingQueryResult, EvUpdateStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvCleanupStreamingQueryResult : public TEvResultBase<TEvCleanupStreamingQueryResult, EvCleanupStreamingQueryResult> {
        TEvCleanupStreamingQueryResult(Ydb::StatusIds::StatusCode status, NKikimrKqp::TStreamingQueryState state, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , State(std::move(state))
        {}

        const NKikimrKqp::TStreamingQueryState State;
    };

    struct TEvStartStreamingQueryResult : public TEvResultBase<TEvStartStreamingQueryResult, EvStartStreamingQueryResult> {
        TEvStartStreamingQueryResult(Ydb::StatusIds::StatusCode status, NKikimrKqp::TStreamingQueryState state, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , State(std::move(state))
        {}

        const NKikimrKqp::TStreamingQueryState State;
    };

    struct TEvSynchronizeStreamingQueryResult : public TEvResultBase<TEvSynchronizeStreamingQueryResult, EvSynchronizeStreamingQueryResult> {
        TEvSynchronizeStreamingQueryResult(Ydb::StatusIds::StatusCode status, NKikimrKqp::TStreamingQueryState state, bool existsInSS, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , State(std::move(state))
            , ExistsInSS(existsInSS)
        {}

        const NKikimrKqp::TStreamingQueryState State;
        const bool ExistsInSS = false;
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
};

// Stored in property LastOperationCase
enum class EOperationCase {
    Create,
    Alter,
    AlterWithRestart,
    Max = AlterWithRestart
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

class TPropertyValidator {
public:
    using TValidator = std::function<TStatus(const TString&)>;

    explicit TPropertyValidator(NKikimrSchemeOp::TStreamingQueryProperties& src)
        : Src(src)
    {}

    [[nodiscard]] TStatus RequiredProperty(const TString& name, TValidator validator = nullptr) {
        auto& srcProperties = *Src.MutableProperties();
        const auto it = srcProperties.find(name);
        if (it == srcProperties.end()) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Missing required property: " << name);
        }

        const TString value = it->second;
        srcProperties.erase(it);

        return AddProperty(name, value, validator);
    }

    [[nodiscard]] TStatus OptionalProperty(const TString& name, const TString& defaultValue, TValidator validator = nullptr) {
        TString value = defaultValue;

        auto& srcProperties = *Src.MutableProperties();
        const auto it = srcProperties.find(name);
        if (it != srcProperties.end()) {
            value = it->second;
            srcProperties.erase(it);
        }

        return AddProperty(name, value, validator);
    }

    [[nodiscard]] TStatus AddProperty(const TString& name, const TString& value, TValidator validator = nullptr) {
        if (validator) {
            if (const auto& status = validator(value); !status.IsSuccess()) {
                return status;
            }
        }

        if (!Dst.MutableProperties()->emplace(name, value).second) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Duplicate property: " << name);
        }

        return TStatus::Success();
    }

    [[nodiscard]] TStatus Finish() {
        if (const auto& properties = Src.GetProperties(); !properties.empty()) {
            auto error = TStringBuilder() << "Got unexpected properties: ";
            for (auto it = properties.begin(); it != properties.end();) {
                error << it->first;
                if (++it != properties.end()) {
                    error << ", ";
                }
            }

            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, error);
        }

        Src = std::move(Dst);
        return TStatus::Success();
    }

private:
    NKikimrSchemeOp::TStreamingQueryProperties& Src;
    NKikimrSchemeOp::TStreamingQueryProperties Dst;
};

class TStreamingQuerySettings {
public:
    TStatus FromProto(const NKikimrSchemeOp::TStreamingQueryProperties& info) {
        for (const auto& [property, value] : info.GetProperties()) {
            if (property == TStreamingQueryConfig::TProperties::QueryText) {
                QueryText = value;
            } else if (property == TStreamingQueryConfig::TProperties::Run) {
                Run = value == "true";
            } else if (property == TStreamingQueryConfig::TProperties::ResourcePool) {
                ResourcePool = value;
            } else if (property == TStreamingQueryConfig::TProperties::LastOperationCase) {
                const auto intValue = TryFromString<ui64>(value);
                if (!intValue) {
                    return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to parse LastOperationCase from value " << value);
                }
                if (*intValue > static_cast<ui64>(EOperationCase::Max)) {
                    return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Value of LastOperationCase is too large " << *intValue << " (max allowed is " << static_cast<ui64>(EOperationCase::Max) << ")");
                }
                LastOperationCase = static_cast<EOperationCase>(*intValue);
            }
        }

        return TStatus::Success();
    }

public:
    TString QueryText;
    bool Run = false;
    TString ResourcePool;
    EOperationCase LastOperationCase = EOperationCase::Create;
};

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
    // Do action before finish
    virtual bool BeforeFinish(Ydb::StatusIds::StatusCode status) {
        Y_UNUSED(status);
        return false;
    }

    virtual void OnFinish(Ydb::StatusIds::StatusCode status) = 0;

protected:
    template <typename TEvent>
    void SendToKqpProxy(std::unique_ptr<TEvent> event, ui64 cookie = 0) const {
        const auto& kqpProxy = MakeKqpProxyID(TBase::SelfId().NodeId());
        TBase::Send(kqpProxy, std::move(event), 0, cookie);
    }

    template <typename TEvPtr>
    bool HandleResult(TEvPtr& ev, const TString& message) {
        const Ydb::StatusIds::StatusCode status = ev->Get()->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D(message << " " << ev->Sender << " success");
            return false;
        }

        const NYql::TIssues& issues = ev->Get()->Issues;
        LOG_W(message << " " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

        FatalError(status, AddRootIssue(TStringBuilder() << message << " failed", issues));
        return true;
    }

    void Finish(Ydb::StatusIds::StatusCode status) {
        if (BeforeFinish(status)) {
            return;
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Successfully finished");
        } else {
            LOG_W("Failed " << status << ", with issues: " << Issues.ToOneLineString());
        }

        OnFinish(status);
        TBase::PassAway();
    }

    void FatalError(Ydb::StatusIds::StatusCode status, NYql::TIssues issues) {
        Issues.AddIssues(std::move(issues));
        Finish(status);
    }

    void FatalError(Ydb::StatusIds::StatusCode status, const TString& message) {
        FatalError(status, {NYql::TIssue(message)});
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
    NYql::TIssues Issues;
};

//// Scheme actions

template <typename TDerived>
class TSchemeActorBase : public TActionActorBase<TDerived> {
    using TBase = TActionActorBase<TDerived>;
    using TRetryPolicy = IRetryPolicy<bool>;

public:
    using TBase::LogPrefix;

    TSchemeActorBase(const TString& database, const TString& operationName, const TString& queryPath, const std::optional<NACLib::TUserToken>& userToken)
        : TBase(operationName, queryPath)
        , Database(database)
        , UserToken(userToken)
    {
        if (UserToken && UserToken->GetSerializedToken().empty()) {
            UserToken->SaveSerializationInfo();
        }
    }

    void Bootstrap() {
        LOG_D("Bootstrap. Database: " << Database);
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
        TBase::FatalError(Ydb::StatusIds::UNAVAILABLE, "Scheme service is unavailable");
    }

protected:
    virtual void StartRequest() = 0;

protected:
    bool ScheduleRetry(NYql::TIssues issues, bool longDelay = false) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay(longDelay)) {
            LOG_W("Schedule retry for error: " << issues.ToOneLineString() << " in " << *delay);
            TBase::Issues.AddIssues(std::move(issues));
            this->Schedule(*delay, new TEvents::TEvWakeup());
            return true;
        }

        return false;
    }

    bool ScheduleRetry(const TString& message, bool longDelay = false) {
        return ScheduleRetry({NYql::TIssue(message)}, longDelay);
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
        if (!UserToken) {
            return nullptr;
        }

        return MakeIntrusiveConst<NACLib::TUserToken>(*UserToken);
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
    const TString Database;

private:
    std::optional<NACLib::TUserToken> UserToken;
    TRetryPolicy::IRetryState::TPtr RetryState;
};

class TDescribeStreamingQuerySchemeActor : public TSchemeActorBase<TDescribeStreamingQuerySchemeActor> {
    using TBase = TSchemeActorBase<TDescribeStreamingQuerySchemeActor>;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

public:
    using TBase::LogPrefix;

    TDescribeStreamingQuerySchemeActor(const TString& database, const TString& queryPath, const std::optional<NACLib::TUserToken>& userToken, ui32 access)
        : TBase(__func__, database, queryPath, userToken)
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
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        const auto& result = results[0];
        LOG_D("Got scheme cache response: " << result.Status);

        switch (result.Status) {
            case EStatus::Unknown:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
            case EStatus::RedirectLookupError: {
                FatalError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid streaming query path " << QueryPath);
                return;
            }
            case EStatus::AccessDenied: {
                FatalError(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for streaming query " << QueryPath);
                return;
            }
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown: {
                Finish(Ydb::StatusIds::SUCCESS);
                return;
            }
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete: {
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                }
                return;
            }
            case EStatus::Ok: {
                if (result.Kind != NSchemeCache::TSchemeCacheNavigate::KindStreamingQuery) {
                    FatalError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Path " << QueryPath << " is not a streaming query");
                } else if (!result.Self || !result.StreamingQueryInfo) {
                    FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response for ok status");
                } else {
                    const auto& pathInfo = result.Self->Info;
                    const auto& description = result.StreamingQueryInfo->Description;
                    Info = TSchemeInfo{
                        .Properties = description.GetProperties(),
                        .Version = description.GetVersion(),
                        .PathId = TPathId(pathInfo.GetSchemeshardId(), pathInfo.GetPathId()),
                    };
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                return;
            }
        }
    }

protected:
    void StartRequest() override {
        LOG_D("Describe streaming query in database: " << Database << ", with access: " << Access);

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = CanonizePath(Database);
        request->UserToken = GetUserToken();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = SplitPath(QueryPath);
        entry.Access = Access;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvDescribeStreamingQueryResult(status, std::move(Info), std::move(Issues)));
    }

private:
    const ui32 Access = 0;
    std::optional<TSchemeInfo> Info;
};

class TExecuteTransactionSchemeActor : public TSchemeActorBase<TExecuteTransactionSchemeActor> {
    using TBase = TSchemeActorBase<TExecuteTransactionSchemeActor>;

public:
    using TBase::LogPrefix;

    TExecuteTransactionSchemeActor(const TString& database, const TString& queryPath, const NKikimrSchemeOp::TModifyScheme& schemeTx, const std::optional<NACLib::TUserToken>& userToken)
        : TBase(__func__, database, queryPath, userToken)
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
                    FatalError(Ydb::StatusIds::INTERNAL_ERROR, ExtractIssues(response, ssStatus, "unable to subscribe on creation transaction"));
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
                    FatalError(Ydb::StatusIds::ALREADY_EXISTS, ExtractIssues(response, ssStatus, TStringBuilder() << "execution completed, streaming query " << QueryPath << " already exists"));
                } else {
                    FatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "unexpected final execution status"));
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
                FatalError(Ydb::StatusIds::UNAUTHORIZED, ExtractIssues(response, ssStatus, TStringBuilder() << "you don't have access permissions for operation on streaming query " << QueryPath));
                break;
            }
            case NTxProxy::TResultStatus::ResolveError: {
                if (ssStatus == NKikimrScheme::EStatus::StatusPathDoesNotExist) {
                    FatalError(Ydb::StatusIds::NOT_FOUND, ExtractIssues(response, ssStatus, TStringBuilder() << "streaming query " << QueryPath << " not found or you don't have access permissions"));
                } else {
                    FatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "resolve error"));
                }
                break;
            }
            case NTxProxy::TResultStatus::NotImplemented: {
                FatalError(Ydb::StatusIds::UNSUPPORTED, ExtractIssues(response, ssStatus, "operation not implemented"));
                break;
            }
            case NTxProxy::TResultStatus::ProxyShardOverloaded:  {
                FatalError(Ydb::StatusIds::OVERLOADED, ExtractIssues(response, ssStatus, "tx proxy is overloaded"));
                break;
            }
            case NTxProxy::TResultStatus::ExecAborted: {
                FatalError(Ydb::StatusIds::ABORTED, ExtractIssues(response, ssStatus, "execution aborted"));
                break;
            }
            case NTxProxy::TResultStatus::ExecTimeout: {
                FatalError(Ydb::StatusIds::TIMEOUT, ExtractIssues(response, ssStatus, "execution timeout"));
                break;
            }
            case NTxProxy::TResultStatus::ExecCancelled: {
                FatalError(Ydb::StatusIds::CANCELLED, ExtractIssues(response, ssStatus, "execution canceled"));
                break;
            }
            case NTxProxy::TResultStatus::ExecError: {
                switch (static_cast<NKikimrScheme::EStatus>(ssStatus)) {
                    case NKikimrScheme::StatusPathDoesNotExist: {
                        FatalError(Ydb::StatusIds::NOT_FOUND, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, streaming query " << QueryPath << " not found or you don't have access permissions"));
                        break;
                    }
                    case NKikimrScheme::StatusAlreadyExists: {
                        FatalError(Ydb::StatusIds::ALREADY_EXISTS, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, streaming query " << QueryPath << " already exists"));
                        break;
                    }
                    case NKikimrScheme::StatusAccessDenied: {
                        FatalError(Ydb::StatusIds::UNAUTHORIZED, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, you don't have access permissions for operation on streaming query " << QueryPath));
                        break;
                    }
                    case NKikimrScheme::StatusNotAvailable: {
                        FatalError(Ydb::StatusIds::UNAVAILABLE, ExtractIssues(response, ssStatus, "execution error, scheme shard is not available"));
                        break;
                    }
                    case NKikimrScheme::StatusPreconditionFailed: {
                        FatalError(Ydb::StatusIds::PRECONDITION_FAILED, ExtractIssues(response, ssStatus, "execution error, precondition failed"));
                        break;
                    }
                    case NKikimrScheme::StatusQuotaExceeded:
                    case NKikimrScheme::StatusResourceExhausted: {
                        FatalError(Ydb::StatusIds::OVERLOADED, ExtractIssues(response, ssStatus, "execution error, resource exhausted"));
                        break;
                    }
                    default: {
                        FatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, "transaction execution failed"));
                        break;
                    }
                }
            }
            default: {
                FatalError(Ydb::StatusIds::SCHEME_ERROR, ExtractIssues(response, ssStatus, TStringBuilder() << "unexpected transaction status " << status));
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
        FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Tablet to pipe not connected: " << NKikimrProto::EReplyStatus_Name(status));
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;
        if (!ClosedSchemePipeActors.contains(clientId)) {
            ClosePipeClient();
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Tablet to pipe unexpectedly destroyed");
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
        LOG_D("Start scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " in database: " << Database);

        auto event = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        event->Record.SetDatabaseName(Database);
        if (const auto token = GetUserToken()) {
            event->Record.SetUserToken(token->GetSerializedToken());
        }

        Send(MakeTxProxyID(), std::move(event));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvExecuteSchemeTransactionResult(status, std::move(Issues)));
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
            FatalError(Ydb::StatusIds::UNAVAILABLE, ExtractIssues(response, ssStatus, TStringBuilder() << "Retry limit exceeded on error: " << message));
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

        const std::optional<TString>& stateJsonString = result.ColumnParser(TStreamingQueryConfig::TColumns::State).GetOptionalJsonDocument();
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
        SetQueryResultHandler(&TUpdateStreamingQueryStateRequestActor::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        const auto result = ParseQueryInfo();
        if (result.IsFail()) {
            FinishWithStatus(result);
            return;
        }

        const auto currentOwner = ActorIdFromProto(State.GetOperationActorId());
        const auto previousOwner = ActorIdFromProto(result.GetResult().GetOperationActorId());
        if (currentOwner != previousOwner) {
            LOG_E("Streaming query was locked by " << previousOwner << " during operation (expected owner: " << currentOwner << ")");
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query was changed during operation");
            return;
        }

        UpdateQuery();
    }

    void UpdateQuery() {
        SetQueryResultHandler(&TUpdateStreamingQueryStateRequestActor::OnQueryResult, "Update query info");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
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
            << ", PreviousOwner: " << Settings.PreviousOwner.value_or(TActorId())
            << ", DefaultStatus: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(Settings.DefaultStatus));

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

        if (HandleResult(ev, "Lock streaming query")) {
            return;
        }

        LOG_D("Lock streaming query finished"
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
            FatalError(Ydb::StatusIds::ABORTED, {NYql::TIssue(TStringBuilder() << "Streaming query already under operation " << Info.PreviousOperationName << " started at " << Info.PreviousOperationStartedAt << ", try repeat request later")});
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeup>(ev->Get()->Tag)) {
            case EWakeup::RetryCheckAlive: {
                WaitRetryCheckAlive = false;
                LOG_D("Retry check alive request for " << Info.PreviousOwner);
                Send(Info.PreviousOwner, new TEvPrivate::TEvCheckAliveRequest(), CheckAliveFlags);
                Schedule(CHECK_ALIVE_REQUEST_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveTimeout)));
                break;
            }
            case EWakeup::CheckAliveTimeout: {
                LOG_W("Deliver streaming query owner " << Info.PreviousOwner << " check alive request timeout, retry check alive");
                RetryCheckAlive(/* longDelay */ false);
                break;
            }
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

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, Info, std::move(Issues)));
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

    TUnlockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TActorId& operationOwner)
        : TQueryBase(__func__, databaseId, queryPath)
        , OperationOwner(operationOwner)
    {}

    void OnRunQuery() override {
        LOG_D("Unlocking streaming query, Owner: " << OperationOwner);
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

        if (const auto previousOwner = ActorIdFromProto(State.GetOperationActorId()); OperationOwner != previousOwner) {
            LOG_E("Streaming query was locked by " << previousOwner << " during operation (expected owner: " << OperationOwner << ")");
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query was changed during operation");
            return;
        }

        if (State.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED) {
            LOG_D("Delete streaming query from table");
            RemoveQuery();
        } else {
            LOG_D("Remove streaming query lock " << OperationOwner);
            UnlockQuery();
        }
    }

    void UnlockQuery() {
        State.ClearOperationName();
        State.ClearOperationStartedAt();
        State.ClearOperationActorId();

        SetQueryResultHandler(&TUnlockStreamingQueryRequestActor::OnQueryResult, "Unlock query");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void RemoveQuery() {
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

        SetQueryResultHandler(&TUnlockStreamingQueryRequestActor::OnQueryResult, "Delete query");
        ExecuteQuery(__func__, sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        Send(Owner, new TEvPrivate::TEvUnlockStreamingQueryResult(status, std::move(issues)));
    }

private:
    const TActorId OperationOwner;
    NKikimrKqp::TStreamingQueryState State;
};

class TCleanupStreamingQueryStateActor : public TActionActorBase<TCleanupStreamingQueryStateActor> {
    using TBase = TActionActorBase<TCleanupStreamingQueryStateActor>;

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        NKikimrKqp::TStreamingQueryState::EStatus FinalStatus = NKikimrKqp::TStreamingQueryState::STATUS_DELETING;
    };

    TCleanupStreamingQueryStateActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap. Final status: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(Settings.FinalStatus));
        Become(&TCleanupStreamingQueryStateActor::StateFunc);

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_DELETING);
            StartUpdateState();
        } else {
            ClearStreamingQueryExecutions();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, Handle);
        hFunc(TEvCancelScriptExecutionOperationResponse, Handle);
        hFunc(TEvForgetScriptExecutionOperationResponse, Handle);
    )

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state")) {
            return;
        }

        ClearStreamingQueryExecutions();
    }

    void Handle(TEvCancelScriptExecutionOperationResponse::TPtr& ev) {
        const auto& executionId = State.GetCurrentExecutionId();
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::NOT_FOUND && status != Ydb::StatusIds::PRECONDITION_FAILED && HandleResult(ev, TStringBuilder() << "Cancel query execution (execution id: " << executionId << ")")) {
            return;
        }

        LOG_D("Cancel streaming query execution " << ev->Sender << " finished " << status << ", execution id: " << executionId);
        if (status != Ydb::StatusIds::NOT_FOUND) {
            State.AddPreviousExecutionIds(executionId);
        }
        State.ClearCurrentExecutionId();

        StartUpdateState();
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < State.PreviousExecutionIdsSize());
        const auto& executionId = State.GetPreviousExecutionIds(ev->Cookie);
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::NOT_FOUND && HandleResult(ev, TStringBuilder() << "Forget query execution (execution id: " << executionId << ")")) {
            return;
        }

        --OperationsToForget;
        LOG_D("Forget streaming query execution #" << ev->Cookie << " " << ev->Sender << " finished " << status << ", execution id: " << executionId << ", remains: " << OperationsToForget);

        if (OperationsToForget == 0) {
            State.ClearPreviousExecutionIds();
            StartUpdateState();
        }
    }

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvCleanupStreamingQueryResult(status, std::move(State), std::move(Issues)));
    }

private:
    void StartUpdateState() const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId);
    }

    void ClearStreamingQueryExecutions() {
        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            LOG_D("Cancel streaming query execution " << executionId);
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)));
            return;
        }

        if (State.PreviousExecutionIdsSize() > 0) {
            LOG_D("Cleanup #" << State.PreviousExecutionIdsSize() << " previous executions");

            for (const auto& executionId : State.GetPreviousExecutionIds()) {
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)), OperationsToForget++);
                LOG_D("Forget streaming query execution #" << OperationsToForget << " " << executionId);
            }
            return;
        }

        if (State.GetStatus() != Settings.FinalStatus) {
            State.SetStatus(Settings.FinalStatus);
            StartUpdateState();
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

private:
    const TExternalContext Context;
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    ui64 OperationsToForget = 0;
};

class TStartStreamingQueryActor : public TActionActorBase<TStartStreamingQueryActor> {
    using TBase = TActionActorBase<TStartStreamingQueryActor>;

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        NKikimrConfig::TStreamingQueriesConfig Config;
        TPathId QueryPathId;
    };

    TStartStreamingQueryActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap. Start new query: " << State.GetQueryText());
        PrepareToStart();
    }

    STRICT_STFUNC(PrepareStateFunc,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandlePrepare);
        hFunc(TEvForgetScriptExecutionOperationResponse, HandlePrepare);
    )

    void HandlePrepare(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state (prepare to start)")) {
            return;
        }

        PrepareToStart();
    }

    void HandlePrepare(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        const ui64 toCleanup = State.PreviousExecutionIdsSize() - Settings.Config.GetMaxQueryExecutions();
        Y_ABORT_UNLESS(ev->Cookie < toCleanup);

        const auto& executionId = State.GetPreviousExecutionIds(ev->Cookie);
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::NOT_FOUND && HandleResult(ev, TStringBuilder() << "Forget query execution (execution id: " << executionId << ")")) {
            return;
        }

        --OperationsToForget;
        LOG_D("Forget streaming query execution #" << ev->Cookie << " " << ev->Sender << " finished " << status << ", execution id: " << executionId << ", remains: " << OperationsToForget);

        if (OperationsToForget == 0) {
            auto& executionIds = *State.MutablePreviousExecutionIds();
            executionIds.erase(executionIds.begin(), executionIds.begin() + toCleanup);
            PrepareToStart();
        }
    }

    STRICT_STFUNC(StartQueryStateFunc,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleStartQuery);
        hFunc(TEvKqp::TEvScriptResponse, HandleStartQuery);
        hFunc(TEvScriptExecutionProgress, HandleStartQuery);
    )

    void HandleStartQuery(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, TStringBuilder() << "Update streaming query state (start query)" << (FinalStatus ? (TStringBuilder() << " query start status: " << *FinalStatus) : TStringBuilder()))) {
            return;
        }

        if (FinalStatus) {
            Finish(*FinalStatus);
        } else {
            StartQuery();
        }
    }

    void HandleStartQuery(TEvKqp::TEvScriptResponse::TPtr& ev) {
        if (HandleResult(ev, "Create script execution operation")) {
            return;
        }

        ExecutionCreated = true;
        LOG_D("Script execution created: " << ev->Get()->ExecutionId << ", wait for saving query state");
    }

    void HandleStartQuery(TEvScriptExecutionProgress::TPtr& ev) {
        if (HandleResult(ev, "Query compilation / starting")) {
            return;
        }

        if (ev->Get()->StateSaved) {
            Finish(Ydb::StatusIds::SUCCESS);
        } else {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, AddRootIssue("Query state is not saved", ev->Get()->Issues));
        }
    }

protected:
    bool BeforeFinish(Ydb::StatusIds::StatusCode status) override {
        if (!StartRequestSent) {
            return false;
        }

        Become(&TStartStreamingQueryActor::StartQueryStateFunc);

        if (status == Ydb::StatusIds::SUCCESS) {
            if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_RUNNING) {
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_RUNNING);
                UpdateQueryState("move query to running");

                FinalStatus = status;
                return true;
            }
        } else if (ExecutionCreated) {
            ExecutionCreated = false;

            const auto& executionId = State.GetCurrentExecutionId();
            State.AddPreviousExecutionIds(executionId);
            State.ClearCurrentExecutionId();
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
            UpdateQueryState("move query to stopped");

            FinalStatus = status;
            return true;
        }

        return false;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvStartStreamingQueryResult(status, State, std::move(Issues)));
    }

private:
    void UpdateQueryState(const TString& info) const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId << " (" << info << ")");
    }

    void PrepareToStart() {
        Become(&TStartStreamingQueryActor::PrepareStateFunc);

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_STARTING) {
            UpdateQueryState("move query to starting");
            return;
        }

        if (const ui64 maxExecutions = Settings.Config.GetMaxQueryExecutions(); State.PreviousExecutionIdsSize() > maxExecutions) {
            const ui64 toCleanup = State.PreviousExecutionIdsSize() - maxExecutions;
            LOG_D("Cleanup #" << toCleanup << " previous executions (max executions: " << maxExecutions << ")");

            for (ui64 i = 0; i < toCleanup; ++i) {
                const auto& executionId = State.GetPreviousExecutionIds(i);
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)), OperationsToForget++);
                LOG_D("Forget streaming query execution #" << OperationsToForget << " " << executionId);
            }
            return;
        }

        // Execution id for streaming queries:
        // <GUID part>-<GUID part>-<GUID part>-<Node id>-<SS id>-<Path id in SS>

        const auto& pathId = Settings.QueryPathId;
        State.SetCurrentExecutionId(TStringBuilder() << CreateGuidAsString() << '-' << SelfId().NodeId() << '-' << pathId.OwnerId << '-' << pathId.LocalPathId);
        UpdateQueryState(TStringBuilder() << "allocate execution id: " << State.GetCurrentExecutionId());
        Become(&TStartStreamingQueryActor::StartQueryStateFunc);
    }

    void StartQuery() {
        if (StartRequestSent) {
            return;
        }

        auto ev = std::make_unique<TEvKqp::TEvScriptRequest>();
        ev->SaveQueryPhysicalGraph = true;
        ev->RetryMapping = CreateRetryMapping();
        ev->ExecutionId = State.GetCurrentExecutionId();
        ev->DisableDefaultTimeout = true;

        auto& record = ev->Record;
        record.SetTraceId(TStringBuilder() << "streaming-query-" << QueryPath << "-" << State.GetCurrentExecutionId());
        if (const auto& token = Context.GetUserToken()) {
            record.SetUserToken(token->SerializeAsString());
        }

        auto& request = *record.MutableRequest();
        request.SetDatabase(Context.GetDatabase());
        request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE);
        request.SetSyntax(Ydb::Query::SYNTAX_YQL_V1);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        request.SetKeepSession(false);
        request.SetPoolId(State.GetResourcePool());
        request.SetQuery(State.GetQueryText());

        StartRequestSent = true;
        LOG_D("Send start streaming query request, execution id: " << State.GetCurrentExecutionId());
        SendToKqpProxy(std::move(ev));
    }

    std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> CreateRetryMapping() const {
        // Retried all statuses except of SUCCESS, CANCELLED

        std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> result;

        {   // Immediate retry policy
            // Query will retry infinitely, if it runtime >= 864s (<= 100 retries per day)
            // Used for internal / user errors and temporary unavailability
            NKikimrKqp::TScriptExecutionRetryState::TMapping mapping;
            mapping.AddStatusCode(Ydb::StatusIds::UNAVAILABLE);
            mapping.AddStatusCode(Ydb::StatusIds::INTERNAL_ERROR);
            mapping.AddStatusCode(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
            mapping.AddStatusCode(Ydb::StatusIds::UNDETERMINED);
            mapping.AddStatusCode(Ydb::StatusIds::ABORTED);
            mapping.AddStatusCode(Ydb::StatusIds::SESSION_BUSY);
            mapping.AddStatusCode(Ydb::StatusIds::BAD_SESSION);
            mapping.AddStatusCode(Ydb::StatusIds::TIMEOUT);
            mapping.AddStatusCode(Ydb::StatusIds::ALREADY_EXISTS);
            mapping.AddStatusCode(Ydb::StatusIds::SCHEME_ERROR);
            mapping.AddStatusCode(Ydb::StatusIds::GENERIC_ERROR);
            mapping.AddStatusCode(Ydb::StatusIds::PRECONDITION_FAILED);
            mapping.AddStatusCode(Ydb::StatusIds::SESSION_EXPIRED);
            mapping.AddStatusCode(Ydb::StatusIds::UNSUPPORTED);

            auto& policy = *mapping.MutableBackoffPolicy();
            policy.SetRetryPeriodMs(TDuration::Days(1).MilliSeconds());
            policy.SetRetryRateLimit(100);

            result.push_back(std::move(mapping));
        }

        {   // Short backoff retry policy
            // Query will retry infinitely, if it runtime >= 662s (<= 130 retries per day), maximal backoff period is 202s
            // Used for potentially external errors
            NKikimrKqp::TScriptExecutionRetryState::TMapping mapping;
            mapping.AddStatusCode(Ydb::StatusIds::EXTERNAL_ERROR);
            mapping.AddStatusCode(Ydb::StatusIds::BAD_REQUEST);
            mapping.AddStatusCode(Ydb::StatusIds::UNAUTHORIZED);
            mapping.AddStatusCode(Ydb::StatusIds::NOT_FOUND);

            auto& policy = *mapping.MutableBackoffPolicy();
            policy.SetRetryPeriodMs(TDuration::Days(1).MilliSeconds());
            policy.SetRetryRateLimit(100);
            policy.SetBackoffPeriodMs(TDuration::Seconds(2).MilliSeconds());

            result.push_back(std::move(mapping));
        }

        {   // Long backoff retry policy
            // Query will retry infinitely, if it runtime >= 448s (<= 192 retries per day), maximal backoff period is 404s
            // Used for cluster overloaded errors
            NKikimrKqp::TScriptExecutionRetryState::TMapping mapping;
            mapping.AddStatusCode(Ydb::StatusIds::OVERLOADED);

            auto& policy = *mapping.MutableBackoffPolicy();
            policy.SetRetryPeriodMs(TDuration::Days(1).MilliSeconds());
            policy.SetRetryRateLimit(100);
            policy.SetBackoffPeriodMs(TDuration::Seconds(4).MilliSeconds());

            result.push_back(std::move(mapping));
        }

        return result;
    }

private:
    const TExternalContext Context;
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    ui64 OperationsToForget = 0;

    // Query starting state
    bool StartRequestSent = false;
    bool ExecutionCreated = false;
    std::optional<Ydb::StatusIds::StatusCode> FinalStatus;
};

class TSynchronizeStreamingQueryTableActor : public TActionActorBase<TSynchronizeStreamingQueryTableActor> {
    using TBase = TActionActorBase<TSynchronizeStreamingQueryTableActor>;

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        std::optional<TSchemeInfo> SchemeInfo;  // nullopt if query does not exists in SS
        NKikimrConfig::TStreamingQueriesConfig Config;
    };

    TSynchronizeStreamingQueryTableActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
        , ExistsInSS(Settings.SchemeInfo.has_value())
    {}

    void Bootstrap() {
        LOG_D("Bootstrap."
            << "Has in SS: " << ExistsInSS
            << ", SS alter version: " << (Settings.SchemeInfo ? Settings.SchemeInfo->Version : 0)
            << ", initial status: " << NKikimrKqp::TStreamingQueryState_EStatus(State.GetStatus()));

        if (!Settings.SchemeInfo || State.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            RemoveQuery();
            return;
        }

        QueryPathId = Settings.SchemeInfo->PathId;
        AlterVersion = Settings.SchemeInfo->Version;

        if (AlterVersion == State.GetAlterVersion()) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        if (const auto status = QuerySettings.FromProto(Settings.SchemeInfo->Properties); status.IsFail()) {
            FatalError(status.GetStatus(), status.GetErrorDescription());
            return;
        }

        SynchronizeQuery();
    }

    STRICT_STFUNC(RemoveQueryStateFunc,
        hFunc(TEvPrivate::TEvCleanupStreamingQueryResult, HandleRemove);
        hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, HandleRemove);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleRemove);
    )

    void HandleRemove(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Cleanup streaming query (remove query)")) {
            return;
        }

        State = ev->Get()->State;
        RemoveQuery();
    }

    void HandleRemove(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute drop scheme operation")) {
            return;
        }

        ExistsInSS = false;
        RemoveQuery();
    }

    void HandleRemove(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state (remove query)")) {
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCleanupStreamingQueryResult, Handle);
        hFunc(TEvCancelScriptExecutionOperationResponse, Handle);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvStartStreamingQueryResult, Handle);
    )

    void Handle(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Cleanup streaming query")) {
            return;
        }

        State = ev->Get()->State;
        SynchronizeQuery();
    }

    void Handle(TEvCancelScriptExecutionOperationResponse::TPtr& ev) {
        const auto& executionId = State.GetCurrentExecutionId();
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::NOT_FOUND && status != Ydb::StatusIds::PRECONDITION_FAILED && HandleResult(ev, TStringBuilder() << "Cancel query execution (execution id: " << executionId << ")")) {
            return;
        }

        LOG_D("Cancel streaming query execution " << ev->Sender << " finished " << status << ", execution id: " << executionId);
        if (status != Ydb::StatusIds::NOT_FOUND) {
            State.AddPreviousExecutionIds(executionId);
        }
        State.ClearCurrentExecutionId();

        SynchronizeQuery();
    }

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state")) {
            return;
        }

        SynchronizeQuery();
    }

    void Handle(TEvPrivate::TEvStartStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Start streaming query")) {
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        Send(Owner, new TEvPrivate::TEvSynchronizeStreamingQueryResult(status, State, ExistsInSS, std::move(Issues)));
    }

private:
    void UpdateQueryState(const TString& info) const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId << " (" << info << ")");
    }

    void CleanupQuery(const TString& info, NKikimrKqp::TStreamingQueryState::EStatus finalStatus = NKikimrKqp::TStreamingQueryState::STATUS_DELETING) const {
        const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateActor(Context, QueryPath, {
            .InitialState = State,
            .FinalStatus = finalStatus,
        }));
        LOG_D("Start TCleanupStreamingQueryStateActor " << cleanupActorId << " (" << info << ")");
    }

    void StopQuery(const TString& info) {
        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_STOPPING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPING);
            UpdateQueryState(TStringBuilder() << "move to stopping" << " (" << info << ")");
            return;
        }

        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            LOG_D("Cancel streaming query execution " << executionId << " (" << info << ")");
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId)));
            return;
        }

        State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
        UpdateQueryState(TStringBuilder() << "move to stopped" << " (" << info << ")");
    }

    void RemoveQuery() {
        Become(&TSynchronizeStreamingQueryTableActor::RemoveQueryStateFunc);

        if (State.HasCurrentExecutionId() || State.PreviousExecutionIdsSize() > 0) {
            CleanupQuery("remove query");
            return;
        }

        if (ExistsInSS) {
            // Remove query from SS
            std::pair<TString, TString> pathPair;
            if (TString error; !NSchemeHelpers::SplitTablePath(QueryPath, Context.GetDatabase(), pathPair, error, /* createDir */ false)) {
                FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Invalid streaming query path '" << QueryPath << "': " << error);
                return;
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropStreamingQuery);
            schemeTx.MutableDrop()->SetName(pathPair.second);

            const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, schemeTx, NACLib::TUserToken(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})));
            LOG_D("Start TExecuteTransactionSchemeActor " << executerId << " (drop streaming query)");
            return;
        }

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED) {
            // Clear query status
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
            State.SetAlterVersion(0);
            UpdateQueryState("remove query");
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

    void EnrichStateFromSS(bool syncAlterVersion) {
        State.SetQueryText(QuerySettings.QueryText);
        State.SetRun(QuerySettings.Run);
        State.SetResourcePool(QuerySettings.ResourcePool);

        if (State.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_CREATED);
        }

        if (syncAlterVersion) {
            State.SetAlterVersion(AlterVersion);
        }

        StateEnrichedFromSS = true;
        UpdateQueryState("enrich state from SS");
    }

    void StartQuery() {
        if (State.HasCurrentExecutionId()) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Can not start query, already started: " << State.GetCurrentExecutionId());
            return;
        }

        const auto& startActorId = Register(new TStartStreamingQueryActor(Context, QueryPath, {
            .InitialState = State,
            .Config = Settings.Config,
            .QueryPathId = QueryPathId,
        }));
        LOG_D("Start TStartStreamingQueryActor " << startActorId);
    }

    void SynchronizeQuery() {
        Become(&TSynchronizeStreamingQueryTableActor::StateFunc);

        switch (State.GetStatus()) {
            case NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED:
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATED:
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPED: {
                if (!StateEnrichedFromSS) {
                    EnrichStateFromSS(/* syncAlterVersion */ !QuerySettings.Run);
                } if (QuerySettings.Run) {
                    StartQuery();
                } else {
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_RUNNING: {
                switch (QuerySettings.LastOperationCase) {
                    case EOperationCase::Create: {
                        CleanupQuery("interrupt running", NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
                        break;
                    }
                    case EOperationCase::Alter: {
                        if (!QuerySettings.Run) {
                            StopQuery("interrupt running for alter");
                        } else if (!StateEnrichedFromSS) {
                            EnrichStateFromSS(/* syncAlterVersion */ true);
                        } else {
                            Finish(Ydb::StatusIds::SUCCESS);
                        }
                        break;
                    }
                    case EOperationCase::AlterWithRestart: {
                        StopQuery("interrupt running for alter with restart");
                        break;
                    }
                }
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATING: {
                CleanupQuery("interrupt creating", NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_STARTING:
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPING: {
                StopQuery("interrupt stopping / starting");
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_DELETING: {
                RemoveQuery();
                break;
            }
        }
    }

private:
    const TExternalContext Context;
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    bool ExistsInSS = false;

    // Current settings from scheme shard
    ui64 AlterVersion = 0;
    TPathId QueryPathId;
    TStreamingQuerySettings QuerySettings;
    bool StateEnrichedFromSS = false;
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
        , Promise(std::move(promise))
        , StartedAt(TInstant::Now())
        , Context(context)
        , SchemeTx(schemeTx)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap. Fetch config");

        QueryServiceConfig = AppData()->QueryServiceConfig;
        Send(NConsole::MakeConfigsDispatcherID(TBase::SelfId().NodeId()), new NConsole::TEvConfigsDispatcher::TEvGetConfigRequest(
            NKikimrConsole::TConfigItem::QueryServiceConfigItem
        ), IEventHandle::FlagTrackDelivery);

        Become(&TDerived::StateFunc);
    }

    STRICT_STFUNC(StateFuncBase,
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(NConsole::TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
        hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvCheckAliveRequest, Handle);
    )

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        LOG_N("Failed to get console configs, got undelivered with reason " << ev->Get()->Reason);
        DescribeQuery();
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvGetConfigResponse::TPtr& ev) {
        const auto config = ev->Get()->Config;
        LOG_D("Got configs response, has config " << (config ? "yes" : "no"));

        if (config) {
            QueryServiceConfig = config->GetQueryServiceConfig();
        }

        DescribeQuery();
    }

    void Handle(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Describe streaming query")) {
            return;
        }

        const auto& info = ev->Get()->Info;
        LOG_D("Describe streaming query success, query exists: " << info.has_value() << ", alter version: " << (info ? info->Version : 0));

        OnQueryDescribed(info);
    }

    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Prepare streaming query before operation")) {
            return;
        }

        const auto& info = ev->Get()->Info;
        IsLockCreated = info.LockCreated;

        const auto& state = info.State;
        const bool queryExists = info.QueryExists;
        LOG_D("Lock streaming query " << ev->Sender << " success"
            << ", lock created: " << IsLockCreated
            << ", query exists: " << queryExists
            << ", status: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(state.GetStatus())
            << ", execution id: " << state.GetCurrentExecutionId()
            << ", alter version: " << state.GetAlterVersion());

        OnQueryLocked(state, queryExists);
    }

    void Handle(TEvPrivate::TEvUnlockStreamingQueryResult::TPtr& ev) {
        IsLockCreated = false;

        if (TBase::HandleResult(ev, TStringBuilder() << "Unlock streaming query (operation status: " << FinalStatus << ")")) {
            return;
        }

        TBase::Finish(FinalStatus);
    }

    void Handle(TEvPrivate::TEvCheckAliveRequest::TPtr& ev) {
        LOG_N("Got check alive request from " << ev->Sender);
        TBase::Send(ev->Sender, new TEvPrivate::TEvCheckAliveResponse());
    }

protected:
    virtual void OnQueryDescribed(const std::optional<TSchemeInfo>& info) = 0;

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

    bool BeforeFinish(Ydb::StatusIds::StatusCode status) override {
        if (!IsLockCreated) {
            return false;
        }

        FinalStatus = status;
        UnlockQuery();
        return true;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Promise.SetValue(TStreamingQueryConfig::TStatus::Success());
        } else {
            Promise.SetValue(TStreamingQueryConfig::TStatus::Fail(NYql::YqlStatusFromYdbStatus(status), std::move(TBase::Issues)));
        }
    }

private:
    void DescribeQuery() {
        if (QueryDescribed) {
            return;
        }

        QueryDescribed = true;
        const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(Context.GetDatabase(), TBase::QueryPath, Context.GetUserToken(), Access));
        LOG_D("Start TDescribeStreamingQuerySchemeActor " << describerId);
    }

private:
    const ui32 Access;
    bool QueryDescribed = false;
    Ydb::StatusIds::StatusCode FinalStatus;
    NThreading::TPromise<TStreamingQueryConfig::TStatus> Promise;

protected:
    const TInstant StartedAt;
    const TExternalContext Context;
    NKikimrSchemeOp::TModifyScheme SchemeTx;
    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    bool IsLockCreated = false;
};

class TCreateStreamingQueryActor : public TRequestHandlerBase<TCreateStreamingQueryActor> {
    using TBase = TRequestHandlerBase<TCreateStreamingQueryActor>;

    enum class EOnExists {
        Replace,
        Ignore,
        Fail,
    };

public:
    using TBase::LogPrefix;

    TCreateStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, NThreading::TPromise<TStreamingQueryConfig::TStatus> promise)
        : TBase(__func__, schemeTx, schemeTx.GetCreateStreamingQuery().GetName(), context, promise, NACLib::CreateTable)
        , ActionOnExists(GetActionOnExists(schemeTx))
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvSynchronizeStreamingQueryResult, Handle);
            hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvSynchronizeStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, TStringBuilder() << "Streaming query initialization failed" << (QueryCreated ? "" : " (recover previous query state, try to repeat request)"))) {
            return;
        }

        State = ev->Get()->State;
        const bool queryExistsInSS = ev->Get()->ExistsInSS;
        LOG_D("Synchronize query with scheme shard success"
            << ", query created: " << QueryCreated
            << ", status: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(State.GetStatus())
            << ", execution id: " << State.GetCurrentExecutionId()
            << ", alter version: " << State.GetAlterVersion()
            << ", query exists in SS: " << queryExistsInSS);

        if (!queryExistsInSS) {
            PreviousSchemeInfo = std::nullopt;
        }

        if (QueryCreated) {
            Finish(Ydb::StatusIds::SUCCESS);
        } else {
            CreateQuery();
        }
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute create scheme operation")) {
            return;
        }

        QueryCreated = true;

        const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(Context.GetDatabase(), TBase::QueryPath, NACLib::TUserToken(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}), 0));
        LOG_D("Start TDescribeStreamingQuerySchemeActor " << describerId << " after query creation");
    }

protected:
    void OnQueryDescribed(const std::optional<TSchemeInfo>& info) override {
        if (QueryCreated) {
            if (!info) {
                FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Query info not found after creation");
            } else {
                const auto& synchronizeActorId = Register(new TSynchronizeStreamingQueryTableActor(Context, QueryPath, {
                    .InitialState = State,
                    .SchemeInfo = *info,
                    .Config = QueryServiceConfig.GetStreamingQueries(),
                }));
                LOG_D("Start TSynchronizeStreamingQueryTableActor " << synchronizeActorId << " to finish creation");
            }
            return;
        }

        PreviousSchemeInfo = info;
        LockQuery(
            TStringBuilder() << "CREATE" << (ActionOnExists == EOnExists::Replace ? " OR REPLACE" : "") << " STREAMING QUERY" << (ActionOnExists == EOnExists::Ignore ? " IF NOT EXISTS" : ""),
            true,
            NKikimrKqp::TStreamingQueryState::STATUS_CREATING
        );
    }

    void OnQueryLocked(const NKikimrKqp::TStreamingQueryState& state, bool queryExists) override {
        State = state;
        QueryExistsInTable = queryExists;

        if (!QueryExistsInTable && !PreviousSchemeInfo) {
            CreateQuery();
            return;
        }

        const auto& synchronizeActorId = Register(new TSynchronizeStreamingQueryTableActor(Context, QueryPath, {
            .InitialState = State,
            .SchemeInfo = PreviousSchemeInfo,
            .Config = QueryServiceConfig.GetStreamingQueries(),
        }));
        LOG_D("Start TSynchronizeStreamingQueryTableActor " << synchronizeActorId << " to prepare before creation");
    }

private:
    static EOnExists GetActionOnExists(const NKikimrSchemeOp::TModifyScheme& schemeTx) {
        if (schemeTx.GetCreateStreamingQuery().GetReplaceIfExists()) {
            return EOnExists::Replace;
        }

        return schemeTx.GetFailedOnAlreadyExists() ? EOnExists::Fail : EOnExists::Ignore;
    }

    void CreateQuery() {
        if (PreviousSchemeInfo) {
            switch (ActionOnExists) {
                case EOnExists::Replace:
                    // Continue query creation after cleaning up previous state
                    break;
                case EOnExists::Ignore:
                    Finish(Ydb::StatusIds::SUCCESS);
                    return;
                case EOnExists::Fail:
                    FatalError(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder() << "Streaming query " << QueryPath << " already exists");
                    return;
            }
        }

        if (const auto& status = ValidateProperties(); status.GetStatus() != Ydb::StatusIds::SUCCESS) {
            FatalError(status.GetStatus(), AddRootIssue("Invalid properties for creation new streaming query", status.GetErrorDescription()));
            return;
        }

        const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, SchemeTx, Context.GetUserToken()));
        LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
    }

    TStatus ValidateProperties() {
        TPropertyValidator validator(*SchemeTx.MutableCreateStreamingQuery()->MutableProperties());

        if (auto status = validator.RequiredProperty(TStreamingQueryConfig::TProperties::QueryText, [](const TString& value) {
            if (value.empty()) {
                return TStatus::Fail("query_text should not be empty");
            }
            return TStatus::Success();
        }); status.IsFail()) {
            return status;
        }

        if (auto status = validator.OptionalProperty(TStreamingQueryConfig::TProperties::Run, "true", [](const TString& value) {
            if (!IsIn({"true", "false"}, value)) {
                return TStatus::Fail("run property must be 'true' or 'false");
            }
            return TStatus::Success();
        }); status.IsFail()) {
            return status;
        }

        if (auto status = validator.OptionalProperty(TStreamingQueryConfig::TProperties::ResourcePool, NResourcePool::DEFAULT_POOL_ID); status.IsFail()) {
            return status;
        }

        if (auto status = validator.AddProperty(TStreamingQueryConfig::TProperties::LastOperationCase, ToString(static_cast<ui64>(EOperationCase::Create))); status.IsFail()) {
            return status;
        }

        return validator.Finish();
    }

private:
    const EOnExists ActionOnExists = EOnExists::Fail;
    std::optional<TSchemeInfo> PreviousSchemeInfo;
    NKikimrKqp::TStreamingQueryState State;
    bool QueryExistsInTable = false;
    bool QueryCreated = false;
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
            hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Cleanup streaming query")) {
            return;
        }

        QueryExistsInTable = false;

        if (!CleanupQuery()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute drop scheme operation")) {
            return;
        }

        QueryExistsInSS = false;

        if (!CleanupQuery()) {
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query status")) {
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

protected:
    void OnQueryDescribed(const std::optional<TSchemeInfo>& info) override {
        QueryExistsInSS = info.has_value();
        LockQuery(
            TStringBuilder() << "DROP STREAMING QUERY" << (SuccessOnNotExist ? " IF EXISTS" : ""),
            QueryExistsInSS,
            NKikimrKqp::TStreamingQueryState::STATUS_DELETING
        );
    }

    void OnQueryLocked(const NKikimrKqp::TStreamingQueryState& state, bool queryExists) override {
        State = state;
        QueryExistsInTable = queryExists;

        if (!QueryExistsInTable && !QueryExistsInSS) {
            if (SuccessOnNotExist) {
                Finish(Ydb::StatusIds::SUCCESS);
            } else {
                FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            }
            return;
        }

        CleanupQuery();
    }

private:
    bool CleanupQuery() {
        if (QueryExistsInTable) {
            // Clear query state
            const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateActor(Context, QueryPath, {
                .InitialState = State,
            }));
            LOG_D("Start TCleanupStreamingQueryStateActor " << cleanupActorId);
            return true;
        }

        if (QueryExistsInSS) {
            // Remove query from SS
            const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, SchemeTx, Context.GetUserToken()));
            LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
            return true;
        }

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED) {
            // Clear query status
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
            State.SetAlterVersion(0);

            const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
            LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId);
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

TStreamingQueryConfig::TAsyncStatus DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context) {
    const auto promise = NThreading::NewPromise<TStreamingQueryConfig::TStatus>();
    context.GetActorSystem()->Register(new TCreateStreamingQueryActor(schemeTx, context, promise));

    return promise.GetFuture();
}

TStreamingQueryConfig::TAsyncStatus DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context) {
    const auto promise = NThreading::NewPromise<TStreamingQueryConfig::TStatus>();
    context.GetActorSystem()->Register(new TDropStreamingQueryActor(schemeTx, context, promise));

    return promise.GetFuture();
}

}  // namespace NKikimr::NKqp
