#include "queries.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
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
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

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
#define CHECK_STATUS(action) if (const auto& status = action; status.IsFail()) return status
#define CHECK_STATUS_RET(name, action) auto name = action; if (name.IsFail()) return name

template <typename TValue>
using TValueStatus = TConclusionImpl<TStatus, TValue>;

//// Events

TString TruncateString(const TString& str, ui64 maxSize = 300) {
    return str.size() > maxSize
        ? TStringBuilder() << str.substr(0, maxSize / 2) << " ... (TRUNCATED) ... " << str.substr(str.size() - maxSize / 2)
        : str;
}

// Streaming query info stored in schemeshard
struct TSchemeInfo {
    NKikimrSchemeOp::TStreamingQueryProperties Properties;
    ui64 Version = 0;
    TPathId PathId;
    TIntrusivePtr<TSecurityObject> SecurityObject;

    bool IsChanged(const NKikimrKqp::TStreamingQueryState& state) const {
        const auto& schemeInfo = state.GetSchemeInfo();
        return schemeInfo.GetAlterVersion() != Version
            || schemeInfo.GetOwnerSchemeshardId() != PathId.OwnerId
            || schemeInfo.GetLocalPathId() != PathId.LocalPathId;
    }

    void Sync(NKikimrKqp::TStreamingQueryState& state) const {
        auto& schemeInfo = *state.MutableSchemeInfo();
        schemeInfo.SetAlterVersion(Version);
        schemeInfo.SetOwnerSchemeshardId(PathId.OwnerId);
        schemeInfo.SetLocalPathId(PathId.LocalPathId);
    }

    TString DebugString() const {
        return TStringBuilder()
            << "{Version: " << Version
            << ", PathId: " << PathId.ToString()
            << ", Properties: " << TruncateString(EscapeC(Properties.ShortDebugString()))
            << "}";
    }
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
        EvSyncStreamingQueryResult,

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

    template <typename TInfo, typename TEv, ui32 EventType>
    struct TEvResulWithInfoBase : public TEvResultBase<TEv, EventType> {
        using TBase = TEvResultBase<TEv, EventType>;

        TEvResulWithInfoBase(Ydb::StatusIds::StatusCode status, TInfo info, NYql::TIssues issues = {})
            : TBase(status, std::move(issues))
            , Info(std::move(info))
        {}

        const TInfo Info;
    };

    struct TEvDescribeStreamingQueryResult : public TEvResulWithInfoBase<std::optional<TSchemeInfo>, TEvDescribeStreamingQueryResult, EvDescribeStreamingQueryResult> {
        using TEvResulWithInfoBase::TEvResulWithInfoBase;
    };

    struct TEvExecuteSchemeTransactionResult : public TEvResultBase<TEvExecuteSchemeTransactionResult, EvExecuteSchemeTransactionResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvUpdateStreamingQueryResult : public TEvResultBase<TEvUpdateStreamingQueryResult, EvUpdateStreamingQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

    struct TEvCleanupStreamingQueryResult : public TEvResulWithInfoBase<NKikimrKqp::TStreamingQueryState, TEvCleanupStreamingQueryResult, EvCleanupStreamingQueryResult> {
        using TEvResulWithInfoBase::TEvResulWithInfoBase;
    };

    struct TEvStartStreamingQueryResult : public TEvResulWithInfoBase<NKikimrKqp::TStreamingQueryState, TEvStartStreamingQueryResult, EvStartStreamingQueryResult> {
        using TEvResulWithInfoBase::TEvResulWithInfoBase;
    };

    struct TEvSyncStreamingQueryResult : public TEvResultBase<TEvSyncStreamingQueryResult, EvSyncStreamingQueryResult> {
        TEvSyncStreamingQueryResult(Ydb::StatusIds::StatusCode status, NKikimrKqp::TStreamingQueryState state, bool existsInSS, NYql::TIssues issues = {})
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

//// Common

TString LogQueryState(const NKikimrKqp::TStreamingQueryState& state) {
    return TStringBuilder()
        << "{Status: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(state.GetStatus())
        << ", CurrentExecutionId: " << state.GetCurrentExecutionId()
        << ", PreviousExecutionIds: " << TruncateString(JoinSeq(", ", state.GetPreviousExecutionIds()))
        << ", OperationName: " << state.GetOperationName()
        << ", OperationActorId: " << state.GetOperationActorId()
        << ", OperationStartedAt: " << NProtoInterop::CastFromProto(state.GetOperationStartedAt())
        << ", SchemeInfo: " << state.GetSchemeInfo().ShortDebugString()
        << ", QueryText: " << TruncateString(EscapeC(state.GetQueryText()))
        << ", Run: " << state.GetRun()
        << ", ResourcePool: " << state.GetResourcePool()
        << "}";
}

// Used for properties validation before saving into schemeshard
class TPropertyValidator {
    using TProperties = google::protobuf::Map<TString, TString>;

public:
    using TValidator = std::function<TStatus(const TString& name, const TString& value)>;

    explicit TPropertyValidator(NKikimrSchemeOp::TStreamingQueryProperties& src)
        : Src(*src.MutableProperties())
    {}

    TValueStatus<TString> ExtractRequired(const TString& name, TValidator validator = nullptr) {
        if (const auto it = Src.find(name); it != Src.end()) {
            const auto value = it->second;
            Src.erase(it);
            return Validate(name, value, validator);
        }
        return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Missing required property: " << name);
    }

    TValueStatus<std::optional<TString>> ExtractOptional(const TString& name, TValidator validator = nullptr) {
        if (const auto it = Src.find(name); it != Src.end()) {
            const auto value = it->second;
            Src.erase(it);
            CHECK_STATUS_RET(result, Validate(name, value, validator));
            return std::optional(result.GetResult());
        }
        return std::nullopt;
    }

    TValueStatus<TString> ExtractDefault(const TString& name, const TString& defaultValue, TValidator validator = nullptr) {
        CHECK_STATUS_RET(value, ExtractOptional(name, validator));
        return value.DetachResult().value_or(defaultValue);
    }

    [[nodiscard]] TStatus Save(const TString& name, const TValueStatus<TString>& value, TValidator validator = nullptr) {
        CHECK_STATUS(value);
        CHECK_STATUS(Validate(name, value.GetResult(), validator));

        if (!Dst.emplace(name, value.GetResult()).second) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Duplicate property: " << name);
        }

        return TStatus::Success();
    }

    [[nodiscard]] TStatus SaveRequired(const TString& name, TValidator validator = nullptr) {
        return Save(name, ExtractRequired(name, validator));
    }

    [[nodiscard]] TStatus SaveDefault(const TString& name, const TString& defaultValue, TValidator validator = nullptr) {
        return Save(name, ExtractDefault(name, defaultValue, validator));
    }

    [[nodiscard]] TStatus Finish() {
        if (!Src.empty()) {
            auto error = TStringBuilder() << "Got unexpected properties: ";
            for (auto it = Src.begin(); it != Src.end();) {
                error << to_upper(it->first);
                if (++it != Src.end()) {
                    error << ", ";
                }
            }

            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, std::move(error));
        }

        Src = std::move(Dst);
        return TStatus::Success();
    }

    static TStatus ValidateNotEmpty(const TString& name, const TString& value) {
        if (value.empty()) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property must not be empty");
        }
        return TStatus::Success();
    }

    static TStatus ValidateBool(const TString& name, const TString& value) {
        if (!IsIn({"true", "false"}, value)) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property must be 'true' or 'false', but got: " << value);
        }
        return TStatus::Success();
    }

private:
    static TValueStatus<TString> Validate(const TString& name, const TString& value, TValidator validator) {
        if (validator) {
            if (const auto& status = validator(name, value); !status.IsSuccess()) {
                return status;
            }
        }
        return value;
    }

private:
    TProperties& Src;
    TProperties Dst;
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
    // Do action before finish, return true if there is required action to perform
    virtual bool BeforeFinish(Ydb::StatusIds::StatusCode status) {
        Y_UNUSED(status);
        return false;
    }

    virtual void OnFinish(Ydb::StatusIds::StatusCode status) = 0;

protected:
    template <typename TEvent>
    void SendToKqpProxy(std::unique_ptr<TEvent> event, ui64 cookie = 0) const {
        TBase::Send(MakeKqpProxyID(TBase::SelfId().NodeId()), std::move(event), 0, cookie);
    }

    template <typename TEvPtr>
    bool HandleResult(TEvPtr& ev, const TString& message) {
        const auto status = ev->Get()->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D(message << " " << ev->Sender << " success");
            return false;
        }

        const auto& issues = ev->Get()->Issues;
        LOG_W(message << " " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());

        FatalError(status, AddRootIssue(TStringBuilder() << message << " failed", issues));
        return true;
    }

    void Finish(Ydb::StatusIds::StatusCode status) {
        if (BeforeFinish(status)) {
            LOG_D("Do action before finish with status " << status);
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
    void Registered(TActorSystem* sys, const TActorId& owner) final {
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

    TSchemeActorBase(const TString& operationName, const TString& database, const TString& queryPath, const std::optional<NACLib::TUserToken>& userToken)
        : TBase(operationName, queryPath)
        , Database(database)
        , UserToken(userToken)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap. Database: " << Database);
        StartRequest();

        TBase::Become(&TDerived::StateFunc);
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
            RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [](bool longDelay) {
                    return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(500),
                TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(),
                TDuration::Seconds(10)
            )->CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay(longDelay)) {
            LOG_W("Schedule retry for error: " << issues.ToOneLineString() << " in " << *delay);
            TBase::Issues.AddIssues(std::move(issues));
            TBase::Schedule(*delay, new TEvents::TEvWakeup());
            return true;
        }

        return false;
    }

    bool ScheduleRetry(const TString& message, bool longDelay = false) {
        return ScheduleRetry({NYql::TIssue(message)}, longDelay);
    }

protected:
    const TString Database;
    const std::optional<NACLib::TUserToken> UserToken;

private:
    TRetryPolicy::IRetryState::TPtr RetryState;
};

class TDescribeStreamingQuerySchemeActor final : public TSchemeActorBase<TDescribeStreamingQuerySchemeActor> {
    using TBase = TSchemeActorBase<TDescribeStreamingQuerySchemeActor>;
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

public:
    using TBase::LogPrefix;

    TDescribeStreamingQuerySchemeActor(const TString& database, const TString& queryPath, const std::optional<NACLib::TUserToken>& userToken)
        : TBase(__func__, database, queryPath, userToken)
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
                    FatalError(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Path " << QueryPath << " exists, but it is not a streaming query: " << result.Kind);
                } else if (!result.Self || !result.StreamingQueryInfo) {
                    FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response for ok status");
                } else {
                    const auto& pathInfo = result.Self->Info;
                    Info = TSchemeInfo{
                        .Properties = result.StreamingQueryInfo->Description.GetProperties(),
                        .Version = pathInfo.GetVersion().GetStreamingQueryVersion(),
                        .PathId = TPathId(pathInfo.GetSchemeshardId(), pathInfo.GetPathId()),
                        .SecurityObject = result.SecurityObject,
                    };
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                return;
            }
        }
    }

protected:
    void StartRequest() final {
        LOG_D("Describe streaming query in database: " << Database);

        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;

        if (UserToken && UserToken->GetSanitizedToken()) {
            request->UserToken = MakeIntrusiveConst<NACLib::TUserToken>(*UserToken);
        }

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = SplitPath(QueryPath);
        entry.SyncVersion = true;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvDescribeStreamingQueryResult(status, std::move(Info), std::move(Issues)));
    }

private:
    std::optional<TSchemeInfo> Info;
};

class TExecuteTransactionSchemeActor final : public TSchemeActorBase<TExecuteTransactionSchemeActor> {
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
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const auto ssStatus = response.GetSchemeShardStatus();
        const auto status = ev->Get()->Status();
        const auto txId = response.GetTxId();

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

                Become(&TExecuteTransactionSchemeActor::StateFuncWaitCompletion);
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

    STFUNC(StateFuncWaitCompletion) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, HandleWaitCompletion);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleWaitCompletion);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleWaitCompletion);
            IgnoreFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            default:
                StateFuncBase(ev);
        }
    }

    void HandleWaitCompletion(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (const auto status = ev->Get()->Status; status != NKikimrProto::OK) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Pipe to tablet is not connected: " << NKikimrProto::EReplyStatus_Name(status) << " " << ev->Get()->ToString());
            return;
        }

        LOG_T("Tablet pipe successfully connected");
    }

    void HandleWaitCompletion(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Pipe to tablet unexpectedly destroyed " << ev->Get()->ToString());
    }

    void HandleWaitCompletion(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        LOG_D("Scheme transaction " << ev->Get()->Record.GetTxId() << " successfully finished");
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void PassAway() final {
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }

        TBase::PassAway();
    }

protected:
    void StartRequest() final {
        LOG_D("Start scheme transaction " << NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType()) << " in database: " << Database);

        auto event = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        *event->Record.MutableTransaction()->MutableModifyScheme() = SchemeTx;
        event->Record.SetDatabaseName(Database);

        if (UserToken) {
            event->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        Send(MakeTxProxyID(), std::move(event));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
        }
        Send(Owner, new TEvPrivate::TEvExecuteSchemeTransactionResult(status, std::move(Issues)));
    }

private:
    void ScheduleRetry(const NKikimrTxUserProxy::TEvProposeTransactionStatus& response, const TString& message, bool longDelay = false) {
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
    TActorId SchemePipeActorId;
};

//// Table actions

class TQueryBase : public NKikimr::TQueryBase {
public:
    TQueryBase(const TString& operationName, const TString& databaseId, const TString& queryPath)
        : NKikimr::TQueryBase(NKikimrServices::KQP_PROXY)
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
                DECLARE $state AS Json;

                UPSERT INTO `{table}` (
                    database_id, query_path, state
                ) VALUES (
                    $database_id, $query_path, $state
                );
            )",
            "table"_a = TablePath
        );

        NJson::TJsonValue stateJson;
        NProtobufJson::Proto2Json(state, stateJson);
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
                .Json(stateWriter.Str())
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

        const std::optional<TString>& stateJsonString = result.ColumnParser(TStreamingQueryConfig::TColumns::State).GetOptionalJson();
        if (!stateJsonString) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query state not found");
        }

        NJson::TJsonValue stateJson;
        if (!NJson::ReadJsonTree(*stateJsonString, &stateJson)) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query state is corrupted");
        }

        NKikimrKqp::TStreamingQueryState state;
        try {
            NProtobufJson::Json2Proto(stateJson, state);
        } catch (const std::exception& e) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Failed to parse streaming query state: " << e.what());
        }

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

// Lock / unlock query row in table .metadata/streaming/queries to prevent concurrent modifications.
// Updates OperationName, OperationActorId and OperationStartedAt according to current operation.
// If OperationActorId already filled, actor will be checked.

class TLockStreamingQueryRequestActor final : public TQueryBase {
    static constexpr TDuration LOCK_TIMEOUT = TDuration::Seconds(10);

public:
    struct TSettings {
        TString OperationName;
        TInstant OperationStartedAt;
        TActorId OperationOwner;
        std::optional<TActorId> PreviousOperationOwner;
        bool CreateLockIfNotExists = false;
        NKikimrKqp::TStreamingQueryState::EStatus DefaultQueryStatus = NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED;
    };

    using TRetry = TQueryRetryActor<TLockStreamingQueryRequestActor, TEvPrivate::TEvLockStreamingQueryResult, TString, TString, TSettings>;

    TLockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TSettings& settings)
        : TQueryBase(__func__, databaseId, queryPath)
        , Settings(settings)
    {}

    void OnRunQuery() final {
        LOG_D("Locking streaming query"
            << ", OperationName: " << Settings.OperationName
            << ", OperationStartedAt: " << Settings.OperationStartedAt
            << ", OperationOwner: " << Settings.OperationOwner
            << ", PreviousOperationOwner: " << Settings.PreviousOperationOwner.value_or(TActorId())
            << ", CreateLockIfNotExists: " << Settings.CreateLockIfNotExists
            << ", DefaultQueryStatus: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(Settings.DefaultQueryStatus));

        SetQueryResultHandler(&TLockStreamingQueryRequestActor::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        auto result = ParseQueryInfo();
        if (result.GetStatus() == Ydb::StatusIds::NOT_FOUND) {
            LOG_D("Streaming query not found, CreateLockIfNotExists: " << Settings.CreateLockIfNotExists);

            if (Settings.CreateLockIfNotExists) {
                State.SetStatus(Settings.DefaultQueryStatus);
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

        if (!ScriptExecutionRunnerActorIdFromString(State.GetOperationActorId(), PreviousOperationOwner)) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query operation owner actor id is corrupted");
            return;
        }

        PreviousOperationStartedAt = NProtoInterop::CastFromProto(State.GetOperationStartedAt());
        PreviousOperationName = State.GetOperationName();
        LOG_D("Streaming query under lock from " << PreviousOperationOwner << " started at " << PreviousOperationStartedAt << ", with operation name " << PreviousOperationName);

        if (!Settings.PreviousOperationOwner) {
            if (Settings.OperationStartedAt - PreviousOperationStartedAt <= LOCK_TIMEOUT) {
                FinishUnderOperation();
            } else {
                LOG_I("Streaming query lock " << PreviousOperationOwner << " expired, start check");
                CheckLockOwner = true;
                Finish();
            }

            return;
        }

        if (PreviousOperationOwner != *Settings.PreviousOperationOwner) {
            LOG_I("Streaming query was locked by " << PreviousOperationOwner << " during lock check");
            FinishUnderOperation();
            return;
        }

        LOG_I("Remove expired lock from " << PreviousOperationOwner);
        LockQuery();
    }

    void LockQuery() {
        State.SetOperationName(Settings.OperationName);
        *State.MutableOperationStartedAt() = NProtoInterop::CastToProto(Settings.OperationStartedAt);
        State.SetOperationActorId(ScriptExecutionRunnerActorIdString(Settings.OperationOwner));

        SetQueryResultHandler(&TLockStreamingQueryRequestActor::OnQueryResult, "Lock query");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() final {
        LockCreated = true;
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, {
            .State = std::move(State),
            .PreviousOwner = PreviousOperationOwner,
            .PreviousOperationStartedAt = PreviousOperationStartedAt,
            .PreviousOperationName = std::move(PreviousOperationName),
            .QueryExists = QueryExists,
            .LockCreated = LockCreated,
            .CheckLockOwner = CheckLockOwner,
        }, std::move(issues)));
    }

private:
    void FinishUnderOperation() {
        Finish(Ydb::StatusIds::ABORTED, TStringBuilder() << "Streaming query " << QueryPath << " already under operation " << PreviousOperationName << " started at " << PreviousOperationStartedAt << ", try repeat request later");
    }

private:
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    TActorId PreviousOperationOwner;
    TInstant PreviousOperationStartedAt;
    TString PreviousOperationName;
    bool QueryExists = false;
    bool LockCreated = false;
    bool CheckLockOwner = false;
};

class TLockStreamingQueryTableActor final : public TActionActorBase<TLockStreamingQueryTableActor> {
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
        TString OperationName;
        TInstant OperationStartedAt;
        TActorId OperationOwner;
        bool CreateLockIfNotExists = false;
        NKikimrKqp::TStreamingQueryState::EStatus DefaultQueryStatus = NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED;
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
            << ", State: " << LogQueryState(Info.State)
            << ", PreviousOwner: " << Info.PreviousOwner
            << ", PreviousOperationStartedAt: " << Info.PreviousOperationStartedAt
            << ", PreviousOperationName: " << Info.PreviousOperationName
            << ", QueryExists: " << Info.QueryExists
            << ", LockCreated: " << Info.LockCreated
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
                LOG_W("Deliver streaming query owner " << Info.PreviousOwner << " check alive request timeouted, retry check alive");
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

    void PassAway() final {
        if (SubscribedOnSession) {
            Send(TActivationContext::InterconnectProxy(*SubscribedOnSession), new TEvents::TEvUnsubscribe());
        }

        TBase::PassAway();
    }

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, Info, std::move(Issues)));
    }

private:
    void StartLockStreamingQueryRequestActor(std::optional<TActorId> previousOperationOwner = std::nullopt) {
        if (std::exchange(WaitLock, true)) {
            return;
        }

        const auto& lockActorId = Register(new TLockStreamingQueryRequestActor::TRetry(SelfId(), DatabaseId, QueryPath, {
            .OperationName = Settings.OperationName,
            .OperationStartedAt = Settings.OperationStartedAt,
            .OperationOwner = Settings.OperationOwner,
            .PreviousOperationOwner = previousOperationOwner,
            .CreateLockIfNotExists = Settings.CreateLockIfNotExists,
            .DefaultQueryStatus = Settings.DefaultQueryStatus,
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

class TUnlockStreamingQueryRequestActor final : public TQueryBase {
public:
    using TRetry = TQueryRetryActor<TUnlockStreamingQueryRequestActor, TEvPrivate::TEvUnlockStreamingQueryResult, TString, TString, TActorId>;

    TUnlockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TActorId& operationOwner)
        : TQueryBase(__func__, databaseId, queryPath)
        , OperationOwner(operationOwner)
    {}

    void OnRunQuery() final {
        LOG_D("Unlocking streaming query, OperationOwner: " << OperationOwner);
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

        if (State.HasOperationActorId()) {
            TActorId currentOperationOwner;
            if (!ScriptExecutionRunnerActorIdFromString(State.GetOperationActorId(), currentOperationOwner)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query operation owner actor id is corrupted");
                return;
            }

            if (OperationOwner != currentOperationOwner) {
                LOG_E("Streaming query was locked by " << currentOperationOwner << " during operation (expected owner: " << OperationOwner << ")");
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query was changed during operation");
                return;
            }
        } else {
            LOG_E("Streaming query lock was lost");
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

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvUnlockStreamingQueryResult(status, std::move(issues)));
    }

private:
    const TActorId OperationOwner;
    NKikimrKqp::TStreamingQueryState State;
};

// Update column "state" of .metadata/streaming/queries table if OperationActorId is not changed

class TUpdateStreamingQueryStateRequestActor final : public TQueryBase {
public:
    using TRetry = TQueryRetryActor<TUpdateStreamingQueryStateRequestActor, TEvPrivate::TEvUpdateStreamingQueryResult, TString, TString, NKikimrKqp::TStreamingQueryState>;

    TUpdateStreamingQueryStateRequestActor(const TString& databaseId, const TString& queryPath, const NKikimrKqp::TStreamingQueryState& state)
        : TQueryBase(__func__, databaseId, queryPath)
        , State(state)
    {}

    void OnRunQuery() final {
        LOG_D("Updating streaming query state to " << LogQueryState(State));
        SetQueryResultHandler(&TUpdateStreamingQueryStateRequestActor::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        const auto result = ParseQueryInfo();
        if (result.IsFail()) {
            FinishWithStatus(result);
            return;
        }

        const auto previousOwner = State.GetOperationActorId();
        const auto currentOwner = result.GetResult().GetOperationActorId();
        if (currentOwner != previousOwner) {
            LOG_E("Streaming query was locked by " << currentOwner << " during operation (expected owner: " << previousOwner << ")");
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query was changed during operation");
            return;
        }

        UpdateQuery();
    }

    void UpdateQuery() {
        SetQueryResultHandler(&TUpdateStreamingQueryStateRequestActor::OnQueryResult, "Update query info");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvUpdateStreamingQueryResult(status, std::move(issues)));
    }

private:
    const NKikimrKqp::TStreamingQueryState State;
};

// Cancel current query execution and forget all previous query executions

class TCleanupStreamingQueryStateTableActor final : public TActionActorBase<TCleanupStreamingQueryStateTableActor> {
    using TBase = TActionActorBase<TCleanupStreamingQueryStateTableActor>;

public:
    using TBase::LogPrefix;

    TCleanupStreamingQueryStateTableActor(const TExternalContext& context, const TString& queryPath, const NKikimrKqp::TStreamingQueryState& state)
        : TBase(__func__, queryPath)
        , Context(context)
        , State(state)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap");
        Become(&TCleanupStreamingQueryStateTableActor::StateFunc);
        ClearStreamingQueryExecutions();
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

        StartUpdateState("clear current execution id");
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
            StartUpdateState("clear previous execution ids");
        }
    }

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvCleanupStreamingQueryResult(status, std::move(State), std::move(Issues)));
    }

private:
    void StartUpdateState(const TString& info) const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId << " (" << info << ")");
    }

    void ClearStreamingQueryExecutions() {
        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_DELETING);
            StartUpdateState("move query to deleting");
            return;
        }

        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            LOG_D("Cancel streaming query execution " << executionId);
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA));
            return;
        }

        if (State.PreviousExecutionIdsSize() > 0) {
            LOG_D("Cleanup #" << State.PreviousExecutionIdsSize() << " previous executions");

            for (const auto& executionId : State.GetPreviousExecutionIds()) {
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA), OperationsToForget++);
                LOG_D("Forget streaming query execution #" << OperationsToForget << " " << executionId);
            }
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

private:
    const TExternalContext Context;
    NKikimrKqp::TStreamingQueryState State;
    ui64 OperationsToForget = 0;
};

// Start new query execution and forget previous executions

class TStartStreamingQueryTableActor final : public TActionActorBase<TStartStreamingQueryTableActor> {
    using TBase = TActionActorBase<TStartStreamingQueryTableActor>;
    using TRetryPolicy = IRetryPolicy<>;

    inline static constexpr ui64 MAX_QUERY_EXECUTIONS = 3;
    inline static constexpr TDuration START_REQUEST_TIMEOUT = TDuration::Seconds(30);

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        TPathId QueryPathId;
        ui64 QueryTextRevision = 0;
        std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> StreamingDisposition;
    };

    TStartStreamingQueryTableActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap. SS text revision: " << Settings.QueryTextRevision << ", last query execution revision: " << State.GetQueryTextRevision() << ", start new query: " << State.GetQueryText());

        if (State.HasCurrentExecutionId()) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Can not start query, already started: " << State.GetCurrentExecutionId());
            return;
        }

        PrepareToStart();
    }

    STRICT_STFUNC(StateFuncPrepare,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandlePrepare);
        hFunc(TEvGetScriptPhysicalGraphResponse, HandlePrepare);
        hFunc(TEvForgetScriptExecutionOperationResponse, HandlePrepare);
    )

    void HandlePrepare(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state (prepare to start)")) {
            return;
        }

        PrepareToStart();
    }

    void HandlePrepare(TEvGetScriptPhysicalGraphResponse::TPtr& ev) {
        const auto status = ev->Get()->Status;
        if (status != Ydb::StatusIds::NOT_FOUND && HandleResult(ev, "Load previous query execution state")) {
            return;
        }

        if (Settings.QueryTextRevision == State.GetQueryTextRevision()) {
            PreviousPhysicalGraph = std::move(ev->Get()->PhysicalGraph);
        }

        PreviousGeneration = ev->Get()->Generation;
        LOG_D("Load previous query execution state " << ev->Sender << " finished " << status << ", generation: " << PreviousGeneration << ", has saved state: " << PreviousPhysicalGraph.has_value());

        PrepareToStart();
    }

    void HandlePrepare(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        const ui64 toCleanup = State.PreviousExecutionIdsSize() - MAX_QUERY_EXECUTIONS;
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

    STRICT_STFUNC(StateFuncStartQuery,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleStartQuery);
        hFunc(TEvKqp::TEvScriptResponse, HandleStartQuery);
        hFunc(TEvents::TEvWakeup, HandleStartQuery);
        hFunc(TEvGetScriptExecutionOperationResponse, HandleStartQuery);
    )

    void HandleStartQuery(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state (start query)")) {
            return;
        }

        StartQuery();
    }

    void HandleStartQuery(TEvKqp::TEvScriptResponse::TPtr& ev) {
        if (HandleResult(ev, "Create script execution operation")) {
            return;
        }

        RequestStarted = true;
        LOG_D("Script execution created: " << ev->Get()->ExecutionId << ", wait for saving query state");

        GetScriptExecutionOperation();
    }

    void HandleStartQuery(TEvents::TEvWakeup::TPtr&) {
        const auto& executionId = State.GetCurrentExecutionId();
        LOG_D("Get streaming query execution " << executionId);
        SendToKqpProxy(std::make_unique<TEvGetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA));
    }

    void HandleStartQuery(TEvGetScriptExecutionOperationResponse::TPtr& ev) {
        const auto& info = *ev->Get();
        LOG_D("Got script execution info, StateSaved: " << info.StateSaved << ", Ready: " << info.Ready);

        if (HandleResult(ev, "Query compilation / planing")) {
            return;
        }

        if (info.StateSaved) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        if (!info.Ready) {
            GetScriptExecutionOperation();
            return;
        }

        FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Query execution unexpectedly finished before saving state");
    }

    STRICT_STFUNC(StateFuncFinalize,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleFinalize);
    )

    void HandleFinalize(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, TStringBuilder() << "Update streaming query state (finish query starting), creation status: " << FinalStatus)) {
            return;
        }

        Finish(FinalStatus);
    }

protected:
    bool BeforeFinish(Ydb::StatusIds::StatusCode status) final {
        Become(&TStartStreamingQueryTableActor::StateFuncFinalize);

        if (status == Ydb::StatusIds::SUCCESS) {
            if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_RUNNING || Settings.QueryTextRevision != State.GetQueryTextRevision()) {
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_RUNNING);
                State.SetQueryTextRevision(Settings.QueryTextRevision);
                UpdateQueryState("move query to running");

                FinalStatus = status;
                return true;
            }
        } else if (State.GetCurrentExecutionId()) {
            if (RequestStarted) {
                State.AddPreviousExecutionIds(State.GetCurrentExecutionId());
            }

            State.ClearCurrentExecutionId();
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
            UpdateQueryState("move query to stopped");

            FinalStatus = status;
            return true;
        }

        return false;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvStartStreamingQueryResult(status, State, std::move(Issues)));
    }

private:
    void UpdateQueryState(const TString& info) const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId << " (" << info << ")");
    }

    void PrepareToStart() {
        Become(&TStartStreamingQueryTableActor::StateFuncPrepare);

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_STARTING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STARTING);
            UpdateQueryState("move query to starting");
            return;
        }

        if (!State.GetPreviousExecutionIds().empty() && !StateLoaded) {
            StateLoaded = true;
            const auto& executionId = *State.GetPreviousExecutionIds().rbegin();
            SendToKqpProxy(std::make_unique<TEvGetScriptExecutionPhysicalGraph>(Context.GetDatabase(), executionId));
            LOG_D("Load previous query state from execution: " << executionId);
            return;
        }

        if (State.PreviousExecutionIdsSize() > MAX_QUERY_EXECUTIONS) {
            const auto toCleanup = State.PreviousExecutionIdsSize() - MAX_QUERY_EXECUTIONS;
            LOG_D("Cleanup #" << toCleanup << " previous executions (max executions: " << MAX_QUERY_EXECUTIONS << ")");

            for (ui64 i = 0; i < toCleanup; ++i) {
                const auto& executionId = State.GetPreviousExecutionIds(i);
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA), OperationsToForget++);
                LOG_D("Forget streaming query execution #" << OperationsToForget << " " << executionId);
            }
            return;
        }

        // Execution id for streaming queries:
        // <GUID part>-<GUID part>-<GUID part>-<SS id>-<Path id in SS>
        // Checkpoint id for streaming queries:
        // <Execution id>-<Query path>

        const auto& pathId = Settings.QueryPathId;
        State.SetCurrentExecutionId(TStringBuilder() << CreateGuidAsString() << '-' << pathId.OwnerId << '-' << pathId.LocalPathId);

        if (!State.GetCheckpointId()) {
            State.SetCheckpointId(TStringBuilder() << State.GetCurrentExecutionId() << '-' << QueryPath);
        }

        UpdateQueryState(TStringBuilder() << "allocate execution id: " << State.GetCurrentExecutionId() << ", checkpoint id: " << State.GetCheckpointId());
        Become(&TStartStreamingQueryTableActor::StateFuncStartQuery);
    }

    void StartQuery() {
        auto ev = std::make_unique<TEvKqp::TEvScriptRequest>();
        ev->SaveQueryPhysicalGraph = true;
        ev->QueryPhysicalGraph = std::move(PreviousPhysicalGraph);
        ev->RetryMapping = CreateDefaultRetryMapping();
        ev->ExecutionId = State.GetCurrentExecutionId();
        ev->DisableDefaultTimeout = true;
        ev->ForgetAfter = TDuration::Max();
        ev->Generation = PreviousGeneration + 1;
        ev->CheckpointId = State.GetCheckpointId();
        ev->StreamingQueryPath = QueryPath;
        ev->CustomerSuppliedId = State.GetCurrentExecutionId();
        ev->StreamingDisposition = Settings.StreamingDisposition;

        if (const auto statsPeriod = AppData()->QueryServiceConfig.GetProgressStatsPeriodMs()) {
            ev->ProgressStatsPeriod = TDuration::MilliSeconds(statsPeriod);
        } else {
            ev->ProgressStatsPeriod = TDuration::Seconds(1);
        }

        auto& record = ev->Record;
        record.SetTraceId(TStringBuilder() << "streaming-query-" << QueryPath << "-" << State.GetCurrentExecutionId());
        if (const auto& token = Context.GetUserToken()) {
            if (const auto& serializedToken = token->GetSerializedToken()) {
                record.SetUserToken(serializedToken);
            }
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
        request.SetTimeoutMs(TDuration::Max().MilliSeconds());

        LOG_D("Send start streaming query request, execution id: " << State.GetCurrentExecutionId());
        SendToKqpProxy(std::move(ev));
    }

    void GetScriptExecutionOperation() {
        if (!GetOperationRetryState) {
            GetOperationRetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                []() {
                    return ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(100),
                TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(),
                START_REQUEST_TIMEOUT
            )->CreateRetryState();
        }

        if (const auto delay = GetOperationRetryState->GetNextRetryDelay()) {
            LOG_D("Schedule get script execution operation in " << *delay);
            Schedule(*delay, new TEvents::TEvWakeup());
        } else {
            LOG_W("Script execution operation not started after " << START_REQUEST_TIMEOUT << " send response");
            Issues.AddIssue(
                NYql::TIssue(TStringBuilder() << "Streaming query not started after " << START_REQUEST_TIMEOUT << ", try to check query status later")
                    .SetCode(NYql::TIssuesIds::KIKIMR_TIMEOUT, NYql::TSeverityIds::S_INFO)
            );
            Finish(Ydb::StatusIds::SUCCESS);
        }
    }

    static std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> CreateDefaultRetryMapping() {
        // Retried all statuses except of SUCCESS, CANCELLED

        NKikimrKqp::TScriptExecutionRetryState::TMapping mapping;

        const auto* statusDescriptor = Ydb::StatusIds::StatusCode_descriptor();
        for (int i = 0; i < statusDescriptor->value_count(); ++i) {
            const auto status = static_cast<Ydb::StatusIds::StatusCode>(statusDescriptor->value(i)->number());
            if (!IsIn({Ydb::StatusIds::SUCCESS, Ydb::StatusIds::CANCELLED}, status)) {
                mapping.AddStatusCode(status);
            }
        }

        auto& policy = *mapping.MutableExponentialDelayPolicy();
        policy.SetBackoffMultiplier(1.5);
        policy.SetJitterFactor(0.1);
        *policy.MutableInitialBackoff() = NProtoInterop::CastToProto(TDuration::Seconds(1));
        *policy.MutableMaxBackoff() = NProtoInterop::CastToProto(TDuration::Minutes(1));
        *policy.MutableResetBackoffThreshold() = NProtoInterop::CastToProto(TDuration::Hours(1)); // Backoff state reset if uptime > 1h (next retry treated as first after reset)
        *policy.MutableQueryUptimeThreshold() = NProtoInterop::CastToProto(TDuration::Minutes(1)); // Query retried immediately if uptime > 1m

        return {std::move(mapping)};
    }

private:
    const TExternalContext Context;
    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    ui64 OperationsToForget = 0;
    i64 PreviousGeneration = 0;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PreviousPhysicalGraph;

    // Query starting state
    bool StateLoaded = false;
    bool RequestStarted = false;
    TRetryPolicy::IRetryState::TPtr GetOperationRetryState;
    Ydb::StatusIds::StatusCode FinalStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
};

// Interrupt previous not completed query action and update query state according to properties from schemeshard

class TSyncStreamingQueryTableActor final : public TActionActorBase<TSyncStreamingQueryTableActor> {
    using TBase = TActionActorBase<TSyncStreamingQueryTableActor>;

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        std::optional<TSchemeInfo> SchemeInfo;  // nullopt if query does not exists in schemeshard
    };

    TSyncStreamingQueryTableActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
        , ExistsInSS(Settings.SchemeInfo.has_value())
    {}

    void Bootstrap() {
        LOG_D("Bootstrap"
            << ". Has in SS: " << ExistsInSS
            << ", SS info: " << (Settings.SchemeInfo ? Settings.SchemeInfo->DebugString() : "null")
            << ", initial status: " << NKikimrKqp::TStreamingQueryState::EStatus_Name(State.GetStatus()));

        if (!Settings.SchemeInfo || State.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            RemoveQuery();
            return;
        }

        SchemeInfo = *Settings.SchemeInfo;

        if (!SchemeInfo.IsChanged(State)) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        QuerySettings.FromProto(SchemeInfo.Properties);
        SyncQuery();
    }

    STRICT_STFUNC(StateFuncRemoveQuery,
        hFunc(TEvPrivate::TEvCleanupStreamingQueryResult, HandleRemove);
        hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, HandleRemove);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleRemove);
    )

    void HandleRemove(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Cleanup streaming query")) {
            return;
        }

        State = ev->Get()->Info;
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

        RemoveQuery();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvCancelScriptExecutionOperationResponse, Handle);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvStartStreamingQueryResult, Handle);
    )

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

        SyncQuery();
    }

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state")) {
            return;
        }

        SyncQuery();
    }

    void Handle(TEvPrivate::TEvStartStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Start streaming query")) {
            return;
        }

        State = ev->Get()->Info;
        Finish(Ydb::StatusIds::SUCCESS);
    }

    STRICT_STFUNC(StateFuncFinalize,
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleFinalize);
    )

    void HandleFinalize(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, TStringBuilder() << "Update streaming query state (finish query synchronization), operation status: " << FinalStatus)) {
            return;
        }

        Finish(FinalStatus);
    }

protected:
    bool BeforeFinish(Ydb::StatusIds::StatusCode status) final {
        if (!SchemeInfo.IsChanged(State)) {
            return false;
        }

        Become(&TSyncStreamingQueryTableActor::StateFuncFinalize);
        SchemeInfo.Sync(State);
        UpdateQueryState("sync scheme info");

        FinalStatus = status;
        return true;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvSyncStreamingQueryResult(status, State, ExistsInSS, std::move(Issues)));
    }

private:
    void UpdateQueryState(const TString& info) const {
        const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId << " (" << info << ")");
    }

    void RemoveQuery() {
        Become(&TSyncStreamingQueryTableActor::StateFuncRemoveQuery);

        if (State.HasCurrentExecutionId() || State.PreviousExecutionIdsSize() > 0) {
            const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateTableActor(Context, QueryPath, State));
            LOG_D("Start TCleanupStreamingQueryStateTableActor " << cleanupActorId << " (remove query)");
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

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED || State.HasSchemeInfo()) {
            // Clear query status
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
            State.ClearSchemeInfo();
            UpdateQueryState("clear status");
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

    void StopQuery(const TString& info) {
        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_STOPPING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPING);
            UpdateQueryState(TStringBuilder() << "move to stopping (" << info << ")");
            return;
        }

        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            LOG_D("Cancel streaming query execution " << executionId << " (" << info << ")");
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA));
            return;
        }

        State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
        UpdateQueryState(TStringBuilder() << "move to stopped" << " (" << info << ")");
    }

    void EnrichStateFromSS() {
        State.SetQueryText(QuerySettings.QueryText);
        State.SetRun(QuerySettings.Run);
        State.SetResourcePool(QuerySettings.ResourcePool);

        if (IsIn({NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED, NKikimrKqp::TStreamingQueryState::STATUS_CREATING}, State.GetStatus())) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_CREATED);
        }

        StateEnrichedFromSS = true;
        UpdateQueryState("enrich state from SS");
    }

    void StartQuery() {
        if (State.HasCurrentExecutionId()) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Can not start query, already started: " << State.GetCurrentExecutionId());
            return;
        }

        const auto& startActorId = Register(new TStartStreamingQueryTableActor(Context, QueryPath, {
            .InitialState = State,
            .QueryPathId = SchemeInfo.PathId,
            .QueryTextRevision = QuerySettings.QueryTextRevision,
            .StreamingDisposition = QuerySettings.StreamingDisposition,
        }));
        LOG_D("Start TStartStreamingQueryTableActor " << startActorId);
    }

    void SyncQuery() {
        Become(&TSyncStreamingQueryTableActor::StateFunc);

        switch (State.GetStatus()) {
            case NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED:
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATING:
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATED:
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPED: {
                if (!StateEnrichedFromSS) {
                    EnrichStateFromSS();
                } else if (QuerySettings.Run) {
                    StartQuery();
                } else {
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_RUNNING:
            case NKikimrKqp::TStreamingQueryState::STATUS_STARTING:
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPING: {
                StopQuery(TStringBuilder() << "interrupt " << NKikimrKqp::TStreamingQueryState::EStatus_Name(State.GetStatus()));
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
    TSchemeInfo SchemeInfo;
    TStreamingQuerySettings QuerySettings;
    bool StateEnrichedFromSS = false;
    Ydb::StatusIds::StatusCode FinalStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
};

//// Request handlers

// Common request handling pipeline:
// Describe -> Lock -> (perform actions) -> Unlock
//                             |
//          TableActors / RequestActors / SchemeActors

template <typename TDerived>
class TRequestHandlerBase : public TActionActorBase<TDerived> {
    using TBase = TActionActorBase<TDerived>;

public:
    using TBase::LogPrefix;

    TRequestHandlerBase(const TString& operationName, const NKikimrSchemeOp::TModifyScheme& schemeTx, const TString& queryName, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller, ui32 access)
        : TBase(operationName, schemeTx.GetWorkingDir(), queryName)
        , Controller(std::move(controller))
        , Access(access)
        , StartedAt(TInstant::Now())
        , Context(context)
        , SchemeTx(schemeTx)
    {}

    void Bootstrap() {
        LOG_D("Bootstrap");

        TBase::Become(&TDerived::StateFunc);
        DescribeQuery("start handling");
    }

    STRICT_STFUNC(StateFuncBase,
        hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvCheckAliveRequest, Handle);
    )

    void Handle(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Describe streaming query")) {
            return;
        }

        SchemeInfo = ev->Get()->Info;
        if (Context.GetUserToken() && Context.GetUserToken()->GetSerializedToken() && SchemeInfo && SchemeInfo->SecurityObject) {
            if (const auto& securityObject = *SchemeInfo->SecurityObject; !securityObject.CheckAccess(Access, *Context.GetUserToken())) {
                LOG_W("Access denied for " << Context.GetUserToken()->GetUserSID() << ", access: " << Access);

                if (!securityObject.CheckAccess(NACLib::DescribeSchema, *Context.GetUserToken())) {
                    TBase::FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << TBase::QueryPath << " not found or you don't have access permissions");
                } else {
                    TBase::FatalError(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for streaming query " << TBase::QueryPath);
                }

                return;
            }
        }

        LOG_D("Describe streaming query success, SchemeInfo: " << (SchemeInfo ? SchemeInfo->DebugString() : "null"));

        OnQueryDescribed();
    }

    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Prepare streaming query before operation")) {
            return;
        }

        const auto& info = ev->Get()->Info;
        IsLockCreated = info.LockCreated;
        QueryState = info.State;
        LOG_D("Lock streaming query " << ev->Sender << " success"
            << ", IsLockCreated: " << IsLockCreated
            << ", QueryExists: " << info.QueryExists
            << ", QueryState: " << LogQueryState(QueryState));

        OnQueryLocked(info.QueryExists);
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
    virtual void OnQueryDescribed() = 0;

    virtual void OnQueryLocked(bool queryExists) = 0;

protected:
    void LockQuery(const TString& operationName, bool createLockIfNotExists, NKikimrKqp::TStreamingQueryState::EStatus defaultQueryStatus) const {
        const auto& lockActorId = TBase::Register(new TLockStreamingQueryTableActor(Context.GetDatabaseId(), TBase::QueryPath, {
            .OperationName = operationName,
            .OperationStartedAt = StartedAt,
            .OperationOwner = TBase::SelfId(),
            .CreateLockIfNotExists = createLockIfNotExists,
            .DefaultQueryStatus = defaultQueryStatus,
        }));
        LOG_D("Start TLockStreamingQueryTableActor " << lockActorId);
    }

    bool BeforeFinish(Ydb::StatusIds::StatusCode status) final {
        if (!IsLockCreated) {
            return false;
        }

        TBase::Become(&TDerived::StateFunc);
        const auto& unlockActorId = TBase::Register(new TUnlockStreamingQueryRequestActor::TRetry(TBase::SelfId(), Context.GetDatabaseId(), TBase::QueryPath, TBase::SelfId()));
        LOG_D("Start TUnlockStreamingQueryRequestActor " << unlockActorId);

        FinalStatus = status;
        return true;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        if (status == Ydb::StatusIds::SUCCESS) {
            Controller->OnAlteringFinished();
        } else {
            Controller->OnAlteringFinishedWithStatus(TStreamingQueryConfig::TStatus::Fail(YdbStatusToYqlStatus(status), TBase::Issues.ToString()));
        }
    }

    void DescribeQuery(const TString& info) {
        // Access by user token will be checked during scheme transaction execution
        const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(Context.GetDatabase(), TBase::QueryPath, NACLib::TUserToken(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})));
        LOG_D("Start TDescribeStreamingQuerySchemeActor " << describerId << " (" << info << ")");
    }

private:
    static NYql::EYqlIssueCode YdbStatusToYqlStatus(Ydb::StatusIds::StatusCode status) {
        switch (status) {
            case Ydb::StatusIds::UNDETERMINED:
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED: return NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN;
            case Ydb::StatusIds::ALREADY_EXISTS:
            case Ydb::StatusIds::SCHEME_ERROR: return NYql::TIssuesIds::KIKIMR_SCHEME_ERROR;
            case Ydb::StatusIds::SESSION_BUSY:
            case Ydb::StatusIds::SESSION_EXPIRED: return NYql::TIssuesIds::KIKIMR_BAD_OPERATION;
            case Ydb::StatusIds::SUCCESS: return NYql::TIssuesIds::SUCCESS;
            case Ydb::StatusIds::BAD_REQUEST: return NYql::TIssuesIds::KIKIMR_BAD_REQUEST;
            case Ydb::StatusIds::UNAUTHORIZED: return NYql::TIssuesIds::KIKIMR_ACCESS_DENIED;
            case Ydb::StatusIds::INTERNAL_ERROR: return NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR;
            case Ydb::StatusIds::ABORTED: return NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED;
            case Ydb::StatusIds::UNAVAILABLE: return NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
            case Ydb::StatusIds::OVERLOADED: return NYql::TIssuesIds::KIKIMR_OVERLOADED;
            case Ydb::StatusIds::TIMEOUT: return NYql::TIssuesIds::KIKIMR_TIMEOUT;
            case Ydb::StatusIds::BAD_SESSION: return NYql::TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS;
            case Ydb::StatusIds::PRECONDITION_FAILED: return NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED;
            case Ydb::StatusIds::CANCELLED: return NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED;
            case Ydb::StatusIds::UNSUPPORTED: return NYql::TIssuesIds::KIKIMR_UNSUPPORTED;
            case Ydb::StatusIds::NOT_FOUND: return NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND;
            default: return NYql::TIssuesIds::DEFAULT_ERROR;
        }
    }

private:
    const IStreamingQueryOperationController::TPtr Controller;
    const ui32 Access = 0;
    Ydb::StatusIds::StatusCode FinalStatus;

protected:
    const TInstant StartedAt;
    const TExternalContext Context;
    NKikimrSchemeOp::TModifyScheme SchemeTx;
    bool IsLockCreated = false;
    std::optional<TSchemeInfo> SchemeInfo;
    NKikimrKqp::TStreamingQueryState QueryState;
};

// Request handling with synchronization pipeline if scheme info changed:
// [Base handler] -> Describe -> Sync -> (perform actions) -> [Base handler]
//                                               |
//                            TableActors / RequestActors / SchemeActors

template <typename TDerived>
class TRequestHandlerWithSync : public TRequestHandlerBase<TDerived> {
    using TBase = TRequestHandlerBase<TDerived>;

    static NYql::NPq::NProto::StreamingDisposition GetDefaultStreamingDisposition() {
        NYql::NPq::NProto::StreamingDisposition result;
        result.mutable_from_last_checkpoint()->set_force(true);
        return result;
    }

public:
    using TBase::LogPrefix;
    using TBase::TBase;

    static inline const TString DefaultStreamingDisposition = GetDefaultStreamingDisposition().SerializeAsString();

    STFUNC(StateFuncSync) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, HandleSync);
            hFunc(TEvPrivate::TEvSyncStreamingQueryResult, HandleSync);
            default:
                TBase::StateFuncBase(ev);
        }
    }

    void HandleSync(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Describe streaming query (recover previous query state, try to repeat request)")) {
            return;
        }

        TBase::SchemeInfo = ev->Get()->Info;
        LOG_D("Describe streaming query success, scheme info: " << (TBase::SchemeInfo ? TBase::SchemeInfo->DebugString() : "null"));

        const auto& syncActorId = TBase::Register(new TSyncStreamingQueryTableActor(TBase::Context, TBase::QueryPath, {
            .InitialState = TBase::QueryState,
            .SchemeInfo = TBase::SchemeInfo,
        }));
        LOG_D("Start TSyncStreamingQueryTableActor " << syncActorId << " (sync previous state)");
    }

    void HandleSync(TEvPrivate::TEvSyncStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Streaming query initialization (recover previous query state, try to repeat request)")) {
            return;
        }

        TBase::QueryState = ev->Get()->State;
        if (!ev->Get()->ExistsInSS) {
            TBase::SchemeInfo = std::nullopt;
        }

        LOG_D("Sync query with scheme shard success"
            << ", QueryState: " << LogQueryState(TBase::QueryState)
            << ", query exists in SS: " << TBase::SchemeInfo.has_value());

        TBase::Become(&TDerived::StateFunc);
        OnQuerySynced();
    }

protected:
    virtual void OnQuerySynced() = 0;

protected:
    void OnQueryLocked(bool queryExists) final {
        if (TBase::SchemeInfo && (!queryExists || TBase::SchemeInfo->IsChanged(TBase::QueryState))) {
            TBase::Become(&TRequestHandlerWithSync::StateFuncSync);
            TBase::DescribeQuery("sync previous state");
            return;
        }

        OnQuerySynced();
    }
};

// Create request handling:
// [Base handler] -> Create in SS -> Describe -> Sync -> [Base handler]

class TCreateStreamingQueryActor final : public TRequestHandlerWithSync<TCreateStreamingQueryActor> {
    using TBase = TRequestHandlerWithSync<TCreateStreamingQueryActor>;

    enum class EOnExists {
        Replace,
        Ignore,
        Fail,
    };

public:
    using TBase::LogPrefix;

    TCreateStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetCreateStreamingQuery().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::CreateTable)
        , ActionOnExists(GetActionOnExists(schemeTx))
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, Handle);
            hFunc(TEvPrivate::TEvSyncStreamingQueryResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute create scheme operation")) {
            return;
        }

        QueryCreated = true;
        DescribeQuery("after query creation");
    }

    void Handle(TEvPrivate::TEvSyncStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Streaming query initialization")) {
            return;
        }

        QueryState = ev->Get()->State;
        if (!ev->Get()->ExistsInSS) {
            SchemeInfo = std::nullopt;
        }

        LOG_D("Sync query with scheme shard success, State: " << LogQueryState(QueryState));
        Finish(Ydb::StatusIds::SUCCESS);
    }

protected:
    void OnQueryDescribed() final {
        if (!QueryCreated) {
            LockQuery(
                TStringBuilder() << "CREATE" << (ActionOnExists == EOnExists::Replace ? " OR REPLACE" : "") << " STREAMING QUERY" << (ActionOnExists == EOnExists::Ignore ? " IF NOT EXISTS" : ""),
                true,
                NKikimrKqp::TStreamingQueryState::STATUS_CREATING
            );
            return;
        }

        if (!SchemeInfo) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Query info not found after creation");
        } else {
            const auto& syncActorId = Register(new TSyncStreamingQueryTableActor(Context, QueryPath, {
                .InitialState = QueryState,
                .SchemeInfo = *SchemeInfo,
            }));
            LOG_D("Start TSyncStreamingQueryTableActor " << syncActorId << " to finish creation");
        }
    }

    void OnQuerySynced() final {
        if (SchemeInfo) {
            switch (ActionOnExists) {
                case EOnExists::Replace:
                    // Continue query creation
                    break;
                case EOnExists::Ignore:
                    Finish(Ydb::StatusIds::SUCCESS);
                    return;
                case EOnExists::Fail:
                    FatalError(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder() << "Streaming query " << QueryPath << " already exists");
                    return;
            }
        }

        if (const auto& status = ValidateProperties(); status.IsFail()) {
            FatalError(status.GetStatus(), AddRootIssue("Invalid properties for creation new streaming query", status.GetErrorDescription()));
            return;
        }

        const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, SchemeTx, Context.GetUserToken()));
        LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
    }

private:
    static EOnExists GetActionOnExists(const NKikimrSchemeOp::TModifyScheme& schemeTx) {
        if (schemeTx.GetReplaceIfExists()) {
            return EOnExists::Replace;
        }

        return schemeTx.GetFailedOnAlreadyExists() ? EOnExists::Fail : EOnExists::Ignore;
    }

    TStatus ValidateProperties() {
        using ESqlSettings = TStreamingQueryConfig::TSqlSettings;
        using EName = TStreamingQueryConfig::TProperties;

        TPropertyValidator validator(*SchemeTx.MutableCreateStreamingQuery()->MutableProperties());
        CHECK_STATUS(validator.SaveRequired(ESqlSettings::QUERY_TEXT_FEATURE, &TPropertyValidator::ValidateNotEmpty));
        CHECK_STATUS(validator.SaveDefault(EName::Run, "true", &TPropertyValidator::ValidateBool));
        CHECK_STATUS(validator.SaveDefault(EName::ResourcePool, NResourcePool::DEFAULT_POOL_ID));
        CHECK_STATUS(validator.SaveDefault(EName::StreamingDisposition, DefaultStreamingDisposition));
        CHECK_STATUS(validator.Save(
            EName::QueryTextRevision,
            ToString(SchemeInfo ? TStreamingQuerySettings().FromProto(SchemeInfo->Properties).QueryTextRevision + 1 : 1)
        ));

        return validator.Finish();
    }

private:
    const EOnExists ActionOnExists = EOnExists::Fail;
    bool QueryCreated = false;
};

// Alter request handling:
// [Base handler] -> Alter in SS -> Sync -> [Base handler]

class TAlterStreamingQueryActor final : public TRequestHandlerWithSync<TAlterStreamingQueryActor> {
    using TBase = TRequestHandlerWithSync<TAlterStreamingQueryActor>;

public:
    using TBase::LogPrefix;

    TAlterStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetCreateStreamingQuery().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::AlterSchema)
        , SuccessOnNotExist(schemeTx.GetSuccessOnNotExist())
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, Handle);
            hFunc(TEvPrivate::TEvSyncStreamingQueryResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute alter scheme operation")) {
            return;
        }

        if (!SchemeInfo) {
            FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Can not continue alter without query state");
        } else {
            const auto& syncActorId = Register(new TSyncStreamingQueryTableActor(Context, QueryPath, {
                .InitialState = QueryState,
                .SchemeInfo = TSchemeInfo{
                    .Properties = SchemeTx.GetCreateStreamingQuery().GetProperties(),
                    .Version = SchemeInfo->Version + 1,
                    .PathId = SchemeInfo->PathId,
                },
            }));
            LOG_D("Start TSyncStreamingQueryTableActor " << syncActorId << " to finish alter");
        }
    }

    void Handle(TEvPrivate::TEvSyncStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Streaming query alter")) {
            return;
        }

        QueryState = ev->Get()->State;
        if (!ev->Get()->ExistsInSS) {
            SchemeInfo = std::nullopt;
        }

        LOG_D("Sync query with scheme shard success, State: " << LogQueryState(QueryState));
        Finish(Ydb::StatusIds::SUCCESS);
    }

protected:
    void OnQueryDescribed() final {
        LockQuery(
            TStringBuilder() << "ALTER STREAMING QUERY" << (SuccessOnNotExist ? " IF EXISTS" : ""),
            SchemeInfo.has_value(),
            NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED
        );
    }

    void OnQuerySynced() final {
        if (!SchemeInfo) {
            if (SuccessOnNotExist) {
                Finish(Ydb::StatusIds::SUCCESS);
            } else {
                FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            }
            return;
        }

        if (const auto& status = ValidateProperties(TStreamingQuerySettings().FromProto(SchemeInfo->Properties)); status.IsFail()) {
            FatalError(status.GetStatus(), AddRootIssue("Invalid properties for alter streaming query", status.GetErrorDescription()));
            return;
        }

        const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, SchemeTx, Context.GetUserToken()));
        LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
    }

private:
    TStatus ValidateProperties(const TStreamingQuerySettings& previousSettings) {
        using ESqlSettings = TStreamingQueryConfig::TSqlSettings;
        using EName = TStreamingQueryConfig::TProperties;

        TPropertyValidator validator(*SchemeTx.MutableCreateStreamingQuery()->MutableProperties());
        CHECK_STATUS(validator.SaveDefault(EName::Run, previousSettings.Run ? "true" : "false", &TPropertyValidator::ValidateBool));
        CHECK_STATUS(validator.SaveDefault(EName::ResourcePool, previousSettings.ResourcePool));
        CHECK_STATUS_RET(force, validator.ExtractDefault(EName::Force, "false", &TPropertyValidator::ValidateBool));
        CHECK_STATUS_RET(queryText, validator.ExtractOptional(ESqlSettings::QUERY_TEXT_FEATURE, &TPropertyValidator::ValidateNotEmpty));
        CHECK_STATUS_RET(streamingDisposition, validator.ExtractOptional(EName::StreamingDisposition));

        const auto queryTextValue = queryText.DetachResult();
        if (queryTextValue && force.GetResult() != "true") {
            return TStatus::Fail(Ydb::StatusIds::PRECONDITION_FAILED, "Changing the query text will result in the loss of the checkpoint. Please use FORCE=true to change the request text");
        }

        const auto streamingDispositionValue = streamingDisposition.DetachResult();
        auto queryTestRevision = previousSettings.QueryTextRevision;
        if (queryTextValue) {
            queryTestRevision++;
        } else if (streamingDispositionValue) {
            NYql::NPq::NProto::StreamingDisposition disposition;
            Y_VALIDATE(disposition.ParseFromString(*streamingDispositionValue), "Failed to parse StreamingDisposition");
            queryTestRevision += !disposition.has_from_last_checkpoint(); // Recompile query and drop checkpoint if disposition changed
        }

        CHECK_STATUS(validator.Save(ESqlSettings::QUERY_TEXT_FEATURE, queryTextValue.value_or(previousSettings.QueryText)));
        CHECK_STATUS(validator.Save(EName::QueryTextRevision, ToString(queryTestRevision)));
        CHECK_STATUS(validator.Save(EName::StreamingDisposition, streamingDispositionValue.value_or(DefaultStreamingDisposition)));

        return validator.Finish();
    }

private:
    const bool SuccessOnNotExist = false;
};

// Drop request handling:
// [Base handler] -> Cleanup -> Drop in SS -> Remove query row

class TDropStreamingQueryActor final : public TRequestHandlerBase<TDropStreamingQueryActor> {
    using TBase = TRequestHandlerBase<TDropStreamingQueryActor>;

public:
    using TBase::LogPrefix;

    TDropStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetDrop().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::RemoveSchema)
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
        CleanupQuery();
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        if (HandleResult(ev, "Execute drop scheme operation")) {
            return;
        }

        QueryExistsInSS = false;
        CleanupQuery();
    }

    void Handle(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query status")) {
            return;
        }

        CleanupQuery();
    }

protected:
    void OnQueryDescribed() final {
        QueryExistsInSS = SchemeInfo.has_value();
        LockQuery(
            TStringBuilder() << "DROP STREAMING QUERY" << (SuccessOnNotExist ? " IF EXISTS" : ""),
            QueryExistsInSS,
            NKikimrKqp::TStreamingQueryState::STATUS_DELETING
        );
    }

    void OnQueryLocked(bool queryExists) final {
        QueryExistsInTable = queryExists;

        if (!QueryExistsInTable && !QueryExistsInSS && !SuccessOnNotExist) {
            FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            return;
        }

        CleanupQuery();
    }

private:
    void CleanupQuery() {
        if (QueryExistsInTable) {
            // Clear query state
            const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateTableActor(Context, QueryPath, QueryState));
            LOG_D("Start TCleanupStreamingQueryStateTableActor " << cleanupActorId);
            return;
        }

        if (QueryExistsInSS) {
            // Remove query from SS
            const auto& executerId = Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), QueryPath, SchemeTx, Context.GetUserToken()));
            LOG_D("Start TExecuteTransactionSchemeActor " << executerId);
            return;
        }

        if (QueryState.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED || QueryState.HasSchemeInfo()) {
            // Clear query status
            QueryState.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED);
            QueryState.ClearSchemeInfo();

            const auto& updaterId = Register(new TUpdateStreamingQueryStateRequestActor::TRetry(SelfId(), Context.GetDatabaseId(), QueryPath, QueryState));
            LOG_D("Start TUpdateStreamingQueryStateRequestActor " << updaterId);
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

private:
    const bool SuccessOnNotExist = false;
    bool QueryExistsInSS = false;
    bool QueryExistsInTable = false;
};

}  // anonymous namespace

void DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const TExternalContext& context) {
    context.GetActorSystem()->Register(new TCreateStreamingQueryActor(schemeTx, context, controller));
}

void DoAlterStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) {
    context.GetActorSystem()->Register(new TAlterStreamingQueryActor(schemeTx, context, controller));
}

void DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const TExternalContext& context) {
    context.GetActorSystem()->Register(new TDropStreamingQueryActor(schemeTx, context, controller));
}

}  // namespace NKikimr::NKqp
