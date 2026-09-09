#include "queries.h"
#include "manager.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/base/path.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
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

#include <yql/essentials/core/sql_types/hopping.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <fmt/format.h>

#include <google/protobuf/util/time_util.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_PROXY

namespace NKikimr::NKqp {

namespace {

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
    TActorId InflightOperationOwnerId;

    bool IsChanged(const NKikimrKqp::TStreamingQueryState& state) const {
        const auto& schemeInfo = state.GetSchemeInfo();
        return schemeInfo.GetAlterVersion() != Version
            || schemeInfo.GetOwnerSchemeshardId() != PathId.OwnerId
            || schemeInfo.GetLocalPathId() != PathId.LocalPathId;
    }

    TString DebugString() const {
        return TStringBuilder()
            << "{Version: " << Version
            << ", PathId: " << PathId.ToString()
            << ", Properties: " << TruncateString(EscapeC(Properties.ShortDebugString()))
            << ", InflightOperationOwnerId: " << InflightOperationOwnerId
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
        EvPingOperationOwnerResult,

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

    template <ui32 EventType>
    struct TEvResult : public TEvResultBase<TEvResult<EventType>, EventType> {
        using TEvResultBase<TEvResult<EventType>, EventType>::TEvResultBase;
    };

    template <typename TInfo, ui32 EventType>
    struct TEvResulWithInfo : public TEvResultBase<TEvResulWithInfo<TInfo, EventType>, EventType> {
        using TBase = TEvResultBase<TEvResulWithInfo<TInfo, EventType>, EventType>;

        TEvResulWithInfo(Ydb::StatusIds::StatusCode status, TInfo info, NYql::TIssues issues = {})
            : TBase(status, std::move(issues))
            , Info(std::move(info))
        {}

        const TInfo Info;
    };

    using TEvDescribeStreamingQueryResult = TEvResulWithInfo<std::optional<TSchemeInfo>, EvDescribeStreamingQueryResult>;

    using TEvExecuteSchemeTransactionResult = TEvResult<EvExecuteSchemeTransactionResult>;

    using TEvUpdateStreamingQueryResult = TEvResult<EvUpdateStreamingQueryResult>;

    using TEvCleanupStreamingQueryResult = TEvResulWithInfo<NKikimrKqp::TStreamingQueryState, EvCleanupStreamingQueryResult>;

    using TEvStartStreamingQueryResult = TEvResulWithInfo<NKikimrKqp::TStreamingQueryState, EvStartStreamingQueryResult>;

    using TEvSyncStreamingQueryResult = TEvResulWithInfo<NKikimrKqp::TStreamingQueryState, EvSyncStreamingQueryResult>;

    struct TEvLockStreamingQueryResult : public TEvResultBase<TEvLockStreamingQueryResult, EvLockStreamingQueryResult> {
        struct TInfo {
            NKikimrKqp::TStreamingQueryState State;
            TActorId StaleOwner;
            bool OperationAlreadyFinished = false;
            bool QueryInfoNotFound = false;
            bool IsTemporary = false; // Acquired row still needs SchemeShard validation.
        };

        TEvLockStreamingQueryResult(Ydb::StatusIds::StatusCode status, const TInfo& info, NYql::TIssues issues = {})
            : TEvResultBase(status, std::move(issues))
            , Info(info)
        {}

        const TInfo Info;
    };

    using TEvPingOperationOwnerResult = TEvResult<EvPingOperationOwnerResult>;

    using TEvUnlockStreamingQueryResult = TEvResult<EvUnlockStreamingQueryResult>;

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
        << ", OperationActorId: " << state.GetOperationActorId()
        << ", OperationOwnerGeneration: " << state.GetOperationOwnerGeneration()
        << ", SchemeInfo: " << state.GetSchemeInfo().ShortDebugString()
        << "}";
}

// Used for properties validation before saving into schemeshard
class TPropertyValidator {
    using TProperties = google::protobuf::Map<TString, TString>;

public:
    static constexpr ui64 MAX_PROTOBUF_DURATION_MICROSECONDS = google::protobuf::util::TimeUtil::kDurationMaxSeconds * static_cast<i64>(1000000);

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

    template<typename T> requires (std::is_enum_v<T>)
    static TStatus ValidateEnum(const TString& name, const TString& value) {
        if (!TryFromString<T>(value)) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property got illegal value: " << value);
        }
        return TStatus::Success();
    }

    template<ui64 MaxMicrosecondsValue = std::numeric_limits<ui64>::max()>
    static TStatus ValidateInterval(const TString& name, const TString& value) {
        const auto duration = NMiniKQL::ValueFromString(NYql::NUdf::EDataSlot::Interval, value);
        if (!duration) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property is not a valid ISO 8601 duration: " << value);
        }

        const i64 signedDuration = duration.Get<i64>();
        if (signedDuration < 0) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property is should be non-negative interval, but got: " << value);
        }

        if (static_cast<ui64>(signedDuration) > MaxMicrosecondsValue) {
            return TStatus::Fail(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << to_upper(name) << " property interval is too large: " << value);
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
class TActionActorBase : public TActorBootstrapped<TDerived>, public IActorExceptionHandler {
    using TBase = TActorBootstrapped<TDerived>;

public:
    TActionActorBase(const TString& operationName, const TString& queryPath)
        : OperationName(operationName)
        , QueryPath(queryPath)
    {}

    TActionActorBase(const TString& operationName, const TString& workingDir, const TString& queryName)
        : TActionActorBase(operationName, JoinPath({workingDir, queryName}))
    {}

    bool OnUnhandledException(const std::exception& e) final {
        FatalError(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unhandled exception: " << e.what());
        return true;
    }

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
            YDB_LOG_DEBUG("[StreamingQueries] Operation succeeded",
                {"logPrefix", LogPrefix()},
                {"message", message},
                {"sender", ev->Sender});
            return false;
        }

        const auto& issues = ev->Get()->Issues;
        YDB_LOG_WARN("[StreamingQueries] Operation failed",
            {"logPrefix", LogPrefix()},
            {"message", message},
            {"sender", ev->Sender},
            {"status", status},
            {"issues", issues.ToOneLineString()});

        FatalError(status, AddRootIssue(TStringBuilder() << message << " failed", issues));
        return true;
    }

    void Finish(Ydb::StatusIds::StatusCode status) {
        if (BeforeFinish(status)) {
            YDB_LOG_DEBUG("[StreamingQueries] Deferring finish to run action before completion",
                {"logPrefix", LogPrefix()},
                {"status", status});
            return;
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            YDB_LOG_DEBUG("[StreamingQueries] Successfully finished",
                {"logPrefix", LogPrefix()});
        } else {
            YDB_LOG_WARN("[StreamingQueries] Operation failed",
                {"logPrefix", LogPrefix()},
                {"status", status},
                {"issues", Issues.ToOneLineString()});
        }

        OnFinish(status);
        this->PassAway();
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

    static NYql::EYqlIssueCode YdbStatusToYqlStatus(Ydb::StatusIds::StatusCode status) {
        switch (status) {
            case Ydb::StatusIds::UNDETERMINED:
            case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED: return NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN;
            case Ydb::StatusIds::ALREADY_EXISTS: return NYql::TIssuesIds::KIKIMR_SCHEME_ERROR;
            case Ydb::StatusIds::INTERNAL_ERROR: return NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR;
            case Ydb::StatusIds::PRECONDITION_FAILED: return NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED;
            case Ydb::StatusIds::NOT_FOUND: return NYql::TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND;
            default: return NYql::YqlStatusFromYdbStatus(status);
        }
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
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping scheme actor",
            {"logPrefix", LogPrefix()},
            {"database", Database});
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

        YDB_LOG_ERROR("[StreamingQueries] Scheme service is unavailable",
            {"logPrefix", LogPrefix()});
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
            YDB_LOG_WARN("[StreamingQueries] Scheduling retry after scheme error",
                {"logPrefix", LogPrefix()},
                {"error", issues.ToOneLineString()},
                {"retryDelay", *delay});
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
        YDB_LOG_DEBUG("[StreamingQueries] Received scheme cache response",
            {"logPrefix", LogPrefix()},
            {"response", result.Status});

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
                    const auto& description = result.StreamingQueryInfo->Description;
                    Info = TSchemeInfo{
                        .Properties = description.GetProperties(),
                        .Version = pathInfo.GetVersion().GetStreamingQueryVersion(),
                        .PathId = TPathId(pathInfo.GetSchemeshardId(), pathInfo.GetPathId()),
                        .SecurityObject = result.SecurityObject,
                        .InflightOperationOwnerId = ActorIdFromProto(description.GetOperationOwnerActorId()),
                    };
                    Finish(Ydb::StatusIds::SUCCESS);
                }
                return;
            }
        }
    }

protected:
    void StartRequest() final {
        YDB_LOG_DEBUG("[StreamingQueries] Describing streaming query in scheme cache",
            {"logPrefix", LogPrefix()},
            {"database", Database});

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
        TxId = response.GetTxId();
        SchemeShardTabletId = response.GetSchemeShardTabletId();

        YDB_LOG_DEBUG("[StreamingQueries] Received propose transaction response",
            {"logPrefix", LogPrefix()},
            {"operationType", NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType())},
            {"status", status},
            {"schemeShardStatus", NKikimrScheme::EStatus_Name(ssStatus)},
            {"txId", TxId},
            {"schemeShardTabletId", SchemeShardTabletId});

        switch (status) {
            case NTxProxy::TResultStatus::ExecInProgress: {
                if (TxId == 0) {
                    FatalError(Ydb::StatusIds::INTERNAL_ERROR, ExtractIssues(response, ssStatus, "unable to subscribe on inprogress transaction"));
                    return;
                }

                Become(&TThis::StateFuncWaitCompletion);
                OpenPipeClientAndWaitCompletion();
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
            case NTxProxy::TResultStatus::ProxyShardTryLater: {
                YDB_LOG_WARN("[StreamingQueries] Retrying scheme transaction after proxy shard error",
                    {"logPrefix", LogPrefix()},
                    {"error", status},
                    {"tabletId", SchemeShardTabletId},
                    {"txId", TxId});
                ScheduleRetry(response, TStringBuilder() << "proxy shard not ready " << status);
                break;
            }
            case NTxProxy::TResultStatus::ProxyShardNotAvailable: {
                FatalError(Ydb::StatusIds::UNAVAILABLE, ExtractIssues(response, ssStatus, "proxy shard not available"));
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
                    case NKikimrScheme::StatusMultipleModifications: {
                        FatalError(Ydb::StatusIds::PRECONDITION_FAILED, ExtractIssues(response, ssStatus, TStringBuilder() << "execution error, streaming query " << QueryPath << " has multiple modifications inflight"));
                        break;
                    }
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
                break;
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
            sFunc(TEvents::TEvWakeup, OpenPipeClientAndWaitCompletion);
            IgnoreFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered);
            default:
                StateFuncBase(ev);
        }
    }

    void HandleWaitCompletion(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (SchemePipeActorId != ev->Get()->ClientId) {
            // Pipe outdated
            return;
        }

        if (const auto status = ev->Get()->Status; status != NKikimrProto::OK) {
            FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Pipe to tablet is not connected: " << NKikimrProto::EReplyStatus_Name(status) << " " << ev->Get()->ToString());
            return;
        }

        YDB_LOG_TRACE("[StreamingQueries] Tablet pipe successfully connected",
            {"logPrefix", LogPrefix()});
    }

    void HandleWaitCompletion(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (SchemePipeActorId != ev->Get()->ClientId) {
            // Pipe already closed
            return;
        }

        ClosePipeClient();

        if (!TBase::ScheduleRetry("Pipe to tablet destroyed")) {
            FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded, pipe to tablet destroyed " << ev->Get()->ToString());
        }
    }

    void HandleWaitCompletion(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        const auto completedTxId = ev->Get()->Record.GetTxId();
        Y_VALIDATE(completedTxId == TxId, "Unexpected completed tx id: " << completedTxId << ", expected tx: " << TxId);
        YDB_LOG_DEBUG("[StreamingQueries] Scheme transaction successfully finished",
            {"logPrefix", LogPrefix()},
            {"completedTxId", completedTxId});
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void PassAway() final {
        ClosePipeClient();
        TBase::PassAway();
    }

protected:
    void StartRequest() final {
        YDB_LOG_DEBUG("[StreamingQueries] Starting scheme transaction",
            {"logPrefix", LogPrefix()},
            {"operationType", NKikimrSchemeOp::EOperationType_Name(SchemeTx.GetOperationType())},
            {"database", Database});

        auto event = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        *event->Record.MutableTransaction()->MutableModifyScheme() = SchemeTx;
        event->Record.SetDatabaseName(Database);

        if (UserToken) {
            event->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        Send(MakeTxProxyID(), std::move(event));
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvExecuteSchemeTransactionResult(status, std::move(Issues)));
    }

private:
    void OpenPipeClientAndWaitCompletion() {
        Y_VALIDATE(!SchemePipeActorId, "Pipe client is not closed before wait completion");

        SchemePipeActorId = Register(NTabletPipe::CreateClient(SelfId(), SchemeShardTabletId, NTabletPipe::TClientRetryPolicy{
            .RetryLimitCount = 10,
            .MaxRetryTime = TDuration::Seconds(5),
        }));

        Y_VALIDATE(TxId, "Cannot subscribe on completion without tx id");
        NTabletPipe::SendData(SelfId(), SchemePipeActorId, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(TxId));

        YDB_LOG_DEBUG("[StreamingQueries] Subscribing to scheme transaction completion on scheme pipe",
            {"logPrefix", LogPrefix()},
            {"tx", TxId},
            {"schemeShardTabletId", SchemeShardTabletId},
            {"schemePipeActorId", SchemePipeActorId});
    }

    void ClosePipeClient() {
        if (SchemePipeActorId) {
            NTabletPipe::CloseClient(SelfId(), SchemePipeActorId);
            SchemePipeActorId = {};
        }
    }

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
                << " (tx status: " << static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(response.GetStatus()) << ")"
                << ": " << message
                << " (reason: " << response.GetSchemeShardReason() << ")",
            issues
        );
    }

private:
    const NKikimrSchemeOp::TModifyScheme SchemeTx;
    ui64 SchemeShardTabletId = 0;
    ui64 TxId = 0;
    TActorId SchemePipeActorId;
};

//// Table actions

template<typename TDerived, typename TResponse>
class TQueryBase : public NKikimr::TQueryBase, public TQueryRetryActorMixin<TDerived, TResponse> {
public:
    using TThis = TDerived;

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

    void PersistQueryInfo(const NKikimrKqp::TStreamingQueryState& state, const TTxControl& txControl, const std::optional<TInstant> expireAt = std::nullopt) {
        const TString sql = fmt::format(R"(
                DECLARE $database_id AS Text;
                DECLARE $query_path AS Text;
                DECLARE $state AS Json;
                DECLARE $expire_at AS Optional<Timestamp>;

                UPSERT INTO `{table}` (
                    database_id, query_path, state, expire_at
                ) VALUES (
                    $database_id, $query_path, $state, $expire_at
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
                .Build()
            .AddParam("$expire_at")
                .OptionalTimestamp(expireAt)
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
    TValueStatus<NKikimrKqp::TStreamingQueryState> ParseQueryInfo() {
        if (ResultSets.size() != 1) {
            return TStatus::Fail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            return TStatus::Fail(Ydb::StatusIds::NOT_FOUND, "No such steaming query");
        }

        ExpireAt = result.ColumnParser(TStreamingQueryConfig::TColumns::ExpireAt).GetOptionalTimestamp();

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
    std::optional<TInstant> ExpireAt;
};

// Update column "state" of .metadata/streaming/queries table if OperationActorId is not changed

class TUpdateStreamingQueryStateRequestActor final : public TQueryBase<TUpdateStreamingQueryStateRequestActor, TEvPrivate::TEvUpdateStreamingQueryResult> {
public:
    TUpdateStreamingQueryStateRequestActor(const TString& databaseId, const TString& queryPath, NKikimrKqp::TStreamingQueryState state)
        : TQueryBase(__func__, databaseId, queryPath)
        , State(std::move(state))
    {}

private:
    void OnRunQuery() final {
        YDB_LOG_DEBUG("[StreamingQueries] Updating streaming query state",
            {"logPrefix", LogPrefix()},
            {"queryState", LogQueryState(State)});
        SetQueryResultHandler(&TThis::OnGetQueryInfo, "Get query info");
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
            YDB_LOG_ERROR("[StreamingQueries] Streaming query lock owner changed during operation",
                {"logPrefix", LogPrefix()},
                {"currentOwner", currentOwner},
                {"previousOwner", previousOwner});
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query was changed during operation");
            return;
        }

        UpdateQuery();
    }

    void UpdateQuery() {
        SetQueryResultHandler(&TThis::OnQueryResult, "Update query info");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvUpdateStreamingQueryResult(status, std::move(issues)));
    }

    const NKikimrKqp::TStreamingQueryState State;
};

// Lock / unlock query row in table .metadata/streaming/queries to prevent concurrent modifications.
// Updates OperationActorId according to current operation.
// If OperationActorId already filled, actor will be checked.
//
// **note:** Lock may be lost during operation execution, stale operation will be
//           stopped on TUpdateStreamingQueryStateRequestActor fail.

class TUnlockStreamingQueryRequestActor final : public TQueryBase<TUnlockStreamingQueryRequestActor, TEvPrivate::TEvUnlockStreamingQueryResult> {
public:
    struct TSettings {
        TActorId OperationOwner;
        bool RemoveQuery = false;
        ui64 NewAlterVersion = 0;
    };

    TUnlockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TSettings& settings)
        : TQueryBase(__func__, databaseId, queryPath)
        , Settings(settings)
    {}

private:
    void OnRunQuery() final {
        YDB_LOG_DEBUG("[StreamingQueries] Unlocking streaming query",
            {"logPrefix", LogPrefix()},
            {"operationOwner", Settings.OperationOwner},
            {"removeQuery", Settings.RemoveQuery});
        SetQueryResultHandler(&TThis::OnGetQueryInfo, "Get query info");
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

            if (Settings.OperationOwner != currentOperationOwner) {
                YDB_LOG_ERROR("[StreamingQueries] Streaming query lock owner changed during operation",
                    {"logPrefix", LogPrefix()},
                    {"currentOperationOwner", currentOperationOwner},
                    {"expectedOwner", Settings.OperationOwner});
                Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query was changed during operation (got unexpected owner id)");
                return;
            }
        } else {
            YDB_LOG_ERROR("[StreamingQueries] Streaming query lock was lost",
                {"logPrefix", LogPrefix()});
            Finish(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query was changed during operation (got empty owner id)");
            return;
        }

        if (Settings.RemoveQuery) {
            YDB_LOG_DEBUG("[StreamingQueries] Delete streaming query from table",
                {"logPrefix", LogPrefix()});
            RemoveQuery();
        } else {
            YDB_LOG_DEBUG("[StreamingQueries] Remove streaming query lock",
                {"logPrefix", LogPrefix()},
                {"operationOwner", Settings.OperationOwner});
            UnlockQuery();
        }
    }

    void UnlockQuery() {
        State.ClearOperationActorId();
        State.ClearOperationOwnerGeneration();
        State.MutableSchemeInfo()->SetAlterVersion(Settings.NewAlterVersion);

        SetQueryResultHandler(&TThis::OnQueryResult, "Unlock query");
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

        SetQueryResultHandler(&TThis::OnQueryResult, "Delete query");
        ExecuteQuery(__func__, sql, &params, TTxControl::ContinueAndCommitTx());
    }

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(const Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvUnlockStreamingQueryResult(status, std::move(issues)));
    }

    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
};

class TLockStreamingQueryRequestActor final : public TQueryBase<TLockStreamingQueryRequestActor, TEvPrivate::TEvLockStreamingQueryResult> {
    // In case of stale operation continuation, query row will be removed by TTL (if not removed after lock)
    static constexpr TDuration INITIAL_OPERATION_TTL = TDuration::Minutes(30);

public:
    struct TSettings {
        TActorId OperationOwner;
        TActorId PreviousOwner;
        bool CreateIfNotExists = true;
        ui64 LockGeneration = 0;
        TPathId QueryPathId;
        ui64 ExpectedAlterVersion = 0;
    };

    TLockStreamingQueryRequestActor(const TString& databaseId, const TString& queryPath, const TSettings& settings)
        : TQueryBase(__func__, databaseId, queryPath)
        , Settings(settings)
    {
        Y_VALIDATE(Settings.ExpectedAlterVersion > 0, "Alter version must be positive");
    }

private:
    void OnRunQuery() final {
        YDB_LOG_DEBUG("[StreamingQueries] Locking streaming query",
            {"logPrefix", LogPrefix()},
            {"operationOwner", Settings.OperationOwner},
            {"previousOwner", Settings.PreviousOwner},
            {"lockGeneration", Settings.LockGeneration},
            {"expectedAlterVersion", Settings.ExpectedAlterVersion});

        SetQueryResultHandler(&TThis::OnGetQueryInfo, "Get query info");
        ReadQueryInfo(TTxControl::BeginTx());
    }

    void OnGetQueryInfo() {
        auto result = ParseQueryInfo();
        if (result.IsFail()) {
            if (result.GetStatus() != Ydb::StatusIds::NOT_FOUND) {
                return FinishWithStatus(result);
            }

            QueryInfoNotFound = true;
            YDB_LOG_INFO("[StreamingQueries] Streaming query not found",
                {"logPrefix", LogPrefix()},
                {"createIfNotExists", Settings.CreateIfNotExists});

            if (Settings.CreateIfNotExists) {
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_CREATING);
                LockQuery(TInstant::Now() + INITIAL_OPERATION_TTL);
            } else {
                Finish();
            }
            return;
        }

        State = result.DetachResult();

        if (State.HasOperationActorId()) {
            if (!ScriptExecutionRunnerActorIdFromString(State.GetOperationActorId(), StaleOwner)) {
                Finish(Ydb::StatusIds::INTERNAL_ERROR, "Streaming query operation owner actor id is corrupted");
                return;
            }

            Y_VALIDATE(StaleOwner, "Operation owner id must be non-empty");
        }

        if (!CheckOperationFinished()) {
            return;
        }

        // The owner may have changed while its predecessor was being checked.
        if (StaleOwner && StaleOwner != Settings.OperationOwner && StaleOwner != Settings.PreviousOwner) {
            return Finish();
        }

        StaleOwner = {};
        YDB_LOG_DEBUG("[StreamingQueries] Creating new lock",
            {"logPrefix", LogPrefix()},
            {"hasOperationActorId", State.HasOperationActorId()});
        LockQuery(ExpireAt);
    }

    void LockQuery(std::optional<TInstant> expireAt = std::nullopt) {
        IsTemporary = expireAt.has_value();
        State.SetOperationActorId(ScriptExecutionRunnerActorIdString(Settings.OperationOwner));
        State.SetOperationOwnerGeneration(Settings.LockGeneration);

        SetQueryResultHandler(&TThis::OnQueryResult, "Lock query");
        PersistQueryInfo(State, TTxControl::ContinueAndCommitTx(), expireAt);
    }

    void OnQueryResult() final {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) final {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, {
            .State = std::move(State),
            .StaleOwner = StaleOwner,
            .OperationAlreadyFinished = OperationAlreadyFinished,
            .QueryInfoNotFound = QueryInfoNotFound,
            .IsTemporary = status == Ydb::StatusIds::SUCCESS && IsTemporary,
        }, std::move(issues)));
    }

    bool CheckOperationFinished() {
        const auto& schemeInfo = State.GetSchemeInfo();
        const auto currentOwnerId = schemeInfo.GetOwnerSchemeshardId();
        const auto currentLocalPathId = schemeInfo.GetLocalPathId();
        if (currentOwnerId && currentLocalPathId && (currentOwnerId != Settings.QueryPathId.OwnerId || currentLocalPathId != Settings.QueryPathId.LocalPathId)) {
            if (currentOwnerId == Settings.QueryPathId.OwnerId && currentLocalPathId < Settings.QueryPathId.LocalPathId) {
                // Older version compatibility
                State.ClearSchemeInfo();
                State.ClearOperationOwnerGeneration();
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_DELETING);
                ExpireAt = TInstant::Now() + INITIAL_OPERATION_TTL;
                return true;
            }

            OperationAlreadyFinished = true;
            Finish(Ydb::StatusIds::SUCCESS, TStringBuilder()
                << "Streaming query path id changed on: " << currentOwnerId << "." << currentLocalPathId
                << " when expected: " << Settings.QueryPathId.OwnerId << "." << Settings.QueryPathId.LocalPathId
            );
            return false;
        }

        if (schemeInfo.GetAlterVersion() && schemeInfo.GetAlterVersion() >= Settings.ExpectedAlterVersion) {
            OperationAlreadyFinished = true;
            Finish(Ydb::StatusIds::SUCCESS, TStringBuilder() << "Streaming query alter version changed on: " << schemeInfo.GetAlterVersion() << " when expected value less then: " << Settings.ExpectedAlterVersion);
            return false;
        }

        if (State.GetOperationOwnerGeneration() > Settings.LockGeneration) {
            OperationAlreadyFinished = true;
            Finish(Ydb::StatusIds::SUCCESS, TStringBuilder()
                << "Streaming query lock belongs to a newer operation generation: " << State.GetOperationOwnerGeneration()
                << ", requested: " << Settings.LockGeneration);
            return false;
        }

        return true;
    }

    const TSettings Settings;
    NKikimrKqp::TStreamingQueryState State;
    TActorId StaleOwner;
    bool OperationAlreadyFinished = false;
    bool QueryInfoNotFound = false;
    bool IsTemporary = false;
};

class TPingStreamingQueryTableActor final : public TActionActorBase<TPingStreamingQueryTableActor> {
    using TBase = TActionActorBase<TPingStreamingQueryTableActor>;
    using TRetryPolicy = IRetryPolicy<bool>;

    static constexpr TDuration CHECK_ALIVE_REQUEST_SOFT_TIMEOUT = TDuration::Seconds(30); // Advance on each retry of check alive
    static constexpr TDuration CHECK_ALIVE_REQUEST_HARD_TIMEOUT = TDuration::Seconds(60); // Hard timeout for all retries
    inline static const ui64 MAX_CHECK_ALIVE_RETRIES = 50;

    enum class EWakeup {
        RetryCheckAlive,
        CheckAliveSoftTimeout,
        CheckAliveHardTimeout,
    };

public:
    using TBase::LogPrefix;

    TPingStreamingQueryTableActor(const TString& queryPath, const TActorId& pingActorId)
        : TBase(__func__, queryPath)
        , PingActorId(pingActorId)
    {
        Y_VALIDATE(PingActorId, "Missing destination actor id");
    }

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping",
            {"logPrefix", LogPrefix()},
            {"pingActorId", PingActorId});

        if (PingActorId.NodeId() != SelfId().NodeId()) {
            CheckAliveFlags |= IEventHandle::FlagSubscribeOnSession;
            SubscribedOnSession = PingActorId.NodeId();
        }

        Send(PingActorId, new TEvPrivate::TEvCheckAliveRequest(), CheckAliveFlags);
        Schedule(CHECK_ALIVE_REQUEST_SOFT_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveSoftTimeout)));
        Schedule(CHECK_ALIVE_REQUEST_HARD_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveHardTimeout)));
        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCheckAliveResponse, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        IgnoreFunc(TEvInterconnect::TEvNodeConnected);
    )

    void Handle(TEvPrivate::TEvCheckAliveResponse::TPtr& ev) {
        YDB_LOG_INFO("[StreamingQueries] Previous query owner is alive",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender});
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeup>(ev->Get()->Tag)) {
            case EWakeup::RetryCheckAlive: {
                WaitRetryCheckAlive = false;
                YDB_LOG_DEBUG("[StreamingQueries] Retrying check-alive request",
                    {"logPrefix", LogPrefix()},
                    {"pingActorId", PingActorId});
                Send(PingActorId, new TEvPrivate::TEvCheckAliveRequest(), CheckAliveFlags);
                Schedule(CHECK_ALIVE_REQUEST_SOFT_TIMEOUT, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::CheckAliveSoftTimeout)));
                break;
            }
            case EWakeup::CheckAliveSoftTimeout: {
                YDB_LOG_WARN("[StreamingQueries] Deliver streaming query owner check alive request timed out, retry check alive",
                    {"logPrefix", LogPrefix()},
                    {"pingActorId", PingActorId});
                RetryCheckAlive(/* longDelay */ false);
                break;
            }
            case EWakeup::CheckAliveHardTimeout: {
                FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Deliver streaming query owner " << PingActorId << " check alive request timed out");
                break;
            }
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (const auto reason = ev->Get()->Reason; reason == TEvents::TEvUndelivered::ReasonActorUnknown) {
            FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query operation owner " << PingActorId << " not found");
        } else {
            YDB_LOG_WARN("[StreamingQueries] Failed to deliver check-alive request to the query owner node",
                {"logPrefix", LogPrefix()},
                {"sender", ev->Sender},
                {"reason", reason});
            RetryCheckAlive(/* longDelay */ true);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        YDB_LOG_WARN("[StreamingQueries] Node with streaming query operation owner was disconnected, retry check alive",
            {"logPrefix", LogPrefix()},
            {"nodeId", ev->Get()->NodeId});
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
        Send(Owner, new TEvPrivate::TEvPingOperationOwnerResult(status, std::move(Issues)));
    }

private:
    void RetryCheckAlive(bool longDelay) {
        if (std::exchange(WaitRetryCheckAlive, true)) {
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
            YDB_LOG_DEBUG("[StreamingQueries] Scheduling check-alive retry",
                {"logPrefix", LogPrefix()},
                {"retryDelay", *delay});
            Schedule(*delay, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RetryCheckAlive)));
        } else {
            FatalError(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded for streaming query operation owner " << PingActorId << " check alive");
        }
    }

private:
    const TActorId PingActorId;
    ui64 CheckAliveFlags = IEventHandle::FlagTrackDelivery;
    std::optional<ui32> SubscribedOnSession;
    TRetryPolicy::IRetryState::TPtr CheckAliveRetryState;
    bool WaitRetryCheckAlive = false;
};

class TLockStreamingQueryTableActor final : public TActionActorBase<TLockStreamingQueryTableActor> {
    using TBase = TActionActorBase<TLockStreamingQueryTableActor>;

public:
    using TBase::LogPrefix;

    struct TSettings {
        TActorId OperationOwner;
        bool CreateIfNotExists = true;
        ui64 LockGeneration = 0;
        TPathId QueryPathId;
        ui64 ExpectedAlterVersion = 0;
    };

    TLockStreamingQueryTableActor(TExternalContext context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(std::move(context))
        , Settings(settings)
    {}

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping lock streaming query table actor",
            {"logPrefix", LogPrefix()},
            {"databaseId", Context.GetDatabaseId()},
            {"queryPath", QueryPath},
            {"operationOwner", Settings.OperationOwner},
            {"lockGeneration", Settings.LockGeneration},
            {"expectedAlterVersion", Settings.ExpectedAlterVersion});
        StartLockStreamingQueryRequestActor();

        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvPingOperationOwnerResult, Handle);
        hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleFinish);
        hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, HandleFinish);
    )

private:
    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        Info = ev->Get()->Info;

        if (HandleResult(ev, "Lock streaming query")) {
            return;
        }

        YDB_LOG_DEBUG("[StreamingQueries] Lock streaming query finished",
            {"logPrefix", LogPrefix()},
            {"state", LogQueryState(Info.State)},
            {"staleOwner", Info.StaleOwner},
            {"operationAlreadyFinished", Info.OperationAlreadyFinished},
            {"queryInfoNotFound", Info.QueryInfoNotFound},
            {"isTemporary", Info.IsTemporary});

        if (Info.IsTemporary) {
            // Validate a newly created or taken-over temporary row before clearing its TTL.
            const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(Context.GetDatabase(), TBase::QueryPath, NACLib::TUserToken(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})));
            YDB_LOG_DEBUG("[StreamingQueries] Start TDescribeStreamingQuerySchemeActor, validate streaming query existence",
                {"logPrefix", LogPrefix()},
                {"describerId", describerId});
            return;
        }

        if (Info.QueryInfoNotFound || Info.OperationAlreadyFinished || !Info.StaleOwner) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        const auto& checkerId = Register(new TPingStreamingQueryTableActor(QueryPath, Info.StaleOwner));
        YDB_LOG_INFO("[StreamingQueries] Starting check-alive request to previous owner",
            {"logPrefix", LogPrefix()},
            {"previousOwner", Info.StaleOwner},
            {"checkerId", checkerId});
    }

    void Handle(TEvPrivate::TEvPingOperationOwnerResult::TPtr& ev) {
        const auto status = ev->Get()->Status;
        if (status == Ydb::StatusIds::SUCCESS) {
            return Finish(Ydb::StatusIds::SUCCESS);
        }

        YDB_LOG_INFO("[StreamingQueries] Check-alive request finished, owner expired",
            {"logPrefix", LogPrefix()},
            {"status", status},
            {"issues", ev->Get()->Issues.ToOneLineString()},
            {"checkerId", ev->Sender});
        StartLockStreamingQueryRequestActor();
    }

    void Handle(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Describe streaming query")) {
            return;
        }

        const auto& info = ev->Get()->Info;
        YDB_LOG_INFO("[StreamingQueries] Describe streaming query finished",
            {"logPrefix", LogPrefix()},
            {"info", (info ? info->DebugString() : "<null>")});

        if (info && info->PathId == Settings.QueryPathId && info->Version == Settings.ExpectedAlterVersion) {
            // Reset TTL for query entry, streaming query existence validated
            auto& schemeInfo = *Info.State.MutableSchemeInfo();
            schemeInfo.SetOwnerSchemeshardId(Settings.QueryPathId.OwnerId);
            schemeInfo.SetLocalPathId(Settings.QueryPathId.LocalPathId);

            const auto& updaterId = Register(TUpdateStreamingQueryStateRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, Info.State));
            YDB_LOG_DEBUG("[StreamingQueries] Start TUpdateStreamingQueryStateRequestActor, reset TTL",
                {"logPrefix", LogPrefix()},
                {"updaterId", updaterId});
        } else {
            // Operation is stale, so just remove lock
            Info.OperationAlreadyFinished = true;
            const auto& unlockActorId = TBase::Register(TUnlockStreamingQueryRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, TUnlockStreamingQueryRequestActor::TSettings{
                .OperationOwner = Settings.OperationOwner,
                .RemoveQuery = true,
            }));
            YDB_LOG_DEBUG("[StreamingQueries] Start TUnlockStreamingQueryRequestActor",
                {"logPrefix", LogPrefix()},
                {"unlockActorId", unlockActorId});
        }
    }

    template <typename TEventPtr>
    void HandleFinish(TEventPtr& ev) {
        if (HandleResult(ev, "Finish streaming query lock")) {
            return;
        }

        Info.IsTemporary = false;
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvLockStreamingQueryResult(status, Info, std::move(Issues)));
    }

    void StartLockStreamingQueryRequestActor() {
        const auto& lockActorId = Register(TLockStreamingQueryRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, TLockStreamingQueryRequestActor::TSettings{
            .OperationOwner = Settings.OperationOwner,
            .PreviousOwner = Info.StaleOwner,
            .CreateIfNotExists = Settings.CreateIfNotExists,
            .LockGeneration = Settings.LockGeneration,
            .QueryPathId = Settings.QueryPathId,
            .ExpectedAlterVersion = Settings.ExpectedAlterVersion,
        }));
        YDB_LOG_DEBUG("[StreamingQueries] Start TLockStreamingQueryRequestActor",
            {"logPrefix", LogPrefix()},
            {"lockActorId", lockActorId});
    }

    const TExternalContext Context;
    const TSettings Settings;
    TEvPrivate::TEvLockStreamingQueryResult::TInfo Info;
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
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping cleanup streaming query state table actor",
            {"logPrefix", LogPrefix()},
            {"queryPath", QueryPath});
        Become(&TThis::StateFunc);
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
        if (HandleResult(ev, TStringBuilder() << "Cancel query execution (execution id: " << executionId << ")")) {
            return;
        }

        const auto entryExists = ev->Get()->ExecutionEntryExists;
        YDB_LOG_DEBUG("[StreamingQueries] Cancel streaming query execution finished",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"exists", entryExists},
            {"id", executionId});

        if (entryExists) {
            State.AddPreviousExecutionIds(executionId);
        }

        State.ClearCurrentExecutionId();
        StartUpdateState("clear current execution id");
    }

    void Handle(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie < State.PreviousExecutionIdsSize());

        const auto& executionId = State.GetPreviousExecutionIds(ev->Cookie);
        if (HandleResult(ev, TStringBuilder() << "Forget query execution (execution id: " << executionId << ")")) {
            return;
        }

        --OperationsToForget;
        YDB_LOG_DEBUG("[StreamingQueries] Forget streaming query execution finished",
            {"logPrefix", LogPrefix()},
            {"cookie", ev->Cookie},
            {"sender", ev->Sender},
            {"id", executionId},
            {"remains", OperationsToForget});

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
        const auto& updaterId = Register(TUpdateStreamingQueryStateRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        YDB_LOG_DEBUG("[StreamingQueries] Start TUpdateStreamingQueryStateRequestActor",
            {"logPrefix", LogPrefix()},
            {"updaterId", updaterId},
            {"info", info});
    }

    void ClearStreamingQueryExecutions() {
        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_DELETING);
            StartUpdateState("move query to deleting");
            return;
        }

        if (State.HasCurrentExecutionId()) {
            const auto& executionId = State.GetCurrentExecutionId();
            YDB_LOG_DEBUG("[StreamingQueries] Cancel streaming query execution",
                {"logPrefix", LogPrefix()},
                {"executionId", executionId});
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA, TEvCancelScriptExecutionOperation::TSettings{
                .FailOnNotFound = false,
                .FailOnAlreadyStopped = false,
            }));
            return;
        }

        if (State.PreviousExecutionIdsSize() > 0) {
            YDB_LOG_DEBUG("[StreamingQueries] Cleanup previous executions",
                {"logPrefix", LogPrefix()},
                {"previousExecutionIdsCount", State.PreviousExecutionIdsSize()});

            for (const auto& executionId : State.GetPreviousExecutionIds()) {
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA, TEvForgetScriptExecutionOperation::TSettings{
                    .FailOnNotFound = false,
                    .CancelIfRunning = true,
                }), OperationsToForget++);
                YDB_LOG_DEBUG("[StreamingQueries] Forget streaming query execution",
                    {"logPrefix", LogPrefix()},
                    {"operationsToForget", OperationsToForget},
                    {"executionId", executionId});
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

public:
    using TBase::LogPrefix;

    struct TSettings {
        NKikimrKqp::TStreamingQueryState InitialState;
        TPathId QueryPathId;
        ui64 QueryPathVersion = 0;
        TStreamingQuerySettings Info;
    };

    TStartStreamingQueryTableActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , Settings(settings)
        , State(settings.InitialState)
    {}

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrap: starting a new streaming query execution",
            {"logPrefix", LogPrefix()},
            {"textRevision", Settings.Info.QueryTextRevision},
            {"executionRevision", State.GetQueryTextRevision()},
            {"query", Settings.Info.QueryText});
        Y_VALIDATE(!State.HasCurrentExecutionId(), "Cannot start query, already started: " << State.GetCurrentExecutionId());

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
        if (HandleResult(ev, "Load previous query execution state")) {
            return;
        }

        if (Settings.Info.QueryTextRevision == State.GetQueryTextRevision()) {
            PreviousPhysicalGraph = std::move(ev->Get()->PhysicalGraph);
        }

        if (!ev->Get()->ExecutionEntryExists) {
            YDB_LOG_WARN("[StreamingQueries] Previous script execution not found, lease generation and graph was reset",
                {"logPrefix", LogPrefix()});
            State.ClearCheckpointId(); // We don't know previous generation, so must start from fresh checkpoint
        }

        PreviousGeneration = ev->Get()->Generation;
        YDB_LOG_DEBUG("[StreamingQueries] Finished loading previous query execution state",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"generation", PreviousGeneration},
            {"state", PreviousPhysicalGraph.has_value()});

        PrepareToStart();
    }

    void HandlePrepare(TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        const ui64 toCleanup = State.PreviousExecutionIdsSize() - MAX_QUERY_EXECUTIONS;
        Y_ABORT_UNLESS(ev->Cookie < toCleanup);

        const auto& executionId = State.GetPreviousExecutionIds(ev->Cookie);
        if (HandleResult(ev, TStringBuilder() << "Forget query execution (execution id: " << executionId << ")")) {
            return;
        }

        --OperationsToForget;
        YDB_LOG_DEBUG("[StreamingQueries] Forget streaming query execution finished",
            {"logPrefix", LogPrefix()},
            {"cookie", ev->Cookie},
            {"sender", ev->Sender},
            {"id", executionId},
            {"remains", OperationsToForget});

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
        YDB_LOG_DEBUG("[StreamingQueries] Script execution started, waiting for query state to be saved",
            {"logPrefix", LogPrefix()},
            {"created", ev->Get()->ExecutionId});

        GetScriptExecutionOperation();
    }

    void HandleStartQuery(TEvents::TEvWakeup::TPtr&) {
        const auto& executionId = State.GetCurrentExecutionId();
        YDB_LOG_DEBUG("[StreamingQueries] Fetching streaming query execution",
            {"logPrefix", LogPrefix()},
            {"executionId", executionId});
        SendToKqpProxy(std::make_unique<TEvGetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA, /* failOnNotFound */ true));
    }

    void HandleStartQuery(TEvGetScriptExecutionOperationResponse::TPtr& ev) {
        const auto& info = *ev->Get();
        YDB_LOG_DEBUG("[StreamingQueries] Received script execution info",
            {"logPrefix", LogPrefix()},
            {"saved", info.StateSaved},
            {"ready", info.Ready});

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
        Become(&TThis::StateFuncFinalize);

        if (status == Ydb::StatusIds::SUCCESS) {
            if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_RUNNING || Settings.Info.QueryTextRevision != State.GetQueryTextRevision()) {
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_RUNNING);
                State.SetQueryTextRevision(Settings.Info.QueryTextRevision);
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
        const auto& updaterId = Register(TUpdateStreamingQueryStateRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        YDB_LOG_DEBUG("[StreamingQueries] Start TUpdateStreamingQueryStateRequestActor",
            {"logPrefix", LogPrefix()},
            {"updaterId", updaterId},
            {"info", info});
    }

    void PrepareToStart() {
        Become(&TThis::StateFuncPrepare);

        if (State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_STARTING) {
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STARTING);
            UpdateQueryState("move query to starting");
            return;
        }

        if (!State.GetPreviousExecutionIds().empty() && !StateLoaded) {
            StateLoaded = true;
            const auto& executionId = *State.GetPreviousExecutionIds().rbegin();
            SendToKqpProxy(std::make_unique<TEvGetScriptExecutionPhysicalGraph>(Context.GetDatabase(), executionId));
            YDB_LOG_DEBUG("[StreamingQueries] Load previous query state",
                {"logPrefix", LogPrefix()},
                {"execution", executionId});
            return;
        }

        if (State.PreviousExecutionIdsSize() > MAX_QUERY_EXECUTIONS) {
            const auto toCleanup = State.PreviousExecutionIdsSize() - MAX_QUERY_EXECUTIONS;
            YDB_LOG_DEBUG("[StreamingQueries] Cleaning up previous executions that exceed the limit",
                {"logPrefix", LogPrefix()},
                {"toCleanup", toCleanup},
                {"executions", MAX_QUERY_EXECUTIONS});

            for (ui64 i = 0; i < toCleanup; ++i) {
                const auto& executionId = State.GetPreviousExecutionIds(i);
                SendToKqpProxy(std::make_unique<TEvForgetScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA, TEvForgetScriptExecutionOperation::TSettings{
                    .FailOnNotFound = false,
                    .CancelIfRunning = true
                }), OperationsToForget++);
                YDB_LOG_DEBUG("[StreamingQueries] Forget streaming query execution",
                    {"logPrefix", LogPrefix()},
                    {"operationsToForget", OperationsToForget},
                    {"executionId", executionId});
            }
            return;
        }

        // Execution id for streaming queries:
        // <GUID part>-<GUID part>-<GUID part>-<GUID part>-<SS id>-<Path id in SS>-<Path version in SS>
        // Checkpoint id for streaming queries:
        // <Execution id>-<Query path>

        const auto& pathId = Settings.QueryPathId;
        State.SetCurrentExecutionId(TStringBuilder() << CreateGuidAsString() << '-' << pathId.OwnerId << '-' << pathId.LocalPathId << '-' << Settings.QueryPathVersion);

        if (!State.GetCheckpointId()) {
            State.SetCheckpointId(TStringBuilder() << State.GetCurrentExecutionId() << '-' << QueryPath);
        }

        UpdateQueryState(TStringBuilder() << "allocate execution id: " << State.GetCurrentExecutionId() << ", checkpoint id: " << State.GetCheckpointId());
        Become(&TThis::StateFuncStartQuery);
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
        ev->StreamingQueryOperationId = State.GetOperationActorId();
        ev->CustomerSuppliedId = State.GetCurrentExecutionId();
        ev->WatermarkLateEventsPolicy = Settings.Info.WatermarkLateEventsPolicy;
        ev->StreamingDisposition = Settings.Info.StreamingDisposition;
        ev->CheckpointInterval = Settings.Info.CheckpointInterval;

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
        request.SetDatabaseId(Context.GetDatabaseId());
        request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        request.SetSyntax(Ydb::Query::SYNTAX_YQL_V1);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        request.SetKeepSession(false);
        request.SetPoolId(Settings.Info.ResourcePool);
        request.SetQuery(Settings.Info.QueryText);
        request.SetTimeoutMs(TDuration::Max().MilliSeconds());

        YDB_LOG_DEBUG("[StreamingQueries] Sending start streaming query request",
            {"logPrefix", LogPrefix()},
            {"id", State.GetCurrentExecutionId()});
        SendToKqpProxy(std::move(ev));
    }

    void GetScriptExecutionOperation() {
        if (!GetOperationRetryState) {
            // We will wait for query start without timeout, because otherwise query may not retry after start
            GetOperationRetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                []() {
                    return ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(100),
                TDuration::Seconds(1),
                std::numeric_limits<size_t>::max(),
                TDuration::Max()
            )->CreateRetryState();
        }

        const auto delay = GetOperationRetryState->GetNextRetryDelay();
        Y_VALIDATE(delay, "Retries unexpectedly finished");
        YDB_LOG_DEBUG("[StreamingQueries] Scheduling get script execution operation",
            {"logPrefix", LogPrefix()},
            {"retryDelay", *delay});
        Schedule(*delay, new TEvents::TEvWakeup());
    }

    static std::vector<NKikimrKqp::TScriptExecutionRetryState::TMapping> CreateDefaultRetryMapping() {
        // Retried all statuses except SUCCESS (user cancel clears retry policy in finalize).

        NKikimrKqp::TScriptExecutionRetryState::TMapping mapping;

        const auto* statusDescriptor = Ydb::StatusIds::StatusCode_descriptor();
        for (int i = 0; i < statusDescriptor->value_count(); ++i) {
            const auto status = static_cast<Ydb::StatusIds::StatusCode>(statusDescriptor->value(i)->number());
            if (status != Ydb::StatusIds::SUCCESS) {
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
        TSchemeInfo SchemeInfo;
    };

    TSyncStreamingQueryTableActor(const TExternalContext& context, const TString& queryPath, const TSettings& settings)
        : TBase(__func__, queryPath)
        , Context(context)
        , SchemeInfo(settings.SchemeInfo)
        , State(settings.InitialState)
    {
        QuerySettings.FromProto(SchemeInfo.Properties);
    }

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrap: syncing streaming query with scheme shard",
            {"logPrefix", LogPrefix()},
            {"info", SchemeInfo.DebugString()},
            {"status", NKikimrKqp::TStreamingQueryState::EStatus_Name(State.GetStatus())});

        if (!SchemeInfo.IsChanged(State)) {
            Finish(Ydb::StatusIds::SUCCESS);
            return;
        }

        SyncQuery();
    }

    STRICT_STFUNC(StateFuncRemoveQuery,
        hFunc(TEvPrivate::TEvCleanupStreamingQueryResult, HandleRemove);
        hFunc(TEvPrivate::TEvUpdateStreamingQueryResult, HandleRemove);
        hFunc(NFq::TEvCheckpointStorage::TEvDeleteGraphResponse, HandleRemove);
    )

    void HandleRemove(TEvPrivate::TEvCleanupStreamingQueryResult::TPtr& ev) {
        State = ev->Get()->Info;

        if (HandleResult(ev, "Cleanup streaming query")) {
            return;
        }

        RemoveQuery();
    }

    void HandleRemove(TEvPrivate::TEvUpdateStreamingQueryResult::TPtr& ev) {
        if (HandleResult(ev, "Update streaming query state (remove query)")) {
            return;
        }

        RemoveQuery();
    }

    void HandleRemove(NFq::TEvCheckpointStorage::TEvDeleteGraphResponse::TPtr& ev) {
        if (HandleResult(ev, "Delete checkpoints (recovery path)")) {
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
        if (HandleResult(ev, TStringBuilder() << "Cancel query execution (execution id: " << executionId << ")")) {
            return;
        }

        const auto entryExists = ev->Get()->ExecutionEntryExists;
        YDB_LOG_DEBUG("[StreamingQueries] Cancel streaming query execution finished",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"id", executionId},
            {"exists", entryExists});

        if (entryExists) {
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
        State = ev->Get()->Info;

        if (HandleResult(ev, "Start streaming query")) {
            return;
        }

        Finish(Ydb::StatusIds::SUCCESS);
    }

protected:
    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        Send(Owner, new TEvPrivate::TEvSyncStreamingQueryResult(status, State, std::move(Issues)));
    }

private:
    void UpdateQueryState(const TString& info) const {
        const auto& updaterId = Register(TUpdateStreamingQueryStateRequestActor::MakeRetry(SelfId(), Context.GetDatabaseId(), QueryPath, State));
        YDB_LOG_DEBUG("[StreamingQueries] Start TUpdateStreamingQueryStateRequestActor",
            {"logPrefix", LogPrefix()},
            {"updaterId", updaterId},
            {"info", info});
    }

    void RemoveQuery() {
        Become(&TThis::StateFuncRemoveQuery);

        if (State.HasCurrentExecutionId() || State.PreviousExecutionIdsSize() > 0 || State.GetStatus() != NKikimrKqp::TStreamingQueryState::STATUS_DELETING) {
            const auto& cleanupActorId = Register(new TCleanupStreamingQueryStateTableActor(Context, QueryPath, State));
            YDB_LOG_DEBUG("[StreamingQueries] Start TCleanupStreamingQueryStateTableActor (remove query)",
                {"logPrefix", LogPrefix()},
                {"cleanupActorId", cleanupActorId});
            return;
        }

        if (State.HasCheckpointId() && !CheckpointDeletionRequested) {
            CheckpointDeletionRequested = true;
            YDB_LOG_DEBUG("[StreamingQueries] Sending TEvDeleteGraphRequest (recovery path)",
                {"logPrefix", LogPrefix()},
                {"graphId", State.GetCheckpointId()});
            Send(NYql::NDq::MakeCheckpointStorageID(),
                 new NFq::TEvCheckpointStorage::TEvDeleteGraphRequest(State.GetCheckpointId()));
            return;
        }

        if (QuerySettings.InflightOperation != TStreamingQueryMeta::TOperations::Drop) {
            // Older version compatibility
            State.ClearCheckpointId();
            State.ClearQueryTextRevision();
            State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_CREATED);
            Become(&TThis::StateFunc);
            UpdateQueryState("finish previous query cleanup");
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
            YDB_LOG_DEBUG("[StreamingQueries] Cancel streaming query execution",
                {"logPrefix", LogPrefix()},
                {"executionId", executionId},
                {"info", info});
            SendToKqpProxy(std::make_unique<TEvCancelScriptExecutionOperation>(Context.GetDatabase(), OperationIdFromExecutionId(executionId), BUILTIN_ACL_METADATA, TEvCancelScriptExecutionOperation::TSettings{
                .FailOnNotFound = false,
                .FailOnAlreadyStopped = false,
            }));
            return;
        }

        State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
        UpdateQueryState(TStringBuilder() << "move to stopped" << " (" << info << ")");
    }

    void StartQuery() {
        Y_VALIDATE(!State.HasCurrentExecutionId(), "Cannot start query, already started: " << State.GetCurrentExecutionId());

        const auto& startActorId = Register(new TStartStreamingQueryTableActor(Context, QueryPath, {
            .InitialState = State,
            .QueryPathId = SchemeInfo.PathId,
            .QueryPathVersion = SchemeInfo.Version,
            .Info = QuerySettings,
        }));
        YDB_LOG_DEBUG("[StreamingQueries] Start TStartStreamingQueryTableActor",
            {"logPrefix", LogPrefix()},
            {"startActorId", startActorId});
    }

    void SyncQuery() {
        if (QuerySettings.InflightOperation == TStreamingQueryMeta::TOperations::Drop) {
            return RemoveQuery();
        }

        Become(&TThis::StateFunc);

        switch (State.GetStatus()) {
            case NKikimrKqp::TStreamingQueryState::STATUS_UNSPECIFIED:
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATING: {
                State.SetStatus(NKikimrKqp::TStreamingQueryState::STATUS_CREATED);
                UpdateQueryState("move to created");
                break;
            }
            case NKikimrKqp::TStreamingQueryState::STATUS_CREATED:
            case NKikimrKqp::TStreamingQueryState::STATUS_STOPPED: {
                if (QuerySettings.Run) {
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
    const TSchemeInfo SchemeInfo;
    TStreamingQuerySettings QuerySettings;
    NKikimrKqp::TStreamingQueryState State;
    bool CheckpointDeletionRequested = false;
};

//// Request handlers

// Common request handling pipeline:
// Describe -> Register operation in SS -> Describe -> Lock -> Sync -> Unlock -> Finish operation in SS

template <typename TDerived>
class TRequestHandlerBase : public TActionActorBase<TDerived> {
    using TBase = TActionActorBase<TDerived>;

public:
    using TBase::LogPrefix;

    TRequestHandlerBase(const TString& operationName, const TString& workingDir, const TString& queryName,
        const TExternalContext& context, IStreamingQueryOperationController::TPtr controller, const ui32 access, const ui64 lockGeneration)
        : TBase(operationName, workingDir, queryName)
        , Controller(std::move(controller))
        , Access(access)
        , LockGeneration(lockGeneration)
        , Context(context)
    {}

    STRICT_STFUNC(StateFuncBase,
        hFunc(TEvPrivate::TEvDescribeStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvLockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvSyncStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, Handle);
        hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, Handle);
        hFunc(TEvPrivate::TEvCheckAliveRequest, Handle);
    )

protected:
    virtual bool ValidateSchemeVersion(const TSchemeInfo& schemeInfo, const std::optional<TSchemeInfo>& previousInfo) const = 0;

    virtual bool HandleStreamingOperationStaleOwner(const TActorId& owner) {
        Y_UNUSED(owner);
        return false;
    }

    virtual std::optional<NKikimrSchemeOp::TModifyScheme> GetBeginSchemeTx() = 0;

    virtual std::optional<NKikimrSchemeOp::TModifyScheme> GetEndSchemeTx(bool success) = 0;

    void Handle(TEvPrivate::TEvLockStreamingQueryResult::TPtr& ev) {
        const auto& info = ev->Get()->Info;
        QueryState = info.State;

        if (TBase::HandleResult(ev, "Prepare streaming query before operation")) {
            return;
        }

        YDB_LOG_DEBUG("[StreamingQueries] Lock streaming query success",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender},
            {"staleOwner", info.StaleOwner},
            {"queryInfoNotFound", info.QueryInfoNotFound},
            {"operationAlreadyFinished", info.OperationAlreadyFinished});

        if (info.OperationAlreadyFinished || info.StaleOwner) {
            if (!info.StaleOwner || !HandleStreamingOperationStaleOwner(info.StaleOwner)) {
                TBase::FatalError(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query has multiple modifications inflight");
            }
            return;
        }

        if (info.QueryInfoNotFound && QuerySettings.InflightOperation != TStreamingQueryConfig::TOperations::Create) {
            if (QuerySettings.InflightOperation == TStreamingQueryConfig::TOperations::Drop) {
                TBase::Finish(Ydb::StatusIds::SUCCESS); // Just continue drop operation
            } else {
                TBase::FatalError(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query state not found, due to multiple modifications inflight");
            }
            return;
        }

        IsLockCreated = true;
        SyncQuery();
    }

    void Handle(TEvPrivate::TEvUnlockStreamingQueryResult::TPtr& ev) {
        IsLockCreated = false;

        if (TBase::HandleResult(ev, TStringBuilder() << "Unlock streaming query (operation status: " << FinalStatus << ")")) {
            return;
        }

        TBase::Finish(FinalStatus);
    }

    void Handle(TEvPrivate::TEvExecuteSchemeTransactionResult::TPtr& ev) {
        SchemeOperationStarted = ev->Get()->Status == Ydb::StatusIds::SUCCESS && !SchemeOperationStarted;

        if (TBase::HandleResult(ev, "Execute create scheme operation")) {
            return;
        }

        YDB_LOG_DEBUG("[StreamingQueries] Execute create scheme operation success",
            {"logPrefix", LogPrefix()},
            {"schemeOperationStarted", SchemeOperationStarted});

        if (SchemeOperationStarted) {
            DescribeQuery("fetch info after update");
        } else {
            TBase::Finish(FinalStatus);
        }
    }

    void DescribeQuery(const TString& info) const {
        // Access by user token will be checked during scheme transaction execution
        const auto& describerId = TBase::Register(new TDescribeStreamingQuerySchemeActor(Context.GetDatabase(), TBase::QueryPath, NACLib::TUserToken(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{})));
        YDB_LOG_DEBUG("[StreamingQueries] Start TDescribeStreamingQuerySchemeActor",
            {"logPrefix", LogPrefix()},
            {"describerId", describerId},
            {"info", info});
    }

private:
    void Handle(TEvPrivate::TEvDescribeStreamingQueryResult::TPtr& ev) {
        if (TBase::HandleResult(ev, "Describe streaming query")) {
            return;
        }

        auto previousInfo = std::exchange(SchemeInfo, ev->Get()->Info);
        if (Context.GetUserToken() && Context.GetUserToken()->GetSerializedToken() && SchemeInfo && Access) {
            if (!SchemeInfo->SecurityObject) {
                return TBase::FatalError(Ydb::StatusIds::INTERNAL_ERROR, "Missing streaming query security object");
            }

            if (const auto& securityObject = *SchemeInfo->SecurityObject; !securityObject.CheckAccess(Access, *Context.GetUserToken())) {
                YDB_LOG_WARN("[StreamingQueries] Access denied",
                    {"logPrefix", LogPrefix()},
                    {"userSid", Context.GetUserToken()->GetUserSID()},
                    {"access", Access});

                if (!securityObject.CheckAccess(NACLib::DescribeSchema, *Context.GetUserToken())) {
                    TBase::FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << TBase::QueryPath << " not found or you don't have access permissions");
                } else {
                    TBase::FatalError(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for streaming query " << TBase::QueryPath);
                }

                return;
            }
        }

        YDB_LOG_DEBUG("[StreamingQueries] Describe streaming query success",
            {"logPrefix", LogPrefix()},
            {"schemeInfo", (SchemeInfo ? SchemeInfo->DebugString() : "null")});

        if (!SchemeOperationStarted) {
            ExecuteSchemeTransaction("start streaming query operation");
            return;
        }

        if (!SchemeInfo || !ValidateSchemeVersion(*SchemeInfo, previousInfo)) {
            SchemeOperationStarted = false;
            return TBase::FatalError(Ydb::StatusIds::PRECONDITION_FAILED, "Streaming query info was changed due to multiple modifications inflight");
        }

        QuerySettings.FromProto(SchemeInfo->Properties);
        LockQuery();
    }

    void Handle(TEvPrivate::TEvSyncStreamingQueryResult::TPtr& ev) {
        QueryState = ev->Get()->Info;

        if (TBase::HandleResult(ev, "Streaming query initialization")) {
            return;
        }

        YDB_LOG_DEBUG("[StreamingQueries] Sync with scheme shard succeeded",
            {"logPrefix", LogPrefix()},
            {"queryState", LogQueryState(QueryState)},
            {"existsInSchemeShard", SchemeInfo.has_value()});

        TBase::Finish(Ydb::StatusIds::SUCCESS);
    }

    void Handle(TEvPrivate::TEvCheckAliveRequest::TPtr& ev) {
        YDB_LOG_NOTICE("[StreamingQueries] Received check-alive request",
            {"logPrefix", LogPrefix()},
            {"sender", ev->Sender});
        TBase::Send(ev->Sender, new TEvPrivate::TEvCheckAliveResponse());
    }

    bool BeforeFinish(Ydb::StatusIds::StatusCode status) final {
        FinalStatus = status;

        if (IsLockCreated) {
            UnlockQuery();
            return true;
        }

        if (SchemeOperationStarted) {
            ExecuteSchemeTransaction("finish streaming query operation");
            return true;
        }

        return false;
    }

    void OnFinish(Ydb::StatusIds::StatusCode status) final {
        if (status == Ydb::StatusIds::SUCCESS) {
            Controller->OnAlteringFinished();
        } else {
            Controller->OnAlteringFinishedWithStatus(TStreamingQueryConfig::TStatus::Fail(TBase::YdbStatusToYqlStatus(status), TBase::Issues.ToString()));
        }
    }

    void ExecuteSchemeTransaction(const TString& info) {
        auto schemeTx = SchemeOperationStarted ? GetEndSchemeTx(FinalStatus == Ydb::StatusIds::SUCCESS) : GetBeginSchemeTx();
        if (!schemeTx) {
            return;
        }

        if (SchemeInfo) {
            schemeTx->MutableApplyIf()->Clear();

            auto& applyIf = *schemeTx->AddApplyIf();
            applyIf.SetPathId(SchemeInfo->PathId.LocalPathId);
            applyIf.SetPathVersion(SchemeInfo->Version);
            applyIf.SetCheckEntityVersion(true);
        }

        if (schemeTx->HasCreateStreamingQuery()) {
            auto& create = *schemeTx->MutableCreateStreamingQuery();

            if (SchemeOperationStarted) {
                schemeTx->SetReplaceIfExists(true);
                create.ClearOperationOwnerActorId();
                create.MutableProperties()->MutableProperties()->erase(TStreamingQueryConfig::TProperties::InflightOperation);
            } else {
                ActorIdToProto(TBase::SelfId(), create.MutableOperationOwnerActorId());
            }
        }

        auto token = Context.GetUserToken();
        if (SchemeOperationStarted || (Access & NACLib::RemoveSchema)) {
            // DROP registers its operation with an internal ALTER after checking the user's RemoveSchema permission.
            token = NACLib::TSystemUsers::Metadata();
            token->SaveSerializationInfo();
        }
        const auto& executerId = TBase::Register(new TExecuteTransactionSchemeActor(Context.GetDatabase(), TBase::QueryPath, *schemeTx, token));
        YDB_LOG_DEBUG("[StreamingQueries] Start TExecuteTransactionSchemeActor",
            {"logPrefix", LogPrefix()},
            {"executerId", executerId},
            {"info", info});
    }

    void SyncQuery() const {
        Y_VALIDATE(SchemeInfo, "Cannot sync query without scheme information");
        const auto& syncActorId = TBase::Register(new TSyncStreamingQueryTableActor(Context, TBase::QueryPath, {
            .InitialState = QueryState,
            .SchemeInfo = *SchemeInfo,
        }));
        YDB_LOG_DEBUG("[StreamingQueries] Start TSyncStreamingQueryTableActor (sync previous state)",
            {"logPrefix", LogPrefix()},
            {"syncActorId", syncActorId});
    }

    void LockQuery() const {
        Y_VALIDATE(SchemeInfo, "Cannot lock query without scheme information");
        const auto& lockActorId = TBase::Register(new TLockStreamingQueryTableActor(Context, TBase::QueryPath, {
            .OperationOwner = TBase::SelfId(),
            .CreateIfNotExists = QuerySettings.InflightOperation == TStreamingQueryConfig::TOperations::Create,
            .LockGeneration = LockGeneration,
            .QueryPathId = SchemeInfo->PathId,
            .ExpectedAlterVersion = SchemeInfo->Version,
        }));
        YDB_LOG_DEBUG("[StreamingQueries] Start TLockStreamingQueryTableActor",
            {"logPrefix", LogPrefix()},
            {"lockActorId", lockActorId});
    }

    void UnlockQuery() const {
        Y_VALIDATE(SchemeInfo, "Cannot unlock query without scheme information");
        const auto& unlockActorId = TBase::Register(TUnlockStreamingQueryRequestActor::MakeRetry(TBase::SelfId(), Context.GetDatabaseId(), TBase::QueryPath, TUnlockStreamingQueryRequestActor::TSettings{
            .OperationOwner = TBase::SelfId(),
            .RemoveQuery = FinalStatus == Ydb::StatusIds::SUCCESS && QuerySettings.InflightOperation == TStreamingQueryConfig::TOperations::Drop,
            .NewAlterVersion = SchemeInfo->Version,
        }));
        YDB_LOG_DEBUG("[StreamingQueries] Start TUnlockStreamingQueryRequestActor",
            {"logPrefix", LogPrefix()},
            {"unlockActorId", unlockActorId});
    }

    const IStreamingQueryOperationController::TPtr Controller;
    const ui32 Access = 0;
    const ui64 LockGeneration = 0;
    NKikimrKqp::TStreamingQueryState QueryState;
    Ydb::StatusIds::StatusCode FinalStatus = Ydb::StatusIds::SUCCESS;

protected:
    const TExternalContext Context;
    std::optional<TSchemeInfo> SchemeInfo;
    TStreamingQuerySettings QuerySettings;
    bool SchemeOperationStarted = false;
    bool IsLockCreated = false;
};

template <typename TDerived>
class TUserRequestHandlerBase : public TRequestHandlerBase<TDerived> {
    using TBase = TRequestHandlerBase<TDerived>;

public:
    using TBase::LogPrefix;

    TUserRequestHandlerBase(const TString& operationName, const NKikimrSchemeOp::TModifyScheme& schemeTx, const TString& queryName, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller, const ui32 access)
        : TBase(operationName, schemeTx.GetWorkingDir(), queryName, context, std::move(controller), access, /* lockGeneration */ 0)
        , SchemeTx(schemeTx)
    {}

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping request handler",
            {"logPrefix", LogPrefix()});

        TBase::Become(&TBase::StateFuncBase);
        TBase::DescribeQuery("start handling");
    }

private:
    bool ValidateSchemeVersion(const TSchemeInfo& schemeInfo, const std::optional<TSchemeInfo>& previousInfo) const final {
        if (schemeInfo.InflightOperationOwnerId) {
            return schemeInfo.InflightOperationOwnerId == TBase::SelfId();
        }

        // Older version compatibility
        return schemeInfo.Properties.GetProperties().contains(TStreamingQueryConfig::TProperties::InflightOperation)
            && schemeInfo.Version == (previousInfo ? previousInfo->Version + 1 : 1)
            && (!previousInfo || schemeInfo.PathId == previousInfo->PathId);
    }

    std::optional<NKikimrSchemeOp::TModifyScheme> GetEndSchemeTx(bool success) override {
        Y_UNUSED(success);
        auto pathPairStatus = TStreamingQueryManager::SplitPath(TBase::QueryPath, TBase::Context.GetDatabase(), /* createDir */ false);
        Y_VALIDATE(!pathPairStatus.IsFail(), "Failed to split path");
        const auto& [workingDir, name] = pathPairStatus.DetachResult();

        auto result = SchemeTx;
        result.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
        result.SetWorkingDir(workingDir);
        result.MutableCreateStreamingQuery()->SetName(name);
        return result;
    }

    static NYql::NPq::NProto::StreamingDisposition GetDefaultStreamingDisposition() {
        NYql::NPq::NProto::StreamingDisposition result;
        result.mutable_from_last_checkpoint()->set_force(true);
        return result;
    }

protected:
    static inline const TString DefaultStreamingDisposition = GetDefaultStreamingDisposition().SerializeAsString();
    NKikimrSchemeOp::TModifyScheme SchemeTx;
};

class TCreateStreamingQueryActor final : public TUserRequestHandlerBase<TCreateStreamingQueryActor> {
    using TBase = TUserRequestHandlerBase<TCreateStreamingQueryActor>;

public:
    TCreateStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetCreateStreamingQuery().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::CreateTable)
    {}

private:
    std::optional<NKikimrSchemeOp::TModifyScheme> GetBeginSchemeTx() final {
        if (SchemeInfo && !SchemeTx.GetReplaceIfExists()) {
            if (SchemeTx.GetFailedOnAlreadyExists()) {
                FatalError(Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder() << "Streaming query " << QueryPath << " already exists");
            } else {
                Finish(Ydb::StatusIds::SUCCESS);
            }
            return std::nullopt;
        }

        if (const auto& status = ValidateProperties(); status.IsFail()) {
            FatalError(status.GetStatus(), AddRootIssue("Invalid properties for creation new streaming query", status.GetErrorDescription()));
            return std::nullopt;
        }

        return SchemeTx;
    }

    TStatus ValidateProperties() {
        using ESqlSettings = TStreamingQueryConfig::TSqlSettings;
        using EName = TStreamingQueryConfig::TProperties;

        TPropertyValidator validator(*SchemeTx.MutableCreateStreamingQuery()->MutableProperties());
        CHECK_STATUS(validator.SaveRequired(ESqlSettings::QUERY_TEXT_FEATURE, &TPropertyValidator::ValidateNotEmpty));
        CHECK_STATUS(validator.SaveDefault(EName::Run, "true", &TPropertyValidator::ValidateBool));
        CHECK_STATUS(validator.SaveDefault(EName::ResourcePool, ""));
        CHECK_STATUS(validator.SaveDefault(EName::WatermarkLateEventsPolicy, "drop", &TPropertyValidator::ValidateEnum<NYql::NHoppingWindow::EPolicy>));
        CHECK_STATUS(validator.SaveDefault(EName::StreamingDisposition, DefaultStreamingDisposition));
        CHECK_STATUS(validator.SaveDefault(EName::CheckpointInterval, "", &TPropertyValidator::ValidateInterval<TPropertyValidator::MAX_PROTOBUF_DURATION_MICROSECONDS>));
        CHECK_STATUS(validator.Save(EName::InflightOperation, TStreamingQueryConfig::TOperations::Create));
        CHECK_STATUS(validator.Save(
            EName::QueryTextRevision,
            ToString(SchemeInfo ? TStreamingQuerySettings().FromProto(SchemeInfo->Properties).QueryTextRevision + 1 : 1)
        ));

        return validator.Finish();
    }
};

class TAlterStreamingQueryActor final : public TUserRequestHandlerBase<TAlterStreamingQueryActor> {
    using TBase = TUserRequestHandlerBase<TAlterStreamingQueryActor>;

public:
    TAlterStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetCreateStreamingQuery().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::AlterSchema)
    {}

private:
    std::optional<NKikimrSchemeOp::TModifyScheme> GetBeginSchemeTx() final {
        if (!SchemeInfo) {
            if (SchemeTx.GetSuccessOnNotExist()) {
                Finish(Ydb::StatusIds::SUCCESS);
            } else {
                FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            }
            return std::nullopt;
        }

        if (const auto& status = ValidateProperties(TStreamingQuerySettings().FromProto(SchemeInfo->Properties)); status.IsFail()) {
            FatalError(status.GetStatus(), AddRootIssue("Invalid properties for alter streaming query", status.GetErrorDescription()));
            return std::nullopt;
        }

        return SchemeTx;
    }

    TStatus ValidateProperties(const TStreamingQuerySettings& previousSettings) {
        using ESqlSettings = TStreamingQueryConfig::TSqlSettings;
        using EName = TStreamingQueryConfig::TProperties;

        TPropertyValidator validator(*SchemeTx.MutableCreateStreamingQuery()->MutableProperties());
        CHECK_STATUS(validator.SaveDefault(EName::Run, previousSettings.Run ? "true" : "false", &TPropertyValidator::ValidateBool));
        CHECK_STATUS(validator.SaveDefault(EName::ResourcePool, previousSettings.ResourcePool));
        CHECK_STATUS(validator.SaveDefault(EName::CheckpointInterval, previousSettings.CheckpointIntervalString, &TPropertyValidator::ValidateInterval<TPropertyValidator::MAX_PROTOBUF_DURATION_MICROSECONDS>));
        CHECK_STATUS_RET(force, validator.ExtractDefault(EName::Force, "false", &TPropertyValidator::ValidateBool));
        CHECK_STATUS_RET(queryText, validator.ExtractOptional(ESqlSettings::QUERY_TEXT_FEATURE, &TPropertyValidator::ValidateNotEmpty));
        CHECK_STATUS_RET(streamingDisposition, validator.ExtractOptional(EName::StreamingDisposition));
        CHECK_STATUS_RET(watermarkLateEventsPolicy, validator.ExtractOptional(EName::WatermarkLateEventsPolicy, &TPropertyValidator::ValidateEnum<NYql::NHoppingWindow::EPolicy>));

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
        } else if (watermarkLateEventsPolicy.GetResult()
            && *watermarkLateEventsPolicy.GetResult() != (previousSettings.WatermarkLateEventsPolicy ? previousSettings.WatermarkLateEventsPolicy : "drop")) {
            queryTestRevision++;
        }

        CHECK_STATUS(validator.Save(ESqlSettings::QUERY_TEXT_FEATURE, queryTextValue.value_or(previousSettings.QueryText)));
        CHECK_STATUS(validator.Save(EName::QueryTextRevision, ToString(queryTestRevision)));
        CHECK_STATUS(validator.Save(EName::WatermarkLateEventsPolicy, watermarkLateEventsPolicy.GetResult().value_or(previousSettings.WatermarkLateEventsPolicy ? previousSettings.WatermarkLateEventsPolicy : "drop")));
        CHECK_STATUS(validator.Save(EName::StreamingDisposition, streamingDispositionValue.value_or(DefaultStreamingDisposition)));
        CHECK_STATUS(validator.Save(EName::InflightOperation, TStreamingQueryConfig::TOperations::Alter));

        return validator.Finish();
    }
};

class TDropStreamingQueryActor final : public TUserRequestHandlerBase<TDropStreamingQueryActor> {
    using TBase = TUserRequestHandlerBase<TDropStreamingQueryActor>;

public:
    TDropStreamingQueryActor(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller)
        : TBase(__func__, schemeTx, schemeTx.GetDrop().GetName(), context, std::move(controller), NACLib::DescribeSchema | NACLib::RemoveSchema)
        , AlterTx(schemeTx)
    {
        AlterTx.ClearDrop();
        AlterTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
        AlterTx.MutableCreateStreamingQuery()->SetName(SchemeTx.GetDrop().GetName());
    }

private:
    std::optional<NKikimrSchemeOp::TModifyScheme> GetBeginSchemeTx() final {
        if (!SchemeInfo) {
            if (SchemeTx.GetSuccessOnNotExist()) {
                Finish(Ydb::StatusIds::SUCCESS);
            } else {
                FatalError(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Streaming query " << QueryPath << " not found or you don't have access permissions");
            }
            return std::nullopt;
        }

        auto& properties = *AlterTx.MutableCreateStreamingQuery()->MutableProperties() = SchemeInfo->Properties;
        (*properties.MutableProperties())[TStreamingQueryConfig::TProperties::InflightOperation] = TStreamingQueryConfig::TOperations::Drop;

        return AlterTx;
    }

    std::optional<NKikimrSchemeOp::TModifyScheme> GetEndSchemeTx(bool success) final {
        return success ? SchemeTx : AlterTx;
    }

    NKikimrSchemeOp::TModifyScheme AlterTx;
};

class TStreamingOperationTrackerActor final : public TRequestHandlerBase<TStreamingOperationTrackerActor> {
    using TBase = TRequestHandlerBase<TStreamingOperationTrackerActor>;
    using TRetryPolicy = IRetryPolicy<bool>;

    static constexpr TDuration PING_PERIOD = TDuration::Seconds(10);

public:
    using TBase::LogPrefix;

    struct TSettings {
        ui64 SchemeShardGeneration = 0;
        TPathId PathId;
        ui64 AlterVersion = 0;
        TActorId OperationOwner;
    };

    TStreamingOperationTrackerActor(const TString& queryName, const TExternalContext& context, IStreamingQueryOperationController::TPtr controller, const TSettings& settings)
        : TBase(__func__, context.GetDatabase(), queryName, context, std::move(controller), /* access */ 0, settings.SchemeShardGeneration)
        , Settings(settings)
    {
        SchemeOperationStarted = true;
    }

    void Bootstrap() {
        YDB_LOG_DEBUG("[StreamingQueries] Bootstrapping operation tracker",
            {"logPrefix", LogPrefix()});

        Become(&TThis::StateFunc);
        PingOperationOwner();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, PingOperationOwner);
            hFunc(TEvPrivate::TEvPingOperationOwnerResult, Handle);
            hFunc(TEvPrivate::TEvLockStreamingQueryResult, HandleRetry);
            hFunc(TEvPrivate::TEvUnlockStreamingQueryResult, HandleRetry);
            hFunc(TEvPrivate::TEvExecuteSchemeTransactionResult, HandleRetry);
            default:
                StateFuncBase(ev);
        }
    }

private:
    void Handle(TEvPrivate::TEvPingOperationOwnerResult::TPtr& ev) {
        const auto status = ev->Get()->Status;
        YDB_LOG_DEBUG("[StreamingQueries] Check-alive request finished",
            {"logPrefix", LogPrefix()},
            {"status", status},
            {"issues", ev->Get()->Issues.ToOneLineString()},
            {"checkerId", ev->Sender});

        if (status == Ydb::StatusIds::SUCCESS) {
            Schedule(PING_PERIOD, new TEvents::TEvWakeup());
            return;
        }

        YDB_LOG_INFO("[StreamingQueries] Check-alive request failed, continue tx",
            {"logPrefix", LogPrefix()},
            {"status", status},
            {"issues", ev->Get()->Issues.ToOneLineString()},
            {"checkerId", ev->Sender});

        IsLockCreated = false;
        DescribeQuery("Check query info");
    }

    template <typename TEvPtr>
    void HandleRetry(TEvPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            ScheduleSchemeRetry(ev->Get()->Status, ev->Get()->Issues);
        } else {
            TBase::Handle(ev);
        }
    }

    bool ValidateSchemeVersion(const TSchemeInfo& schemeInfo, const std::optional<TSchemeInfo>&) const final {
        return schemeInfo.PathId == Settings.PathId && schemeInfo.Version == Settings.AlterVersion && schemeInfo.InflightOperationOwnerId;
    }

    bool HandleStreamingOperationStaleOwner(const TActorId& owner) final {
        Y_VALIDATE(owner, "Unexpected empty owner");
        Settings.OperationOwner = owner;
        Schedule(PING_PERIOD, new TEvents::TEvWakeup());
        return true;
    }

    std::optional<NKikimrSchemeOp::TModifyScheme> GetBeginSchemeTx() final {
        Y_VALIDATE(false, "Unexpected begin tx");
    }

    std::optional<NKikimrSchemeOp::TModifyScheme> GetEndSchemeTx(bool success) final {
        auto pathPairStatus = TStreamingQueryManager::SplitPath(QueryPath, Context.GetDatabase(), /* createDir */ false);
        Y_VALIDATE(!pathPairStatus.IsFail(), "Failed to split path");
        const auto& [workingDir, name] = pathPairStatus.DetachResult();

        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetWorkingDir(workingDir);

        if (success && QuerySettings.InflightOperation == TStreamingQueryConfig::TOperations::Drop) {
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropStreamingQuery);
            schemeTx.MutableDrop()->SetName(name);
        } else if (SchemeInfo) {
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
            schemeTx.MutableCreateStreamingQuery()->SetName(name);
            *schemeTx.MutableCreateStreamingQuery()->MutableProperties() = SchemeInfo->Properties;
        } else {
            ScheduleSchemeRetry(Ydb::StatusIds::PRECONDITION_FAILED, {NYql::TIssue("Failed to get query properties for finalization")});
            return std::nullopt;
        }

        return schemeTx;
    }

    void PingOperationOwner() {
        const auto& checkerId = Register(new TPingStreamingQueryTableActor(QueryPath, Settings.OperationOwner));
        YDB_LOG_INFO("[StreamingQueries] Starting check-alive request to owner",
            {"logPrefix", LogPrefix()},
            {"operationOwner", Settings.OperationOwner},
            {"checkerId", checkerId});
    }

    void ScheduleSchemeRetry(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        if (!SchemeRetryState) {
            SchemeRetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [](bool) { return ERetryErrorClass::ShortRetry; },
                TDuration::MilliSeconds(100), TDuration::MilliSeconds(500), TDuration::Seconds(1)
            )->CreateRetryState();
        }

        const auto delay = SchemeRetryState->GetNextRetryDelay(false);
        Y_VALIDATE(delay, "Failed to schedule retry");
        YDB_LOG_WARN("[StreamingQueries] Retrying scheme transaction from owner check",
            {"logPrefix", LogPrefix()},
            {"status", status},
            {"issues", issues.ToOneLineString()},
            {"retryDelay", *delay});

        Issues.Clear();
        Schedule(*delay, new TEvents::TEvWakeup());
    }

    TSettings Settings;
    TRetryPolicy::IRetryState::TPtr SchemeRetryState;
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

void DoTrackStreamingQueryOperation(const TString& queryName, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TOperationTrackContext& context) {
    const auto& externalContext = context.GetExternalData();
    externalContext.GetActorSystem()->Register(new TStreamingOperationTrackerActor(queryName, externalContext, std::move(controller), {
        .SchemeShardGeneration = context.GetRequestGeneration(),
        .PathId = context.GetPathId(),
        .AlterVersion = context.GetObjectGeneration(),
        .OperationOwner = context.GetOperationOwner(),
    }));
}

}  // namespace NKikimr::NKqp
