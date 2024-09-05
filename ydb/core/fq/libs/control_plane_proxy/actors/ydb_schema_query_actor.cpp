#include "ydb_schema_query_actor.h"
#include "base_actor.h"
#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/string/join.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/lib/fq/scope.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq::NPrivate {

using namespace NActors;
using namespace ::NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

namespace {

template<class TEventRequest, class TEventResponse>
class TSchemaQueryYDBActor;

}

template<class TEventRequest, class TEventResponse>
struct TBaseActorTypeTag<TSchemaQueryYDBActor<TEventRequest, TEventResponse>> {
    using TRequest  = TEventRequest;
    using TResponse = TEventResponse;
};

namespace {

using TScheduleErrorRecoverySQLGeneration =
    std::function<bool(TActorId sender, const TStatus& issues)>;

using TShouldSkipStepOnError =
    std::function<bool(const TStatus& issues)>;

TScheduleErrorRecoverySQLGeneration NoRecoverySQLGeneration() {
    return TScheduleErrorRecoverySQLGeneration{};
}

TShouldSkipStepOnError NoSkipOnError() {
    return TShouldSkipStepOnError{};
}

struct TSchemaQueryTask {
    TString SQL;
    TMaybe<TString> RollbackSQL;
    TScheduleErrorRecoverySQLGeneration ScheduleErrorRecoverySQLGeneration;
    TShouldSkipStepOnError ShouldSkipStepOnError;
};

struct TEvPrivate {
    enum EEv {
        EvProcessNextTaskRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvQueryExecutionResponse,
        EvRecoveryResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
                  "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvProcessNextTaskRequest :
        TEventLocal<TEvProcessNextTaskRequest, EvProcessNextTaskRequest> { };

    struct TEvQueryExecutionResponse :
        TEventLocal<TEvQueryExecutionResponse, EvQueryExecutionResponse> {
        explicit TEvQueryExecutionResponse(TStatus result)
            : Result(std::move(result)) { }

        TStatus Result;
    };

    struct TEvRecoveryResponse : TEventLocal<TEvRecoveryResponse, EvRecoveryResponse> {
        TEvRecoveryResponse(TMaybe<TString> recoverySQL, TStatus result)
            : RecoverySQL(std::move(recoverySQL))
            , Result(std::move(result)) { }

        TMaybe<TString> RecoverySQL;
        TStatus Result;
    };
};

template<class TEventRequest, class TEventResponse>
class TSchemaQueryYDBActor :
    public TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>> {

    using TBase = TBaseActor<TSchemaQueryYDBActor>;
    using TBase::Become;
    using TBase::Request;
    using TBase::SelfId;

    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    using TTasks              = std::vector<TSchemaQueryTask>;
    using TTasksFactoryMethod = std::function<TTasks(const TEventRequestPtr& request)>;
    using TQueryFactoryMethod = std::function<TString(const TEventRequestPtr& request)>;
    using TErrorMessageFactoryMethod =
        std::function<TString(EStatus status, const NYql::TIssues& issues)>;

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const TRequestCommonCountersPtr& counters,
                         TQueryFactoryMethod queryFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod)
        : TBaseActor<TSchemaQueryYDBActor>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks{TSchemaQueryTask{.SQL = queryFactoryMethod(Request)}}
        , CompletionStatuses(Tasks.size(), ETaskCompletionStatus::NONE)
        , ErrorMessageFactoryMethod(std::move(errorMessageFactoryMethod))
        , DBPath(Request->Get()->ComputeDatabase->connection().database()) { }

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const TRequestCommonCountersPtr& counters,
                         TTasksFactoryMethod tasksFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod)
        : TBaseActor<TSchemaQueryYDBActor>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks{tasksFactoryMethod(Request)}
        , CompletionStatuses(Tasks.size(), ETaskCompletionStatus::NONE)
        , ErrorMessageFactoryMethod(std::move(errorMessageFactoryMethod))
        , DBPath(Request->Get()->ComputeDatabase->connection().database()) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_YDB_SCHEMA_QUERY_ACTOR";

    void BootstrapImpl() override {
        CPP_LOG_I("TSchemaQueryYDBActor BootstrapImpl. Actor id: " << TBase::SelfId());
        ScheduleNextTask();
    }

    // Normal state
    void HandleProcessNextTask(typename TEvPrivate::TEvProcessNextTaskRequest::TPtr& event) {
        Y_UNUSED(event);
        auto schemeQuery = NormalSelectTask();
        if (schemeQuery) {
            InitiateSchemaQueryExecution(*schemeQuery);
        } else {
            FinishSuccessfully();
        }
    }

    TMaybe<TString> NormalSelectTask() {
        if (CurrentTaskIndex < static_cast<i32>(Tasks.size())) {
            return Tasks[CurrentTaskIndex].SQL;
        }
        return Nothing();
    }

    void NormalRecordCurrentProgressAndScheduleNextTask(ETaskCompletionStatus status) {
        CompletionStatuses[CurrentTaskIndex] = status;
        CurrentTaskIndex++;
        ScheduleNextTask();
    }

    void NormalHandleExecutionResponse(
        typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        const auto& executeSchemeQueryStatus = event->Get()->Result;

        if (executeSchemeQueryStatus.IsSuccess()) {
            NormalRecordCurrentProgressAndScheduleNextTask(ETaskCompletionStatus::SUCCESS);
        } else {
            auto& task = Tasks[CurrentTaskIndex];
            if (task.ScheduleErrorRecoverySQLGeneration &&
                task.ScheduleErrorRecoverySQLGeneration(SelfId(),
                                                        executeSchemeQueryStatus)) {
                SaveIssues("Couldn't execute SQL script", executeSchemeQueryStatus);
                TransitionToRecoveryState();
                return;
            }

            if (task.ShouldSkipStepOnError &&
                task.ShouldSkipStepOnError(executeSchemeQueryStatus)) {
                NormalRecordCurrentProgressAndScheduleNextTask(ETaskCompletionStatus::SKIPPED);
                return;
            }

            SaveIssues("Couldn't execute SQL script", executeSchemeQueryStatus);
            TransitionToRollbackState();
        }
    }

    // Rollback state
    void RollbackHandleProcessNextTask(
        typename TEvPrivate::TEvProcessNextTaskRequest::TPtr& event) {
        Y_UNUSED(event);

        auto rollbackSchemeQuery = RollbackSelectTask();
        if (rollbackSchemeQuery) {
            InitiateSchemaQueryExecution(*rollbackSchemeQuery);
        } else {
            SendError();
        }
    }

    TMaybe<TString> RollbackSelectTask() {
        while (CurrentTaskIndex >= 0) {
            const auto& maybeRollback = Tasks[CurrentTaskIndex].RollbackSQL;
            if (maybeRollback &&
                CompletionStatuses[CurrentTaskIndex] == ETaskCompletionStatus::SUCCESS) {
                return maybeRollback;
            }
            CurrentTaskIndex--;
        }
        return Nothing();
    }

    void RollbackHandleExecutionResponse(
        typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        const auto& executeSchemeQueryStatus = event->Get()->Result;

        if (executeSchemeQueryStatus.IsSuccess()) {
            CompletionStatuses[CurrentTaskIndex] = ETaskCompletionStatus::ROLL_BACKED;
            CurrentTaskIndex--;
            ScheduleNextTask();
            return;
        } else {
            SaveIssues("Couldn't execute rollback SQL", executeSchemeQueryStatus);
            SendError();
            return;
        }
    }

    // Recovery state
    void Handle(typename TEvPrivate::TEvRecoveryResponse::TPtr& event) {
        if (event->Get()->Result.IsSuccess()) {
            InitiateSchemaQueryExecution(*event->Get()->RecoverySQL);
        } else {
            SaveIssues("Failed to generate recovery SQL", event->Get()->Result);
            TransitionToRollbackState();
        }
    }

    void RecoveryHandleExecutionResponse(
        typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        const auto& executeSchemeQueryStatus = event->Get()->Result;

        if (executeSchemeQueryStatus.IsSuccess()) {
            ClearIssues();
            TransitionToNormalState();
        } else {
            SaveIssues("Failed to execute recovery SQL", event->Get()->Result);
            TransitionToRollbackState();
        }
    }

    // FSM description
    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
                  hFunc(TEvPrivate::TEvQueryExecutionResponse,
                        NormalHandleExecutionResponse);
                  hFunc(TEvPrivate::TEvProcessNextTaskRequest, HandleProcessNextTask);)

    STRICT_STFUNC(
        RollbackStateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        hFunc(TEvPrivate::TEvQueryExecutionResponse, RollbackHandleExecutionResponse);
        hFunc(TEvPrivate::TEvProcessNextTaskRequest, RollbackHandleProcessNextTask);)

    STRICT_STFUNC(RecoveryStateFunc,
                  cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
                  hFunc(TEvPrivate::TEvRecoveryResponse, Handle);
                  hFunc(TEvPrivate::TEvQueryExecutionResponse,
                        RecoveryHandleExecutionResponse);)

    void ScheduleNextTask() {
        TBase::Send(SelfId(), new TEvPrivate::TEvProcessNextTaskRequest{});
    }

    void TransitionToRollbackState() {
        CPP_LOG_I("TSchemaQueryYDBActor TransitionToRollbackState. Actor id: "
                  << TBase::SelfId());
        CompletionStatuses[CurrentTaskIndex] = ETaskCompletionStatus::ERROR;
        CurrentTaskIndex--;
        Become(&TSchemaQueryYDBActor::RollbackStateFunc);
        ScheduleNextTask();
    }

    void TransitionToNormalState() {
        CPP_LOG_I("TSchemaQueryYDBActor TransitionToNormalState. Actor id: "
                  << TBase::SelfId());
        Become(&TSchemaQueryYDBActor::StateFunc);
        ScheduleNextTask();
    }

    void TransitionToRecoveryState() {
        CPP_LOG_I("TSchemaQueryYDBActor TransitionToRecoveryState. Actor id: "
                  << TBase::SelfId());
        Become(&TSchemaQueryYDBActor::RecoveryStateFunc);
    }

    void FinishSuccessfully() {
        LogCurrentState("Query finished successfully");
        Request->Get()->ComputeYDBOperationWasPerformed = true;
        TBase::SendRequestToSender();
    }

    void SendError() {
        Y_ENSURE(FirstStatus, "Status of first issue was not recorded");
        LogCurrentState("Query finished with issues");
        TString errorMessage = ErrorMessageFactoryMethod(*FirstStatus, Issues);
        TBase::HandleError(errorMessage, *FirstStatus, std::move(Issues));
    }

    void SaveIssues(const TString& message, const TStatus& status) {
        auto issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, message);
        for (const auto& subIssue : RemoveDatabaseFromIssues(status.GetIssues(), DBPath)) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
        }

        Issues.AddIssue(std::move(issue));
        if (!FirstStatus) {
            FirstStatus = status.GetStatus();
        }
    }

    void ClearIssues() {
        Issues.Clear();
        FirstStatus.Clear();
    }

    static TStatus ExtractStatus(const TAsyncStatus& future) {
        try {
            return std::move(future.GetValueSync()); // can throw an exception
        } catch (...) {
            return TStatus{EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}};
        }
    }

    void InitiateSchemaQueryExecution(const TString& schemeQuery) {
        CPP_LOG_I("TSchemaQueryYDBActor Executing schema query. Actor id: "
                  << TBase::SelfId() << " SchemeQuery: " << HideSecrets(schemeQuery));
        Request->Get()
            ->YDBClient
            ->RetryOperation([query = schemeQuery](TSession session) {
                return session.ExecuteSchemeQuery(query);
            })
            .Subscribe([actorSystem = TActivationContext::ActorSystem(),
                        self        = SelfId()](const TAsyncStatus& future) {
                actorSystem->Send(self,
                                  new TEvPrivate::TEvQueryExecutionResponse{
                                      ExtractStatus(future),
                                  });
            });
    }

    void LogCurrentState(const TString& message) {
        using TEnumToString = TString(const ETaskCompletionStatus&);
        CPP_LOG_I("TSchemaQueryYDBActor Logging current state. Message: '"
                  << message << "', Actor id: " << TBase::SelfId()
                  << ". CompletionStatuses: ["
                  << JoinMapRange(", ",
                                  CompletionStatuses.cbegin(),
                                  CompletionStatuses.cend(),
                                  (TEnumToString*)ToString<ETaskCompletionStatus>)
                  << "], CurrentTaskIndex: " << CurrentTaskIndex);
    }

private:
    TTasks Tasks;
    std::vector<ETaskCompletionStatus> CompletionStatuses;
    TErrorMessageFactoryMethod ErrorMessageFactoryMethod;
    i32 CurrentTaskIndex = 0;
    TMaybe<EStatus> FirstStatus;
    NYql::TIssues Issues;
    TString DBPath;
};

class TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor :
    public TPlainBaseActor<TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor> {
public:
    using TBase = TPlainBaseActor;

    TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor(
        const TActorId sender,
        const TString& scope,
        const TString& user,
        const TString& token,
        const TString& cloudId,
        const TMaybe<TQuotaMap>& quotas,
        const TTenantInfo::TPtr& tenantInfo,
        const TString& connectionName,
        const TPermissions& permissions,
        const TDuration& requestTimeout,
        const TRequestCommonCountersPtr& counters)
        : TPlainBaseActor(sender, sender, requestTimeout, counters)
        , Scope(scope)
        , User(user)
        , Token(token)
        , CloudId(cloudId)
        , Quotas(quotas)
        , TenantInfo(tenantInfo)
        , ConnectionName(connectionName)
        , Permissions(permissions) { }

    void BootstrapImpl() override { CheckConnectionExistenceInCPS(); }

    IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) override {
        return new TEvPrivate::TEvRecoveryResponse(
            Nothing(), TStatus{EStatus::TIMEOUT, NYql::TIssues{std::move(issue)}});
    };

    void CheckConnectionExistenceInCPS() {
        FederatedQuery::ListConnectionsRequest result;
        result.mutable_filter()->set_name(ConnectionName);
        result.set_limit(2);

        auto event = new TEvControlPlaneStorage::TEvListConnectionsRequest(
            Scope, result, User, Token, CloudId, Permissions, Quotas, TenantInfo, {});

        event->IsExactNameMatch = true;

        TBase::Send(::NFq::ControlPlaneStorageServiceActorId(), event);
    }

    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
                  hFunc(TEvControlPlaneStorage::TEvListConnectionsResponse, Handle);)

    void Handle(const TEvControlPlaneStorage::TEvListConnectionsResponse::TPtr& event) {
        auto connectionSize = event->Get()->Result.connection_size();
        if (connectionSize != 0) {
            // Already exist in CPS
            TBase::SendErrorMessageToSender(
                new TEvPrivate::TEvRecoveryResponse(Nothing(),
                                                    TStatus{EStatus::TIMEOUT, {}}));
            return;
        }
        TBase::SendRequestToSender(new TEvPrivate::TEvRecoveryResponse(
            MakeDeleteExternalDataSourceQuery(ConnectionName),
            TStatus{EStatus::SUCCESS, {}}));
    }

private:
    const TString Scope;
    const TString User;
    const TString Token;
    const TString CloudId;
    const TMaybe<TQuotaMap> Quotas;
    const TTenantInfo::TPtr TenantInfo;
    const TString ConnectionName;
    const TPermissions Permissions;
};

class TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor :
    public TPlainBaseActor<TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor> {
public:
    using TBase = TPlainBaseActor;

    TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor(
        const TActorId& sender,
        const TString& scope,
        const TString& user,
        const TString& token,
        const TString& cloudId,
        const TMaybe<TQuotaMap>& quotas,
        const TTenantInfo::TPtr& tenantInfo,
        const TString& bindingName,
        const TPermissions& permissions,
        const TDuration& requestTimeout,
        const TRequestCommonCountersPtr& counters)
        : TPlainBaseActor(sender, sender, requestTimeout, counters)
        , Scope(scope)
        , User(user)
        , Token(token)
        , CloudId(cloudId)
        , Quotas(quotas)
        , TenantInfo(tenantInfo)
        , BindingName(bindingName)
        , Permissions(permissions) { }

    void BootstrapImpl() override { CheckBindingExistenceInCPS(); }

    IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) override {
        return new TEvPrivate::TEvRecoveryResponse(
            Nothing(), TStatus{EStatus::TIMEOUT, NYql::TIssues{std::move(issue)}});
    }

    void CheckBindingExistenceInCPS() {
        FederatedQuery::ListBindingsRequest result;
        result.mutable_filter()->set_name(BindingName);
        result.set_limit(2);

        auto event = new TEvControlPlaneStorage::TEvListBindingsRequest(
            Scope, result, User, Token, CloudId, Permissions, Quotas, TenantInfo, {});

        event->IsExactNameMatch = true;

        TBase::Send(::NFq::ControlPlaneStorageServiceActorId(), event);
    }

    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
                  hFunc(TEvControlPlaneStorage::TEvListBindingsResponse, Handle);)

    void Handle(const TEvControlPlaneStorage::TEvListBindingsResponse::TPtr& event) {
        auto bindingSize = event->Get()->Result.binding_size();
        if (bindingSize != 0) {
            // Already exist in CPS
            TBase::SendErrorMessageToSender(
                new TEvPrivate::TEvRecoveryResponse(Nothing(),
                                                    TStatus{EStatus::TIMEOUT, {}}));
            return;
        }
        TBase::SendRequestToSender(new TEvPrivate::TEvRecoveryResponse(
            MakeDeleteExternalDataTableQuery(BindingName),
            TStatus{EStatus::SUCCESS, {}}));
    }

private:
    const TString Scope;
    const TString User;
    const TString Token;
    const TString CloudId;
    const TMaybe<TQuotaMap> Quotas;
    const TTenantInfo::TPtr TenantInfo;
    const TString BindingName;
    const TPermissions Permissions;
};

bool IsPathDoesNotExistIssue(const TStatus& status) {
    auto oneLineError = status.GetIssues().ToOneLineString();
    return oneLineError.Contains("Path does not exist") || oneLineError.Contains("path hasn't been resolved");
}

bool IsPathExistsIssue(const TStatus& status) {
    return status.GetIssues().ToOneLineString().Contains("error: path exist");
}

} // namespace

/// Connection actors
IActor* MakeCreateConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TPermissions permissions,
    const TCommonConfig& commonConfig,
    const ::NFq::TComputeConfig& computeConfig,
    TSigner::TPtr signer,
    bool withoutRollback,
    TMaybe<TString> connectionId) {
    auto queryFactoryMethod =
        [signer = std::move(signer),
         requestTimeout,
         &counters,
         permissions,
         withoutRollback,
         commonConfig,
         computeConfig](const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& req)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = req->Get()->Request.content();
        const auto& scope = req->Get()->Scope;
        const TString folderId = NYdb::NFq::TScope{scope}.ParseFolder();

        auto createSecretStatement = CreateSecretObjectQuery(connectionContent.setting(),
                                                             connectionContent.name(),
                                                             signer,
                                                             folderId);

        std::vector<TSchemaQueryTask> statements;
        if (createSecretStatement) {
            statements.push_back(TSchemaQueryTask{.SQL = *createSecretStatement,
                                                  .ShouldSkipStepOnError =
                                                      withoutRollback ? IsPathExistsIssue
                                                                      : NoSkipOnError()});
        }

        TScheduleErrorRecoverySQLGeneration alreadyExistRecoveryActorFactoryMethod =
            [scope          = req->Get()->Scope,
             user           = req->Get()->User,
             token          = req->Get()->Token,
             cloudId        = req->Get()->CloudId,
             quotas         = req->Get()->Quotas,
             tenantInfo     = req->Get()->TenantInfo,
             connectionName = req->Get()->Request.content().name(),
             requestTimeout,
             &counters,
             permissions](TActorId sender, const TStatus& status) {
                if (status.GetStatus() == EStatus::ALREADY_EXISTS ||
                    status.GetIssues().ToOneLineString().Contains("error: path exist")) {
                    TActivationContext::ActorSystem()->Register(
                        new TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor(
                            sender,
                            scope,
                            user,
                            token,
                            cloudId,
                            quotas,
                            tenantInfo,
                            connectionName,
                            permissions,
                            requestTimeout,
                            counters.GetCommonCounters(RTC_CREATE_CONNECTION_IN_YDB)));
                    return true;
                }
                return false;
            };
        statements.push_back(TSchemaQueryTask{
            .SQL = MakeCreateExternalDataSourceQuery(
                connectionContent, signer, commonConfig,
                computeConfig.IsReplaceIfExistsSyntaxSupported(), folderId),
                             .ScheduleErrorRecoverySQLGeneration =
                                 withoutRollback
                                     ? NoRecoverySQLGeneration()
                                     : std::move(alreadyExistRecoveryActorFactoryMethod),
            .ShouldSkipStepOnError =
                withoutRollback ? IsPathExistsIssue : NoSkipOnError()});
        return statements;
    };

    auto errorMessageFactoryMethod =
        [connectionId, connectionName = request->Get()->Request.content().name()](
            const EStatus status, const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        TStringBuilder message = TStringBuilder{} << "Synchronization of connection";
        if (connectionId.Defined()) {
            message << " with id '" << connectionId << "'";
        }
        if (status == NYdb::EStatus::ALREADY_EXISTS) {
            message << " failed, because external data source with name '"
                    << connectionName << "' already exists";
        } else {
            message << " failed, because creation of external data source with name '"
                    << connectionName << "' wasn't successful";
        }
        return TString{message};
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvCreateConnectionRequest,
                                    TEvControlPlaneProxy::TEvCreateConnectionResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_CREATE_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

IActor* MakeModifyConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TCommonConfig& commonConfig,
    const ::NFq::TComputeConfig& computeConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [signer = std::move(signer),
         commonConfig, computeConfig](
            const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        using namespace fmt::literals;

        auto& oldConnectionContent = (*request->Get()->OldConnectionContent);
        auto& oldBindings          = request->Get()->OldBindingContents;
        auto& newConnectionContent = request->Get()->Request.content();
        const auto& scope = request->Get()->Scope;
        const TString folderId = NYdb::NFq::TScope{scope}.ParseFolder();

        auto dropOldSecret =
            DropSecretObjectQuery(oldConnectionContent.name(), folderId);
        auto createNewSecret =
            CreateSecretObjectQuery(newConnectionContent.setting(),
                                    newConnectionContent.name(),
                                    signer,
                                    folderId);

        bool replaceSupported = computeConfig.IsReplaceIfExistsSyntaxSupported();
        if (replaceSupported &&
            oldConnectionContent.name() == newConnectionContent.name()) {
            // CREATE OR REPLACE
            auto createSecretStatement =
                CreateSecretObjectQuery(newConnectionContent.setting(),
                                        newConnectionContent.name(), signer, folderId);

            std::vector<TSchemaQueryTask> statements;
            if (createSecretStatement) {
              statements.push_back(
                  TSchemaQueryTask{.SQL = *createSecretStatement});
            }

            statements.push_back(TSchemaQueryTask{
                .SQL = MakeCreateExternalDataSourceQuery(
                    newConnectionContent, signer, commonConfig, replaceSupported, folderId)});
            return statements;
        }

        std::vector<TSchemaQueryTask> statements;
        // remove and create new version
        if (!oldBindings.empty()) {
            statements.push_back(TSchemaQueryTask{
                .SQL         = JoinMapRange("\n",
                                    oldBindings.begin(),
                                    oldBindings.end(),
                                    [](const FederatedQuery::BindingContent& binding) {
                                        return MakeDeleteExternalDataTableQuery(
                                            binding.name());
                                    }),
                .RollbackSQL = JoinMapRange(
                    "\n",
                    oldBindings.begin(),
                    oldBindings.end(),
                    [&oldConnectionContent](const FederatedQuery::BindingContent& binding) {
                    return MakeCreateExternalDataTableQuery(
                        binding, oldConnectionContent.name(), false);
                    }),
                .ShouldSkipStepOnError = IsPathDoesNotExistIssue});
        }

        statements.push_back(TSchemaQueryTask{
            .SQL = TString{MakeDeleteExternalDataSourceQuery(oldConnectionContent.name())},
            .RollbackSQL           = TString{MakeCreateExternalDataSourceQuery(
                oldConnectionContent, signer, commonConfig, false, folderId)},
            .ShouldSkipStepOnError = IsPathDoesNotExistIssue});

        if (dropOldSecret) {
            statements.push_back(TSchemaQueryTask{
                .SQL         = *dropOldSecret,
                .RollbackSQL = CreateSecretObjectQuery(oldConnectionContent.setting(),
                                                       oldConnectionContent.name(),
                                                       signer, folderId),
                .ShouldSkipStepOnError = IsPathDoesNotExistIssue});
        }
        if (createNewSecret) {
            statements.push_back(TSchemaQueryTask{.SQL         = *createNewSecret,
                                                  .RollbackSQL = DropSecretObjectQuery(
                                                      newConnectionContent.name(), folderId)});
        }

        statements.push_back(
            TSchemaQueryTask{.SQL         = TString{MakeCreateExternalDataSourceQuery(
                                 newConnectionContent, signer, commonConfig, false, folderId)},
                             .RollbackSQL = TString{MakeDeleteExternalDataSourceQuery(
                                 newConnectionContent.name())}});

        if (!oldBindings.empty()) {
            statements.push_back(TSchemaQueryTask{
                .SQL = JoinMapRange("\n",
                                    oldBindings.begin(),
                                    oldBindings.end(),
                                    [&newConnectionContent](
                                        const FederatedQuery::BindingContent& binding) {
                                        return MakeCreateExternalDataTableQuery(
                                            binding, newConnectionContent.name(), false);
                                    }),
                .RollbackSQL =
                    JoinMapRange("\n",
                                 request->Get()->OldBindingContents.begin(),
                                 request->Get()->OldBindingContents.end(),
                                 [](const FederatedQuery::BindingContent& binding) {
                                     return MakeDeleteExternalDataTableQuery(binding.name());
                                 })});
        }

        return statements;
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(status);
        Y_UNUSED(issues);
        return "Couldn't modify external data source in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvModifyConnectionRequest,
                                    TEvControlPlaneProxy::TEvModifyConnectionResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_MODIFY_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

IActor* MakeDeleteConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TCommonConfig& commonConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [signer = std::move(signer),
         commonConfig](
            const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = *request->Get()->ConnectionContent;
        const auto& scope = request->Get()->Scope;
        const TString folderId = NYdb::NFq::TScope{scope}.ParseFolder();

        auto dropSecret =
            DropSecretObjectQuery(connectionContent.name(), folderId);

        std::vector statements = {
            TSchemaQueryTask{.SQL = TString{MakeDeleteExternalDataSourceQuery(
                                 connectionContent.name())},
                             .RollbackSQL = MakeCreateExternalDataSourceQuery(
                                 connectionContent, signer, commonConfig, false, folderId),
                             .ShouldSkipStepOnError = IsPathDoesNotExistIssue}};
        if (dropSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL = *dropSecret,
                                 .RollbackSQL =
                                     CreateSecretObjectQuery(connectionContent.setting(),
                                                             connectionContent.name(),
                                                             signer, folderId),
                                 .ShouldSkipStepOnError = IsPathDoesNotExistIssue});
        }
        return statements;
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(status);
        Y_UNUSED(issues);
        return "Couldn't delete external data source in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest,
                                    TEvControlPlaneProxy::TEvDeleteConnectionResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_DELETE_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

/// Bindings actors
IActor* MakeCreateBindingActor(const TActorId& proxyActorId,
                               TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
                               TDuration requestTimeout,
                               TCounters& counters,
                               TPermissions permissions,
                               const ::NFq::TComputeConfig& computeConfig,bool withoutRollback,
                               TMaybe<TString> bindingId) {
    auto queryFactoryMethod =
        [requestTimeout, &counters, permissions, withoutRollback, computeConfig](
            const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& req)
        -> std::vector<TSchemaQueryTask> {
        auto& bindingContent     = req->Get()->Request.content();
        auto& externalSourceName = req->Get()->ConnectionContent->name();
        std::vector<TSchemaQueryTask> statements;

        TScheduleErrorRecoverySQLGeneration alreadyExistRecoveryActorFactoryMethod =
            [scope       = req->Get()->Scope,
             user        = req->Get()->User,
             token       = req->Get()->Token,
             cloudId     = req->Get()->CloudId,
             quotas      = req->Get()->Quotas,
             tenantInfo  = req->Get()->TenantInfo,
             bindingName = req->Get()->Request.content().name(),
             requestTimeout,
             &counters,
             permissions](TActorId sender, const TStatus& status) {
                if (status.GetStatus() == EStatus::ALREADY_EXISTS ||
                    status.GetIssues().ToOneLineString().Contains("error: path exist")) {
                    TActivationContext::ActorSystem()->Register(
                        new TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor(
                            sender,
                            scope,
                            user,
                            token,
                            cloudId,
                            quotas,
                            tenantInfo,
                            bindingName,
                            permissions,
                            requestTimeout,
                            counters.GetCommonCounters(RTC_CREATE_BINDING_IN_YDB)));
                    return true;
                }
                return false;
            };
        statements.push_back(TSchemaQueryTask{
            .SQL = TString{MakeCreateExternalDataTableQuery(
                bindingContent, externalSourceName,
                computeConfig.IsReplaceIfExistsSyntaxSupported())},
            .ScheduleErrorRecoverySQLGeneration =
                withoutRollback ? NoRecoverySQLGeneration()
                                : std::move(alreadyExistRecoveryActorFactoryMethod),
            .ShouldSkipStepOnError =
                withoutRollback ? IsPathExistsIssue : NoSkipOnError()});
        return statements;
    };

    auto errorMessageFactoryMethod =
        [bindingId, bindingName = request->Get()->Request.content().name()](
            const EStatus status, const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        TStringBuilder message = TStringBuilder{} << "Synchronization of binding";
        if (bindingId.Defined()) {
            message << " with id '" << bindingId << "'";
        }
        if (status == EStatus::ALREADY_EXISTS) {
            message << " failed, because external data table with name '" << bindingName
                    << "' already exists";
        } else {
            message << " failed, because creation of external data table with name '"
                    << bindingName << "' wasn't successful";
        }
        return TString{message};
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvCreateBindingRequest,
                                    TEvControlPlaneProxy::TEvCreateBindingResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_CREATE_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

IActor* MakeModifyBindingActor(const TActorId& proxyActorId,
                               TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
                               TDuration requestTimeout,
                               TCounters& counters,
    const ::NFq::TComputeConfig& computeConfig) {
    auto queryFactoryMethod =
        [computeConfig](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto sourceName   = request->Get()->ConnectionContent->name();
        auto oldTableName = request->Get()->OldBindingContent->name();

        bool replaceSupported = computeConfig.IsReplaceIfExistsSyntaxSupported();
        if (replaceSupported &&
            oldTableName == request->Get()->Request.content().name()) {
          // CREATE OR REPLACE
          return {TSchemaQueryTask{.SQL = MakeCreateExternalDataTableQuery(
                                       request->Get()->Request.content(),
                                       sourceName, replaceSupported)}};
        }

        // remove and create new version
        auto deleteOldEntities = MakeDeleteExternalDataTableQuery(oldTableName);
        auto createOldEntities = MakeCreateExternalDataTableQuery(
            *request->Get()->OldBindingContent, sourceName, false);
        auto createNewEntities = MakeCreateExternalDataTableQuery(
            request->Get()->Request.content(), sourceName, false);

        return {
            TSchemaQueryTask{.SQL = deleteOldEntities,
                             .RollbackSQL = createOldEntities,
                             .ShouldSkipStepOnError = IsPathDoesNotExistIssue},
            TSchemaQueryTask{.SQL = createNewEntities}};
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(status);
        Y_UNUSED(issues);
        return "Couldn't modify external data table in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvModifyBindingRequest,
                                    TEvControlPlaneProxy::TEvModifyBindingResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_MODIFY_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

IActor* MakeDeleteBindingActor(const TActorId& proxyActorId,
                               TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
                               TDuration requestTimeout,
                               TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        return {{.SQL = MakeDeleteExternalDataTableQuery(
                     request->Get()->OldBindingContent->name()),
                 .ShouldSkipStepOnError = IsPathDoesNotExistIssue}};
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(status);
        Y_UNUSED(issues);
        return "Couldn't delete external data source in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvDeleteBindingRequest,
                                    TEvControlPlaneProxy::TEvDeleteBindingResponse>(
        proxyActorId,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_DELETE_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

} // namespace NFq::NPrivate

