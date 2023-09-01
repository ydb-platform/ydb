#include "base_actor.h"
#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/string/join.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;
using namespace ::NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

template<class TEventRequest, class TEventResponse>
class TSchemaQueryYDBActor;

template<class TEventRequest, class TEventResponse>
struct TBaseActorTypeTag<TSchemaQueryYDBActor<TEventRequest, TEventResponse>> {
    using TRequest  = TEventRequest;
    using TResponse = TEventResponse;
};

using TScheduleErrorRecoverySQLGeneration =
    std::function<bool(NActors::TActorId sender, const TStatus& issues)>;

struct TSchemaQueryTask {
    TString SQL;
    TMaybe<TString> RollbackSQL;
    TScheduleErrorRecoverySQLGeneration ScheduleErrorRecoverySQLGeneration;
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
        NActors::TEventLocal<TEvProcessNextTaskRequest, EvProcessNextTaskRequest> { };

    struct TEvQueryExecutionResponse :
        NActors::TEventLocal<TEvQueryExecutionResponse, EvQueryExecutionResponse> {
        TEvQueryExecutionResponse(TStatus result)
            : Result(std::move(result)) { }

        TStatus Result;
    };

    struct TEvRecoveryResponse :
        NActors::TEventLocal<TEvRecoveryResponse, EvRecoveryResponse> {
        TEvRecoveryResponse(TMaybe<TString> recoverySQL, TStatus result)
            : RecoverySQL(recoverySQL)
            , Result(std::move(result)) { }

        TMaybe<TString> RecoverySQL;
        TStatus Result;
    };
};

template<class TEventRequest, class TEventResponse>
class TSchemaQueryYDBActor :
    public TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>> {
private:
    using TBase = TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>;
    using TBase::Become;
    using TBase::Request;
    using TBase::SelfId;

    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    using TTasks              = std::vector<TSchemaQueryTask>;
    using TTasksFactoryMethod = std::function<TTasks(const TEventRequestPtr& request)>;
    using TQueryFactoryMethod = std::function<TString(const TEventRequestPtr& request)>;
    using TErrorMessageFactoryMethod =
        std::function<TString(const EStatus status, const NYql::TIssues& issues)>;

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const NPrivate::TRequestCommonCountersPtr& counters,
                         TQueryFactoryMethod queryFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod)
        : TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks{TSchemaQueryTask{.SQL = queryFactoryMethod(Request)}}
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod) { }

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const NPrivate::TRequestCommonCountersPtr& counters,
                         TTasksFactoryMethod tasksFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod)
        : TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks(tasksFactoryMethod(Request))
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod) { }

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

    void NormalHandleExecutionResponse(
        typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        const auto& executeSchemeQueryStatus = event->Get()->Result;

        if (executeSchemeQueryStatus.IsSuccess()) {
            CurrentTaskIndex++;
            ScheduleNextTask();
        } else {
            SaveIssues("Couldn't execute SQL script", executeSchemeQueryStatus);

            auto& task = Tasks[CurrentTaskIndex];
            if (task.ScheduleErrorRecoverySQLGeneration &&
                task.ScheduleErrorRecoverySQLGeneration(SelfId(),
                                                        executeSchemeQueryStatus)) {
                TransitionToRecoveryState();
                return;
            }
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
            if (maybeRollback) {
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
        TBase::Send(SelfId(), new typename TEvPrivate::TEvProcessNextTaskRequest{});
    }

    void TransitionToRollbackState() {
        CPP_LOG_I("TSchemaQueryYDBActor TransitionToRollbackState. Actor id: "
                  << TBase::SelfId());
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
        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished successfully. Actor id: "
                  << TBase::SelfId());
        Request->Get()->ComputeYDBOperationWasPerformed = true;
        TBase::SendRequestToSender();
    }

    void SendError() {
        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished with issues. Actor id: "
                  << TBase::SelfId());
        TString errorMessage = ErrorMessageFactoryMethod(*FirstStatus, Issues);

        TBase::HandleError(errorMessage, *FirstStatus, std::move(Issues));
    }

    void SaveIssues(const TString& message, const TStatus& status) {
        auto issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, message);
        for (const auto& subIssue : status.GetIssues()) {
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

    static NYdb::TStatus ExtractStatus(const TAsyncStatus& future) {
        try {
            return std::move(future.GetValueSync()); // can throw an exception
        } catch (...) {
            return NYdb::TStatus{EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}};
        }
    }

    void InitiateSchemaQueryExecution(const TString& schemeQuery) {
        CPP_LOG_I("TSchemaQueryYDBActor Executing schema query. Actor id: "
                  << TBase::SelfId() << " SchemeQuery: " << schemeQuery);
        Request->Get()
            ->YDBClient
            ->RetryOperation([query = schemeQuery](TSession session) {
                return session.ExecuteSchemeQuery(query);
            })
            .Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(),
                        self        = SelfId()](const TAsyncStatus& future) {
                actorSystem->Send(self,
                                  new typename TEvPrivate::TEvQueryExecutionResponse{
                                      ExtractStatus(future),
                                  });
            });
    }

private:
    TTasks Tasks;
    TErrorMessageFactoryMethod ErrorMessageFactoryMethod;
    i32 CurrentTaskIndex = 0;
    TMaybe<EStatus> FirstStatus;
    NYql::TIssues Issues;
};

class TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor :
    public TPlainBaseActor<TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor> {
public:
    using TBase = TPlainBaseActor<TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor>;

    TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor(
        NActors::TActorId sender,
        const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request,
        TPermissions permissions,
        TDuration requestTimeout,
        const NPrivate::TRequestCommonCountersPtr& counters)
        : TPlainBaseActor<TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor>(
              sender, sender, requestTimeout, counters)
        , Request(request)
        , Permissions(std::move(permissions)) { }

    void BootstrapImpl() { CheckConnectionExistenceInCPS(); }

    IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) {
        return new TEvPrivate::TEvRecoveryResponse(
            Nothing(), TStatus{EStatus::TIMEOUT, NYql::TIssues{std::move(issue)}});
    };

    void CheckConnectionExistenceInCPS() {
        FederatedQuery::ListConnectionsRequest result;
        auto connectionName = Request->Get()->Request.content().name();
        result.mutable_filter()->set_name(connectionName);
        result.set_limit(2);

        auto event =
            new TEvControlPlaneStorage::TEvListConnectionsRequest(Request->Get()->Scope,
                                                                  result,
                                                                  Request->Get()->User,
                                                                  Request->Get()->Token,
                                                                  Request->Get()->CloudId,
                                                                  Permissions,
                                                                  Request->Get()->Quotas,
                                                                  Request->Get()->TenantInfo,
                                                                  {});

        event->IsExactNameMatch = true;

        TBase::Send(NFq::ControlPlaneStorageServiceActorId(), event);
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
        }
        TBase::SendRequestToSender(new TEvPrivate::TEvRecoveryResponse(
            MakeDeleteExternalDataSourceQuery(Request->Get()->Request.content().name()),
            TStatus{EStatus::SUCCESS, {}}));
    }

private:
    NActors::TActorId Sender;
    const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& Request;
    TPermissions Permissions;
};

class TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor :
    public TPlainBaseActor<TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor> {
public:
    using TBase = TPlainBaseActor<TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor>;

    TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor(
        NActors::TActorId sender,
        const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
        TPermissions permissions,
        TDuration requestTimeout,
        const NPrivate::TRequestCommonCountersPtr& counters)
        : TPlainBaseActor<TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor>(
              sender, sender, requestTimeout, counters)
        , Request(request)
        , Permissions(std::move(permissions)) { }

    void BootstrapImpl() { CheckBindingExistenceInCPS(); }

    IEventBase* MakeTimeoutEventImpl(NYql::TIssue issue) {
        return new TEvPrivate::TEvRecoveryResponse(
            Nothing(), TStatus{EStatus::TIMEOUT, NYql::TIssues{std::move(issue)}});
    };

    void CheckBindingExistenceInCPS() {
        FederatedQuery::ListBindingsRequest result;
        auto bindingName = Request->Get()->Request.content().name();
        result.mutable_filter()->set_name(bindingName);
        result.set_limit(2);

        auto event =
            new TEvControlPlaneStorage::TEvListBindingsRequest(Request->Get()->Scope,
                                                               result,
                                                               Request->Get()->User,
                                                               Request->Get()->Token,
                                                               Request->Get()->CloudId,
                                                               Permissions,
                                                               Request->Get()->Quotas,
                                                               Request->Get()->TenantInfo,
                                                               {});

        event->IsExactNameMatch = true;

        TBase::Send(NFq::ControlPlaneStorageServiceActorId(), event);
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
        }
        TBase::SendRequestToSender(new TEvPrivate::TEvRecoveryResponse(
            MakeDeleteExternalDataTableQuery(Request->Get()->Request.content().name()),
            TStatus{EStatus::SUCCESS, {}}));
    }

private:
    NActors::TActorId Sender;
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& Request;
    TPermissions Permissions;
};

/// Connection actors
NActors::IActor* MakeCreateConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TPermissions permissions,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint = commonConfig.GetObjectStorageEndpoint(),
         signer                = std::move(signer),
         requestTimeout,
         &counters, permissions](const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = request->Get()->Request.content();

        auto createSecretStatement =
            CreateSecretObjectQuery(connectionContent.setting(),
                                    connectionContent.name(),
                                    signer);

        std::vector<TSchemaQueryTask> statements;
        if (createSecretStatement) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *createSecretStatement,
                                 .RollbackSQL = DropSecretObjectQuery(connectionContent.name())});
        }

        TScheduleErrorRecoverySQLGeneration alreadyExistRecoveryActorFactoryMethod =
            [&request, requestTimeout, &counters, permissions](NActors::TActorId sender,
                                                  const TStatus& status) {
                if (status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS ||
                    status.GetIssues().ToOneLineString().Contains("error: path exist")) {
                    TActivationContext::ActorSystem()->Register(
                        new TGenerateRecoverySQLIfExternalDataSourceAlreadyExistsActor(
                            sender,
                            request,
                            permissions,
                            requestTimeout,
                            counters.GetCommonCounters(
                                RTC_CREATE_CONNECTION_IN_YDB))); // change counter
                    return true;
                }
                return false;
            };
        statements.push_back(
            TSchemaQueryTask{.SQL = TString{MakeCreateExternalDataSourceQuery(
                                 connectionContent, objectStorageEndpoint, signer)},
                             .ScheduleErrorRecoverySQLGeneration =
                                 alreadyExistRecoveryActorFactoryMethod});
        return statements;
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        if (status == NYdb::EStatus::ALREADY_EXISTS) {
            return "External data source with such name already exists";
        } else {
            return "Couldn't create external data source in YDB";
        }
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

NActors::IActor* MakeModifyConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint = commonConfig.GetObjectStorageEndpoint(),
         signer                = std::move(signer)](
            const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        using namespace fmt::literals;

        auto& oldConnectionContent = (*request->Get()->OldConnectionContent);
        auto& oldBindings          = request->Get()->OldBindingContents;
        auto& newConnectionContent = request->Get()->Request.content();

        auto dropOldSecret =
            DropSecretObjectQuery(oldConnectionContent.name());
        auto createNewSecret =
            CreateSecretObjectQuery(newConnectionContent.setting(),
                                    newConnectionContent.name(),
                                    signer);
        std::vector<TSchemaQueryTask> statements;

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
                        return MakeCreateExternalDataTableQuery(binding,
                                                                oldConnectionContent.name());
                    })});
        };

        statements.push_back(TSchemaQueryTask{
            .SQL = TString{MakeDeleteExternalDataSourceQuery(oldConnectionContent.name())},
            .RollbackSQL = TString{MakeCreateExternalDataSourceQuery(
                oldConnectionContent, objectStorageEndpoint, signer)}});

        if (dropOldSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *dropOldSecret,
                                 .RollbackSQL = CreateSecretObjectQuery(
                                     oldConnectionContent.setting(),
                                     oldConnectionContent.name(),
                                     signer)});
        }
        if (createNewSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *createNewSecret,
                                 .RollbackSQL = DropSecretObjectQuery(newConnectionContent.name())});
        }

        statements.push_back(
            TSchemaQueryTask{.SQL         = TString{MakeCreateExternalDataSourceQuery(
                                 newConnectionContent, objectStorageEndpoint, signer)},
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
                                            binding, newConnectionContent.name());
                                    }),
                .RollbackSQL =
                    JoinMapRange("\n",
                                 request->Get()->OldBindingContents.begin(),
                                 request->Get()->OldBindingContents.end(),
                                 [](const FederatedQuery::BindingContent& binding) {
                                     return MakeDeleteExternalDataTableQuery(binding.name());
                                 })});
        };

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

NActors::IActor* MakeDeleteConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint = commonConfig.GetObjectStorageEndpoint(),
         signer                = std::move(signer)](
            const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = *request->Get()->ConnectionContent;

        auto dropSecret =
            DropSecretObjectQuery(connectionContent.name());

        std::vector<TSchemaQueryTask> statements = {TSchemaQueryTask{
            .SQL = TString{MakeDeleteExternalDataSourceQuery(connectionContent.name())},
            .RollbackSQL = MakeCreateExternalDataSourceQuery(connectionContent,
                                                             objectStorageEndpoint,
                                                             signer)}};
        if (dropSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *dropSecret,
                                 .RollbackSQL = CreateSecretObjectQuery(
                                     connectionContent.setting(),
                                     connectionContent.name(),
                                     signer)});
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
NActors::IActor* MakeCreateBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TPermissions permissions) {
    auto queryFactoryMethod =
        [requestTimeout,
         &counters, permissions](const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& bindingContent     = request->Get()->Request.content();
        auto& externalSourceName = *request->Get()->ConnectionName;
        std::vector<TSchemaQueryTask> statements;

        TScheduleErrorRecoverySQLGeneration alreadyExistRecoveryActorFactoryMethod =
            [&request, requestTimeout, &counters, permissions](NActors::TActorId sender,
                                                  const TStatus& status) {
                if (status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS ||
                    status.GetIssues().ToOneLineString().Contains("error: path exist")) {
                    TActivationContext::ActorSystem()->Register(
                        new TGenerateRecoverySQLIfExternalDataTableAlreadyExistsActor(
                            sender,
                            request,
                            permissions,
                            requestTimeout,
                            counters.GetCommonCounters(
                                RTC_CREATE_CONNECTION_IN_YDB))); // change counter
                    return true;
                }
                return false;
            };
        statements.push_back(TSchemaQueryTask{
            .SQL = TString{MakeCreateExternalDataTableQuery(bindingContent,
                                                            externalSourceName)},
            .ScheduleErrorRecoverySQLGeneration = alreadyExistRecoveryActorFactoryMethod});
        return statements;
    };

    auto errorMessageFactoryMethod = [](const EStatus status,
                                        const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        if (status == NYdb::EStatus::ALREADY_EXISTS) {
            return "External data table with such name already exists";
        } else {
            return "Couldn't create external data table in YDB";
        }
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

NActors::IActor* MakeModifyBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto sourceName   = *request->Get()->ConnectionName;
        auto oldTableName = request->Get()->OldBindingContent->name();

        auto deleteOldEntities = MakeDeleteExternalDataTableQuery(oldTableName);
        auto createOldEntities =
            MakeCreateExternalDataTableQuery(*request->Get()->OldBindingContent,
                                             sourceName);
        auto createNewEntities =
            MakeCreateExternalDataTableQuery(request->Get()->Request.content(), sourceName);

        return {TSchemaQueryTask{.SQL = deleteOldEntities, .RollbackSQL = createOldEntities},
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

NActors::IActor* MakeDeleteBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request) -> TString {
        return MakeDeleteExternalDataTableQuery(*request->Get()->OldBindingName);
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

} // namespace NPrivate
} // namespace NFq
