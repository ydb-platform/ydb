#include "base_actor.h"
#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <util/string/join.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
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

struct TSchemaQueryTask {
    TString SQL;
    TMaybe<TString> RollbackSQL;
};

template<class TEventRequest, class TEventResponse>
class TSchemaQueryYDBActor :
    public TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>> {
private:
    struct TEvPrivate {
        enum EEv {
            EvQueryExecutionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
                      "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvQueryExecutionResponse :
            NActors::TEventLocal<TEvQueryExecutionResponse, EvQueryExecutionResponse> {
            TStatus Result;
            size_t TaskIndex = 0u;
            bool Rollback    = false;
            TMaybe<TStatus> MaybeInitialStatus;

            TEvQueryExecutionResponse(TStatus result,
                                      size_t taskIndex,
                                      bool rollback,
                                      TMaybe<TStatus> MaybeInitialStatus)
                : Result(std::move(result))
                , TaskIndex(taskIndex)
                , Rollback(rollback)
                , MaybeInitialStatus(std::move(MaybeInitialStatus)) { }
        };
    };

    using TBase = TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>;
    using TBase::SelfId;
    using TBase::Request;

    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    using TTasks                     = std::vector<TSchemaQueryTask>;
    using TTasksFactoryMethod        = std::function<TTasks(const TEventRequestPtr& request)>;
    using TQueryFactoryMethod        = std::function<TString(const TEventRequestPtr& request)>;
    using TErrorMessageFactoryMethod = std::function<TString(const TStatus& status)>;

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const NPrivate::TRequestCommonCountersPtr& counters,
                         TQueryFactoryMethod queryFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod,
                         bool successOnAlreadyExists = false)
        : TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks{TSchemaQueryTask{.SQL = queryFactoryMethod(Request)}}
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod)
        , SuccessOnAlreadyExists(successOnAlreadyExists)
    { }

    TSchemaQueryYDBActor(const TActorId& proxyActorId,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const NPrivate::TRequestCommonCountersPtr& counters,
                         TTasksFactoryMethod tasksFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod,
                         bool successOnAlreadyExists = false)
        : TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>(
              proxyActorId, std::move(request), requestTimeout, counters)
        , Tasks(tasksFactoryMethod(Request))
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod)
        , SuccessOnAlreadyExists(successOnAlreadyExists)
        { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_YDB_SCHEMA_QUERY_ACTOR";

    void BootstrapImpl() override {
        CPP_LOG_I("TSchemaQueryYDBActor BootstrapImpl. Actor id: " << TBase::SelfId());
        InitiateSchemaQueryExecution(0, false, Nothing());
    }

    TMaybe<TString> SelectTask(size_t taskIndex, bool rollback) {
        if (!rollback) {
            if (taskIndex < Tasks.size()) {
                return Tasks[taskIndex].SQL;
            }
            return Nothing();
        }

        while (true) {
            const auto& maybeRollback = Tasks[taskIndex].RollbackSQL;
            if (maybeRollback) {
                return maybeRollback;
            }
            if (taskIndex == 0u) {
                return Nothing();
            }
            taskIndex--;
        }
    }

    bool InitiateSchemaQueryExecution(size_t taskIndex,
                                      bool rollback,
                                      const TMaybe<TStatus>& maybeInitialStatus) {
        CPP_LOG_I(
            "TSchemaQueryYDBActor Executing schema query. Actor id: " << TBase::SelfId());
        auto schemeQuery = SelectTask(taskIndex, rollback);
        if (schemeQuery) {
            CPP_LOG_I("TSchemaQueryYDBActor Executing schema query. schemeQuery: "
                      << schemeQuery);
            Request->Get()
                ->YDBClient
                ->RetryOperation([query = *schemeQuery](TSession session) {
                    return session.ExecuteSchemeQuery(query);
                })
                .Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(),
                            self        = SelfId(),
                            taskIndex,
                            rollback,
                            maybeInitialStatus](const TAsyncStatus& future) {
                    actorSystem->Send(self,
                                      new typename TEvPrivate::TEvQueryExecutionResponse{
                                          std::move(future.GetValueSync()),
                                          taskIndex,
                                          rollback,
                                          std::move(maybeInitialStatus)});
                });
        }
        return schemeQuery.Defined();
    }

    STRICT_STFUNC(StateFunc,
                  cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
                  hFunc(TEvPrivate::TEvQueryExecutionResponse, Handle);
    )

    void FinishSuccessfully(bool isAlreadyExistSuccessStatus) {
        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished successfully. Actor id: "
                  << TBase::SelfId());
        Request->Get()->ComputeYDBOperationWasPerformed = true;
        Request->Get()->ComputeYDBIsAlreadyExistFlag    = isAlreadyExistSuccessStatus;
        TBase::SendRequestToSender();
    }

    void SendError(const TStatus& executeSchemeQueryStatus) {
        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished with issues. Actor id: "
                  << TBase::SelfId());
        TString errorMessage = ErrorMessageFactoryMethod(executeSchemeQueryStatus);

        TBase::HandleError(errorMessage,
                           executeSchemeQueryStatus.GetStatus(),
                           executeSchemeQueryStatus.GetIssues());
    }



    void Handle(typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        const auto& executeSchemeQueryStatus = event->Get()->Result;
        auto isRollback                      = event->Get()->Rollback;
        auto isAlreadyExistSuccessStatus =
            IsAlreadyExistSuccessStatus(executeSchemeQueryStatus);
        auto isExecuteStatementSuccessful =
            executeSchemeQueryStatus.IsSuccess() || isAlreadyExistSuccessStatus;
        auto successExecutionRunMode      = isExecuteStatementSuccessful && !isRollback;
        auto successExecutionRollbackMode = isExecuteStatementSuccessful && isRollback;
        auto failedExecutionRunMode       = !isExecuteStatementSuccessful && !isRollback;

        if (successExecutionRunMode) {
            if (!InitiateSchemaQueryExecution(event->Get()->TaskIndex + 1, false, Nothing())) {
                FinishSuccessfully(isAlreadyExistSuccessStatus);
                return;
            }
        } else if (successExecutionRollbackMode) {
            if (event->Get()->TaskIndex == 0 ||
                !InitiateSchemaQueryExecution(event->Get()->TaskIndex - 1,
                                              true,
                                              std::move(event->Get()->MaybeInitialStatus))) {
                SendError(*event->Get()->MaybeInitialStatus);
                return;
            }
        } else if (failedExecutionRunMode) {
            if (event->Get()->TaskIndex == 0 ||
                !InitiateSchemaQueryExecution(event->Get()->TaskIndex - 1,
                                              true,
                                              std::move(event->Get()->Result))) {
                SendError(event->Get()->Result);
                return;
            }
        } else {
            // Failed during rollback
            const auto& initialIssues = *(event->Get()->MaybeInitialStatus);

            auto originalIssue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Couldn't execute SQL script");
            for (const auto& subIssue : initialIssues.GetIssues()) {
                originalIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            auto rollbackIssue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR,
                               "Couldn't execute rollback SQL script");
            for (const auto& subIssue : event->Get()->Result.GetIssues()) {
                originalIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendError(TStatus{initialIssues.GetStatus(),
                              NYql::TIssues{std::move(originalIssue),
                                            std::move(rollbackIssue)}});
            return;
        }
    }

private:
    bool IsAlreadyExistSuccessStatus(const TStatus& status) const {
        return SuccessOnAlreadyExists &&
               (status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS ||
                status.GetIssues().ToOneLineString().Contains("error: path exist"));
    }

private:
    TTasks Tasks;
    TErrorMessageFactoryMethod ErrorMessageFactoryMethod;
    bool SuccessOnAlreadyExists = false;
};

/// Connection actors
NActors::IActor* MakeCreateConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer,
    bool successOnAlreadyExists) {
    auto queryFactoryMethod =
        [objectStorageEndpoint = commonConfig.GetObjectStorageEndpoint(),
         signer                = std::move(signer)](
            const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = request->Get()->Request.content();

        auto createSecretStatement =
            CreateSecretObjectQuery(connectionContent.setting().object_storage().auth(),
                                    connectionContent.name(),
                                    signer);

        std::vector<TSchemaQueryTask> statements;
        if (createSecretStatement) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *createSecretStatement,
                                 .RollbackSQL = DropSecretObjectQuery(
                                     connectionContent.setting().object_storage().auth(),
                                     connectionContent.name(),
                                     signer)});
        }
        statements.push_back(
            TSchemaQueryTask{.SQL = TString{MakeCreateExternalDataSourceQuery(
                                 connectionContent, objectStorageEndpoint, signer)}});
        return statements;
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        if (queryStatus.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
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
        errorMessageFactoryMethod,
        successOnAlreadyExists);
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
            DropSecretObjectQuery(oldConnectionContent.setting().object_storage().auth(),
                                  oldConnectionContent.name(),
                                  signer);
        auto createNewSecret =
            CreateSecretObjectQuery(newConnectionContent.setting().object_storage().auth(),
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
                                     oldConnectionContent.setting().object_storage().auth(),
                                     oldConnectionContent.name(),
                                     signer)});
        }
        if (createNewSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *createNewSecret,
                                 .RollbackSQL = DropSecretObjectQuery(
                                     newConnectionContent.setting().object_storage().auth(),
                                     newConnectionContent.name(),
                                     signer)});
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

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
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
            DropSecretObjectQuery(connectionContent.setting().object_storage().auth(),
                                  connectionContent.name(),
                                  signer);

        std::vector<TSchemaQueryTask> statements = {TSchemaQueryTask{
            .SQL = TString{MakeDeleteExternalDataSourceQuery(connectionContent.name())},
            .RollbackSQL = MakeCreateExternalDataSourceQuery(connectionContent,
                                                             objectStorageEndpoint,
                                                             signer)}};
        if (dropSecret) {
            statements.push_back(
                TSchemaQueryTask{.SQL         = *dropSecret,
                                 .RollbackSQL = CreateSecretObjectQuery(
                                     connectionContent.setting().object_storage().auth(),
                                     connectionContent.name(),
                                     signer)});
        }
        return statements;
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
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

NActors::IActor* MakeDropCreateConnectionActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const NConfig::TCommonConfig& commonConfig,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint = commonConfig.GetObjectStorageEndpoint(),
         signer                = std::move(signer)](
            const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request)
        -> std::vector<TSchemaQueryTask> {
        auto& connectionContent = request->Get()->Request.content();
        return {TSchemaQueryTask{.SQL = TString{MakeDeleteExternalDataSourceQuery(
                                     connectionContent.name())}},
                TSchemaQueryTask{.SQL = TString{MakeCreateExternalDataSourceQuery(
                                     connectionContent, objectStorageEndpoint, signer)}}};
    };
    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
        return "Couldn't recreate external source in YDB";
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

/// Bindings actors
NActors::IActor* MakeCreateBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    bool successOnAlreadyExists) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request) -> TString {
        auto externalSourceName = *request->Get()->ConnectionName;
        return MakeCreateExternalDataTableQuery(request->Get()->Request.content(),
                                                externalSourceName);
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        if (queryStatus.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
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
        errorMessageFactoryMethod,
        successOnAlreadyExists);
}

NActors::IActor* MakeModifyBindingActor(
    const TActorId& proxyActorId,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request) -> std::vector<TSchemaQueryTask> {
        auto sourceName   = *request->Get()->ConnectionName;
        auto oldTableName = request->Get()->OldBindingContent->name();

        auto deleteOldEntities = MakeDeleteExternalDataTableQuery(oldTableName);
        auto createOldEntities =
            MakeCreateExternalDataTableQuery(*request->Get()->OldBindingContent, sourceName);
        auto createNewEntities =
            MakeCreateExternalDataTableQuery(request->Get()->Request.content(), sourceName);

        return {TSchemaQueryTask{.SQL = deleteOldEntities, .RollbackSQL = createOldEntities},
                TSchemaQueryTask{.SQL = createNewEntities}};
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
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

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
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
