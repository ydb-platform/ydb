#include "base_actor.h"
#include "query_utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;
using namespace NFq::NConfig;
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

            TEvQueryExecutionResponse(TStatus result)
                : Result(std::move(result)) { }
        };
    };

    using TBase = TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>;
    using TBase::SelfId;
    using TBase::Request;

    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    using TQueryFactoryMethod = std::function<TString(const TEventRequestPtr& request)>;
    using TErrorMessageFactoryMethod = std::function<TString(const TStatus& status)>;

    TSchemaQueryYDBActor(const TActorId& sender,
                         const TEventRequestPtr request,
                         TDuration requestTimeout,
                         const NPrivate::TRequestCommonCountersPtr& counters,
                         TQueryFactoryMethod queryFactoryMethod,
                         TErrorMessageFactoryMethod errorMessageFactoryMethod)
        : TBaseActor<TSchemaQueryYDBActor<TEventRequest, TEventResponse>>(
              sender, std::move(request), requestTimeout, counters)
        , QueryFactoryMethod(queryFactoryMethod)
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_DELETE_CONNECTION_IN_YDB";

    void BootstrapImpl() override {
        CPP_LOG_I("TSchemaQueryYDBActor BootstrapImpl. Actor id: " << TBase::SelfId());
        InitiateSchemaQueryExecution();
    }

    void InitiateSchemaQueryExecution() {
        CPP_LOG_I("TSchemaQueryYDBActor Executing schema query. Actor id: " << TBase::SelfId());

        const auto& request = Request;
        TString schemeQuery = QueryFactoryMethod(request);
        request->Get()
            ->YDBSession->ExecuteSchemeQuery(schemeQuery)
            .Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(),
                        self        = SelfId()](const TAsyncStatus& future) {
                actorSystem->Send(self,
                                  new typename TEvPrivate::TEvQueryExecutionResponse(
                                      std::move(future.GetValueSync())));
            });
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        hFunc(TEvPrivate::TEvQueryExecutionResponse, Handle);
    )

    void Handle(typename TEvPrivate::TEvQueryExecutionResponse::TPtr& event) {
        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Actor id: " << TBase::SelfId());
        const auto& executeSchemeQueryStatus = event->Get()->Result;
        if (!executeSchemeQueryStatus.IsSuccess()) {
            CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished with issues. Actor id: " << TBase::SelfId());
            TString errorMessage = ErrorMessageFactoryMethod(executeSchemeQueryStatus);

            TBase::HandleError(errorMessage,
                               executeSchemeQueryStatus.GetStatus(),
                               executeSchemeQueryStatus.GetIssues());
            return;
        }

        CPP_LOG_I("TSchemaQueryYDBActor Handling query execution response. Query finished successfully. Actor id: " << TBase::SelfId());
        Request->Get()->ComputeYDBOperationWasPerformed = true;
        TBase::SendRequestToSender();
    }

private:
    TQueryFactoryMethod QueryFactoryMethod;
    TErrorMessageFactoryMethod ErrorMessageFactoryMethod;
};

/// Connection actors
NActors::IActor* MakeCreateConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TString& objectStorageEndpoint,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint, signer = std::move(signer)](
            const TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& request) -> TString {
        using namespace fmt::literals;

        return MakeCreateExternalDataSourceQuery(request->Get()->Request.content(),
                                                 objectStorageEndpoint,
                                                 signer);
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
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_CREATE_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

NActors::IActor* MakeModifyConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    const TString& objectStorageEndpoint,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [objectStorageEndpoint, signer = std::move(signer)](
            const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request) -> TString {
        using namespace fmt::literals;

        auto& oldConnectionContent = (*request->Get()->OldConnectionContent);
        auto& newConnectionContent  = request->Get()->Request.content();
        return fmt::format(
            R"(
                {delete_external_data_source};
                {create_external_data_source};
            )",
            "delete_external_data_source"_a =
                MakeDeleteExternalDataSourceQuery(oldConnectionContent, signer),
            "create_external_data_source"_a = MakeCreateExternalDataSourceQuery(
                newConnectionContent, objectStorageEndpoint, signer));
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
        return "Couldn't modify external data source in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvModifyConnectionRequest,
                                    TEvControlPlaneProxy::TEvModifyConnectionResponse>(
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_MODIFY_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

NActors::IActor* MakeDeleteConnectionActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters,
    TSigner::TPtr signer) {
    auto queryFactoryMethod =
        [signer = std::move(signer)](
            const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request) -> TString {
        using namespace fmt::literals;
        return MakeDeleteExternalDataSourceQuery(*request->Get()->ConnectionContent,
                                                 signer);
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        if (queryStatus.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
            return "External data source with such name already exists";
        } else {
            return "Couldn't delete external data source in YDB";
        }
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest,
                                    TEvControlPlaneProxy::TEvDeleteConnectionResponse>(
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_DELETE_CONNECTION_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

/// Bindings actors
NActors::IActor* MakeCreateBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request) -> TString {
        using namespace fmt::literals;

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
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_CREATE_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

NActors::IActor* MakeModifyBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request) -> TString {
        using namespace fmt::literals;

        auto sourceName   = *request->Get()->ConnectionName;
        auto oldTableName = *request->Get()->OldBindingName;
        return fmt::format(
            R"(
                {delete_external_data_table};
                {create_external_data_table};
            )",
            "delete_external_data_table"_a = MakeDeleteExternalDataTableQuery(oldTableName),
            "create_external_data_table"_a =
                MakeCreateExternalDataTableQuery(request->Get()->Request.content(),
                                                 sourceName));
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        Y_UNUSED(queryStatus);
        return "Couldn't modify external data table in YDB";
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvModifyBindingRequest,
                                    TEvControlPlaneProxy::TEvModifyBindingResponse>(
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_MODIFY_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

NActors::IActor* MakeDeleteBindingActor(
    const TActorId& sender,
    TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr request,
    TDuration requestTimeout,
    TCounters& counters) {
    auto queryFactoryMethod =
        [](const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request) -> TString {
        using namespace fmt::literals;
        return MakeDeleteExternalDataTableQuery(*request->Get()->OldBindingName);
    };

    auto errorMessageFactoryMethod = [](const TStatus& queryStatus) -> TString {
        if (queryStatus.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
            return "External data source with such name already exists";
        } else {
            return "Couldn't delete external data source in YDB";
        }
    };

    return new TSchemaQueryYDBActor<TEvControlPlaneProxy::TEvDeleteBindingRequest,
                                    TEvControlPlaneProxy::TEvDeleteBindingResponse>(
        sender,
        std::move(request),
        requestTimeout,
        counters.GetCommonCounters(RTC_DELETE_BINDING_IN_YDB),
        queryFactoryMethod,
        errorMessageFactoryMethod);
}

} // namespace NPrivate
} // namespace NFq
