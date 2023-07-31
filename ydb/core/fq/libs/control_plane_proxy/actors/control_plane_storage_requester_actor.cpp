#include "control_plane_storage_requester_actor.h"
#include "base_actor.h"
#include "util/generic/maybe.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <library/cpp/actors/core/event.h>

#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;
using namespace NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;

template<class TEventRequest, class TEventResponse, class TCPSEventRequest, class TCPSEventResponse>
class TControlPlaneStorageRequesterActor;

template<class TEventRequest, class TEventResponse, class TCPSEventRequest, class TCPSEventResponse>
struct TBaseActorTypeTag<
    TControlPlaneStorageRequesterActor<TEventRequest, TEventResponse, TCPSEventRequest, TCPSEventResponse>> {
    using TRequest  = TEventRequest;
    using TResponse = TEventResponse;
};

template<class TEventRequest, class TEventResponse, class TCPSEventRequest, class TCPSEventResponse>
class TControlPlaneStorageRequesterActor :
    public TBaseActor<
        TControlPlaneStorageRequesterActor<TEventRequest, TEventResponse, TCPSEventRequest, TCPSEventResponse>> {
private:
    using TBase = TBaseActor<
        TControlPlaneStorageRequesterActor<TEventRequest, TEventResponse, TCPSEventRequest, TCPSEventResponse>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::Request;

    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    using TCPSRequestFactory =
        std::function<typename TCPSEventRequest::TProto(const TEventRequestPtr& request)>;
    using TErrorMessageFactoryMethod = std::function<TString(const NYql::TIssues& issues)>;
    using TEntityNameExtractorFactoryMethod =
        std::function<void(const TEventRequestPtr& request,
                           const typename TCPSEventResponse::TProto& response)>;

    TControlPlaneStorageRequesterActor(const TActorId& sender,
                            const TEventRequestPtr request,
                            TDuration requestTimeout,
                            const NPrivate::TRequestCommonCountersPtr& counters,
                            TPermissions permissions,
                            TCPSRequestFactory cpsRequestFactory,
                            TErrorMessageFactoryMethod errorMessageFactoryMethod,
                            TEntityNameExtractorFactoryMethod entityNameExtractorFactoryMethod)
        : TBaseActor<
              TControlPlaneStorageRequesterActor<TEventRequest, TEventResponse, TCPSEventRequest, TCPSEventResponse>>(
              sender, std::move(request), requestTimeout, counters)
        , Permissions(permissions)
        , CPSRequestFactory(cpsRequestFactory)
        , ErrorMessageFactoryMethod(errorMessageFactoryMethod)
        , EntityNameExtractorFactoryMethod(entityNameExtractorFactoryMethod) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_CONTROL_PLANE_STORAGE";

    void BootstrapImpl() override {
        SendCPSRequest();
    }

    void SendCPSRequest() {
        CPP_LOG_I("TControlPlaneStorageRequesterActor Sending CPS request. Actor id: " << TBase::SelfId());
        const auto& request = Request;
        auto event = new TCPSEventRequest("yandexcloud://" + request->Get()->FolderId,
                                          CPSRequestFactory(request),
                                          request->Get()->User,
                                          request->Get()->Token,
                                          request->Get()->CloudId,
                                          Permissions,
                                          request->Get()->Quotas,
                                          request->Get()->TenantInfo,
                                          {});
        Send(ControlPlaneStorageServiceActorId(), event);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        hFunc(TCPSEventResponse, Handle);
    )

    void Handle(typename TCPSEventResponse::TPtr& event) {
        CPP_LOG_I("TControlPlaneStorageRequesterActor Handling CPS response. Actor id: " << TBase::SelfId());
        auto issues = event->Get()->Issues;
        if (!issues.Empty()) {
            CPP_LOG_I("TControlPlaneStorageRequesterActor Handling CPS response. Request finished with issues. Actor id: " << TBase::SelfId());
            TString errorMessage = ErrorMessageFactoryMethod(issues);
            TBase::HandleError(errorMessage, issues);
            return;
        }

        CPP_LOG_I("TControlPlaneStorageRequesterActor Handling CPS response. Request finished successfully. Actor id: " << TBase::SelfId());
        EntityNameExtractorFactoryMethod(Request, event->Get()->Result);
        TBase::SendRequestToSender();
    }

private:
    TPermissions Permissions;
    TCPSRequestFactory CPSRequestFactory;
    TErrorMessageFactoryMethod ErrorMessageFactoryMethod;
    TEntityNameExtractorFactoryMethod EntityNameExtractorFactoryMethod;
};

/// Discover connection_name
TString DescribeConnectionErrorMessageFactoryMethod(const NYql::TIssues& issues) {
    Y_UNUSED(issues);
    return "Couldn't resolve connection";
};
NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& event) {
            FederatedQuery::DescribeConnectionRequest result;
            auto connectionId = event->Get()->Request.content().connection_id();
            result.set_connection_id(connectionId);
            return result;
        };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& event,
           const FederatedQuery::DescribeConnectionResult& result) {
            event->Get()->ConnectionName = result.connection().content().name();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvCreateBindingRequest,
                                       TEvControlPlaneProxy::TEvCreateBindingResponse,
                                       TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                       TEvControlPlaneStorage::TEvDescribeConnectionResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        DescribeConnectionErrorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event) {
            FederatedQuery::DescribeConnectionRequest result;
            auto connectionId = event->Get()->Request.connection_id();
            result.set_connection_id(connectionId);
            return result;
        };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event,
           const FederatedQuery::DescribeConnectionResult& result) {
            event->Get()->OldConnectionContent = result.connection().content();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvModifyConnectionRequest,
                                       TEvControlPlaneProxy::TEvModifyConnectionResponse,
                                       TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                       TEvControlPlaneStorage::TEvDescribeConnectionResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        DescribeConnectionErrorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& event) {
            FederatedQuery::DescribeConnectionRequest result;
            auto connectionId = event->Get()->Request.connection_id();
            result.set_connection_id(connectionId);
            return result;
        };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& event,
           const FederatedQuery::DescribeConnectionResult& result) {
            event->Get()->ConnectionContent = result.connection().content();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest,
                                       TEvControlPlaneProxy::TEvDeleteConnectionResponse,
                                       TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                       TEvControlPlaneStorage::TEvDescribeConnectionResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        DescribeConnectionErrorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeDiscoverYDBConnectionName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& event) {
            FederatedQuery::DescribeConnectionRequest result;
            auto connectionId = event->Get()->OldBindingContent->connection_id();
            result.set_connection_id(connectionId);
            return result;
        };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& event,
           const FederatedQuery::DescribeConnectionResult& result) {
            event->Get()->ConnectionName = result.connection().content().name();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvModifyBindingRequest,
                                       TEvControlPlaneProxy::TEvModifyBindingResponse,
                                       TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                       TEvControlPlaneStorage::TEvDescribeConnectionResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        DescribeConnectionErrorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

/// Discover binding_name

NActors::IActor* MakeDiscoverYDBBindingName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& event) {
            FederatedQuery::DescribeBindingRequest result;
            result.set_binding_id(event->Get()->Request.binding_id());
            return result;
        };

    auto errorMessageFactoryMethod = [](const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        return "Couldn't resolve binding name";
    };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& event,
           const FederatedQuery::DescribeBindingResult& result) {
            event->Get()->OldBindingContent = result.binding().content();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvModifyBindingRequest,
                                       TEvControlPlaneProxy::TEvModifyBindingResponse,
                                       TEvControlPlaneStorage::TEvDescribeBindingRequest,
                                       TEvControlPlaneStorage::TEvDescribeBindingResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        errorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeDiscoverYDBBindingName(
    const TActorId& sender,
    const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& event) {
            FederatedQuery::DescribeBindingRequest result;
            result.set_binding_id(event->Get()->Request.binding_id());
            return result;
        };

    auto errorMessageFactoryMethod = [](const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        return "Couldn't resolve binding name";
    };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& event,
           const FederatedQuery::DescribeBindingResult& result) {
            event->Get()->OldBindingName = result.binding().content().name();
        };

    return new TControlPlaneStorageRequesterActor<TEvControlPlaneProxy::TEvDeleteBindingRequest,
                                       TEvControlPlaneProxy::TEvDeleteBindingResponse,
                                       TEvControlPlaneStorage::TEvDescribeBindingRequest,
                                       TEvControlPlaneStorage::TEvDescribeBindingResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        errorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeListBindingIds(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event) {
            FederatedQuery::ListBindingsRequest result;
            auto connectionId = event->Get()->Request.connection_id();
            result.mutable_filter()->set_connection_id(connectionId);
            result.set_limit(100);
            if (event->Get()->NextListingBindingsToken) {
                result.set_page_token(*event->Get()->NextListingBindingsToken);
            }
            return result;
        };

    auto errorMessageFactoryMethod = [](const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        return "Couldn't resolve binding id(s)";
    };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event,
           const FederatedQuery::ListBindingsResult& result) {
            for (auto& binding: result.binding()) {
                event->Get()->OldBindingIds.emplace_back(binding.meta().id());
            }

            TString nextPageToken = result.next_page_token();
            if (nextPageToken == "") {
                event->Get()->NextListingBindingsToken = Nothing();
                event->Get()->OldBindingNamesDiscoveryFinished = true;
            } else {
                event->Get()->NextListingBindingsToken = nextPageToken;
            }
        };

    return new TControlPlaneStorageRequesterActor<
        TEvControlPlaneProxy::TEvModifyConnectionRequest,
        TEvControlPlaneProxy::TEvModifyConnectionResponse,
        TEvControlPlaneStorage::TEvListBindingsRequest,
        TEvControlPlaneStorage::TEvListBindingsResponse>(sender,
                                                         request,
                                                         requestTimeout,
                                                         counters.GetCommonCounters(
                                                             RTC_DESCRIBE_CPS_ENTITY),
                                                         permissions,
                                                         cpsRequestFactory,
                                                         errorMessageFactoryMethod,
                                                         entityNameExtractorFactoryMethod);
}

NActors::IActor* MakeDescribeListedBinding(
    const TActorId sender,
    const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& request,
    TCounters& counters,
    TDuration requestTimeout,
    TPermissions permissions) {
    auto cpsRequestFactory =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event) {
            auto bindingId = event->Get()->OldBindingIds[event->Get()->OldBindingContents.size()];

            FederatedQuery::DescribeBindingRequest result;
            result.set_binding_id(bindingId);
            return result;
        };

    auto errorMessageFactoryMethod = [](const NYql::TIssues& issues) -> TString {
        Y_UNUSED(issues);
        return "Couldn't resolve binding content";
    };
    auto entityNameExtractorFactoryMethod =
        [](const TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& event,
           const FederatedQuery::DescribeBindingResult& result) {
            event->Get()->OldBindingContents.push_back(result.binding().content());
        };

    return new TControlPlaneStorageRequesterActor<
        TEvControlPlaneProxy::TEvModifyConnectionRequest,
        TEvControlPlaneProxy::TEvModifyConnectionResponse,
        TEvControlPlaneStorage::TEvDescribeBindingRequest,
        TEvControlPlaneStorage::TEvDescribeBindingResponse>(
        sender,
        request,
        requestTimeout,
        counters.GetCommonCounters(RTC_DESCRIBE_CPS_ENTITY),
        permissions,
        cpsRequestFactory,
        errorMessageFactoryMethod,
        entityNameExtractorFactoryMethod);
}

} // namespace NPrivate
} // namespace NFq
