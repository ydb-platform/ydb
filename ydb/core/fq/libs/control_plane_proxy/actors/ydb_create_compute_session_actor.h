#pragma once

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/base_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/control_plane_storage_requester_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/counters.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/query_utils.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/ydb_schema_query_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <contrib/libs/fmt/include/fmt/format.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/join.h>
#include <util/system/types.h>

namespace NFq {
namespace NPrivate {

using namespace NActors;
using namespace NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using TTableClientPtr = std::shared_ptr<NYdb::NTable::TTableClient>;

struct TEvPrivate {
    enum EEv {
        EvCreateSessionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvCreateSessionResponse :
        NActors::TEventLocal<TEvCreateSessionResponse, EvCreateSessionResponse> {
        TCreateSessionResult Result;

        TEvCreateSessionResponse(TCreateSessionResult result)
            : Result(std::move(result)) { }
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
                  "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
};

template<typename TEventRequest, typename TEventResponse>
class TCreateYdbComputeSessionActor :
    public TBaseActor<TCreateYdbComputeSessionActor<TEventRequest, TEventResponse>> {
private:
    using TBase = TBaseActor<TCreateYdbComputeSessionActor<TEventRequest, TEventResponse>>;
    using TBase::SelfId;
    using TBase::SendRequestToSender;
    using TBase::Request;
    using TEventRequestPtr = typename TEventRequest::TPtr;

public:
    TCreateYdbComputeSessionActor(
        const TActorId& sender,
        const TEventRequestPtr request,
        TDuration requestTimeout,
        const NPrivate::TRequestCommonCountersPtr& counters,
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
        : TBaseActor<TCreateYdbComputeSessionActor<TEventRequest, TEventResponse>>(
              sender, std::move(request), requestTimeout, counters)
        , ComputeConfig(computeConfig)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_DELETE_CONNECTION_IN_YDB";

    void BootstrapImpl() override {
        InitiateConnectionCreation();
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        hFunc(TEvPrivate::TEvCreateSessionResponse, Handle);
    )

    void InitiateConnectionCreation() {
        CPP_LOG_D("TCreateYdbComputeSessionActor InitiateConnectionCreation called. Actor id: "
                  << SelfId());
        auto tableClient = CreateNewTableClient(ComputeConfig,
                                                YqSharedResources,
                                                CredentialsProviderFactory);
        tableClient->CreateSession().Subscribe(
            [actorSystem = NActors::TActivationContext::ActorSystem(),
             self        = SelfId()](const TAsyncCreateSessionResult& future) {
                actorSystem->Send(self,
                                  new TEvPrivate::TEvCreateSessionResponse(
                                      future.GetValueSync()));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResponse::TPtr& event) {
        CPP_LOG_D("TCreateYdbComputeSessionActor "
                  "Handle for TEvCreateSessionResponse called. Actor id: "
                  << SelfId());
        const auto& createSessionResult = event->Get()->Result;
        if (!createSessionResult.IsSuccess()) {
            TBase::HandleError("Couldn't create YDB session",
                               createSessionResult.GetStatus(),
                               createSessionResult.GetIssues());
            return;
        }

        CPP_LOG_D("TCreateYdbComputeSessionActor Session was successfully acquired. Actor id: "
                  << SelfId());
        Request->Get()->YDBSession = createSessionResult.GetSession();
        SendRequestToSender();
    }

private:
    const NFq::TComputeConfig ComputeConfig;
    const TYqSharedResources::TPtr YqSharedResources;
    const NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;

    TTableClientPtr CreateNewTableClient(
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {

        auto scope = "yandexcloud://" + Request->Get()->FolderId;
        NFq::NConfig::TYdbStorageConfig computeConnection =
            computeConfig.GetConnection(scope);
        computeConnection.set_endpoint(
            Request->Get()->ComputeDatabase->connection().endpoint());
        computeConnection.set_database(
            Request->Get()->ComputeDatabase->connection().database());
        computeConnection.set_usessl(
            Request->Get()->ComputeDatabase->connection().usessl());

        auto tableSettings =
            GetClientSettings<NYdb::NTable::TClientSettings>(computeConnection,
                                                             credentialsProviderFactory);
        return std::make_shared<NYdb::NTable::TTableClient>(
            yqSharedResources->UserSpaceYdbDriver, tableSettings);
    }
};

template<typename TEventRequest, typename TEventResponse>
struct TBaseActorTypeTag<TCreateYdbComputeSessionActor<TEventRequest, TEventResponse>> {
    using TRequest  = TEventRequest;
    using TResponse = TEventResponse;
};

template<typename TEventRequest, typename TEventResponse>
NActors::IActor* MakeComputeYDBSessionActor(
    const NActors::TActorId sender,
    const typename TEventRequest::TPtr request,
    TDuration requestTimeout,
    const NPrivate::TRequestCommonCountersPtr& counters,
    const NFq::TComputeConfig& computeConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    return new NPrivate::TCreateYdbComputeSessionActor<TEventRequest, TEventResponse>(
        sender,
        std::move(request),
        requestTimeout,
        counters,
        computeConfig,
        yqSharedResources,
        credentialsProviderFactory);
}

} // namespace NPrivate
} // namespace NFq
