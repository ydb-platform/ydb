#include "service_control.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

namespace NCloud {
namespace {

using TGrpcService = yandex::cloud::priv::servicecontrol::v1::ServiceControlService;

class TServiceControl final
    : public NActors::TActor<TServiceControl>
    , private NGrpcActorClient::TGrpcServiceClient<TGrpcService>
{
    using TThis = TServiceControl;
    using TActorBase = NActors::TActor<TServiceControl>;
    using TGrpcBase = NGrpcActorClient::TGrpcServiceClient<TGrpcService>;

    struct TEnsureEnabledCall : TGrpcBase::TGrpcRequest {
        static constexpr auto Request = &TGrpcService::Stub::AsyncEnsureEnabled;
        using TRequestEventType = TEvServiceControl::TEvEnsureEnabledRequest;
        using TResponseEventType = TEvServiceControl::TEvEnsureEnabledResponse;
    };

    struct TSetupDelegationCall : TGrpcBase::TGrpcRequest {
        static constexpr auto Request = &TGrpcService::Stub::AsyncSetupDelegation;
        using TRequestEventType = TEvServiceControl::TEvSetupDelegationRequest;
        using TResponseEventType = TEvServiceControl::TEvSetupDelegationResponse;
    };

    struct TRevokeDelegationCall : TGrpcBase::TGrpcRequest {
        static constexpr auto Request = &TGrpcService::Stub::AsyncRevokeDelegation;
        using TRequestEventType = TEvServiceControl::TEvRevokeDelegationRequest;
        using TResponseEventType = TEvServiceControl::TEvRevokeDelegationResponse;
    };

public:
    explicit TServiceControl(const TServiceControlSettings& settings)
        : TActorBase(&TThis::StateWork)
        , TGrpcBase(settings)
    {}

private:
    void Handle(TEvServiceControl::TEvEnsureEnabledRequest::TPtr& ev) {
        MakeCall<TEnsureEnabledCall>(std::move(ev));
    }

    void Handle(TEvServiceControl::TEvSetupDelegationRequest::TPtr& ev) {
        MakeCall<TSetupDelegationCall>(std::move(ev));
    }

    void Handle(TEvServiceControl::TEvRevokeDelegationRequest::TPtr& ev) {
        MakeCall<TRevokeDelegationCall>(std::move(ev));
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvServiceControl::TEvEnsureEnabledRequest, Handle);
        hFunc(TEvServiceControl::TEvSetupDelegationRequest, Handle);
        hFunc(TEvServiceControl::TEvRevokeDelegationRequest, Handle);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    )
};

} // anonymous namespace

NActors::IActor* CreateServiceControl(const TServiceControlSettings& settings) {
    return new TServiceControl(settings);
}

} // namespace NCloud
