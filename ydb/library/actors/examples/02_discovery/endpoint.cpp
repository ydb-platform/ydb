#include "services.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/system/hostname.h>
#include <util/string/builder.h>

class TExampleHttpRequest : public TActor<TExampleHttpRequest> {
    TIntrusivePtr<TExampleStorageConfig> Config;
    const TString PublishKey;

    TActorId HttpProxy;
    NHttp::THttpIncomingRequestPtr Request;

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr &ev) {
        Request = std::move(ev->Get()->Request);
        HttpProxy = ev->Sender;

        Register(CreateLookupActor(Config.Get(), PublishKey, SelfId()));
    }

    void Handle(TEvExample::TEvInfo::TPtr &ev) {
        auto *msg = ev->Get();

        TStringBuilder body;
        for (const auto &x : msg->Payloads)
            body << x << Endl;

        auto response = Request->CreateResponseOK(body, "application/text; charset=utf-8");
        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));

        PassAway();
    }
public:
    static constexpr IActor::EActivityType ActorActivityType() {
        // define app-specific activity tag to track elapsed cpu | handled events | actor count in Solomon
        return EActorActivity::ACTORLIB_COMMON;
    }

    TExampleHttpRequest(TExampleStorageConfig *config, const TString &publishKey)
        : TActor(&TThis::StateWork)
        , Config(config)
        , PublishKey(publishKey)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            hFunc(TEvExample::TEvInfo, Handle);
        }
    }
};

class TExampleHttpEndpoint : public TActorBootstrapped<TExampleHttpEndpoint> {
    TIntrusivePtr<TExampleStorageConfig> Config;
    const TString PublishKey;
    const ui16 HttpPort;

    TActorId PublishActor;
    TActorId HttpProxy;

    std::shared_ptr<NMonitoring::TMetricRegistry> SensorsRegistry = std::make_shared<NMonitoring::TMetricRegistry>();

    void PassAway() override {
        Send(PublishActor, new TEvents::TEvPoison());
        Send(HttpProxy, new TEvents::TEvPoison());

        return TActor::PassAway();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr &ev) {
        const TActorId reqActor = Register(new TExampleHttpRequest(Config.Get(), PublishKey));
        TlsActivationContext->Send(ev->Forward(reqActor));
    }

public:
    static constexpr IActor::EActivityType ActorActivityType() {
        // define app-specific activity tag to track elapsed cpu | handled events | actor count in Solomon
        return EActorActivity::ACTORLIB_COMMON;
    }

    TExampleHttpEndpoint(TExampleStorageConfig *config, const TString &publishKey, ui16 port)
        : Config(config)
        , PublishKey(publishKey)
        , HttpPort(port)
    {
    }

    void Bootstrap() {
        const TString publishPayload = ToString(HttpPort);
        PublishActor = Register(CreatePublishActor(Config.Get(), PublishKey, publishPayload));
        HttpProxy = Register(NHttp::CreateHttpProxy(SensorsRegistry));

        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpPort, FQDNHostName()));
        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvRegisterHandler("/list", SelfId()));

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        default:
            break;
        }
    }
};

IActor* CreateEndpointActor(TExampleStorageConfig *config, const TString &publishKey, ui16 port) {
    return new TExampleHttpEndpoint(config, publishKey, port);
}
