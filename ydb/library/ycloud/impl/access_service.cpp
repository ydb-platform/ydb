#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/json/json_value.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/accessservice/access_service.grpc.pb.h>
#include "access_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>

namespace NCloud {

namespace {

template <bool ClearIamCookie, typename TMaskerFunc, typename T>
T ObfuscateImpl(const T& p, const TMaskerFunc& maskFunc) {
    T r(p);
    if (r.iam_token()) {
        r.set_iam_token(std::invoke(maskFunc, r.iam_token()));
    }
    if (r.api_key()) {
        r.set_api_key(std::invoke(maskFunc, r.api_key()));
    }
    if constexpr (ClearIamCookie) {
        r.clear_iam_cookie();
    }
    return r;
}

} // namespace

using namespace NKikimr;

class TAccessServiceV1 : public NActors::TActor<TAccessServiceV1>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::servicecontrol::v1::AccessService> {
    using TThis = TAccessServiceV1;
    using TBase = NActors::TActor<TAccessServiceV1>;

    struct TAuthenticateRequestV1 : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::servicecontrol::v1::AccessService::Stub::AsyncAuthenticate;
        using TRequestEventType = TEvAccessService::TEvAuthenticateRequestV1;
        using TResponseEventType = TEvAccessService::TEvAuthenticateResponseV1;

        static yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest& p) {
            return ObfuscateImpl<true>(p, MaskToken);
        }

        static const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthenticateRequestV1::TPtr& ev) {
        MakeCall<TAuthenticateRequestV1>(std::move(ev));
    }

    struct TAuthorizeRequestV1 : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::servicecontrol::v1::AccessService::Stub::AsyncAuthorize;
        using TRequestEventType = TEvAccessService::TEvAuthorizeRequestV1;
        using TResponseEventType = TEvAccessService::TEvAuthorizeResponseV1;

        static yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest& p) {
            return ObfuscateImpl<false>(p, MaskToken);
        }

        static const yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse& Obfuscate(const yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthorizeRequestV1::TPtr& ev) {
        MakeCall<TAuthorizeRequestV1>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::ACCESS_SERVICE_ACTOR; }

    TAccessServiceV1(const TAccessServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequestV1, Handle);
            hFunc(TEvAccessService::TEvAuthorizeRequestV1, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};

class TAccessServiceV2 : public NActors::TActor<TAccessServiceV2>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::accessservice::v2::AccessService> {
    using TThis = TAccessServiceV2;
    using TBase = NActors::TActor<TAccessServiceV2>;

    struct TAuthenticateRequestV2 : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncAuthenticate;
        using TRequestEventType = TEvAccessService::TEvAuthenticateRequestV2;
        using TResponseEventType = TEvAccessService::TEvAuthenticateResponseV2;

        static yandex::cloud::priv::accessservice::v2::AuthenticateRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthenticateRequest& p) {
            return ObfuscateImpl<true>(p, MaskToken);
        }

        static const yandex::cloud::priv::accessservice::v2::AuthenticateResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthenticateResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthenticateRequestV2::TPtr& ev) {
        MakeCall<TAuthenticateRequestV2>(std::move(ev));
    }

    struct TAuthorizeRequestV2 : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncAuthorize;
        using TRequestEventType = TEvAccessService::TEvAuthorizeRequestV2;
        using TResponseEventType = TEvAccessService::TEvAuthorizeResponseV2;

        static yandex::cloud::priv::accessservice::v2::AuthorizeRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthorizeRequest& p) {
            return ObfuscateImpl<false>(p, MaskToken);
        }

        static const yandex::cloud::priv::accessservice::v2::AuthorizeResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::AuthorizeResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvAuthorizeRequestV2::TPtr& ev) {
        MakeCall<TAuthorizeRequestV2>(std::move(ev));
    }

    struct TBulkAuthorizeRequestV2 : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::accessservice::v2::AccessService::Stub::AsyncBulkAuthorize;
        using TRequestEventType = TEvAccessService::TEvBulkAuthorizeRequestV2;
        using TResponseEventType = TEvAccessService::TEvBulkAuthorizeResponseV2;

        static yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest Obfuscate(const yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest& p) {
            return ObfuscateImpl<false>(p, MaskToken);
        }

        static const yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse& Obfuscate(const yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse& p) {
            return p;
        }
    };

    void Handle(TEvAccessService::TEvBulkAuthorizeRequestV2::TPtr& ev) {
        MakeCall<TBulkAuthorizeRequestV2>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::ACCESS_SERVICE_ACTOR; }

    TAccessServiceV2(const TAccessServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvAccessService::TEvAuthenticateRequestV2, Handle);
            hFunc(TEvAccessService::TEvAuthorizeRequestV2, Handle);
            hFunc(TEvAccessService::TEvBulkAuthorizeRequestV2, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateAccessServiceV1(const TAccessServiceSettings& settings) {
    return new TAccessServiceV1(settings);
}

IActor* CreateAccessServiceV2(const TAccessServiceSettings& settings) {
    return new TAccessServiceV2(settings);
}

IActor* CreateAccessServiceV1WithCache(const TAccessServiceSettings& settings) {
    IActor* accessService = CreateAccessServiceV1(settings);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<
        TEvAccessService::TEvAuthenticateRequestV1,
        TEvAccessService::TEvAuthenticateResponseV1>(accessService);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<
        TEvAccessService::TEvAuthorizeRequestV1,
        TEvAccessService::TEvAuthorizeResponseV1>(accessService);
    return accessService;
}

IActor* CreateAccessServiceV2WithCache(const TAccessServiceSettings& settings) {
    IActor* accessService = CreateAccessServiceV2(settings);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<
        TEvAccessService::TEvAuthenticateRequestV2,
        TEvAccessService::TEvAuthenticateResponseV2>(accessService);
    accessService = NGrpcActorClient::CreateGrpcServiceCache<
        TEvAccessService::TEvAuthorizeRequestV2,
        TEvAccessService::TEvAuthorizeResponseV2>(accessService);
    return accessService;
}

} // namespace NCloud
