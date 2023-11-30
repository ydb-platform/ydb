#include "http_service.h"
#include "http_handler.h"
#include "http_req.h"
#include "http_router.h"
#include "fq_handlers.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/file.h>

#include <ydb/library/http_proxy/error/error.h>

namespace NKikimr::NPublicHttp {
namespace {
    template <typename T>
    THttpHandler CreateHttpHandler() {
        return [](const THttpRequestContext& request) {
            return new T(request);
        };
    }

    TString LoadOpenApiSpec() {
        TString blob;
        if (!NResource::FindExact("resources/openapi.yaml", &blob)) {
            return {};
        }
        return blob;
    }

    TString GetOpenApiSpec() {
        // cache in static var
        static TString spec = LoadOpenApiSpec();
        return spec;
    }
}
    using namespace NActors;

    class TPublicHttpActor : public NActors::TActorBootstrapped<TPublicHttpActor> {
        using TBase = NActors::TActorBootstrapped<TPublicHttpActor>;
    private:
        const NKikimrConfig::THttpProxyConfig Config;
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        THttpRequestRouter Router;

    public:
        TPublicHttpActor(const NKikimrConfig::THttpProxyConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
            : Config(config)
            , Counters(counters)
        {
            Router.RegisterHandler(HTTP_METHOD_POST, "/api/fq/v1/queries", CreateHttpHandler<TJsonCreateQuery>());
            Router.RegisterHandler(HTTP_METHOD_GET,  "/api/fq/v1/queries/{query_id}", CreateHttpHandler<TJsonGetQuery>());
            Router.RegisterHandler(HTTP_METHOD_GET,  "/api/fq/v1/queries/{query_id}/status", CreateHttpHandler<TJsonGetQueryStatus>());
            Router.RegisterHandler(HTTP_METHOD_GET,  "/api/fq/v1/queries/{query_id}/results/{result_set_index}", CreateHttpHandler<TJsonGetResultData>());
            Router.RegisterHandler(HTTP_METHOD_POST, "/api/fq/v1/queries/{query_id}/stop", CreateHttpHandler<TJsonStopQuery>());
        }

        void Bootstrap(const TActorContext& ctx) {
            TBase::Become(&TPublicHttpActor::StateWork);
            THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> ev = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(Config.GetPort());
            ev->Secure = Config.GetSecure();
            ev->CertificateFile = Config.GetCert();
            ev->PrivateKeyFile = Config.GetKey();

            ctx.Send(new NActors::IEventHandle(MakePublicHttpServerID(), TActorId(), ev.Release(), 0, true));
            ctx.Send(MakePublicHttpServerID(), new NHttp::TEvHttpProxy::TEvRegisterHandler("/", MakePublicHttpID()));
        }


    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, const TActorContext& ctx) {
            auto httpRequest = ev->Get()->Request;
            THttpRequestContext requestContext(ctx.ActorSystem(), httpRequest, ev->Sender, TInstant::Now(), Counters);
            if (AsciiEqualsIgnoreCase(httpRequest->URL, "/ping")) {
                requestContext.ResponseOK();
                return;
            }

            if (AsciiEqualsIgnoreCase(httpRequest->URL, "/resources/v1/openapi.yaml")) {
                requestContext.ResponseOKUtf8Text(GetOpenApiSpec());
                return;
            }

            TStringBuf url = httpRequest->URL.Before('?');
            auto handlerWithParamsO = Router.ResolveHandler(httpRequest->Method, url);
            if (!handlerWithParamsO) {
                requestContext.ResponseNotFound();
                return;
            }
            requestContext.SetPathParams(std::move(handlerWithParamsO->PathParams));
            requestContext.SetPathPattern(handlerWithParamsO->PathPattern);

            try {
                ctx.ExecutorThread.RegisterActor(handlerWithParamsO->Handler(requestContext));
            } catch (const std::exception& e) {
                requestContext.ResponseBadRequest(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Error in request processing: " << e.what());
                return;
            }

        }
    };

    NActors::IActor* CreatePublicHttp(const NKikimrConfig::THttpProxyConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
        return new TPublicHttpActor(config, counters);
    }

    void Initialize(NActors::TActorSystemSetup::TLocalServices& localServices, const TAppData& appData, const NKikimrConfig::THttpProxyConfig& config) {
        if (!config.GetPort()) {
            return;
        }


        auto actor = NKikimr::NPublicHttp::CreatePublicHttp(config, appData.Counters->GetSubgroup("counters", "public_http"));
        localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            MakePublicHttpID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

        actor = NHttp::CreateHttpProxy();
        localServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            MakePublicHttpServerID(),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData.UserPoolId)));

    }
} // namespace NKikimr::NPublicHttp
