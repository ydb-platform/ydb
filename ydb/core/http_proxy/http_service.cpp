#include "http_service.h"
#include "http_req.h"
#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/http_proxy/error/error.h>

#include <ydb/core/protos/config.pb.h>

#include <util/stream/file.h>

namespace NKikimr::NHttpProxy {

    using namespace NActors;

    class THttpProxyActor : public NActors::TActorBootstrapped<THttpProxyActor> {
        using TBase = NActors::TActorBootstrapped<THttpProxyActor>;
    public:
        explicit THttpProxyActor(const THttpProxyConfig& cfg);

        void Bootstrap(const TActorContext& ctx);
        TStringBuilder LogPrefix() const;

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            }
        }

        void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, const TActorContext& ctx);

        NKikimrConfig::TServerlessProxyConfig Config;
        THolder<THttpRequestProcessors> Processors;
        THolder<NYdb::TDriver> Driver;
        std::shared_ptr<NYdb::ICredentialsProvider> ServiceAccountCredentialsProvider;
    };

    THttpProxyActor::THttpProxyActor(const THttpProxyConfig& cfg)
        : Config(cfg.Config)
    {
        ServiceAccountCredentialsProvider = cfg.CredentialsProvider;
        Processors = MakeHolder<THttpRequestProcessors>();
        Processors->Initialize();
        if (cfg.UseSDK) {
            auto config = NYdb::TDriverConfig().SetNetworkThreadsNum(1)
                .SetClientThreadsNum(1)
                .SetMaxQueuedRequests(10000)
                .SetGRpcKeepAlivePermitWithoutCalls(true)
                .SetGRpcKeepAliveTimeout(TDuration::Seconds(90))
                .SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
            if (Config.GetCaCert()) {
                config.UseSecureConnection(TFileInput(Config.GetCaCert()).ReadAll());
            }
            Driver = MakeHolder<NYdb::TDriver>(std::move(config));
        }
    }

    TStringBuilder THttpProxyActor::LogPrefix() const {
        return TStringBuilder() << "proxy service:";
    }

    void THttpProxyActor::Bootstrap(const TActorContext& ctx) {
        TBase::Become(&THttpProxyActor::StateWork);
        const auto& config = Config.GetHttpConfig();
        THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> ev =
            MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(config.GetPort());
        ev->MaxRecycledRequestsCount = 0;
        ev->Secure = config.GetSecure();
        ev->CertificateFile = config.GetCert();
        ev->PrivateKeyFile = config.GetKey();

        ctx.Send(new NActors::IEventHandle(MakeHttpServerServiceID(), TActorId(),
                                           ev.Release(), 0, true));
        ctx.Send(MakeHttpServerServiceID(),
                 new NHttp::TEvHttpProxy::TEvRegisterHandler("/", MakeHttpProxyID()));
    }

    void THttpProxyActor::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                                 const TActorContext& ctx) {
        if (AsciiEqualsIgnoreCase(ev->Get()->Request->URL, "/ping")) {
            auto res = ev->Get()->Request->CreateResponseOK("");
            ctx.Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
            return;
        }

        THttpRequestContext context(Config,
                                    ev->Get()->Request,
                                    ev->Sender,
                                    Driver.Get(),
                                    ServiceAccountCredentialsProvider);

        LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                      " incoming request from [" << context.SourceAddress << "]" <<
                      " request [" << context.MethodName << "]" <<
                      " url [" << context.Request->URL << "]" <<
                      " database [" << context.DatabasePath << "]" <<
                      " requestId: " << context.RequestId);

        try {
            auto signature = context.GetSignature();
            auto methodName = context.MethodName;
            Processors->Execute(std::move(context.MethodName), std::move(context), std::move(signature), ctx);
        } catch (const NKikimr::NSQS::TSQSException& e) {
            context.ResponseData.Status = NYdb::EStatus::BAD_REQUEST;
            context.ResponseData.ErrorText = e.what();
            context.DoReply(ctx, static_cast<size_t>(NYds::EErrorCodes::ACCESS_DENIED));
            return;
        }
    }

    NActors::IActor* CreateHttpProxy(const THttpProxyConfig& config) {
        return new THttpProxyActor(config);
    }

} // namespace NKikimr::NHttpProxy
