#include "http_service.h"
#include "http_req.h"
#include "events.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/cache/cache.h>
#include <library/cpp/json/json_writer.h>

#include <ydb/library/http_proxy/error/error.h>

#include <util/stream/file.h>

namespace NKikimr::NHttpProxy {

    using namespace NActors;

    class THttpProxyActor : public NActors::TActorBootstrapped<THttpProxyActor> {
        using TBase = NActors::TActorBootstrapped<THttpProxyActor>;
    public:
        explicit THttpProxyActor(const THttpProxyConfig& cfg)
            : Config(cfg.Config)
        {
            ServiceAccountCredentialsProvider = cfg.CredentialsProvider;
            Processors = MakeHolder<THttpRequestProcessors>();
            Processors->Initialize();
            if (cfg.UseSDK) {
                auto config = NYdb::TDriverConfig().SetNetworkThreadsNum(1)
                                          .SetGRpcKeepAlivePermitWithoutCalls(true)
                                          .SetGRpcKeepAliveTimeout(TDuration::Seconds(90))
                                          .SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
                if (Config.GetCaCert()) {
                    config.UseSecureConnection(TFileInput(Config.GetCaCert()).ReadAll());
                }
                Driver = MakeHolder<NYdb::TDriver>(std::move(config));
            }
        }

        void Bootstrap(const TActorContext& ctx) {
            TBase::Become(&THttpProxyActor::StateWork);
            const auto& config = Config.GetHttpConfig();
            THolder<NHttp::TEvHttpProxy::TEvAddListeningPort> ev = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(config.GetPort());
            ev->Secure = config.GetSecure();
            ev->CertificateFile = config.GetCert();
            ev->PrivateKeyFile = config.GetKey();

            ctx.Send(new NActors::IEventHandle(MakeHttpServerServiceID(), TActorId(),
                                ev.Release(), 0, true));
            ctx.Send(MakeHttpServerServiceID(), new NHttp::TEvHttpProxy::TEvRegisterHandler("/", MakeHttpProxyID()));
        }

        TStringBuilder LogPrefix() const {
            return TStringBuilder() << "proxy service:";
        }

        void Initialize();


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

    void THttpRequestContext::SendBadRequest(NYdb::EStatus status, const TString& errorText,
                                             const TActorContext& ctx) {
        NJson::TJsonValue value;
        value.SetType(NJson::JSON_MAP);
        value["message"] = errorText;
        value["__type"] = StatusToErrorType(status);

        LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "reply with status: " << status << " message: " << errorText);
        auto res = Request->CreateResponse(
                TStringBuilder() << (int)StatusToHttpCode(status),
                StatusToErrorType(status),
                strByMime(MIME_JSON),
                NJson::WriteJson(value, false)
            );
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
    }

    void THttpRequestContext::DoReply(const TActorContext& ctx) {
        if (ResponseData.Status == NYdb::EStatus::SUCCESS) {
            LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, "reply ok");
            auto res = Request->CreateResponseOK(NJson::WriteJson(ResponseData.ResponseBody, false),
                                                 strByMime(MIME_JSON));
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
        } else {
            SendBadRequest(ResponseData.Status, ResponseData.ErrorText, ctx);
        }
    }

    void THttpProxyActor::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                                 const TActorContext& ctx) {
        if (AsciiEqualsIgnoreCase(ev->Get()->Request->URL, "/ping")) {
            auto res = ev->Get()->Request->CreateResponseOK("");
            ctx.Send(ev->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(res));
            return;
        }

        THttpRequestContext context(Config, ServiceAccountCredentialsProvider);
        context.Request = ev->Get()->Request;
        context.Sender = ev->Sender;
        context.ParseHeaders(context.Request->Headers);

        char address[INET6_ADDRSTRLEN];
        if (inet_ntop(AF_INET6, &(context.Request->Address), address, INET6_ADDRSTRLEN) != nullptr) {
            context.SourceAddress = address;
        } else {
            context.SourceAddress = "unknown";
        }

        context.Driver = Driver.Get();
        context.DatabaseName = context.Request->URL;

        if (context.DatabaseName == "/") {
           context.DatabaseName = "";
        }

        LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY, " incoming request from [" << context.SourceAddress << "] request [" << context.MethodName << "] url [" << context.Request->URL
                                                   << "] database [" << context.DatabaseName << "] requestId: " << context.RequestId);

        if (!NJson::ReadJsonTree(context.Request->Body, &context.RequestBody)) {
            context.SendBadRequest(NYdb::EStatus::BAD_REQUEST, "Couldn't parse json from http request body", ctx);
            return;
        }
        THolder<NKikimr::NSQS::TAwsRequestSignV4> signature;
        if (context.IamToken.empty()) {
            try {
                TString fullRequest = TString(context.Request->Method) + " "
                                             + context.Request->URL + " " + context.Request->Protocol
                                             + "/" + context.Request->Version + "\r\n"
                                             + context.Request->Headers
                                             + context.Request->Content;
                signature = MakeHolder<NKikimr::NSQS::TAwsRequestSignV4>(fullRequest);

            } catch(NKikimr::NSQS::TSQSException& e) {
                context.SendBadRequest(NYdb::EStatus::BAD_REQUEST, TStringBuilder() << "Malformed signature: " << e.what(), ctx);
                return;
            }
        }

        Processors->Execute(context.MethodName, std::move(context), std::move(signature), ctx);
    }


    NActors::IActor* CreateHttpProxy(const THttpProxyConfig& config) {
        return new THttpProxyActor(config);
    }

}
