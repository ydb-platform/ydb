#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/digest/crc32c/crc32c.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/library/services/services.pb.h>
#include <util/string/ascii.h>
#include "grpc_service_settings.h"

#define BLOG_GRPC_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::GRPC_CLIENT, stream)
#define BLOG_GRPC_DC(context, stream) LOG_DEBUG_S(context, NKikimrServices::GRPC_CLIENT, stream)

inline IOutputStream& operator <<(IOutputStream& out, const NYdbGrpc::TGrpcStatus& status) {
    return out << status.GRpcStatusCode << " " << status.Msg;
}

namespace NGrpcActorClient {

template <typename TGrpcService>
class TGrpcServiceClient  {
    using TServiceConnection = NYdbGrpc::TServiceConnection<TGrpcService>;

    NYdbGrpc::TGRpcClientConfig Config;
    NYdbGrpc::TGRpcClientLow Client;
    std::unique_ptr<TServiceConnection> Connection;

    TString Prefix(const TString& requestId = {}) const {
        if (requestId) {
            return Sprintf("[%08lx]{%s} ", (ptrdiff_t)this, requestId.c_str());
        } else {
            return Sprintf("[%08lx] ", (ptrdiff_t)this);
        }
    }

    static TString Trim(const TString& line) {
        if (line.size() > 512) {
            return line.substr(0, 512) + "...(truncated)";
        }
        return line;
    }

    template <typename TProtoMessageType>
    static TString Trim(const TProtoMessageType& message) {
        TStringBuilder log;
        log << message.GetDescriptor()->name() << " { " << Trim(message.ShortDebugString()) << " }";
        return log;
    }

public:
    static TString MaskToken(const TString& token) {
        TStringBuilder mask;
        if (token.size() >= 16) {
            mask << token.substr(0, 4);
            mask << "****";
            mask << token.substr(token.size() - 4, 4);
        } else {
            mask << "****";
        }
        mask << " (";
        mask << Sprintf("%08X", Crc32c(token.data(), token.size()));
        mask << ")";
        return mask;
    }

    static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(10);

    struct TGrpcRequest {
        static const google::protobuf::Message& Obfuscate(const google::protobuf::Message& p) {
            return p;
        }
    };

    template <typename TCallType>
    void MakeCall(typename TCallType::TRequestEventType::TPtr ev) {
        using TRequestType = decltype(typename TCallType::TRequestEventType().Request);
        using TResponseType = decltype(typename TCallType::TResponseEventType().Response);
        const auto& requestId = ev->Get()->RequestId;
        if (!Connection) {
            BLOG_GRPC_D(Prefix(requestId) << "Connect to "
                        << ((Config.EnableSsl || !Config.SslCredentials.pem_root_certs.empty()) ? "grpcs://" : "grpc://")
                        << Config.Locator);
            Connection = Client.CreateGRpcServiceConnection<TGrpcService>(Config);
        }

        const TRequestType& request = ev->Get()->Request;
        NYdbGrpc::TCallMeta meta;
        meta.Timeout = Config.Timeout;
        if (auto token = ev->Get()->Token) {
            if (!AsciiHasPrefixIgnoreCase(token, "Bearer "sv)) {
                token = "Bearer " + token;
            }
            meta.Aux.push_back({"authorization", token});
        }
        if (requestId) {
            meta.Aux.push_back({"x-request-id", requestId});
        }
        for (const auto& [k, v] : ev->Get()->Headers) {
            meta.Aux.push_back({k, v});
        }

        NYdbGrpc::TResponseCallback<TResponseType> callback =
            [actorSystem = NActors::TActivationContext::ActorSystem(), prefix = Prefix(requestId), request = ev](NYdbGrpc::TGrpcStatus&& status, TResponseType&& response) -> void {
                if (status.Ok()) {
                    BLOG_GRPC_DC(*actorSystem, prefix << "Response " << Trim(TCallType::Obfuscate(response)));
                } else {
                    BLOG_GRPC_DC(*actorSystem, prefix << "Status " << status);
                }
                auto respEv = MakeHolder<typename TCallType::TResponseEventType>();
                const auto sender = request->Sender;
                const auto cookie = request->Cookie;
                respEv->Request = request;
                respEv->Status = status;
                respEv->Response = response;
                actorSystem->Send(sender, respEv.Release(), 0, cookie);
            };

        BLOG_GRPC_D(Prefix(requestId) << "Request " << Trim(TCallType::Obfuscate(request)));
        Connection->DoRequest(request, std::move(callback), TCallType::Request, meta);
    }

    static NYdbGrpc::TGRpcClientConfig InitGrpcConfig(const NGrpcActorClient::TGrpcClientSettings& settings) {
        NYdbGrpc::TGRpcClientConfig config(settings.Endpoint, DEFAULT_TIMEOUT, NYdbGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT, 0, settings.CertificateRootCA);
        config.EnableSsl = settings.EnableSsl;
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIME_MS] = settings.GrpcKeepAliveTimeMs;
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIMEOUT_MS] = settings.GrpcKeepAliveTimeoutMs;
        config.IntChannelParams[GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS] = 1;
        config.IntChannelParams[GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA] = 0;
        config.IntChannelParams[GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS] = settings.GrpcKeepAlivePingInterval;
        config.IntChannelParams[GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS] = settings.GrpcKeepAlivePingInterval;
        return config;
    }

    TGrpcServiceClient(const NGrpcActorClient::TGrpcClientSettings& settings)
        : Config(InitGrpcConfig(settings))
    {}
};

} // namespace NGrpcActorClient
