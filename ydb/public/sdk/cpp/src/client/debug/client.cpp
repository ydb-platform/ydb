#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/debug/client.h>

#include <ydb/public/api/grpc/ydb_debug_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_debug_v1.pb.h>
#include <ydb/public/api/protos/ydb_debug.pb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

namespace NYdb::inline Dev::NDebug {

using namespace Ydb;

using namespace NThreading;

class TDebugClient::TImpl: public TClientImplCommon<TDebugClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    template<typename TRequest, typename TResponse, typename TResult, typename TSettings>
    auto Ping(const TSettings& settings, auto serviceMethod) {
        auto pingPromise = NewPromise<TResult>();
        auto responseCb = [pingPromise] (TResponse*, TPlainStatus status) mutable {
            TResult val(TStatus(std::move(status)));
            pingPromise.SetValue(std::move(val));
        };

        Connections_->Run<Debug::V1::DebugService, TRequest, TResponse>(
            TRequest(),
            responseCb,
            serviceMethod,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return pingPromise;
    }

    auto PingPlainGrpc(const TPlainGrpcPingSettings& settings) {
        auto pingPromise = NewPromise<TPlainGrpcPingResult>();
        auto responseCb = [pingPromise] (Debug::PlainGrpcResponse* msg, TPlainStatus status) mutable {
            TPlainGrpcPingResult val(TStatus(std::move(status)));
            val.CallBackTs = msg->GetCallBackTs();
            pingPromise.SetValue(std::move(val));
        };

        Debug::PlainGrpcRequest request;

        Connections_->Run<Debug::V1::DebugService, Debug::PlainGrpcRequest, Debug::PlainGrpcResponse>(
            std::move(request),
            responseCb,
            &Debug::V1::DebugService::Stub::AsyncPingPlainGrpc,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return pingPromise;
    }

    auto PingActorChain(const TActorChainPingSettings& settings) {
        auto pingPromise = NewPromise<TActorChainPingResult>();
        auto responseCb = [pingPromise] (Debug::ActorChainResponse*, TPlainStatus status) mutable {
            TActorChainPingResult val(TStatus(std::move(status)));
            pingPromise.SetValue(std::move(val));
        };

        Debug::ActorChainRequest request;
        request.set_chainlength(settings.ChainLength_);
        request.set_workusec(settings.WorkUsec_);
        request.set_notailchain(settings.NoTailChain_);

        Connections_->Run<Debug::V1::DebugService, Debug::ActorChainRequest, Debug::ActorChainResponse>(
            std::move(request),
            responseCb,
            &Debug::V1::DebugService::Stub::AsyncPingActorChain,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return pingPromise;
    }

    ~TImpl() = default;
};

TDebugClient::TDebugClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncPlainGrpcPingResult TDebugClient::PingPlainGrpc(const TPlainGrpcPingSettings& settings) {
    return Impl_->PingPlainGrpc(settings);
}

TAsyncGrpcProxyPingResult TDebugClient::PingGrpcProxy(const TGrpcProxyPingSettings& settings) {
    return Impl_->Ping<Debug::GrpcProxyRequest, Debug::GrpcProxyResponse, TGrpcProxyPingResult>(
        settings, &Debug::V1::DebugService::Stub::AsyncPingGrpcProxy);
}

TAsyncKqpProxyPingResult TDebugClient::PingKqpProxy(const TKqpProxyPingSettings& settings) {
    return Impl_->Ping<Debug::KqpProxyRequest, Debug::KqpProxyResponse, TKqpProxyPingResult>(
        settings, &Debug::V1::DebugService::Stub::AsyncPingKqpProxy);
}

TAsyncSchemeCachePingResult TDebugClient::PingSchemeCache(const TSchemeCachePingSettings& settings) {
    return Impl_->Ping<Debug::SchemeCacheRequest, Debug::SchemeCacheResponse, TSchemeCachePingResult>(
        settings, &Debug::V1::DebugService::Stub::AsyncPingSchemeCache);
}

TAsyncTxProxyPingResult TDebugClient::PingTxProxy(const TTxProxyPingSettings& settings) {
    return Impl_->Ping<Debug::TxProxyRequest, Debug::TxProxyResponse, TTxProxyPingResult>(
        settings, &Debug::V1::DebugService::Stub::AsyncPingTxProxy);
}

TAsyncActorChainPingResult TDebugClient::PingActorChain(const TActorChainPingSettings& settings) {
    return Impl_->PingActorChain(settings);
}

} // namespace NYdb::NDebug
