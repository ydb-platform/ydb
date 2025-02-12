#include "client.h"

#include <ydb/public/api/grpc/ydb_debug_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_debug_v1.pb.h>
#include <ydb/public/api/protos/ydb_debug.pb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::inline V2::NDebug {

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

    ~TImpl() = default;
};

TDebugClient::TDebugClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncPlainGrpcPingResult TDebugClient::PingPlainGrpc(const TPlainGrpcPingSettings& settings) {
    return Impl_->Ping<Debug::PlainGrpcRequest, Debug::PlainGrpcResponse, TPlainGrpcPingResult>(
        settings, &Debug::V1::DebugService::Stub::AsyncPingPlainGrpc);
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

} // namespace NYdb::NDebug