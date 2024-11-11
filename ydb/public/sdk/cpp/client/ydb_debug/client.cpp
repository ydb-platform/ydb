#include "client.h"

#include <ydb/public/api/grpc/ydb_debug_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_debug_v1.pb.h>
#include <ydb/public/api/protos/ydb_debug.pb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::NDebug {

using namespace NThreading;

class TDebugClient::TImpl: public TClientImplCommon<TDebugClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncPlainGrpcPingResult PlainGrpcPing(const TPlainGrpcPingSettings& settings) {
        auto pingPromise = NewPromise<TPlainGrpcPingResult>();
        auto responseCb = [pingPromise] (Ydb::Debug::PlainGrpcResponse*, TPlainStatus status) mutable {
            TPlainGrpcPingResult val(TStatus(std::move(status)));
            pingPromise.SetValue(std::move(val));
        };

        Connections_->Run<Ydb::Debug::V1::DebugService, Ydb::Debug::PlainGrpcRequest, Ydb::Debug::PlainGrpcResponse>(
            Ydb::Debug::PlainGrpcRequest(),
            responseCb,
            &Ydb::Debug::V1::DebugService::Stub::AsyncPingPlainGrpc,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return pingPromise;
    }

    TAsyncGrpcProxyPingResult GrpcProxyPing(const TGrpcProxyPingSettings& settings) {
        auto pingPromise = NewPromise<TGrpcProxyPingResult>();
        auto responseCb = [pingPromise] (Ydb::Debug::GrpcProxyResponse*, TPlainStatus status) mutable {
            TGrpcProxyPingResult val(TStatus(std::move(status)));
            pingPromise.SetValue(std::move(val));
        };

        Connections_->Run<Ydb::Debug::V1::DebugService, Ydb::Debug::GrpcProxyRequest, Ydb::Debug::GrpcProxyResponse>(
            Ydb::Debug::GrpcProxyRequest(),
            responseCb,
            &Ydb::Debug::V1::DebugService::Stub::AsyncPingGrpcProxy,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return pingPromise;
    }

    TAsyncKqpProxyPingResult KqpProxyPing(const TKqpProxyPingSettings& settings) {
        auto pingPromise = NewPromise<TKqpProxyPingResult>();
        auto responseCb = [pingPromise] (Ydb::Debug::KqpProxyResponse*, TPlainStatus status) mutable {
            TKqpProxyPingResult val(TStatus(std::move(status)));
            pingPromise.SetValue(std::move(val));
        };

        Connections_->Run<Ydb::Debug::V1::DebugService, Ydb::Debug::KqpProxyRequest, Ydb::Debug::KqpProxyResponse>(
            Ydb::Debug::KqpProxyRequest(),
            responseCb,
            &Ydb::Debug::V1::DebugService::Stub::AsyncPingKqpProxy,
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

TAsyncPlainGrpcPingResult TDebugClient::PlainGrpcPing(const TPlainGrpcPingSettings& settings) {
    return Impl_->PlainGrpcPing(settings);
}

TAsyncGrpcProxyPingResult TDebugClient::GrpcProxyPing(const TGrpcProxyPingSettings& settings) {
    return Impl_->GrpcProxyPing(settings);
}

TAsyncKqpProxyPingResult TDebugClient::KqpProxyPing(const TKqpProxyPingSettings& settings) {
    return Impl_->KqpProxyPing(settings);
}

} // namespace NYdb::NDebug