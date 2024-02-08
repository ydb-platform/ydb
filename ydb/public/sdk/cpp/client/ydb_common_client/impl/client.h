#pragma once

#include "iface.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/ssl_credentials.h>

#include <memory>

namespace NYdb {

template<typename T>
class TClientImplCommon
    : public IClientImplCommon
    , public std::enable_shared_from_this<T> {
public:
    TClientImplCommon(
        std::shared_ptr<TGRpcConnectionsImpl>&& connections,
        const TMaybe<TString>& database,
        const TMaybe<TString>& discoveryEndpoint,
        const TMaybe<EDiscoveryMode>& discoveryMode,
        const TMaybe<TSslCredentials>& sslCredentials,
        const TMaybe<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory)
        : Connections_(std::move(connections))
        , DbDriverState_(Connections_->GetDriverState(
            database, discoveryEndpoint, discoveryMode, sslCredentials, credentialsProviderFactory))
    {
        Y_ABORT_UNLESS(DbDriverState_);
    }

    TClientImplCommon(
        std::shared_ptr<TGRpcConnectionsImpl>&& connections,
        const TCommonClientSettings& settings)
        : Connections_(std::move(connections))
        , DbDriverState_(
            Connections_->GetDriverState(
                settings.Database_,
                settings.DiscoveryEndpoint_,
                settings.DiscoveryMode_,
                settings.SslCredentials_,
                settings.CredentialsProviderFactory_
            )
        )
    {
        Y_ABORT_UNLESS(DbDriverState_);
    }

    NThreading::TFuture<void> DiscoveryCompleted() const {
        return DbDriverState_->DiscoveryCompleted();
    }

    void ScheduleTask(const std::function<void()>& fn, TDuration timeout) override {
        std::weak_ptr<IClientImplCommon> weak = this->shared_from_this();
        auto cbGuard = [weak, fn]() {
            auto strongClient = weak.lock();
            if (strongClient) {
                fn();
            }
        };
        Connections_->ScheduleOneTimeTask(std::move(cbGuard), timeout);
    }

protected:
    template<typename TService, typename TRequest, typename TResponse>
    using TAsyncRequest = typename NYdbGrpc::TSimpleRequestProcessor<
        typename TService::Stub,
        TRequest,
        TResponse>::TAsyncRequest;

    template<typename TService, typename TRequest, typename TResponse>
    NThreading::TFuture<TStatus> RunSimple(
        TRequest&& request,
        TAsyncRequest<TService, TRequest, TResponse> rpc,
        const TRpcRequestSettings& requestSettings = {})
    {
        auto promise = NThreading::NewPromise<TStatus>();

        auto extractor = [promise]
            (google::protobuf::Any*, TPlainStatus status) mutable {
                TStatus st(std::move(status));
                promise.SetValue(std::move(st));
            };

        Connections_->RunDeferred<TService, TRequest, TResponse>(
            std::move(request),
            extractor,
            rpc,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            requestSettings);

        return promise.GetFuture();
    }

    template<typename TService, typename TRequest, typename TResponse, typename TOp>
    NThreading::TFuture<TOp> RunOperation(
        TRequest&& request,
        TAsyncRequest<TService, TRequest, TResponse> rpc,
        const TRpcRequestSettings& requestSettings = {})
    {
        auto promise = NThreading::NewPromise<TOp>();

        auto extractor = [promise]
            (Ydb::Operations::Operation* operation, TPlainStatus status) mutable {
                TStatus st(std::move(status));
                if (!operation) {
                    promise.SetValue(TOp(std::move(st)));
                } else {
                    promise.SetValue(TOp(std::move(st), std::move(*operation)));
                }
            };

        Connections_->RunDeferred<TService, TRequest, TResponse>(
            std::move(request),
            extractor,
            rpc,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            requestSettings);

        return promise.GetFuture();
    }

protected:
    std::shared_ptr<TGRpcConnectionsImpl> Connections_;
    TDbDriverStatePtr DbDriverState_;
};

} // namespace NYdb
