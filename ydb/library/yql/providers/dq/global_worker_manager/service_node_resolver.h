#pragma once

#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>

#include <util/generic/string.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/threading/future/future.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NYql {

class IServiceNodeResolver {
public:
    using TPtr = std::shared_ptr<IServiceNodeResolver>;

    struct TConnectionResult : public NCommon::TOperationResult {

        TConnectionResult() {}

        TConnectionResult(
            std::unique_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>>&& connection,
            std::shared_ptr<NYdbGrpc::IQueueClientContext>&& ctx = nullptr,
            ui32 nodeId = 0,
            const TString& location = "")
            : Connection(connection.release())
            , GRpcContext(std::move(ctx))
            , NodeId(nodeId)
            , Location(location)
        {
            if (GRpcContext) {
                SetSuccess();
            }
        }

        std::shared_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>> Connection;
        std::shared_ptr<NYdbGrpc::IQueueClientContext> GRpcContext;
        ui32 NodeId;
        TString Location;
    };

    virtual ~IServiceNodeResolver() = default;
    virtual NThreading::TFuture<TConnectionResult> GetConnection() = 0;
    virtual void InvalidateCache() = 0;
    virtual void Stop() = 0;
};

struct TDynamicResolverOptions {
    NActors::TActorId YtWrapper;
    TString Prefix;
    TDuration UpdatePeriod = TDuration::Seconds(5);
    TDuration RetryPeriod = TDuration::Seconds(10);
};

class TSingleNodeResolver: public IServiceNodeResolver {
public:
    TSingleNodeResolver();

    ~TSingleNodeResolver();

    NThreading::TFuture<IServiceNodeResolver::TConnectionResult> GetConnection() override;

    void SetLeaderHostPort(const TString& leaderHostPort);

    void InvalidateCache() override { }

    void Stop() override { }

private:
    NYdbGrpc::TGRpcClientLow ClientLow;
    NYdbGrpc::TChannelPool ChannelPool;

    TString LeaderHostPort;
};

IServiceNodeResolver::TPtr CreateStaticResolver(const TVector<TString>& hostPortPairs);
IServiceNodeResolver::TPtr CreateDynamicResolver(NActors::TActorSystem* actorSystem, const TDynamicResolverOptions& options);

} // namespace NYql
