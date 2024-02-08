#pragma once

#include <ydb/library/grpc/server/grpc_server.h>

namespace NKikimr {

// Parameterize YDB binary with grpc services registry
class TGrpcServiceFactory {
private:
    using TServicePtr = TIntrusivePtr<NYdbGrpc::IGRpcService>;
    using TFactoryMethod = std::function<
            TServicePtr(
                NActors::TActorSystem*,
                TIntrusivePtr<::NMonitoring::TDynamicCounters>,
                NActors::TActorId
            )
        >;

    struct TServiceParams {
        TServiceParams(TFactoryMethod method, bool enableByDefault, std::optional<NActors::TActorId> proxyId)
            : Method(method)
            , EnableByDefault(enableByDefault)
            , GrpcRequestProxyId(proxyId)
        {}

        TFactoryMethod Method;
        bool EnableByDefault;
        std::optional<NActors::TActorId> GrpcRequestProxyId;
    };

private:
    std::unordered_map<TString, std::vector<TServiceParams>> Registry;

public:
    template <class TService>
    void Register(
        const TString& name,
        bool enableByDefault = false,
        std::optional<NActors::TActorId> grpcRequestProxyIdForService = std::nullopt
    ) {
        auto method = [](
            NActors::TActorSystem* actorSystem,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId
        ) {
            return TServicePtr(new TService(actorSystem, counters, grpcRequestProxyId));
        };
        Registry[name].emplace_back(
            method,
            enableByDefault,
            grpcRequestProxyIdForService
        );
    }

    std::vector<TServicePtr> Create(
        const std::unordered_set<TString>& enabled,
        const std::unordered_set<TString>& disabled,
        NActors::TActorSystem* actorSystem,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        NActors::TActorId grpcRequestProxyId
    ) {
        std::vector<TServicePtr> services;
        for (const auto& [name, methods] : Registry) {
            for (const auto& [method, enableByDefault, grpcRequestProxyIdForService] : methods) {
                if (!disabled.count(name) && (enabled.count(name) || enableByDefault)) {
                    services.emplace_back(method(
                        actorSystem,
                        counters,
                        grpcRequestProxyIdForService.value_or(grpcRequestProxyId)
                    ));
                }
            }
        }
        return services;
    }

    bool Has(const TString& name) const {
        return Registry.find(name) != Registry.end();
    }
};

} // NKikimr
