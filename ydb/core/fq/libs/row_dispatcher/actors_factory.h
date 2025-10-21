#pragma once

#include <util/generic/ptr.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace NKikimrConfig {
class TSharedReadingConfig;
} // namespace NKikimrConfig

namespace NFq::NRowDispatcher {

struct IActorFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IActorFactory>;

    virtual NActors::TActorId RegisterTopicSession(
        const TString& readGroup,
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NKikimrConfig::TSharedReadingConfig& config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NActors::TActorId rowDispatcherActorId,
        NActors::TActorId compileServiceActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NMonitoring::TDynamicCounterPtr& countersRoot,
        const NYql::IPqGateway::TPtr& pqGateway,
        ui64 maxBufferSize) const = 0;
};

IActorFactory::TPtr CreateActorFactory();

}
