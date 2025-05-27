#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>

namespace NFq::NRowDispatcher {


struct TActorFactory : public IActorFactory {
    TActorFactory() {}

    NActors::TActorId RegisterTopicSession(
        const TString& readGroup,
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        NActors::TActorId compileServiceActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NMonitoring::TDynamicCounterPtr& countersRoot,
        const NYql::IPqGateway::TPtr& pqGateway,
        ui64 maxBufferSize) const override {

        auto actorPtr = NFq::NewTopicSession(
            readGroup,
            topicPath,
            endpoint,
            database,
            config,
            rowDispatcherActorId,
            compileServiceActorId,
            partitionId,
            std::move(driver),
            credentialsProviderFactory,
            counters,
            countersRoot,
            pqGateway,
            maxBufferSize
        );
        return NActors::TActivationContext::Register(actorPtr.release(), {}, NActors::TMailboxType::HTSwap, Max<ui32>());
    }
};

IActorFactory::TPtr CreateActorFactory() {
    return MakeIntrusive<TActorFactory>();
}

}
