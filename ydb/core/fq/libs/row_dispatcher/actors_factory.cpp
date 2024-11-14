#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>

namespace NFq::NRowDispatcher {


struct TActorFactory : public IActorFactory {
    TActorFactory() {}

    NActors::TActorId RegisterTopicSession(
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway) const override {

        auto actorPtr = NFq::NewTopicSession(
            topicPath,
            endpoint,
            database,
            config,
            rowDispatcherActorId,
            partitionId,
            std::move(driver),
            credentialsProviderFactory,
            pureCalcProgramFactory,
            counters,
            pqGateway
        );
        return NActors::TlsActivationContext->ExecutorThread.RegisterActor(actorPtr.release(), NActors::TMailboxType::HTSwap, Max<ui32>());
    }
};

IActorFactory::TPtr CreateActorFactory() {
    return MakeIntrusive<TActorFactory>();
}

}
