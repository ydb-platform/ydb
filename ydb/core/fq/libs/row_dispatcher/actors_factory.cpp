#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>

namespace NFq::NRowDispatcher {


struct TActorFactory : public IActorFactory {
    TActorFactory() {}

    NActors::TActorId RegisterTopicSession(
        NActors::TActorSystem* actorSystem,
        const TString& topicPath,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters) const override {

        auto actorPtr = NFq::NewTopicSession(
            topicPath,
            config,
            rowDispatcherActorId,
            partitionId,
            std::move(driver),
            credentialsProviderFactory,
            counters
        );
        return actorSystem->Register(actorPtr.release());
    }
};

IActorFactory::TPtr CreateActorFactory() {
    return MakeIntrusive<TActorFactory>();
}

}
