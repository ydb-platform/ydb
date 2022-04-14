#pragma once
#include "actor_wrappers.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NActors {
class TActorSystem;
} // namespace NActors

namespace NPersQueue {

class TPQLibActorsWrapper : public TNonCopyable {
public:
    explicit TPQLibActorsWrapper(NActors::TActorSystem* actorSystem, const TPQLibSettings& settings = TPQLibSettings());
    TPQLibActorsWrapper(NActors::TActorSystem* actorSystem, std::shared_ptr<TPQLib> pqLib);
    ~TPQLibActorsWrapper();

    // Producers creation
    template <ui32 TObjectIsDeadEventId, ui32 TCreateResponseEventId, ui32 TCommitResponseEventId>
    THolder<TProducerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TCommitResponseEventId>> CreateProducer(
        const NActors::TActorId& parentActorID,
        const TProducerSettings& settings,
        TIntrusivePtr<ILogger> logger = nullptr,
        bool deprecated = false
    ) {
        return new TProducerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TCommitResponseEventId>(
            ActorSystem,
            parentActorID,
            PQLib->CreateProducer(settings, std::move(logger), deprecated)
        );
    }

    template <ui32 TObjectIsDeadEventId, ui32 TCreateResponseEventId, ui32 TCommitResponseEventId>
    THolder<TProducerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TCommitResponseEventId>> CreateMultiClusterProducer(
        const NActors::TActorId& parentActorID,
        const TMultiClusterProducerSettings& settings,
        TIntrusivePtr<ILogger> logger = nullptr,
        bool deprecated = false
    ) {
        return new TProducerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TCommitResponseEventId>(
            ActorSystem,
            parentActorID,
            PQLib->CreateMultiClusterProducer(settings, std::move(logger), deprecated)
        );
    }

    // Consumers creation
    template <ui32 TObjectIsDeadEventId, ui32 TCreateResponseEventId, ui32 TGetMessageEventId>
    THolder<TConsumerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TGetMessageEventId>> CreateConsumer(
        const NActors::TActorId& parentActorID,
        const TConsumerSettings& settings,
        TIntrusivePtr<ILogger> logger = nullptr,
        bool deprecated = false
    ) {
        return MakeHolder<TConsumerActorWrapper<TObjectIsDeadEventId, TCreateResponseEventId, TGetMessageEventId>>(
            ActorSystem,
            parentActorID,
            PQLib->CreateConsumer(settings, std::move(logger),deprecated)
        );
    }

    // Processors creation
    template <ui32 TOriginDataEventId>
    THolder<TProcessorActorWrapper<TOriginDataEventId>> CreateProcessor(
        const NActors::TActorId& parentActorID,
        const TProcessorSettings& settings,
        TIntrusivePtr<ILogger> logger = nullptr,
        bool deprecated = false
    ) {
        return new TProcessorActorWrapper<TOriginDataEventId>(
            ActorSystem,
            parentActorID,
            PQLib->CreateProcessor(settings, std::move(logger), deprecated)
        );
    }

private:
    std::shared_ptr<TPQLib> PQLib;
    NActors::TActorSystem* ActorSystem;
};

} // namespace NPersQueue
