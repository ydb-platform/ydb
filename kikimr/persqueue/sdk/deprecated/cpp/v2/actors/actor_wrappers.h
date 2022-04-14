#pragma once
#include "actor_interface.h"
#include "responses.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iproducer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iconsumer.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iprocessor.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>

#include <util/datetime/base.h>

namespace NPersQueue {

template <ui32 TObjectIsDeadEventId, ui32 TCreateResponseEventId, ui32 TCommitResponseEventId>
class TProducerActorWrapper: public TActorHolder {
public:
    using TObjectIsDeadEvent = TPQLibResponseEvent<TError, TObjectIsDeadEventId>;
    using TCreateResponseEvent = TPQLibResponseEvent<TProducerCreateResponse, TCreateResponseEventId>;
    using TCommitResponseEvent = TPQLibResponseEvent<TProducerCommitResponse, TCommitResponseEventId>;

public:
    TProducerActorWrapper(NActors::TActorSystem* actorSystem, const NActors::TActorId& parentActorID, THolder<IProducer> producer)
        : TActorHolder(actorSystem, parentActorID)
        , Producer(std::move(producer))
    {
    }

    void Start(TInstant deadline, ui64 requestId = 0, ui64 isDeadRequestId = 0) noexcept {
        Subscribe<TCreateResponseEvent>(requestId, Producer->Start(deadline));
        Subscribe<TObjectIsDeadEvent>(isDeadRequestId, Producer->IsDead());
    }

    void Start(TDuration timeout, ui64 requestId = 0, ui64 isDeadRequestId = 0) noexcept {
        Start(TInstant::Now() + timeout, requestId, isDeadRequestId);
    }

    void Write(TProducerSeqNo seqNo, TData data, ui64 requestId = 0) noexcept {
        Subscribe<TCommitResponseEvent>(requestId, Producer->Write(seqNo, std::move(data)));
    }

    void Write(TData data, ui64 requestId = 0) noexcept {
        Subscribe<TCommitResponseEvent>(requestId, Producer->Write(std::move(data)));
    }

private:
    THolder<IProducer> Producer;
};

template <ui32 TObjectIsDeadEventId, ui32 TCreateResponseEventId, ui32 TGetMessageEventId>
class TConsumerActorWrapper: public TActorHolder {
public:
    using TObjectIsDeadEvent = TPQLibResponseEvent<TError, TObjectIsDeadEventId>;
    using TCreateResponseEvent = TPQLibResponseEvent<TConsumerCreateResponse, TCreateResponseEventId>;
    using TGetMessageEvent = TPQLibResponseEvent<TConsumerMessage, TGetMessageEventId>;

public:
    TConsumerActorWrapper(NActors::TActorSystem* actorSystem, const NActors::TActorId& parentActorID, THolder<IConsumer> consumer)
        : TActorHolder(actorSystem, parentActorID)
        , Consumer(std::move(consumer))
    {
    }

    void Start(TInstant deadline, ui64 requestId = 0, ui64 isDeadRequestId = 0) noexcept {
        Subscribe<TCreateResponseEvent>(requestId, Consumer->Start(deadline));
        Subscribe<TObjectIsDeadEvent>(isDeadRequestId, Consumer->IsDead());
    }

    void Start(TDuration timeout, ui64 requestId = 0, ui64 isDeadRequestId = 0) noexcept {
        Start(TInstant::Now() + timeout, requestId, isDeadRequestId);
    }

    void GetNextMessage(ui64 requestId = 0) noexcept {
        Subscribe<TGetMessageEvent>(requestId, Consumer->GetNextMessage());
    }

    void Commit(const TVector<ui64>& cookies) noexcept {
        Consumer->Commit(cookies); // no future in PQLib API
    }

    void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept {
        Consumer->RequestPartitionStatus(topic, partition, generation);  // no future in PQLib API
    }

private:
    THolder<IConsumer> Consumer;
};

template <ui32 TOriginDataEventId>
class TProcessorActorWrapper: public TActorHolder {
public:
    using TOriginDataEvent = TPQLibResponseEvent<TOriginData, TOriginDataEventId>;

public:
    TProcessorActorWrapper(NActors::TActorSystem* actorSystem, const NActors::TActorId& parentActorID, THolder<IProcessor> processor)
        : TActorHolder(actorSystem, parentActorID)
        , Processor(std::move(processor))
    {
    }

    void GetNextData(ui64 requestId = 0) noexcept {
        Subscribe<TOriginDataEvent>(requestId, Processor->GetNextData());
    }

private:
    THolder<IProcessor> Processor;
};

} // namespace NPersQueue
