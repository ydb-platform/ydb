#pragma once

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ::NBatching {

struct TReadProcessingContext {
    TReadProcessingContext() = default;

    TReadProcessingContext(
        TString user,
        ui64 destination,
        ui64 offset,
        ui16 partNo,
        ui64 size,
        bool isInternal,
        const NActors::TActorId& replyTo,
        const NActors::TActorId& responseActor,
        THolder<NActors::IEventBase> event)
        : User(std::move(user))
        , Destination(destination)
        , Offset(offset)
        , PartNo(partNo)
        , Size(size)
        , IsInternal(isInternal)
        , ReplyTo(replyTo)
        , ResponseActor(responseActor)
        , Event(std::move(event))
    {
    }

    TString User;
    ui64 Destination = 0;
    ui64 Offset = 0;
    ui16 PartNo = 0;
    ui64 Size = 0;
    bool IsInternal = false;
    NActors::TActorId ReplyTo;
    NActors::TActorId ResponseActor;
    THolder<NActors::IEventBase> Event;
};

struct TEvProcessBatch : public NActors::TEventLocal<TEvProcessBatch, TEvPQ::EvProcessBatchRead> {
    explicit TEvProcessBatch(TReadProcessingContext&& context)
        : Context(std::move(context))
    {
    }

    TReadProcessingContext Context;
};

struct TEvProcessBatchResult : public NActors::TEventLocal<TEvProcessBatchResult, TEvPQ::EvProcessBatchReadResult> {
    explicit TEvProcessBatchResult(TReadProcessingContext&& context)
        : Context(std::move(context))
    {
    }

    TReadProcessingContext Context;
};

class TBatchProcessor : public TBaseTabletActor<TBatchProcessor> {
public:
    TBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId);

    void Bootstrap(const NActors::TActorContext& ctx);

    void Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);

    const TString& GetLogPrefix() const override;

private:
    STFUNC(StateWork);

    NActors::TActorId GetOrCreateConsumerProcessor(const TString& user);

private:
    TString LogPrefix;
    THashMap<TString, NActors::TActorId> ConsumerProcessors;
};

NActors::IActor* CreateBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId);

} // namespace NKikimr::NPQ::NBatching
