#pragma once

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <limits>

namespace NKikimr::NPQ::NBatching {

struct TReadProcessingContext {
    TString User;
    ui32 PartitionId = 0;
    ui64 Destination = 0;
    ui64 Offset = 0;
    ui32 Count = std::numeric_limits<ui32>::max();
    ui64 LastOffset = 0;
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

struct TBatchKeysProcessingContext {
    ui32 PartitionId = 0;
    NActors::TActorId ResponseActor;
    TVector<NKikimrClient::TCmdReadResult::TResult> Results;
};

struct TEvProcessBatchKeys : public NActors::TEventLocal<TEvProcessBatchKeys, TEvPQ::EvProcessBatchKeys> {
    explicit TEvProcessBatchKeys(TBatchKeysProcessingContext&& context)
        : Context(std::move(context))
    {
    }

    TBatchKeysProcessingContext Context;
};

struct TEvProcessBatchKeysResult : public NActors::TEventLocal<TEvProcessBatchKeysResult, TEvPQ::EvProcessBatchKeysResult> {
    explicit TEvProcessBatchKeysResult(THashMap<ui64, TString>&& offsetToKey)
        : OffsetToKey(std::move(offsetToKey))
    {
    }

    THashMap<ui64, TString> OffsetToKey;
};

class TBatchProcessor : public TBaseTabletActor<TBatchProcessor>, private TConstantLogPrefix {
public:
    TBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId);

    void Bootstrap(const NActors::TActorContext& ctx);

    void Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvProcessBatchKeys::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    NActors::TActorId GetOrCreateConsumerProcessor(const TString& user);

private:
    THashMap<TString, NActors::TActorId> ConsumerProcessors;
};

NActors::IActor* CreateBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId);

} // namespace NKikimr::NPQ::NBatching
