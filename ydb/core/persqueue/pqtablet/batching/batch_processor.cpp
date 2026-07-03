#include "batch_processor.h"

#include "consumer_batch_processor.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NBatching {

TBatchProcessor::TBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
{
}

void TBatchProcessor::Bootstrap(const NActors::TActorContext&) {
    Become(&TThis::StateWork);
}

NActors::TActorId TBatchProcessor::GetOrCreateConsumerProcessor(const TString& user) {
    auto [it, inserted] = ConsumerProcessors.emplace(user, NActors::TActorId{});
    if (inserted) {
        it->second = Register(
            CreateConsumerBatchProcessor(TabletId, TabletActorId, user),
            TMailboxType::HTSwap,
            AppData()->BatchPoolId
            );
    }
    return it->second;
}

void TBatchProcessor::Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx) {
    const auto actorId = GetOrCreateConsumerProcessor(ev->Get()->Context.User);
    ctx.Send(actorId, new TEvProcessBatch(std::move(ev->Get()->Context)));
}

void TBatchProcessor::HandleConsumerRemoved(TEvPQ::TEvConsumerRemoved::TPtr& ev, const NActors::TActorContext&) {
    auto it = ConsumerProcessors.find(ev->Get()->Consumer);
    if (it != ConsumerProcessors.end()) {
        Send(it->second, new NActors::TEvents::TEvPoisonPill());
        ConsumerProcessors.erase(it);
    }
}

void TBatchProcessor::Handle(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext&) {
    for (const auto& [_, actorId] : ConsumerProcessors) {
        Send(actorId, new NActors::TEvents::TEvPoisonPill());
    }
    PassAway();
}

STFUNC(TBatchProcessor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvProcessBatch, Handle);
        HFunc(TEvPQ::TEvConsumerRemoved, HandleConsumerRemoved);
        HFunc(NActors::TEvents::TEvPoisonPill, Handle);
    default:
        YDB_LOG_WARN("Unexpected event",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"inTBatchProcessor", ev->GetTypeRewrite()});
        break;
    }
}

NActors::IActor* CreateBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId) {
    return new TBatchProcessor(tabletId, tabletActorId);
}

} // namespace NKikimr::NPQ::NBatching
