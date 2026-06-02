#include "consumer_batch_processor.h"

namespace NKikimr::NPQ::NBatching {

TConsumerBatchProcessor::TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , User(std::move(user))
    , LogPrefix(TStringBuilder() << "ConsumerBatchProcessor " << TabletId << " [" << User << "]: ")
{
}

const TString& TConsumerBatchProcessor::GetLogPrefix() const {
    return LogPrefix;
}

void TConsumerBatchProcessor::Bootstrap(const NActors::TActorContext&) {
    Become(&TThis::StateWork);
}

void TConsumerBatchProcessor::Handle(TEvProcessRead::TPtr& ev, const NActors::TActorContext& ctx) {
    // TODO: split batched read results into separate messages for consumers that do not support batches.
    ctx.Send(ev->Get()->Context.ResponseActor, new TEvProcessReadResult(std::move(ev->Get()->Context)));
}

void TConsumerBatchProcessor::Handle(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext&) {
    PassAway();
}

STFUNC(TConsumerBatchProcessor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvProcessRead, Handle);
        HFunc(NActors::TEvents::TEvPoisonPill, Handle);
    default:
        LOG_W("Unexpected event in TConsumerBatchProcessor for user " << User << ": " << ev->GetTypeRewrite());
        break;
    }
}

NActors::IActor* CreateConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user) {
    return new TConsumerBatchProcessor(tabletId, tabletActorId, std::move(user));
}

} // namespace NKikimr::NPQ::NBatching
