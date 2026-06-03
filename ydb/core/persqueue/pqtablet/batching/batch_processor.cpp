#include "batch_processor.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NBatching {

TBatchProcessor::TBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , User(std::move(user))
    , LogPrefix(TStringBuilder() << "BatchProcessor " << TabletId << " [" << User << "]: ")
{
    BatchCutters.emplace(NKikimrClient::KAFKA_BATCH, MakeHolder<TKafkaBatchCutter>());
}

const TString& TBatchProcessor::GetLogPrefix() const {
    return LogPrefix;
}

void TBatchProcessor::Bootstrap(const NActors::TActorContext&) {
    Become(&TThis::StateWork);
}

void TBatchProcessor::Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& context = ev->Get()->Context;

    auto* event = context.Event.Get();
    AFL_ENSURE(event)("description", "Unexpected empty event in TBatchProcessor");
    AFL_ENSURE(event->Type() == TEvPQ::EvProxyResponse)
        ("description", "Unexpected event type in TBatchProcessor")
        ("eventType", event->Type());

    auto* nativeEvent = static_cast<TEvPQ::TEvProxyResponse*>(event);
    AFL_ENSURE(nativeEvent->Response->HasPartitionResponse())
        ("description", "Unexpected TEvProxyResponse without PartitionResponse in TBatchProcessor");
    AFL_ENSURE(nativeEvent->Response->GetPartitionResponse().HasCmdReadResult())
        ("description", "Unexpected TEvProxyResponse without CmdReadResult in TBatchProcessor");

    auto* readResult = nativeEvent->Response->MutablePartitionResponse()->MutableCmdReadResult();
    auto* results = readResult->MutableResult();

    TVector<TReadResult> originalResults;
    originalResults.reserve(results->size());
    for (const auto& result : *results) {
        originalResults.push_back(result);
    }
    results->Clear();

    for (auto& originalResult : originalResults) {
        const auto messageFormat = static_cast<NKikimrClient::EMessageFormat>(originalResult.GetMessageFormat());
        auto it = BatchCutters.find(messageFormat);
        if (it == BatchCutters.end()) {
            readResult->AddResult()->Swap(&originalResult);
            continue;
        }

        auto cutResults = it->second->Cut(originalResult, context.Offset);
        for (auto& cutResult : cutResults) {
            readResult->AddResult()->Swap(&cutResult);
        }
    }

    ctx.Send(context.ResponseActor, new TEvProcessBatchResult(std::move(context)));
}

void TBatchProcessor::Handle(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext&) {
    PassAway();
}

STFUNC(TBatchProcessor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvProcessBatch, Handle);
        HFunc(NActors::TEvents::TEvPoisonPill, Handle);
    default:
        LOG_W("Unexpected event in TBatchProcessor for user " << User << ": " << ev->GetTypeRewrite());
        break;
    }
}

NActors::IActor* CreateBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user) {
    return new TBatchProcessor(tabletId, tabletActorId, std::move(user));
}

} // namespace NKikimr::NPQ::NBatching
