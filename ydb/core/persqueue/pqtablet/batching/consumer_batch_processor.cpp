#include "consumer_batch_processor.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NBatching {

TConsumerBatchProcessor::TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , User(std::move(user))
    , LogPrefix(TStringBuilder() << "ConsumerBatchProcessor " << TabletId << " [" << User << "]: ")
{
    BatchCutters.emplace(NKikimrClient::KAFKA_BATCH, MakeHolder<TKafkaBatchCutter>());
}

const TString& TConsumerBatchProcessor::GetLogPrefix() const {
    return LogPrefix;
}

void TConsumerBatchProcessor::Bootstrap(const NActors::TActorContext&) {
    Become(&TThis::StateWork);
}

void TConsumerBatchProcessor::Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& context = ev->Get()->Context;

    auto* event = context.Event.Get();
    AFL_ENSURE(event)("description", "Unexpected empty event in TConsumerBatchProcessor");
    AFL_ENSURE(event->Type() == TEvPQ::EvProxyResponse)
        ("description", "Unexpected event type in TConsumerBatchProcessor")
        ("eventType", event->Type());

    auto* nativeEvent = static_cast<TEvPQ::TEvProxyResponse*>(event);
    AFL_ENSURE(nativeEvent->Response->HasPartitionResponse())
        ("description", "Unexpected TEvProxyResponse without PartitionResponse in TConsumerBatchProcessor");
    AFL_ENSURE(nativeEvent->Response->GetPartitionResponse().HasCmdReadResult())
        ("description", "Unexpected TEvProxyResponse without CmdReadResult in TConsumerBatchProcessor");

    auto* readResult = nativeEvent->Response->MutablePartitionResponse()->MutableCmdReadResult();
    auto* results = readResult->MutableResult();

    TVector<TReadResult> originalResults;
    originalResults.reserve(results->size());
    for (const auto& result : *results) {
        originalResults.push_back(result);
    }
    results->Clear();

    ui32 resultsCount = 0;
    auto addResult = [&](TReadResult& result) {
        if (result.GetOffset() < context.Offset) {
            return false;
        }
        if (context.LastOffset != 0 && result.GetOffset() >= context.LastOffset) {
            return false;
        }
        if (resultsCount >= context.Count) {
            return true;
        }

        resultsCount += result.GetMessageCount();
        readResult->AddResult()->Swap(&result);
        return resultsCount >= context.Count;
    };

    for (auto& originalResult : originalResults) {
        const auto messageFormat = static_cast<NKikimrClient::EMessageFormat>(originalResult.GetMessageFormat());
        auto it = BatchCutters.find(messageFormat);
        if (it == BatchCutters.end()) {
            if (addResult(originalResult)) {
                break;
            }
            continue;
        }

        auto cutResults = it->second->Cut(originalResult, context.Offset);
        for (auto& cutResult : cutResults) {
            if (addResult(cutResult)) {
                break;
            }
        }
        if (resultsCount >= context.Count) {
            break;
        }
    }

    ctx.Send(context.ResponseActor, new TEvProcessBatchResult(std::move(context)));
}

void TConsumerBatchProcessor::Handle(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext&) {
    PassAway();
}

STFUNC(TConsumerBatchProcessor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvProcessBatch, Handle);
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
