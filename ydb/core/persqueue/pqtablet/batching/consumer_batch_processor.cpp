#include "consumer_batch_processor.h"

#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <utility>

namespace NKikimr::NPQ::NBatching {

namespace {
    constexpr TDuration CPUUsageFlushInterval = TDuration::Seconds(1);
}

TConsumerBatchProcessor::TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , User(std::move(user))
    , LogPrefix(TStringBuilder() << "ConsumerBatchProcessor " << TabletId << " [" << User << "]: ")
{
    BatchCutters.emplace(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1, MakeHolder<TKafkaBatchCutter>());
}

const TString& TConsumerBatchProcessor::GetLogPrefix() const {
    return LogPrefix;
}

void TConsumerBatchProcessor::Bootstrap(const NActors::TActorContext& ctx) {
    Become(&TThis::StateWork);
    ctx.Schedule(CPUUsageFlushInterval, new NActors::TEvents::TEvWakeup);
}

void TConsumerBatchProcessor::Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& context = ev->Get()->Context;
    CurrentCPUUsagePartitionId = context.PartitionId;
    HasCurrentCPUUsagePartitionId = true;

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
        auto dataChunk = NKikimr::GetDeserializedData(originalResult.GetData());

        if (!originalResult.GetIsBatch()) {
            if (addResult(originalResult)) {
                break;
            }
            continue;
        }

        auto it = BatchCutters.find(dataChunk.GetCodec());
        if (it == BatchCutters.end()) {
            if (addResult(originalResult)) {
                break;
            }
            continue;
        }

        TBatchCutterData data(originalResult, std::move(dataChunk));

        auto cutResults = it->second->Cut(data, context.Offset);
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

void TConsumerBatchProcessor::FlushCPUUsageMetrics(const NActors::TActorContext& ctx, bool scheduleNext) {
    for (auto& [partitionId, cpuUsage] : CPUUsageMetricByPartition) {
        if (cpuUsage) {
            ctx.Send(TabletActorId, new TEvPQ::TEvConsumerBatchProcessorMetrics(partitionId, User, cpuUsage));
        }
    }
    CPUUsageMetricByPartition.clear();

    if (scheduleNext) {
        ctx.Schedule(CPUUsageFlushInterval, new NActors::TEvents::TEvWakeup);
    }
}

void TConsumerBatchProcessor::Handle(NActors::TEvents::TEvWakeup::TPtr&, const NActors::TActorContext& ctx) {
    FlushCPUUsageMetrics(ctx, true);
}

void TConsumerBatchProcessor::Handle(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext& ctx) {
    FlushCPUUsageMetrics(ctx, false);
    PassAway();
}

STFUNC(TConsumerBatchProcessor::StateWork) {
    CurrentCPUUsageMetric = 0;
    HasCurrentCPUUsagePartitionId = false;

    {
        NPersQueue::TCounterTimeKeeper<ui64> keeper(CurrentCPUUsageMetric);

        switch (ev->GetTypeRewrite()) {
            HFunc(TEvProcessBatch, Handle);
            HFunc(NActors::TEvents::TEvWakeup, Handle);
            HFunc(NActors::TEvents::TEvPoisonPill, Handle);
        default:
            LOG_W("Unexpected event in TConsumerBatchProcessor for user " << User << ": " << ev->GetTypeRewrite());
            break;
        }
    }

    if (HasCurrentCPUUsagePartitionId && CurrentCPUUsageMetric) {
        CPUUsageMetricByPartition[CurrentCPUUsagePartitionId] += CurrentCPUUsageMetric;
    }
}

NActors::IActor* CreateConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user) {
    return new TConsumerBatchProcessor(tabletId, tabletActorId, std::move(user));
}

} // namespace NKikimr::NPQ::NBatching
