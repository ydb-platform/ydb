#include "consumer_batch_processor.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <utility>

#include <util/generic/strbuf.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NBatching {

namespace {
    constexpr TDuration CPUUsageFlushInterval = TDuration::Seconds(1);

    TString GetCompactionKey(const NKikimrPQClient::TDataChunk& dataChunk) {
        TString key;
        for (const auto& metadata : dataChunk.GetMessageMeta()) {
            if (metadata.key() == MESSAGE_ATTRIBUTE_KEY) {
                key = metadata.value();
                break;
            }
        }
        return key;
    }

    void LogKafkaBatchUserError(
        TStringBuf message,
        const TLogPrefix& logPrefix,
        ui32 partition,
        ui64 offset,
        const TString& error,
        TStringBuf user = {})
    {
        YDB_LOG_ERROR_COMP(NKikimrServices::PERSQUEUE, message,
            logPrefix,
            {"errorType", "user"},
            {"user", user},
            {"partition", partition},
            {"offset", offset},
            {"error", error});
    }

    TVector<TReadResult> CutOrKeepOriginal(
        const IBatchCutter& cutter,
        const TBatchCutterData& data,
        ui64 readStartOffset,
        const TLogPrefix& logPrefix,
        const TString& user,
        ui32 partition)
    {
        auto outcome = cutter.Cut(data, readStartOffset);
        if (!outcome) {
            LogKafkaBatchUserError(
                "Failed to cut kafka batch, keeping original result",
                logPrefix,
                partition,
                data.ReadResult.GetOffset(),
                outcome.error(),
                user);
            return {data.ReadResult};
        }
        return *std::move(outcome);
    }

    THashMap<TString, ui64> GetKeysOrEmpty(
        const IBatchCutter& cutter,
        const TBatchCutterData& data,
        ui64 readStartOffset,
        const TLogPrefix& logPrefix,
        ui32 partition)
    {
        auto outcome = cutter.GetKeys(data, readStartOffset);
        if (!outcome) {
            LogKafkaBatchUserError(
                "Failed to get keys from kafka batch",
                logPrefix,
                partition,
                data.ReadResult.GetOffset(),
                outcome.error());
            return {};
        }
        return *std::move(outcome);
    }
}

TConsumerBatchProcessor::TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user)
    : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , User(std::move(user))
    , LogPrefix(YDB_LOG_CREATE_MESSAGE(
        {"actorClassName", "ConsumerBatchProcessor"},
        {"consumer", User}))
{
    BatchCutters.emplace(static_cast<int>(Ydb::Topic::CODEC_KAFKA_BATCH) - 1, MakeHolder<TKafkaBatchCutter>());
}

const TLogPrefix& TConsumerBatchProcessor::GetLogPrefix() const {
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
    for (int i = 0; i < results->size(); ++i) {
        originalResults.emplace_back();
        originalResults.back().Swap(results->Mutable(i));
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

        resultsCount += result.GetLogicalMessageCount();
        readResult->AddResult()->Swap(&result);
        return resultsCount >= context.Count && context.Count > 0;
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
        auto cutResults = CutOrKeepOriginal(
            *it->second,
            data,
            context.Offset,
            GetLogPrefix(),
            User,
            context.PartitionId);
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

void TConsumerBatchProcessor::Handle(TEvProcessBatchKeys::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& context = ev->Get()->Context;
    CurrentCPUUsagePartitionId = context.PartitionId;
    HasCurrentCPUUsagePartitionId = true;

    THashMap<ui64, TString> offsetToKey;

    for (const auto& result : context.Results) {
        if (result.GetData().empty()) {
            continue;
        }

        auto dataChunk = NKikimr::GetDeserializedData(result.GetData());
        if (dataChunk.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue;
        }

        if (!result.GetIsBatch()) {
            auto key = GetCompactionKey(dataChunk);
            offsetToKey[result.GetOffset()] = std::move(key);
            continue;
        }

        auto it = BatchCutters.find(dataChunk.GetCodec());
        if (it != BatchCutters.end()) {
            TBatchCutterData data(result, std::move(dataChunk));
            auto batchKeys = GetKeysOrEmpty(
                *it->second,
                data,
                result.GetOffset(),
                GetLogPrefix(),
                context.PartitionId);
            for (auto& [key, offset] : batchKeys) {
                offsetToKey[offset] = std::move(key);
            }
        }
    }

    ctx.Send(context.ResponseActor, new TEvProcessBatchKeysResult(std::move(offsetToKey)));
}

void TConsumerBatchProcessor::FlushCPUUsageMetrics(const NActors::TActorContext& ctx, bool scheduleNext) {
    for (auto& [partitionId, cpuUsage] : CPUUsageMetricByPartition) {
        ctx.Send(TabletActorId, new TEvPQ::TEvConsumerBatchProcessorMetrics(partitionId, User, cpuUsage));
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
            HFunc(TEvProcessBatchKeys, Handle);
            HFunc(NActors::TEvents::TEvWakeup, Handle);
            HFunc(NActors::TEvents::TEvPoisonPill, Handle);
        default:
            LOG_W(
                "Unexpected event in TConsumerBatchProcessor",
                {"user", User},
                {"eventType", ev->GetTypeRewrite()}
            );
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
