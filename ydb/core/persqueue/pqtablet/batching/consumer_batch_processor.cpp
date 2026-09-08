#include "consumer_batch_processor.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <exception>
#include <utility>

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

    TVector<TReadResult> CutOrKeepOriginal(
        const IBatchCutter& cutter,
        const TBatchCutterData& data,
        ui64 readStartOffset,
        const TString& logPrefix,
        const TString& user,
        ui32 partitionId)
    {
        try {
            return cutter.Cut(data, readStartOffset);
        } catch (const std::exception& e) {
            YDB_LOG_ERROR_COMP(PERSQUEUE, "Failed to cut kafka batch, keeping original result",
                {"logPrefix", logPrefix},
                {"user", user},
                {"partitionId", partitionId},
                {"offset", data.ReadResult.GetOffset()},
                {"error", TString(e.what())});
            return {data.ReadResult};
        }
    }

    THashMap<TString, ui64> GetKeysOrEmpty(
        const IBatchCutter& cutter,
        const TBatchCutterData& data,
        ui64 readStartOffset,
        const TString& logPrefix,
        ui32 partitionId)
    {
        try {
            return cutter.GetKeys(data, readStartOffset);
        } catch (const std::exception& e) {
            YDB_LOG_ERROR_COMP(PERSQUEUE, "Failed to get keys from kafka batch",
                {"logPrefix", logPrefix},
                {"partitionId", partitionId},
                {"offset", data.ReadResult.GetOffset()},
                {"error", TString(e.what())});
            return {};
        }
    }
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

    TVector<TReadResult> expanded;
    expanded.reserve(originalResults.size());

    try {
        ui32 resultsCount = 0;
        auto addResult = [&](TReadResult& result) {
            if (result.GetOffset() < context.Offset) {
                return false;
            }
            if (context.LastOffset != 0 && result.GetOffset() >= context.LastOffset) {
                return false;
            }
            if (resultsCount >= context.Count && context.Count > 0) {
                return true;
            }

            resultsCount += result.GetLogicalMessageCount();
            expanded.emplace_back();
            expanded.back().Swap(&result);
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

        results->Clear();
        for (auto& result : expanded) {
            readResult->AddResult()->Swap(&result);
        }
    } catch (const std::exception& e) {
        YDB_LOG_ERROR("Failed to process read batch, returning original results",
            {"logPrefix", GetLogPrefix()},
            {"user", User},
            {"partitionId", context.PartitionId},
            {"error", TString(e.what())});
    }

    ctx.Send(context.ResponseActor, new TEvProcessBatchResult(std::move(context)));
}

void TConsumerBatchProcessor::Handle(TEvProcessBatchKeys::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& context = ev->Get()->Context;
    CurrentCPUUsagePartitionId = context.PartitionId;
    HasCurrentCPUUsagePartitionId = true;

    THashMap<ui64, TString> offsetToKey;

    try {
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
    } catch (const std::exception& e) {
        YDB_LOG_ERROR("Failed to process batch keys, returning collected keys",
            {"logPrefix", GetLogPrefix()},
            {"partitionId", context.PartitionId},
            {"error", TString(e.what())});
    }

    ctx.Send(context.ResponseActor, new TEvProcessBatchKeysResult(std::move(offsetToKey)));
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
            HFunc(TEvProcessBatchKeys, Handle);
            HFunc(NActors::TEvents::TEvWakeup, Handle);
            HFunc(NActors::TEvents::TEvPoisonPill, Handle);
        default:
            YDB_LOG_WARN("Unexpected event in TConsumerBatchProcessor",
                {"logPrefix", GetLogPrefix()},
                {"user", User},
                {"eventType", ev->GetTypeRewrite()});
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
