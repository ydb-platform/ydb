#include "dq_compute_actor.h"

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/system/hostname.h>

namespace NYql {
namespace NDq {

void FillAsyncStats(NDqProto::TDqAsyncBufferStats& proto, TDqAsyncStats stats) {
    if (stats.CollectBasic()) {
        proto.SetBytes(stats.Bytes);
        proto.SetDecompressedBytes(stats.DecompressedBytes);
        proto.SetRows(stats.Rows);
        proto.SetChunks(stats.Chunks);
        proto.SetSplits(stats.Splits);
        if (stats.CollectFull()) {
            proto.SetFirstMessageMs(stats.FirstMessageTs.MilliSeconds());
            proto.SetPauseMessageMs(stats.PauseMessageTs.MilliSeconds());
            proto.SetResumeMessageMs(stats.ResumeMessageTs.MilliSeconds());
            proto.SetLastMessageMs(stats.LastMessageTs.MilliSeconds());
            proto.SetWaitTimeUs(stats.WaitTime.MicroSeconds());
            proto.SetWaitPeriods(stats.WaitPeriods);
        }
    }
}

void MergeMinTs(TInstant& current, const TInstant value) {
    if (value) {
        if (!current || current > value) {
            current = value;
        }
    }
}

void MergeMaxTs(TInstant& current, const TInstant value) {
    if (current < value) {
        current = value;
    }
}

void FillTaskRunnerStats(ui64 taskId, ui32 stageId, const TTaskRunnerStatsBase& taskStats,
    NDqProto::TDqTaskStats* protoTask, TCollectStatsLevel level)
{
    if (StatsLevelCollectNone(level)) {
        return;
    }

    protoTask->SetTaskId(taskId);
    protoTask->SetStageId(stageId);
    protoTask->SetCpuTimeUs(taskStats.ComputeCpuTime.MicroSeconds() + taskStats.BuildCpuTime.MicroSeconds());

    TInstant finishTime = taskStats.FinishTs;
    TInstant startTime;

    if (NActors::TlsActivationContext && NActors::TlsActivationContext->ActorSystem()) {
        protoTask->SetNodeId(NActors::TlsActivationContext->ActorSystem()->NodeId);
    }
    protoTask->SetHostName(HostName());
    protoTask->SetComputeCpuTimeUs(taskStats.ComputeCpuTime.MicroSeconds());
    protoTask->SetBuildCpuTimeUs(taskStats.BuildCpuTime.MicroSeconds());

    protoTask->SetWaitInputTimeUs(taskStats.WaitInputTime.MicroSeconds());
    protoTask->SetWaitOutputTimeUs(taskStats.WaitOutputTime.MicroSeconds());

    if (StatsLevelCollectProfile(level)) {
        if (taskStats.ComputeCpuTimeByRun) {
            auto snapshot = taskStats.ComputeCpuTimeByRun->Snapshot();
            for (ui32 i = 0; i < snapshot->Count(); i++) {
                auto* protoBucket = protoTask->AddComputeCpuTimeByRun();
                protoBucket->SetBound(snapshot->UpperBound(i));
                protoBucket->SetValue(snapshot->Value(i));
            }
        }

        for (const auto& stat : taskStats.MkqlStats) {
            auto* s = protoTask->MutableMkqlStats()->Add();
            s->SetName(TString(stat.Key.GetName()));
            s->SetValue(stat.Value);
            s->SetDeriv(stat.Key.IsDeriv());
        }
    }

    TDqAsyncStats taskPushStats;

    for (auto& [srcStageId, inputChannels] : taskStats.InputChannels) {
        switch (level) {
            case TCollectStatsLevel::None:
                break;
            case TCollectStatsLevel::Basic:
                for (auto& [channelId, inputChannel] : inputChannels) {
                    taskPushStats.MergeData(inputChannel->GetPushStats());
                }
                break;
            case TCollectStatsLevel::Full:
                {
                    TDqInputChannelStats pushStats;
                    TDqAsyncStats popStats;
                    bool firstChannelInStage = true;
                    for (auto& [channelId, inputChannel] : inputChannels) {
                        taskPushStats.MergeData(inputChannel->GetPushStats());
                        if (firstChannelInStage) {
                            pushStats = inputChannel->GetPushStats();
                            popStats = inputChannel->GetPopStats();
                            firstChannelInStage = false;
                        } else {
                            pushStats.Merge(inputChannel->GetPushStats());
                            pushStats.DeserializationTime += inputChannel->GetPushStats().DeserializationTime;
                            pushStats.MaxMemoryUsage += inputChannel->GetPushStats().MaxMemoryUsage;
                            popStats.Merge(inputChannel->GetPopStats());
                        }
                    }
                    if (inputChannels.size() > 1) {
                        pushStats.WaitTime /= inputChannels.size();
                        popStats.WaitTime /= inputChannels.size();
                    }
                    {
                        auto& protoChannel = *protoTask->AddInputChannels();
                        protoChannel.SetChannelId(pushStats.ChannelId); // only one of ids
                        protoChannel.SetSrcStageId(srcStageId);
                        FillAsyncStats(*protoChannel.MutablePush(), pushStats);
                        FillAsyncStats(*protoChannel.MutablePop(), popStats);
                        protoChannel.SetDeserializationTimeUs(pushStats.DeserializationTime.MicroSeconds());
                        protoChannel.SetMaxMemoryUsage(pushStats.MaxMemoryUsage);
                    }
                    MergeMinTs(startTime, pushStats.FirstMessageTs);
                }
                break;
            case TCollectStatsLevel::Profile:
                for (auto& [channelId, inputChannel] : inputChannels) {
                    const auto& pushStats = inputChannel->GetPushStats();
                    taskPushStats.MergeData(pushStats);
                    auto& protoChannel = *protoTask->AddInputChannels();
                    protoChannel.SetChannelId(channelId);
                    protoChannel.SetSrcStageId(srcStageId);
                    FillAsyncStats(*protoChannel.MutablePush(), pushStats);
                    FillAsyncStats(*protoChannel.MutablePop(), inputChannel->GetPopStats());
                    protoChannel.SetDeserializationTimeUs(pushStats.DeserializationTime.MicroSeconds());
                    protoChannel.SetMaxMemoryUsage(pushStats.MaxMemoryUsage);
                    MergeMinTs(startTime, pushStats.FirstMessageTs);
                }
                break;
        }
    }

    protoTask->SetInputRows(taskPushStats.Rows);
    protoTask->SetInputBytes(taskPushStats.Bytes);

    //
    // task runner is not aware of ingress/egress stats, fill it in CA
    //
    if (StatsLevelCollectFull(level)) {
        for (auto& [inputIndex, sources] : taskStats.Sources) {
            const auto& pushStats = sources->GetPushStats();
            auto& protoSource = *protoTask->AddSources();
            protoSource.SetInputIndex(inputIndex);
            FillAsyncStats(*protoSource.MutablePush(), pushStats);
            FillAsyncStats(*protoSource.MutablePop(), sources->GetPopStats());
            protoSource.SetMaxMemoryUsage(pushStats.MaxMemoryUsage);
            MergeMinTs(startTime, pushStats.FirstMessageTs);
        }
    }

    TDqAsyncStats taskPopStats;
    TDqAsyncStats resultStats;

    for (auto& [dstStageId, outputChannels] : taskStats.OutputChannels) {
        switch (level) {
            case TCollectStatsLevel::None:
                break;
            case TCollectStatsLevel::Basic:
                for (auto& [channelId, outputChannel] : outputChannels) {
                    taskPopStats.MergeData(outputChannel->GetPopStats());
                    if (dstStageId == 0) {
                        resultStats.MergeData(outputChannel->GetPopStats());
                    }
                }
                break;
            case TCollectStatsLevel::Full:
                {
                    TDqAsyncStats pushStats;
                    TDqOutputChannelStats popStats;
                    bool firstChannelInStage = true;
                    for (auto& [channelId, outputChannel] : outputChannels) {
                        taskPopStats.MergeData(outputChannel->GetPopStats());
                        if (dstStageId == 0) {
                            resultStats.MergeData(outputChannel->GetPopStats());
                        }
                        if (firstChannelInStage) {
                            pushStats = outputChannel->GetPushStats();
                            popStats = outputChannel->GetPopStats();
                            firstChannelInStage = false;
                        } else {
                            pushStats.Merge(outputChannel->GetPushStats());
                            popStats.Merge(outputChannel->GetPopStats());
                            popStats.MaxMemoryUsage += outputChannel->GetPopStats().MaxMemoryUsage;
                            popStats.MaxRowsInMemory += outputChannel->GetPopStats().MaxRowsInMemory;
                            popStats.SerializationTime += outputChannel->GetPopStats().SerializationTime;
                            popStats.SpilledBytes += outputChannel->GetPopStats().SpilledBytes;
                            popStats.SpilledRows += outputChannel->GetPopStats().SpilledRows;
                            popStats.SpilledBlobs += outputChannel->GetPopStats().SpilledBlobs;
                        }
                    }
                    if (outputChannels.size() > 1) {
                        pushStats.WaitTime /= outputChannels.size();
                        popStats.WaitTime /= outputChannels.size();
                    }
                    {
                        auto& protoChannel = *protoTask->AddOutputChannels();
                        protoChannel.SetChannelId(popStats.ChannelId); // only one of ids
                        protoChannel.SetDstStageId(dstStageId);
                        FillAsyncStats(*protoChannel.MutablePush(), pushStats);
                        FillAsyncStats(*protoChannel.MutablePop(), popStats);
                        protoChannel.SetMaxMemoryUsage(popStats.MaxMemoryUsage);
                        protoChannel.SetMaxRowsInMemory(popStats.MaxRowsInMemory);
                        protoChannel.SetSerializationTimeUs(popStats.SerializationTime.MicroSeconds());
                        protoChannel.SetSpilledBytes(popStats.SpilledBytes);
                        protoChannel.SetSpilledRows(popStats.SpilledRows);
                        protoChannel.SetSpilledBlobs(popStats.SpilledBlobs);
                    }
                    MergeMaxTs(finishTime, popStats.LastMessageTs);
                }
                break;
            case TCollectStatsLevel::Profile:
                for (auto& [channelId, outputChannel] : outputChannels) {
                    const auto& popStats = outputChannel->GetPopStats();
                    taskPopStats.MergeData(popStats);
                    if (dstStageId == 0) {
                        resultStats.MergeData(popStats);
                    }
                    auto& protoChannel = *protoTask->AddOutputChannels();
                    protoChannel.SetChannelId(channelId);
                    protoChannel.SetDstStageId(dstStageId);
                    FillAsyncStats(*protoChannel.MutablePush(), outputChannel->GetPushStats());
                    FillAsyncStats(*protoChannel.MutablePop(), popStats);
                    protoChannel.SetMaxMemoryUsage(popStats.MaxMemoryUsage);
                    protoChannel.SetMaxRowsInMemory(popStats.MaxRowsInMemory);
                    protoChannel.SetSerializationTimeUs(popStats.SerializationTime.MicroSeconds());
                    protoChannel.SetSpilledBytes(popStats.SpilledBytes);
                    protoChannel.SetSpilledRows(popStats.SpilledRows);
                    protoChannel.SetSpilledBlobs(popStats.SpilledBlobs);
                    MergeMaxTs(finishTime, popStats.LastMessageTs);
                }
                break;
        }
    }

    protoTask->SetOutputRows(taskPopStats.Rows);
    protoTask->SetOutputBytes(taskPopStats.Bytes);
    protoTask->SetResultRows(resultStats.Rows);
    protoTask->SetResultBytes(resultStats.Bytes);

    protoTask->SetFinishTimeMs(finishTime.MilliSeconds());
    protoTask->SetStartTimeMs(startTime.MilliSeconds());
}

} // namespace NDq
} // namespace NYql
