#include "dq_compute_actor.h"

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/system/hostname.h>

namespace NYql {
namespace NDq {

void FillTaskRunnerStats(ui64 taskId, ui32 stageId, const TTaskRunnerStatsBase& taskStats,
    NDqProto::TDqTaskStats* protoTask, bool withProfileStats, const THashMap<ui64, ui64>& ingressBytesMap)
{
    protoTask->SetTaskId(taskId);
    protoTask->SetStageId(stageId);
    protoTask->SetCpuTimeUs(taskStats.ComputeCpuTime.MicroSeconds() + taskStats.BuildCpuTime.MicroSeconds());
    protoTask->SetFinishTimeMs(taskStats.FinishTs.MilliSeconds());
    protoTask->SetStartTimeMs(taskStats.StartTs.MilliSeconds());
    // Cerr << (TStringBuilder() << "FillTaskRunnerStats: " << taskStats.ComputeCpuTime << ", " << taskStats.BuildCpuTime << Endl);

    if (Y_UNLIKELY(withProfileStats)) {
        if (NActors::TlsActivationContext && NActors::TlsActivationContext->ActorSystem()) {
            protoTask->SetNodeId(NActors::TlsActivationContext->ActorSystem()->NodeId);
        }
        protoTask->SetHostName(HostName());
        protoTask->SetComputeCpuTimeUs(taskStats.ComputeCpuTime.MicroSeconds());
        protoTask->SetBuildCpuTimeUs(taskStats.BuildCpuTime.MicroSeconds());
        protoTask->SetWaitTimeUs(taskStats.WaitTime.MicroSeconds());
        protoTask->SetWaitOutputTimeUs(taskStats.WaitOutputTime.MicroSeconds());

        // All run statuses metrics
        protoTask->SetPendingInputTimeUs(taskStats.RunStatusTimeMetrics[ERunStatus::PendingInput].MicroSeconds());
        protoTask->SetPendingOutputTimeUs(taskStats.RunStatusTimeMetrics[ERunStatus::PendingOutput].MicroSeconds());
        protoTask->SetFinishTimeUs(taskStats.RunStatusTimeMetrics[ERunStatus::Finished].MicroSeconds());
        static_assert(TRunStatusTimeMetrics::StatusesCount == 3); // Add all statuses here

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

    ui64 firstRowTs = std::numeric_limits<ui64>::max();

    for (auto& [channelId, inputChannelStats] : taskStats.InputChannels) {
        if (!inputChannelStats) {
            continue;
        }

        if (inputChannelStats->FirstRowTs) {
            firstRowTs = std::min(firstRowTs, inputChannelStats->FirstRowTs.MilliSeconds());
        }

        protoTask->SetInputRows(protoTask->GetInputRows() + inputChannelStats->RowsOut); // Yes, OutputRows of input channel, i.e. rows that were visible by the task runner
        protoTask->SetInputBytes(protoTask->GetInputBytes() + inputChannelStats->Bytes);  // should be bytes out...

        if (Y_UNLIKELY(withProfileStats)) {
            auto protoChannel = protoTask->AddInputChannels();
            protoChannel->SetChannelId(channelId);
            protoChannel->SetChunks(inputChannelStats->Chunks);
            protoChannel->SetBytes(inputChannelStats->Bytes);
            protoChannel->SetRowsIn(inputChannelStats->RowsIn);
            protoChannel->SetRowsOut(inputChannelStats->RowsOut);
            protoChannel->SetMaxMemoryUsage(inputChannelStats->MaxMemoryUsage);
            protoChannel->SetDeserializationTimeUs(inputChannelStats->DeserializationTime.MicroSeconds());
        }
    }

    for (auto& [inputIndex, sourceStats] : taskStats.Sources) {
        if (!sourceStats) {
            continue;
        }

        if (sourceStats->FirstRowTs) {
            firstRowTs = std::min(firstRowTs, sourceStats->FirstRowTs.MilliSeconds());
        }

        protoTask->SetInputRows(protoTask->GetInputRows() + sourceStats->RowsOut); // the same comment here ... ^^^
        protoTask->SetInputBytes(protoTask->GetInputBytes() + sourceStats->Bytes);

        if (Y_UNLIKELY(withProfileStats)) {
            auto* protoSource = protoTask->AddSources();
            protoSource->SetInputIndex(inputIndex);
            protoSource->SetChunks(sourceStats->Chunks);
            protoSource->SetBytes(sourceStats->Bytes);
            protoSource->SetRowsIn(sourceStats->RowsIn);
            protoSource->SetRowsOut(sourceStats->RowsOut);
            protoSource->SetIngressBytes(ingressBytesMap.Value(inputIndex, 0));

            protoSource->SetMaxMemoryUsage(sourceStats->MaxMemoryUsage);
        }
    }

    for (auto& [channelId, outputChannelStats] : taskStats.OutputChannels) {
        if (!outputChannelStats) {
            continue;
        }

        if (outputChannelStats->FirstRowIn) {
            firstRowTs = std::min(firstRowTs, outputChannelStats->FirstRowIn.MilliSeconds());
        }

        protoTask->SetOutputRows(protoTask->GetOutputRows() + outputChannelStats->RowsIn);
        protoTask->SetOutputBytes(protoTask->GetOutputBytes() + outputChannelStats->Bytes);

        if (Y_UNLIKELY(withProfileStats)) {
            auto* protoChannel = protoTask->AddOutputChannels();
            protoChannel->SetChannelId(channelId);

            protoChannel->SetChunks(outputChannelStats->Chunks);
            protoChannel->SetBytes(outputChannelStats->Bytes);
            protoChannel->SetRowsIn(outputChannelStats->RowsIn);
            protoChannel->SetRowsOut(outputChannelStats->RowsOut);

            protoChannel->SetMaxMemoryUsage(outputChannelStats->MaxMemoryUsage);
            protoChannel->SetMaxRowsInMemory(outputChannelStats->MaxRowsInMemory);
            protoChannel->SetSerializationTimeUs(outputChannelStats->SerializationTime.MicroSeconds());

            protoChannel->SetSpilledBytes(outputChannelStats->SpilledBytes);
            protoChannel->SetSpilledRows(outputChannelStats->SpilledRows);
            protoChannel->SetSpilledBlobs(outputChannelStats->SpilledBlobs);
        }
    }

    protoTask->SetFirstRowTimeMs(firstRowTs);
}

} // namespace NDq
} // namespace NYql
