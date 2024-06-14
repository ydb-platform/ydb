#pragma once

#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>


namespace NYql::NDq {

using TChannelLogFunc = std::function<void(ui64 channel, ui64 from, ui64 to, TStringBuf type, bool enableSpilling)>;

template <class TGraphMeta, class TStageInfoMeta, class TTaskMeta, class TInputMeta, class TOutputMeta>
void CommonBuildTasks(double aggrTasksRatio, ui32 maxHashShuffleTasks, TDqTasksGraph<TGraphMeta, TStageInfoMeta, TTaskMeta, TInputMeta, TOutputMeta>& graph, const NNodes::TDqPhyStage& stage) {
    ui32 partitionsCount = 1;

    const auto partitionMode = TDqStageSettings::Parse(stage).PartitionMode;

    auto& stageInfo = graph.GetStageInfo(stage);
    for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
        const auto& input = stage.Inputs().Item(inputIndex);
        YQL_ENSURE(input.Maybe<NNodes::TCallable>());

        // Current assumptions:
        // 1. `Broadcast` can not be the 1st stage input unless it's a single input
        // 2. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll` !!! or `HashShuffle` !!!
        if (inputIndex == 0) {
            YQL_ENSURE(stage.Inputs().Size() == 1 || !input.Maybe<NNodes::TDqCnBroadcast>());
        } else {
            YQL_ENSURE(
                    input.Maybe<NNodes::TDqCnBroadcast>() ||
                    input.Maybe<NNodes::TDqCnUnionAll>() ||
                    input.Maybe<NNodes::TDqCnHashShuffle>(), "" << input.Ref().Content());
        }

        if (input.Maybe<NNodes::TDqCnUnionAll>() || input.Maybe<NNodes::TDqCnMerge>()) {
            // Prevent UnionAll after Map or Shuffle
            YQL_ENSURE(partitionsCount == 1);
        } else if (auto maybeCnShuffle = input.Maybe<NNodes::TDqCnHashShuffle>()) {
            auto shuffle = maybeCnShuffle.Cast();
            const auto& originStageInfo = graph.GetStageInfo(shuffle.Output().Stage());
            ui32 srcTasks = originStageInfo.Tasks.size();
            if (TDqStageSettings::EPartitionMode::Aggregate == partitionMode) {
                srcTasks = ui32(srcTasks * aggrTasksRatio);
            } else {
                YQL_ENSURE(TDqStageSettings::EPartitionMode::Default == partitionMode);
            }
            partitionsCount = std::max(partitionsCount, srcTasks);
            partitionsCount = std::min(partitionsCount, maxHashShuffleTasks);
        } else if (auto maybeCnMap = input.Maybe<NNodes::TDqCnMap>()) {
            auto cnMap = maybeCnMap.Cast();
            const auto& originStageInfo = graph.GetStageInfo(cnMap.Output().Stage());
            maxHashShuffleTasks = partitionsCount = originStageInfo.Tasks.size();
        }
    }

    for (ui32 i = 0; i < partitionsCount; ++i) {
        graph.AddTask(stageInfo);
    }
}

template <typename TGraph>
void BuildUnionAllChannels(TGraph& graph, const typename TGraph::TStageInfoType& stageInfo, ui32 inputIndex,
    const typename TGraph::TStageInfoType& inputStageInfo, ui32 outputIndex, bool enableSpilling,
    const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == 1, "Multiple tasks on union all input. StageId: " << stageInfo.Id << " " << stageInfo.Tasks.size());
    auto& targetTask = graph.GetTask(stageInfo.Tasks[0]);

    for (auto& originTaskId : inputStageInfo.Tasks) {
        auto& originTask = graph.GetTask(originTaskId);

        auto& channel = graph.AddChannel();
        channel.SrcStageId = inputStageInfo.Id;
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIndex;
        channel.DstStageId = stageInfo.Id;
        channel.DstTask = targetTask.Id;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTaskId, targetTask.Id, "UnionAll/Map", !channel.InMemory);
    }
}

template <typename TGraph>
void BuildUnionAllChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);

    auto dqUnion = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnUnionAll>();
    auto& inputStageInfo = graph.GetStageInfo(dqUnion.Output().Stage());
    auto outputIndex = FromString<ui32>(dqUnion.Output().Index().Value());

    BuildUnionAllChannels(graph, stageInfo, inputIndex, inputStageInfo, outputIndex, false, logFunc);
}

template <typename TGraph, typename TKeyColumns>
void BuildHashShuffleChannels(TGraph& graph, const typename TGraph::TStageInfoType& stageInfo, ui32 inputIndex,
    const typename TGraph::TStageInfoType& inputStageInfo, ui32 outputIndex, const TKeyColumns& keyColumns,
    bool enableSpilling, const TChannelLogFunc& logFunc)
{
    for (auto& originTaskId : inputStageInfo.Tasks) {
        auto& originTask = graph.GetTask(originTaskId);
        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::HashPartition;
        for (const auto& keyColumn : keyColumns) {
            taskOutput.KeyColumns.push_back(keyColumn);
        }

        taskOutput.PartitionsCount = stageInfo.Tasks.size();

        for (auto& targetTaskId : stageInfo.Tasks) {
            auto& targetTask = graph.GetTask(targetTaskId);

            auto& channel = graph.AddChannel();
            channel.SrcStageId = inputStageInfo.Id;
            channel.SrcTask = originTask.Id;
            channel.SrcOutputIndex = outputIndex;
            channel.DstStageId = stageInfo.Id;
            channel.DstTask = targetTask.Id;
            channel.DstInputIndex = inputIndex;
            channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;
            taskOutput.Channels.push_back(channel.Id);

            auto& taskInput = targetTask.Inputs[inputIndex];
            taskInput.Channels.push_back(channel.Id);

            logFunc(channel.Id, originTask.Id, targetTask.Id, "Shuffle/HashPartition", !channel.InMemory);
        }
    }
}

template <typename TGraph>
void BuildHashShuffleChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);
    auto shuffle = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnHashShuffle>();
    auto& originStageInfo = graph.GetStageInfo(shuffle.Output().Stage());
    auto outputIndex = FromString<ui32>(shuffle.Output().Index().Value());

    TVector<TString> keyColumns;
    for (const auto& keyColumn : shuffle.KeyColumns()) {
        keyColumns.push_back(TString(keyColumn));
    }

    BuildHashShuffleChannels(graph, stageInfo, inputIndex, originStageInfo, outputIndex, keyColumns, false, logFunc);
}

template <typename TGraph>
void BuildMapChannels(TGraph& graph, const typename TGraph::TStageInfoType& stageInfo, ui32 inputIndex,
    const typename TGraph::TStageInfoType& inputStageInfo, ui32 outputIndex, bool enableSpilling,
    const TChannelLogFunc& logFunc)
{
    auto& originTasks = inputStageInfo.Tasks;
    auto& targetTasks = stageInfo.Tasks;

    YQL_ENSURE(originTasks.size() == targetTasks.size(),
        "" << originTasks.size() << " != " << targetTasks.size());

    for (size_t i = 0; i < originTasks.size(); ++i) {
        auto originTaskId = originTasks[i];
        auto targetTaskId = targetTasks[i];

        auto& channel = graph.AddChannel();
        channel.SrcStageId = inputStageInfo.Id;
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIndex;
        channel.DstStageId = stageInfo.Id;
        channel.DstTask = targetTaskId;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& originTask = graph.GetTask(originTaskId);
        auto& targetTask = graph.GetTask(targetTaskId);

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTaskId, targetTaskId, "Map/Map", !channel.InMemory);
    }
}

template <typename TGraph>
void BuildMapChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);
    auto cnMap = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnMap>();

    auto& originStageInfo = graph.GetStageInfo(cnMap.Output().Stage());
    auto outputIndex = FromString<ui32>(cnMap.Output().Index().Value());

    BuildMapChannels(graph, stageInfo, inputIndex, originStageInfo, outputIndex, false /*spilling*/, logFunc);
}

template <typename TGraph>
void BuildStreamLookupChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);
    auto cnStreamLookup = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnStreamLookup>();

    auto& originStageInfo = graph.GetStageInfo(cnStreamLookup.Output().Stage());
    auto outputIndex = FromString<ui32>(cnStreamLookup.Output().Index().Value());

    BuildMapChannels(graph, stageInfo, inputIndex, originStageInfo, outputIndex, false /*spilling*/, logFunc);
}

template <typename TGraph>
void BuildBroadcastChannels(TGraph& graph, const typename TGraph::TStageInfoType& stageInfo, ui32 inputIndex,
    const typename TGraph::TStageInfoType& inputStageInfo, ui32 outputIndex, bool enableSpilling,
    const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(inputStageInfo.Tasks.size() == 1);

    auto originTaskId = inputStageInfo.Tasks[0];
    auto& targetTasks = stageInfo.Tasks;

    auto& originTask = graph.GetTask(originTaskId);
    auto& taskOutput = originTask.Outputs[outputIndex];
    taskOutput.Type = TTaskOutputType::Broadcast;

    for (size_t i = 0; i < targetTasks.size(); ++i) {
        auto targetTaskId = targetTasks[i];

        auto& channel = graph.AddChannel();
        channel.SrcStageId = inputStageInfo.Id;
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIndex;
        channel.DstStageId = stageInfo.Id;
        channel.DstTask = targetTaskId;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = !enableSpilling || inputStageInfo.OutputsCount == 1;

        auto& targetTask = graph.GetTask(targetTaskId);

        auto& taskInput = targetTask.Inputs[inputIndex];
        taskInput.Channels.push_back(channel.Id);
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTaskId, targetTaskId, "Broadcast/Broadcast", !channel.InMemory);
    }
}

template <typename TGraph>
void BuildBroadcastChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);
    auto cnBroadcast = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnBroadcast>();

    auto& originStageInfo = graph.GetStageInfo(cnBroadcast.Output().Stage());
    auto outputIndex = FromString<ui32>(cnBroadcast.Output().Index().Value());

    BuildBroadcastChannels(graph, stageInfo, inputIndex, originStageInfo, outputIndex, false, logFunc);
}

template <typename TGraph>
void BuildMergeChannels(TGraph& graph, const typename TGraph::TStageInfoType& stageInfo, ui32 inputIndex,
    const typename TGraph::TStageInfoType& inputStageInfo, ui32 outputIndex, const TVector<TSortColumn>& sortColumns,
    const TChannelLogFunc& logFunc)
{
    YQL_ENSURE(stageInfo.Tasks.size() == 1, "Multiple tasks on merge input. StageId: " << stageInfo.Id);
    auto& targetTask = graph.GetTask(stageInfo.Tasks[0]);

    for (auto& originTaskId : inputStageInfo.Tasks) {
        auto& originTask = graph.GetTask(originTaskId);

        auto& channel = graph.AddChannel();
        channel.SrcStageId = inputStageInfo.Id;
        channel.SrcTask = originTaskId;
        channel.SrcOutputIndex = outputIndex;
        channel.DstStageId = stageInfo.Id;
        channel.DstTask = targetTask.Id;
        channel.DstInputIndex = inputIndex;
        channel.InMemory = true;

        auto& taskInput = targetTask.Inputs[inputIndex];
        TMergeTaskInput mergeTaskInput(sortColumns);
        taskInput.ConnectionInfo = std::move(mergeTaskInput);
        taskInput.Channels.push_back(channel.Id);

        auto& taskOutput = originTask.Outputs[outputIndex];
        taskOutput.Type = TTaskOutputType::Map;
        taskOutput.Channels.push_back(channel.Id);

        logFunc(channel.Id, originTaskId, targetTask.Id, "Merge/Map", !channel.InMemory);
    }
}

template <typename TGraph>
void BuildMergeChannels(TGraph& graph, const NNodes::TDqPhyStage& stage, ui32 inputIndex,
    const TChannelLogFunc& logFunc)
{
    auto& stageInfo = graph.GetStageInfo(stage);
    auto cnMerge = stage.Inputs().Item(inputIndex).Cast<NNodes::TDqCnMerge>();
    auto& originStageInfo = graph.GetStageInfo(cnMerge.Output().Stage());
    auto outputIndex = FromString<ui32>(cnMerge.Output().Index().Value());

    TVector<TSortColumn> sortColumns;
    for (const auto& sortKeyColumn : cnMerge.SortColumns()) {
        auto sortDirection = sortKeyColumn.SortDirection().Value();
        YQL_ENSURE(sortDirection == NNodes::TTopSortSettings::AscendingSort
            || sortDirection == NNodes::TTopSortSettings::DescendingSort);
        sortColumns.emplace_back(
            TSortColumn(sortKeyColumn.Column().StringValue(), sortDirection == NNodes::TTopSortSettings::AscendingSort)
        );
    }

    BuildMergeChannels(graph, stageInfo, inputIndex, originStageInfo, outputIndex, sortColumns, logFunc);
}

}
