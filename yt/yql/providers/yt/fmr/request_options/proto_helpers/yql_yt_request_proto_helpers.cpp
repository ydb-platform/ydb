#include "yql_yt_request_proto_helpers.h"
#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

namespace NYql::NFmr {

NProto::TFmrError FmrErrorToProto(const TFmrError& error) {
    NProto::TFmrError protoError;
    protoError.SetComponent(static_cast<NProto::EFmrComponent>(error.Component));
    protoError.SetReason(static_cast<NProto::EFmrReason>(error.Reason));
    protoError.SetErrorMessage(error.ErrorMessage);
    if (error.WorkerId) {
        protoError.SetWorkerId(*error.WorkerId);
    }
    if (error.TaskId) {
        protoError.SetTaskId(*error.TaskId);
    }
    if (error.OperationId) {
        protoError.SetOperationId(*error.OperationId);
    }
    protoError.SetJobId(*error.JobId);
    return protoError;
}

TFmrError FmrErrorFromProto(const NProto::TFmrError& protoError) {
    TFmrError fmrError;
    fmrError.Component = static_cast<EFmrComponent>(protoError.GetComponent());
    fmrError.Reason = static_cast<EFmrErrorReason>(protoError.GetReason());
    fmrError.ErrorMessage = protoError.GetErrorMessage();
    if (protoError.HasWorkerId()) {
        fmrError.WorkerId = protoError.GetWorkerId();
    }
    if (protoError.HasTaskId()) {
        fmrError.TaskId = protoError.GetTaskId();
    }
    if (protoError.HasOperationId()) {
        fmrError.OperationId = protoError.GetOperationId();
    }
    fmrError.JobId = protoError.GetJobId();
    return fmrError;
}

NProto::TYtTableRef YtTableRefToProto(const TYtTableRef& ytTableRef) {
    NProto::TYtTableRef protoYtTableRef;
    protoYtTableRef.SetRichPath(SerializeRichPath(ytTableRef.RichPath));
    if (ytTableRef.FilePath) {
        protoYtTableRef.SetFilePath(*ytTableRef.FilePath);
    }
    return protoYtTableRef;
}

TYtTableRef YtTableRefFromProto(const NProto::TYtTableRef protoYtTableRef) {
    TYtTableRef ytTableRef;
    ytTableRef.RichPath = DeserializeRichPath(protoYtTableRef.GetRichPath());
    if (protoYtTableRef.HasFilePath()) {
        ytTableRef.FilePath = protoYtTableRef.GetFilePath();
    }
    return ytTableRef;
}

NProto::TYtTableTaskRef YtTableTaskRefToProto(const TYtTableTaskRef& ytTableTaskRef) {
    NProto::TYtTableTaskRef protoYtTableTaskRef;
    for (auto& richPath: ytTableTaskRef.RichPaths) {
        protoYtTableTaskRef.AddRichPath(SerializeRichPath(richPath));
    }
    for (auto& filePath: ytTableTaskRef.FilePaths) {
        protoYtTableTaskRef.AddFilePath(filePath);
    }
    return protoYtTableTaskRef;
}

TYtTableTaskRef YtTableTaskRefFromProto(const NProto::TYtTableTaskRef protoYtTableTaskRef) {
    TYtTableTaskRef ytTableTaskRef;
    for (auto& serializedPath: protoYtTableTaskRef.GetRichPath()) {
        ytTableTaskRef.RichPaths.emplace_back(DeserializeRichPath(serializedPath));
    }
    for (auto& filePath: protoYtTableTaskRef.GetFilePath()) {
        ytTableTaskRef.FilePaths.emplace_back(filePath);
    }
    return ytTableTaskRef;
}

NProto::TFmrTableId FmrTableIdToProto(const TFmrTableId& fmrTableId) {
    NProto::TFmrTableId protoFmrTableId;
    protoFmrTableId.SetId(fmrTableId.Id);
    return protoFmrTableId;
}

TFmrTableId FmrTableIdFromProto(const NProto::TFmrTableId& protoFmrTableId) {
    return TFmrTableId(protoFmrTableId.GetId());
}

NProto::TFmrTableRef FmrTableRefToProto(const TFmrTableRef& fmrTableRef) {
    NProto::TFmrTableRef protoFmrTableRef;
    auto protoFmrTableId = FmrTableIdToProto(fmrTableRef.FmrTableId);
    protoFmrTableRef.MutableFmrTableId()->Swap(&protoFmrTableId);
    return protoFmrTableRef;
}

TFmrTableRef FmrTableRefFromProto(const NProto::TFmrTableRef protoFmrTableRef) {
    auto tableId = FmrTableIdFromProto(protoFmrTableRef.GetFmrTableId());
    return TFmrTableRef(tableId);
}

NProto::TTableRange TableRangeToProto(const TTableRange& tableRange) {
    NProto::TTableRange protoTableRange;
    protoTableRange.SetPartId(tableRange.PartId);
    protoTableRange.SetMinChunk(tableRange.MinChunk);
    protoTableRange.SetMaxChunk(tableRange.MaxChunk);
    return protoTableRange;
}

TTableRange TableRangeFromProto(const NProto::TTableRange& protoTableRange) {
    return TTableRange {
        .PartId = protoTableRange.GetPartId(),
        .MinChunk = protoTableRange.GetMinChunk(),
        .MaxChunk = protoTableRange.GetMaxChunk()
    };
}

NProto::TFmrTableInputRef FmrTableInputRefToProto(const TFmrTableInputRef& fmrTableInputRef) {
    NProto::TFmrTableInputRef protoFmrTableInputRef;
    protoFmrTableInputRef.SetTableId(fmrTableInputRef.TableId);
    for (auto& tableRange: fmrTableInputRef.TableRanges) {
        NProto::TTableRange protoTableRange = TableRangeToProto(tableRange);
        auto* curTableRange = protoFmrTableInputRef.AddTableRanges();
        curTableRange->Swap(&protoTableRange);
    }
    return protoFmrTableInputRef;
}

TFmrTableInputRef FmrTableInputRefFromProto(const NProto::TFmrTableInputRef& protoFmrTableInputRef) {
    TFmrTableInputRef fmrTableInputRef;
    fmrTableInputRef.TableId = protoFmrTableInputRef.GetTableId();
    std::vector<TTableRange> tableRanges;
    for (size_t i = 0; i < protoFmrTableInputRef.TableRangesSize(); ++i) {
        TTableRange tableRange = TableRangeFromProto(protoFmrTableInputRef.GetTableRanges(i));
        tableRanges.emplace_back(tableRange);
    }
    fmrTableInputRef.TableRanges = tableRanges;
    return fmrTableInputRef;
}

NProto::TFmrTableOutputRef FmrTableOutputRefToProto(const TFmrTableOutputRef& fmrTableOutputRef) {
    NProto::TFmrTableOutputRef protoFmrTableOutputRef;
    protoFmrTableOutputRef.SetTableId(fmrTableOutputRef.TableId);
    protoFmrTableOutputRef.SetPartId(fmrTableOutputRef.PartId);
    return protoFmrTableOutputRef;
}

TFmrTableOutputRef FmrTableOutputRefFromProto(const NProto::TFmrTableOutputRef& protoFmrTableOutputRef) {
    return TFmrTableOutputRef{
        .TableId = protoFmrTableOutputRef.GetTableId(),
        .PartId = protoFmrTableOutputRef.GetPartId()
    };
}

NProto::TTableStats TableStatsToProto(const TTableStats& tableStats) {
    NProto::TTableStats protoTableStats;
    protoTableStats.SetChunks(tableStats.Chunks);
    protoTableStats.SetRows(tableStats.Rows);
    protoTableStats.SetDataWeight(tableStats.DataWeight);
    return protoTableStats;
}

TTableStats TableStatsFromProto(const NProto::TTableStats& protoTableStats) {
    return TTableStats {
        .Chunks = protoTableStats.GetChunks(),
        .Rows = protoTableStats.GetRows(),
        .DataWeight = protoTableStats.GetDataWeight()
    };
}

NProto::TChunkStats ChunkStatsToProto(const TChunkStats& chunkStats) {
    NProto::TChunkStats protoChunkStats;
    protoChunkStats.SetRows(chunkStats.Rows);
    protoChunkStats.SetDataWeight(chunkStats.DataWeight);
    return protoChunkStats;
}

TChunkStats ChunkStatsFromProto(const NProto::TChunkStats& protoChunkStats) {
    return TChunkStats{.Rows = protoChunkStats.GetRows(), .DataWeight = protoChunkStats.GetDataWeight()};
}

NProto::TTableChunkStats TableChunkStatsToProto(const TTableChunkStats& tableChunkStats) {
    NProto::TTableChunkStats protoTableChunkStats;
    protoTableChunkStats.SetPartId(tableChunkStats.PartId);
    for (auto& chunkStats: tableChunkStats.PartIdChunkStats) {
        NProto::TChunkStats protoChunkStats = ChunkStatsToProto(chunkStats);
        auto* curPartIdChunkStats = protoTableChunkStats.AddPartIdChunkStats();
        curPartIdChunkStats->Swap(&protoChunkStats);
    }
    return protoTableChunkStats;
}

TTableChunkStats TableChunkStatsFromProto(const NProto::TTableChunkStats& protoTableChunkStats) {
    TTableChunkStats tableChunkStats;
    tableChunkStats.PartId = protoTableChunkStats.GetPartId();
    std::vector<TChunkStats> partIdChunkStats;
    for (auto& stat: protoTableChunkStats.GetPartIdChunkStats()) {
        partIdChunkStats.emplace_back(ChunkStatsFromProto(stat));
    }
    tableChunkStats.PartIdChunkStats = partIdChunkStats;
    return tableChunkStats;
}

NProto::TStatistics StatisticsToProto(const TStatistics& stats) {
    NProto::TStatistics protoStatistics;
    for (auto& [fmrTableOutputRef, tableChunkStats]: stats.OutputTables) {
        NProto::TFmrTableOutputRef protoFmrTableOutputRef = FmrTableOutputRefToProto(fmrTableOutputRef);
        NProto::TTableChunkStats protoTableChunkStats = TableChunkStatsToProto(tableChunkStats);
        NProto::TStatisticsObject statTableObject;
        statTableObject.MutableFmrTableOutputRef()->Swap(&protoFmrTableOutputRef);
        statTableObject.MutableTableChunkStats()->Swap(&protoTableChunkStats);
        auto* curOutputTable = protoStatistics.AddOutputTables();
        curOutputTable->Swap(&statTableObject);
    }
    return protoStatistics;
}

TStatistics StatisticsFromProto(const NProto::TStatistics& protoStats) {
    std::unordered_map<TFmrTableOutputRef, TTableChunkStats> outputTables;
    for (size_t i = 0; i < protoStats.OutputTablesSize(); ++i) {
        NProto::TStatisticsObject protoStatTableObject = protoStats.GetOutputTables(i);
        TFmrTableOutputRef fmrTableOutputRef = FmrTableOutputRefFromProto(protoStatTableObject.GetFmrTableOutputRef());
        TTableChunkStats tableChunkStats = TableChunkStatsFromProto(protoStatTableObject.GetTableChunkStats());
        outputTables[fmrTableOutputRef] = tableChunkStats;
    }
    return TStatistics{.OutputTables = outputTables};
}

NProto::TOperationTableRef OperationTableRefToProto(const TOperationTableRef& operationTableRef) {
    NProto::TOperationTableRef protoOperationTableRef;
    if (auto* ytTableRefPtr = std::get_if<TYtTableRef>(&operationTableRef)) {
        NProto::TYtTableRef protoYtTableRef = YtTableRefToProto(*ytTableRefPtr);
        protoOperationTableRef.MutableYtTableRef()->Swap(&protoYtTableRef);
    } else {
        auto* fmrTableRefPtr = std::get_if<TFmrTableRef>(&operationTableRef);
        NProto::TFmrTableRef protoFmrTableRef = FmrTableRefToProto(*fmrTableRefPtr);
        protoOperationTableRef.MutableFmrTableRef()->Swap(&protoFmrTableRef);
    }
    return protoOperationTableRef;
}

TOperationTableRef OperationTableRefFromProto(const NProto::TOperationTableRef& protoOperationTableRef) {
    std::variant<TYtTableRef, TFmrTableRef> tableRef;
    if (protoOperationTableRef.HasYtTableRef()) {
        tableRef = YtTableRefFromProto(protoOperationTableRef.GetYtTableRef());
    } else {
        tableRef = FmrTableRefFromProto(protoOperationTableRef.GetFmrTableRef());
    }
    return tableRef;
}

NProto::TTaskTableRef TaskTableRefToProto(const TTaskTableRef& taskTableRef) {
    NProto::TTaskTableRef protoTaskTableRef;
    if (auto* ytTableTaskRefPtr = std::get_if<TYtTableTaskRef>(&taskTableRef)) {
        NProto::TYtTableTaskRef protoYtTableTaskRef = YtTableTaskRefToProto(*ytTableTaskRefPtr);
        protoTaskTableRef.MutableYtTableTaskRef()->Swap(&protoYtTableTaskRef);
    } else {
        auto* fmrTableInputRefPtr = std::get_if<TFmrTableInputRef>(&taskTableRef);
        NProto::TFmrTableInputRef protoFmrTableInputRef = FmrTableInputRefToProto(*fmrTableInputRefPtr);
        protoTaskTableRef.MutableFmrTableInputRef()->Swap(&protoFmrTableInputRef);
    }
    return protoTaskTableRef;

}

TTaskTableRef TaskTableRefFromProto(const NProto::TTaskTableRef& protoTaskTableRef) {
    std::variant<TYtTableTaskRef, TFmrTableInputRef> tableRef;
    if (protoTaskTableRef.HasYtTableTaskRef()) {
        tableRef = YtTableTaskRefFromProto(protoTaskTableRef.GetYtTableTaskRef());
    } else {
        tableRef = FmrTableInputRefFromProto(protoTaskTableRef.GetFmrTableInputRef());
    }
    return tableRef;
}

NProto::TTaskTableInputRef TaskTableInputRefToProto(const TTaskTableInputRef& taskTableInputRef) {
    NProto::TTaskTableInputRef protoTaskTableInputRef;
    for (auto& taskTableRef: taskTableInputRef.Inputs) {
        auto protoTaskTableRef = TaskTableRefToProto(taskTableRef);
        auto* curInput = protoTaskTableInputRef.AddInputs();
        curInput->Swap(&protoTaskTableRef);
    }
    return protoTaskTableInputRef;
}

TTaskTableInputRef TaskTableInputRefFromProto(const NProto::TTaskTableInputRef& protoTaskTableInputRef) {
    std::vector<TTaskTableRef> inputs;
    for (auto& protoTaskTableRef: protoTaskTableInputRef.GetInputs()) {
        inputs.emplace_back(TaskTableRefFromProto(protoTaskTableRef));
    }
    return TTaskTableInputRef{.Inputs = inputs};
}

NProto::TUploadOperationParams UploadOperationParamsToProto(const TUploadOperationParams& uploadOperationParams) {
    NProto::TUploadOperationParams protoUploadOperationParams;
    auto input = FmrTableRefToProto(uploadOperationParams.Input);
    auto output = YtTableRefToProto(uploadOperationParams.Output);
    protoUploadOperationParams.MutableInput()->Swap(&input);
    protoUploadOperationParams.MutableOutput()->Swap(&output);
    return protoUploadOperationParams;
}

TUploadOperationParams UploadOperationParamsFromProto(const NProto::TUploadOperationParams& protoUploadOperationParams) {
    return TUploadOperationParams(
        FmrTableRefFromProto(protoUploadOperationParams.GetInput()),
        YtTableRefFromProto(protoUploadOperationParams.GetOutput())
    );
}

NProto::TUploadTaskParams UploadTaskParamsToProto(const TUploadTaskParams& uploadTaskParams) {
    NProto::TUploadTaskParams protoUploadTaskParams;
    auto input = FmrTableInputRefToProto(uploadTaskParams.Input);
    auto output = YtTableRefToProto(uploadTaskParams.Output);
    protoUploadTaskParams.MutableInput()->Swap(&input);
    protoUploadTaskParams.MutableOutput()->Swap(&output);
    return protoUploadTaskParams;
}

TUploadTaskParams UploadTaskParamsFromProto(const NProto::TUploadTaskParams& protoUploadTaskParams) {
    TUploadTaskParams uploadTaskParams;
    uploadTaskParams.Input = FmrTableInputRefFromProto(protoUploadTaskParams.GetInput());
    uploadTaskParams.Output = YtTableRefFromProto(protoUploadTaskParams.GetOutput());
    return uploadTaskParams;
}

NProto::TDownloadOperationParams DownloadOperationParamsToProto(const TDownloadOperationParams& downloadOperationParams) {
    NProto::TDownloadOperationParams protoDownloadOperationParams;
    auto input = YtTableRefToProto(downloadOperationParams.Input);
    auto output = FmrTableRefToProto(downloadOperationParams.Output);
    protoDownloadOperationParams.MutableInput()->Swap(&input);
    protoDownloadOperationParams.MutableOutput()->Swap(&output);
    return protoDownloadOperationParams;
}

TDownloadOperationParams DownloadOperationParamsFromProto(const NProto::TDownloadOperationParams& protoDownloadOperationParams) {
    return TDownloadOperationParams(
        YtTableRefFromProto(protoDownloadOperationParams.GetInput()),
        FmrTableRefFromProto(protoDownloadOperationParams.GetOutput())
    );
}

NProto::TDownloadTaskParams DownloadTaskParamsToProto(const TDownloadTaskParams& downloadTaskParams) {
    NProto::TDownloadTaskParams protoDownloadTaskParams;
    auto input = YtTableTaskRefToProto(downloadTaskParams.Input);
    auto output = FmrTableOutputRefToProto(downloadTaskParams.Output);
    protoDownloadTaskParams.MutableInput()->Swap(&input);
    protoDownloadTaskParams.MutableOutput()->Swap(&output);
    return protoDownloadTaskParams;
}

TDownloadTaskParams DownloadTaskParamsFromProto(const NProto::TDownloadTaskParams& protoDownloadTaskParams) {
    TDownloadTaskParams downloadTaskParams;
    downloadTaskParams.Input = YtTableTaskRefFromProto(protoDownloadTaskParams.GetInput());
    downloadTaskParams.Output = FmrTableOutputRefFromProto(protoDownloadTaskParams.GetOutput());
    return downloadTaskParams;
}

NProto::TMergeOperationParams MergeOperationParamsToProto(const TMergeOperationParams& mergeOperationParams) {
    NProto::TMergeOperationParams protoMergeOperationParams;
    for (size_t i = 0; i < mergeOperationParams.Input.size(); ++i) {
        auto inputTable = OperationTableRefToProto(mergeOperationParams.Input[i]);
        auto* curInput = protoMergeOperationParams.AddInput();
        curInput->Swap(&inputTable);
    }
    auto outputTable = FmrTableRefToProto(mergeOperationParams.Output);
    protoMergeOperationParams.MutableOutput()->Swap(&outputTable);
    return protoMergeOperationParams;
}

TMergeOperationParams MergeOperationParamsFromProto(const NProto::TMergeOperationParams& protoMergeOperationParams) {
    TMergeOperationParams mergeOperationParams(
        {},
        FmrTableRefFromProto(protoMergeOperationParams.GetOutput())
    );
    for (size_t i = 0; i < protoMergeOperationParams.InputSize(); ++i) {
        TOperationTableRef inputTable = OperationTableRefFromProto(protoMergeOperationParams.GetInput(i));
        mergeOperationParams.Input.emplace_back(inputTable);
    }
    return mergeOperationParams;
}

NProto::TMergeTaskParams MergeTaskParamsToProto(const TMergeTaskParams& mergeTaskParams) {
    NProto::TMergeTaskParams protoMergeTaskParams;
    auto inputTables = TaskTableInputRefToProto(mergeTaskParams.Input);
    protoMergeTaskParams.MutableInput()->Swap(&inputTables);
    auto outputTable = FmrTableOutputRefToProto(mergeTaskParams.Output);
    protoMergeTaskParams.MutableOutput()->Swap(&outputTable);
    return protoMergeTaskParams;
}

TMergeTaskParams MergeTaskParamsFromProto(const NProto::TMergeTaskParams& protoMergeTaskParams) {
    TMergeTaskParams mergeTaskParams;
    mergeTaskParams.Input = TaskTableInputRefFromProto(protoMergeTaskParams.GetInput());
    mergeTaskParams.Output = FmrTableOutputRefFromProto(protoMergeTaskParams.GetOutput());
    return mergeTaskParams;
}

NProto::TMapOperationParams MapOperationParamsToProto(const TMapOperationParams& mapOperationParams) {
    NProto::TMapOperationParams protoMapOperationParams;
    for (auto& operationTableRef: mapOperationParams.Input) {
        auto protoOperationTableRef = OperationTableRefToProto(operationTableRef);
        protoMapOperationParams.AddInput()->Swap(&protoOperationTableRef);
    }
    for (auto& fmrTableRef: mapOperationParams.Output) {
        auto protoFmrTableRef = FmrTableRefToProto(fmrTableRef);
        protoMapOperationParams.AddOutput()->Swap(&protoFmrTableRef);
    }
    protoMapOperationParams.SetSerializedMapJobState(mapOperationParams.SerializedMapJobState);
    return protoMapOperationParams;
}

TMapOperationParams MapOperationParamsFromProto(const NProto::TMapOperationParams& protoMapOperationParams) {
    std::vector<TOperationTableRef> inputTables;
    std::vector<TFmrTableRef> outputTables;
    for (auto& protoOperationTableRef: protoMapOperationParams.GetInput()) {
        inputTables.emplace_back(OperationTableRefFromProto(protoOperationTableRef));
    }
    for (auto& protoFmrTableRef: protoMapOperationParams.GetOutput()) {
        outputTables.emplace_back(FmrTableRefFromProto(protoFmrTableRef));
    }
    return TMapOperationParams{.Input = inputTables, .Output = outputTables, .SerializedMapJobState = protoMapOperationParams.GetSerializedMapJobState()};
}

NProto::TMapTaskParams MapTaskParamsToProto(const TMapTaskParams& mapTaskParams) {
    NProto::TMapTaskParams protoMapTaskParams;
    auto protoTaskTableInputRef = TaskTableInputRefToProto(mapTaskParams.Input);
    protoMapTaskParams.MutableInput()->Swap(&protoTaskTableInputRef);
    for (auto& fmrTableOutputRef: mapTaskParams.Output) {
        auto protoFmrTableOutputRef = FmrTableOutputRefToProto(fmrTableOutputRef);
        protoMapTaskParams.AddOutput()->Swap(&protoFmrTableOutputRef);
    }
    protoMapTaskParams.SetSerializedMapJobState(mapTaskParams.SerializedMapJobState);
    return protoMapTaskParams;
}

TMapTaskParams MapTaskParamsFromProto(const NProto::TMapTaskParams& protoMapTaskParams) {
    TMapTaskParams mapTaskParams;
    mapTaskParams.Input = TaskTableInputRefFromProto(protoMapTaskParams.GetInput());
    std::vector<TFmrTableOutputRef> outputTables;
    for (auto& protoFmrTableOutputRef: protoMapTaskParams.GetOutput()) {
        outputTables.emplace_back(FmrTableOutputRefFromProto(protoFmrTableOutputRef));
    }
    mapTaskParams.Output = outputTables;
    mapTaskParams.SerializedMapJobState = protoMapTaskParams.GetSerializedMapJobState();
    return mapTaskParams;
}

NProto::TOperationParams OperationParamsToProto(const TOperationParams& operationParams) {
    NProto::TOperationParams protoOperationParams;
    if (auto* uploadOperationParamsPtr = std::get_if<TUploadOperationParams>(&operationParams)) {
        NProto::TUploadOperationParams protoUploadOperationParams = UploadOperationParamsToProto(*uploadOperationParamsPtr);
        protoOperationParams.MutableUploadOperationParams()->Swap(&protoUploadOperationParams);
    } else if (auto* downloadOperationParamsPtr = std::get_if<TDownloadOperationParams>(&operationParams)) {
        NProto::TDownloadOperationParams protoDownloadOperationParams = DownloadOperationParamsToProto(*downloadOperationParamsPtr);
        protoOperationParams.MutableDownloadOperationParams()->Swap(&protoDownloadOperationParams);
    } else if (auto* mergeOperationParamsPtr = std::get_if<TMergeOperationParams>(&operationParams)) {
        NProto::TMergeOperationParams protoMergeOperationParams = MergeOperationParamsToProto(*mergeOperationParamsPtr);
        protoOperationParams.MutableMergeOperationParams()->Swap(&protoMergeOperationParams);
    } else {
        auto* mapOperationParamsPtr = std::get_if<TMapOperationParams>(&operationParams);
        NProto::TMapOperationParams protoMapOperationParams = MapOperationParamsToProto(*mapOperationParamsPtr);
        protoOperationParams.MutableMapOperationParams()->Swap(&protoMapOperationParams);
    }
    return protoOperationParams;
}

TOperationParams OperationParamsFromProto(const NProto::TOperationParams& protoOperationParams) {
    if (protoOperationParams.HasDownloadOperationParams()) {
        return DownloadOperationParamsFromProto(protoOperationParams.GetDownloadOperationParams());
    } else if (protoOperationParams.HasUploadOperationParams()) {
        return UploadOperationParamsFromProto(protoOperationParams.GetUploadOperationParams());
    } else if (protoOperationParams.HasMergeOperationParams()) {
        return MergeOperationParamsFromProto(protoOperationParams.GetMergeOperationParams());
    } else {
        return MapOperationParamsFromProto(protoOperationParams.GetMapOperationParams());
    }
}

NProto::TTaskParams TaskParamsToProto(const TTaskParams& taskParams) {
    NProto::TTaskParams protoTaskParams;
    if (auto* uploadTaskParamsPtr = std::get_if<TUploadTaskParams>(&taskParams)) {
        NProto::TUploadTaskParams protoUploadTaskParams = UploadTaskParamsToProto(*uploadTaskParamsPtr);
        protoTaskParams.MutableUploadTaskParams()->Swap(&protoUploadTaskParams);
    } else if (auto* downloadTaskParamsPtr = std::get_if<TDownloadTaskParams>(&taskParams)) {
        NProto::TDownloadTaskParams protoDownloadTaskParams = DownloadTaskParamsToProto(*downloadTaskParamsPtr);
        protoTaskParams.MutableDownloadTaskParams()->Swap(&protoDownloadTaskParams);
    } else if (auto* mergeTaskParamsPtr = std::get_if<TMergeTaskParams>(&taskParams)) {
        NProto::TMergeTaskParams protoMergeTaskParams = MergeTaskParamsToProto(*mergeTaskParamsPtr);
        protoTaskParams.MutableMergeTaskParams()->Swap(&protoMergeTaskParams);
    } else {
        auto* mapTaskParamsPtr = std::get_if<TMapTaskParams>(&taskParams);
        NProto::TMapTaskParams protoMapTaskParams = MapTaskParamsToProto(*mapTaskParamsPtr);
        protoTaskParams.MutableMapTaskParams()->Swap(&protoMapTaskParams);
    }
    return protoTaskParams;
}

TTaskParams TaskParamsFromProto(const NProto::TTaskParams& protoTaskParams) {
    TTaskParams taskParams;
    if (protoTaskParams.HasDownloadTaskParams()) {
        taskParams = DownloadTaskParamsFromProto(protoTaskParams.GetDownloadTaskParams());
    } else if (protoTaskParams.HasUploadTaskParams()) {
        taskParams = UploadTaskParamsFromProto(protoTaskParams.GetUploadTaskParams());
    } else if (protoTaskParams.HasMergeTaskParams()) {
        taskParams = MergeTaskParamsFromProto(protoTaskParams.GetMergeTaskParams());
    } else {
        taskParams = MapTaskParamsFromProto(protoTaskParams.GetMapTaskParams());
    }
    return taskParams;
}

NProto::TClusterConnection ClusterConnectionToProto(const TClusterConnection& clusterConnection) {
    NProto::TClusterConnection protoClusterConnection;
    protoClusterConnection.SetTransactionId(clusterConnection.TransactionId);
    protoClusterConnection.SetYtServerName(clusterConnection.YtServerName);
    if (clusterConnection.Token) {
        protoClusterConnection.SetToken(*clusterConnection.Token);
    }
    return protoClusterConnection;
}

TClusterConnection ClusterConnectionFromProto(const NProto::TClusterConnection& protoClusterConnection) {
    TClusterConnection clusterConnection{};
    clusterConnection.TransactionId = protoClusterConnection.GetTransactionId();
    clusterConnection.YtServerName = protoClusterConnection.GetYtServerName();
    if (protoClusterConnection.HasToken()) {
        clusterConnection.Token = protoClusterConnection.GetToken();
    }
    return clusterConnection;
}

NProto::TTask TaskToProto(const TTask& task) {
    NProto::TTask protoTask;
    protoTask.SetTaskType(static_cast<NProto::ETaskType>(task.TaskType));
    protoTask.SetTaskId(task.TaskId);
    auto taskParams = TaskParamsToProto(task.TaskParams);
    protoTask.MutableTaskParams()->Swap(&taskParams);
    protoTask.SetSessionId(task.SessionId);
    protoTask.SetNumRetries(task.NumRetries);
    auto& clusterConnections = *protoTask.MutableClusterConnections();
    for (auto& [tableName, conn]: task.ClusterConnections) {
        clusterConnections[tableName.Id] = ClusterConnectionToProto(conn);
    }
    if (task.JobSettings) {
        protoTask.SetJobSettings(NYT::NodeToYsonString(*task.JobSettings));
    }
    return protoTask;
}

TTask TaskFromProto(const NProto::TTask& protoTask) {
    TTask task;
    task.TaskType = static_cast<ETaskType>(protoTask.GetTaskType());
    task.TaskId = protoTask.GetTaskId();
    task.TaskParams = TaskParamsFromProto(protoTask.GetTaskParams());
    task.SessionId = protoTask.GetSessionId();
    task.NumRetries = protoTask.GetNumRetries();
    std::unordered_map<TFmrTableId, TClusterConnection> taskClusterConnections;
    for (auto& [tableName, conn]: protoTask.GetClusterConnections()) {
        taskClusterConnections[tableName] = ClusterConnectionFromProto(conn);
    }
    task.ClusterConnections = taskClusterConnections;
    if (protoTask.HasJobSettings()) {
        task.JobSettings = NYT::NodeFromYsonString(protoTask.GetJobSettings());
    }
    return task;
}

NProto::TTaskState TaskStateToProto(const TTaskState& taskState) {
    NProto::TTaskState protoTaskState;
    protoTaskState.SetTaskStatus(static_cast<NProto::ETaskStatus>(taskState.TaskStatus));
    protoTaskState.SetTaskId(taskState.TaskId);
    if (taskState.TaskErrorMessage) {
        auto fmrError = FmrErrorToProto(*taskState.TaskErrorMessage);
        protoTaskState.MutableTaskErrorMessage()->Swap(&fmrError);
    }
    auto protoStatistics = StatisticsToProto(taskState.Stats);
    protoTaskState.MutableStats()->Swap(&protoStatistics);
    return protoTaskState;
}

TTaskState TaskStateFromProto(const NProto::TTaskState& protoTaskState) {
    TTaskState taskState;
    taskState.TaskStatus = static_cast<ETaskStatus>(protoTaskState.GetTaskStatus());
    taskState.TaskId = protoTaskState.GetTaskId();
    if (protoTaskState.HasTaskErrorMessage()) {
        taskState.TaskErrorMessage = FmrErrorFromProto(protoTaskState.GetTaskErrorMessage());
    }
    taskState.Stats = StatisticsFromProto(protoTaskState.GetStats());
    return taskState;
}

} // namespace NYql::NFmr
