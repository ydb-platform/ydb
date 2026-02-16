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
    if (error.JobId) {
        protoError.SetJobId(*error.JobId);
    }
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
    if (protoError.HasJobId()) {
        fmrError.JobId = protoError.GetJobId();
    }
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

TSortingColumns SortingColumnsFromProto(const NProto::TSortingColumns& protoSortingColumns) {
    TSortingColumns sortingColumns;
    for (auto& column: protoSortingColumns.GetColumns()) {
        sortingColumns.Columns.emplace_back(column);
    }
    for (auto& sortOrder: protoSortingColumns.GetSortOrder()) {
        sortingColumns.SortOrders.emplace_back(static_cast<ESortOrder>(sortOrder));
    }
    return sortingColumns;
}

NProto::TSortingColumns SortingColumnsToProto(const TSortingColumns& sortingColumns) {
    NProto::TSortingColumns protoSortingColumns;
    for (auto& column: sortingColumns.Columns) {
        protoSortingColumns.AddColumns(column);
    }
    for (auto& sortOrder: sortingColumns.SortOrders) {
        auto sortOrderProto = static_cast<NProto::ESortOrder>(sortOrder);
        protoSortingColumns.AddSortOrder(sortOrderProto);
    }
    return protoSortingColumns;
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
    for (auto& column: fmrTableRef.Columns) {
        protoFmrTableRef.AddColumns(column);
    }
    for (auto& sortColumn: fmrTableRef.SortColumns) {
        protoFmrTableRef.AddSortColumns(sortColumn);
    }
    for (auto& sortOrder: fmrTableRef.SortOrder) {
        auto sortOrderProto = static_cast<NProto::ESortOrder>(sortOrder);
        protoFmrTableRef.AddSortOrder(sortOrderProto);
    }
    protoFmrTableRef.SetColumnGroups(fmrTableRef.SerializedColumnGroups);
    return protoFmrTableRef;
}

TFmrTableRef FmrTableRefFromProto(const NProto::TFmrTableRef protoFmrTableRef) {
    TFmrTableRef fmrTableRef;
    fmrTableRef.FmrTableId = FmrTableIdFromProto(protoFmrTableRef.GetFmrTableId());
    for (auto& column: protoFmrTableRef.GetColumns()) {
        fmrTableRef.Columns.emplace_back(column);
    }
    for (auto& sortColumn: protoFmrTableRef.GetSortColumns()) {
        fmrTableRef.SortColumns.emplace_back(sortColumn);
    }
    for (auto& sortOrder: protoFmrTableRef.GetSortOrder()) {
        fmrTableRef.SortOrder.emplace_back(static_cast<ESortOrder>(sortOrder));
    }
    fmrTableRef.SerializedColumnGroups = protoFmrTableRef.GetColumnGroups();
    return fmrTableRef;
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
    for (auto& column: fmrTableInputRef.Columns) {
        protoFmrTableInputRef.AddColumns(column);
    }
    protoFmrTableInputRef.SetColumnGroups(fmrTableInputRef.SerializedColumnGroups);
    if (fmrTableInputRef.IsFirstRowInclusive) {
        protoFmrTableInputRef.SetIsFirstRowInclusive(*fmrTableInputRef.IsFirstRowInclusive);
    }
    if (fmrTableInputRef.FirstRowKeys) {
        protoFmrTableInputRef.SetFirstRowKeys(*fmrTableInputRef.FirstRowKeys);
    }
    if (fmrTableInputRef.LastRowKeys) {
        protoFmrTableInputRef.SetLastRowKeys(*fmrTableInputRef.LastRowKeys);
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
    for (auto& column: protoFmrTableInputRef.GetColumns()) {
        fmrTableInputRef.Columns.emplace_back(column);
    }
    fmrTableInputRef.SerializedColumnGroups = protoFmrTableInputRef.GetColumnGroups();
    fmrTableInputRef.FirstRowKeys = protoFmrTableInputRef.HasFirstRowKeys() ? TMaybe<TString>(protoFmrTableInputRef.GetFirstRowKeys()) : Nothing();
    fmrTableInputRef.LastRowKeys = protoFmrTableInputRef.HasLastRowKeys() ? TMaybe<TString>(protoFmrTableInputRef.GetLastRowKeys()) : Nothing();
    return fmrTableInputRef;
}

NProto::TFmrTableOutputRef FmrTableOutputRefToProto(const TFmrTableOutputRef& fmrTableOutputRef) {
    NProto::TFmrTableOutputRef protoFmrTableOutputRef;
    protoFmrTableOutputRef.SetTableId(fmrTableOutputRef.TableId);
    protoFmrTableOutputRef.SetPartId(fmrTableOutputRef.PartId);
    protoFmrTableOutputRef.SetColumnGroups(fmrTableOutputRef.SerializedColumnGroups);
    auto protoSortingColumns = SortingColumnsToProto(fmrTableOutputRef.SortingColumns);
    protoFmrTableOutputRef.MutableSortingColumns()->Swap(&protoSortingColumns);
    return protoFmrTableOutputRef;
}

TFmrTableOutputRef FmrTableOutputRefFromProto(const NProto::TFmrTableOutputRef& protoFmrTableOutputRef) {
    TFmrTableOutputRef fmrTableOutputRef;
    fmrTableOutputRef.TableId = protoFmrTableOutputRef.GetTableId();
    fmrTableOutputRef.PartId = protoFmrTableOutputRef.GetPartId();
    fmrTableOutputRef.SerializedColumnGroups = protoFmrTableOutputRef.GetColumnGroups();
    fmrTableOutputRef.SortingColumns = SortingColumnsFromProto(protoFmrTableOutputRef.GetSortingColumns());
    return fmrTableOutputRef;
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

NProto::TSortedChunkStats SortedChunkStatsToProto(const TSortedChunkStats& sortedChunkStats) {
    NProto::TSortedChunkStats protoSortedChunkStats;
    protoSortedChunkStats.SetIsSorted(sortedChunkStats.IsSorted);
    if (!sortedChunkStats.FirstRowKeys.IsUndefined()) {
        protoSortedChunkStats.SetFirstRowKeys(NYT::NodeToYsonString(sortedChunkStats.FirstRowKeys));
    }
    if (!sortedChunkStats.LastRowKeys.IsUndefined()) {
        protoSortedChunkStats.SetLastRowKeys(NYT::NodeToYsonString(sortedChunkStats.LastRowKeys));
    }
    return protoSortedChunkStats;
}

TSortedChunkStats SortedChunkStatsFromProto(const NProto::TSortedChunkStats& protoSortedChunkStats) {
    TSortedChunkStats sortedChunkStats;
    sortedChunkStats.IsSorted = protoSortedChunkStats.GetIsSorted();
    if (!protoSortedChunkStats.GetFirstRowKeys().empty()) {
        sortedChunkStats.FirstRowKeys = NYT::NodeFromYsonString(protoSortedChunkStats.GetFirstRowKeys());
    }
    if (!protoSortedChunkStats.GetLastRowKeys().empty()) {
        sortedChunkStats.LastRowKeys = NYT::NodeFromYsonString(protoSortedChunkStats.GetLastRowKeys());
    }
    return sortedChunkStats;
}

NProto::TChunkStats ChunkStatsToProto(const TChunkStats& chunkStats) {
    NProto::TChunkStats protoChunkStats;
    protoChunkStats.SetRows(chunkStats.Rows);
    protoChunkStats.SetDataWeight(chunkStats.DataWeight);
    auto protoSortedChunkStats = SortedChunkStatsToProto(chunkStats.SortedChunkStats);
    protoChunkStats.MutableSortedChunkStats()->Swap(&protoSortedChunkStats);
    return protoChunkStats;
}

TChunkStats ChunkStatsFromProto(const NProto::TChunkStats& protoChunkStats) {
    TChunkStats chunkStats;
    chunkStats.Rows = protoChunkStats.GetRows();
    chunkStats.DataWeight = protoChunkStats.GetDataWeight();
    chunkStats.SortedChunkStats = SortedChunkStatsFromProto(protoChunkStats.GetSortedChunkStats());
    return chunkStats;
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
    NProto::TTaskResult protoTaskResult = TaskResultToProto(stats.TaskResult);
    protoStatistics.MutableTaskResult()->Swap(&protoTaskResult);
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
    TStatistics stats{.OutputTables = std::move(outputTables)};
    if (protoStats.HasTaskResult()) {
        stats.TaskResult = TaskResultFromProto(protoStats.GetTaskResult());
    }
    return stats;
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
    return TUploadOperationParams{
        .Input = FmrTableRefFromProto(protoUploadOperationParams.GetInput()),
        .Output = YtTableRefFromProto(protoUploadOperationParams.GetOutput())
    };
}

NProto::TSortedUploadOperationParams SortedUploadOperationParamsToProto(const TSortedUploadOperationParams& SortedUploadOperationParams) {
    NProto::TSortedUploadOperationParams protoSortedUploadOperationParams;
    auto input = FmrTableRefToProto(SortedUploadOperationParams.Input);
    auto output = YtTableRefToProto(SortedUploadOperationParams.Output);
    protoSortedUploadOperationParams.MutableInput()->Swap(&input);
    protoSortedUploadOperationParams.MutableOutput()->Swap(&output);
    protoSortedUploadOperationParams.SetSessionId(SortedUploadOperationParams.SessionId);
    for (const auto& cookie : SortedUploadOperationParams.Cookies) {
        protoSortedUploadOperationParams.AddCookies(cookie);
    }
    protoSortedUploadOperationParams.SetIsOrdered(SortedUploadOperationParams.IsOrdered);
    protoSortedUploadOperationParams.SetPartitionId(SortedUploadOperationParams.PartitionId);
    return protoSortedUploadOperationParams;
}

TSortedUploadOperationParams SortedUploadOperationParamsFromProto(const NProto::TSortedUploadOperationParams& protoSortedUploadOperationParams) {
    std::vector<TString> cookies;
    for (const auto& cookie : protoSortedUploadOperationParams.GetCookies()) {
        cookies.push_back(cookie);
    }
    return TSortedUploadOperationParams{
        .Input = FmrTableRefFromProto(protoSortedUploadOperationParams.GetInput()),
        .Output = YtTableRefFromProto(protoSortedUploadOperationParams.GetOutput()),
        .SessionId = protoSortedUploadOperationParams.GetSessionId(),
        .Cookies = cookies,
        .PartitionId = protoSortedUploadOperationParams.GetPartitionId(),
        .IsOrdered = protoSortedUploadOperationParams.GetIsOrdered()
    };
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
    return TDownloadOperationParams{
        .Input = YtTableRefFromProto(protoDownloadOperationParams.GetInput()),
        .Output = FmrTableRefFromProto(protoDownloadOperationParams.GetOutput())
    };
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
    TMergeOperationParams mergeOperationParams{
        .Input = {},
        .Output = FmrTableRefFromProto(protoMergeOperationParams.GetOutput())
    };
    for (size_t i = 0; i < protoMergeOperationParams.InputSize(); ++i) {
        TOperationTableRef inputTable = OperationTableRefFromProto(protoMergeOperationParams.GetInput(i));
        mergeOperationParams.Input.emplace_back(inputTable);
    }
    return mergeOperationParams;
}

NProto::TSortedMergeOperationParams SortedMergeOperationParamsToProto(const TSortedMergeOperationParams& sortedMergeOperationParams) {
    NProto::TSortedMergeOperationParams protoSortedMergeOperationParams;
    for (ui64 i = 0; i < sortedMergeOperationParams.Input.size(); ++i) {
        auto inputTable = OperationTableRefToProto(sortedMergeOperationParams.Input[i]);
        auto* curInput = protoSortedMergeOperationParams.AddInput();
        curInput->Swap(&inputTable);
    }
    auto outputTable = FmrTableRefToProto(sortedMergeOperationParams.Output);
    protoSortedMergeOperationParams.MutableOutput()->Swap(&outputTable);
    return protoSortedMergeOperationParams;
}

TSortedMergeOperationParams SortedMergeOperationParamsFromProto(const NProto::TSortedMergeOperationParams& protoMergeOperationParams) {
    TSortedMergeOperationParams mergeOperationParams{
        .Input = {},
        .Output = FmrTableRefFromProto(protoMergeOperationParams.GetOutput())
    };
    for (ui64 i = 0; i < protoMergeOperationParams.InputSize(); ++i) {
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

NProto::TSortedMergeTaskParams SortedMergeTaskParamsToProto(const TSortedMergeTaskParams& sortedMergeTaskParams) {
    NProto::TSortedMergeTaskParams protoSortedMergeTaskParams;
    auto inputTables = TaskTableInputRefToProto(sortedMergeTaskParams.Input);
    protoSortedMergeTaskParams.MutableInput()->Swap(&inputTables);
    auto outputTable = FmrTableOutputRefToProto(sortedMergeTaskParams.Output);
    protoSortedMergeTaskParams.MutableOutput()->Swap(&outputTable);
    return protoSortedMergeTaskParams;
}

TSortedMergeTaskParams SortedMergeTaskParamsFromProto(const NProto::TSortedMergeTaskParams& protoSortedMergeTaskParams) {
    TSortedMergeTaskParams sortedMergeTaskParams;
    sortedMergeTaskParams.Input = TaskTableInputRefFromProto(protoSortedMergeTaskParams.GetInput());
    sortedMergeTaskParams.Output = FmrTableOutputRefFromProto(protoSortedMergeTaskParams.GetOutput());
    return sortedMergeTaskParams;
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
    protoMapOperationParams.SetIsOrdered(mapOperationParams.IsOrdered);
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
    return TMapOperationParams{.Input = inputTables, .Output = outputTables, .SerializedMapJobState = protoMapOperationParams.GetSerializedMapJobState(), .IsOrdered = protoMapOperationParams.GetIsOrdered()};
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
    protoMapTaskParams.SetIsOrdered(mapTaskParams.IsOrdered);
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
    mapTaskParams.IsOrdered = protoMapTaskParams.GetIsOrdered();
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
    } else if (auto* mapOperationParamsPtr = std::get_if<TMapOperationParams>(&operationParams)) {
        NProto::TMapOperationParams protoMapOperationParams = MapOperationParamsToProto(*mapOperationParamsPtr);
        protoOperationParams.MutableMapOperationParams()->Swap(&protoMapOperationParams);
    } else if (auto* SortedUploadOperationParamsPtr = std::get_if<TSortedUploadOperationParams>(&operationParams)) {
        NProto::TSortedUploadOperationParams protoSortedUploadOperationParams = SortedUploadOperationParamsToProto(*SortedUploadOperationParamsPtr);
        protoOperationParams.MutableSortedUploadOperationParams()->Swap(&protoSortedUploadOperationParams);
    } else if (auto* SortedMergeOperationParamsPtr = std::get_if<TSortedMergeOperationParams>(&operationParams)) {
        NProto::TSortedMergeOperationParams protoSortedMergeOperationParams = SortedMergeOperationParamsToProto(*SortedMergeOperationParamsPtr);
        protoOperationParams.MutableSortedMergeOperationParams()->Swap(&protoSortedMergeOperationParams);
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
    } else if (protoOperationParams.HasMapOperationParams()) {
        return MapOperationParamsFromProto(protoOperationParams.GetMapOperationParams());
    } else if (protoOperationParams.HasSortedUploadOperationParams()) {
        return SortedUploadOperationParamsFromProto(protoOperationParams.GetSortedUploadOperationParams());
    } else if (protoOperationParams.HasSortedMergeOperationParams()) {
        return SortedMergeOperationParamsFromProto(protoOperationParams.GetSortedMergeOperationParams());
    }
    return TOperationParams();
}

TSortedUploadTaskParams SortedUploadTaskParamsFromProto(const NProto::TSortedUploadTaskParams& protoSortedUploadTaskParams) {
    TSortedUploadTaskParams SortedUploadTaskParams;
    SortedUploadTaskParams.Input = FmrTableInputRefFromProto(protoSortedUploadTaskParams.GetInput());
    SortedUploadTaskParams.Output = YtTableRefFromProto(protoSortedUploadTaskParams.GetOutput());
    SortedUploadTaskParams.CookieYson = protoSortedUploadTaskParams.GetCookieYson();
    SortedUploadTaskParams.Order = protoSortedUploadTaskParams.GetOrder();
    return SortedUploadTaskParams;
}

NProto::TSortedUploadTaskParams SortedUploadTaskParamsToProto(const TSortedUploadTaskParams& SortedUploadTaskParams) {
    NProto::TSortedUploadTaskParams protoSortedUploadTaskParams;
    auto input = FmrTableInputRefToProto(SortedUploadTaskParams.Input);
    auto output = YtTableRefToProto(SortedUploadTaskParams.Output);
    protoSortedUploadTaskParams.MutableInput()->Swap(&input);
    protoSortedUploadTaskParams.MutableOutput()->Swap(&output);
    protoSortedUploadTaskParams.SetCookieYson(SortedUploadTaskParams.CookieYson);
    protoSortedUploadTaskParams.SetOrder(SortedUploadTaskParams.Order);

    return protoSortedUploadTaskParams;
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
    } else if (auto* mapTaskParamsPtr = std::get_if<TMapTaskParams>(&taskParams)) {
        NProto::TMapTaskParams protoMapTaskParams = MapTaskParamsToProto(*mapTaskParamsPtr);
        protoTaskParams.MutableMapTaskParams()->Swap(&protoMapTaskParams);
    } else if (auto* SortedUploadTaskParamsPtr = std::get_if<TSortedUploadTaskParams>(&taskParams)) {
        NProto::TSortedUploadTaskParams protoSortedUploadTaskParams = SortedUploadTaskParamsToProto(*SortedUploadTaskParamsPtr);
        protoTaskParams.MutableSortedUploadTaskParams()->Swap(&protoSortedUploadTaskParams);
    } else if (auto* SortedMergeTaskParamsPtr = std::get_if<TSortedMergeTaskParams>(&taskParams)) {
        NProto::TSortedMergeTaskParams protoSortedMergeTaskParams = SortedMergeTaskParamsToProto(*SortedMergeTaskParamsPtr);
        protoTaskParams.MutableSortedMergeTaskParams()->Swap(&protoSortedMergeTaskParams);
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
    } else if (protoTaskParams.HasMapTaskParams()) {
        taskParams = MapTaskParamsFromProto(protoTaskParams.GetMapTaskParams());
    } else if (protoTaskParams.HasSortedUploadTaskParams()) {
        taskParams = SortedUploadTaskParamsFromProto(protoTaskParams.GetSortedUploadTaskParams());
    } else if (protoTaskParams.HasSortedMergeTaskParams()) {
        taskParams = SortedMergeTaskParamsFromProto(protoTaskParams.GetSortedMergeTaskParams());
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

NProto::TFileInfo FileInfoToProto(const TFileInfo& fileInfo) {
    NProto::TFileInfo protoFileInfo;
    protoFileInfo.SetMd5Key(fileInfo.Md5Key);
    protoFileInfo.SetAlias(fileInfo.Alias);
    protoFileInfo.SetLocalPath(fileInfo.LocalPath);
    return protoFileInfo;
}

TFileInfo FileInfoFromProto(const NProto::TFileInfo& protoFileInfo) {
    return TFileInfo{
        .LocalPath = protoFileInfo.GetLocalPath(),
        .Md5Key = protoFileInfo.GetMd5Key(),
        .Alias = protoFileInfo.GetAlias()
    };
}

NProto::TYtResourceInfo YtResourceInfoToProto(const TYtResourceInfo& ytResourceInfo) {
    NProto::TYtResourceInfo protoYtResourceInfo;
    protoYtResourceInfo.SetRichPath(SerializeRichPath(ytResourceInfo.RichPath));
    protoYtResourceInfo.SetYtServerName(ytResourceInfo.YtServerName);
    protoYtResourceInfo.SetToken(ytResourceInfo.Token);
    protoYtResourceInfo.SetLocalPath(ytResourceInfo.LocalPath);
    return protoYtResourceInfo;
}

TYtResourceInfo YtResourceInfoFromProto(const NProto::TYtResourceInfo& protoYtResourceInfo) {
    return TYtResourceInfo {
        .RichPath = DeserializeRichPath(protoYtResourceInfo.GetRichPath()),
        .YtServerName = protoYtResourceInfo.GetYtServerName(),
        .Token = protoYtResourceInfo.GetToken(),
        .LocalPath = protoYtResourceInfo.GetLocalPath()
    };
}

NProto::TFmrResourceOperationInfo FmrResourceOperationInfoToProto(const TFmrResourceOperationInfo& fmrResourceOperationInfo) {
    NProto::TFmrResourceOperationInfo protoFmrResourceOperationInfo;
    auto protoFmrTable = FmrTableRefToProto(fmrResourceOperationInfo.FmrTable);
    protoFmrResourceOperationInfo.MutableFmrTable()->Swap(&protoFmrTable);
    protoFmrResourceOperationInfo.SetAlias(fmrResourceOperationInfo.Alias);
    return protoFmrResourceOperationInfo;
}

TFmrResourceOperationInfo FmrResourceOperationInfoFromProto(const NProto::TFmrResourceOperationInfo& protoFmrResourceOperationInfo) {
    return TFmrResourceOperationInfo{
        .FmrTable = FmrTableRefFromProto(protoFmrResourceOperationInfo.GetFmrTable()),
        .Alias = protoFmrResourceOperationInfo.GetAlias()
    };
}

NProto::TFmrResourceTaskInfo FmrResourceTaskInfoToProto(const TFmrResourceTaskInfo& fmrResourceTaskInfo) {
    NProto::TFmrResourceTaskInfo protoFmrResourceTaskInfo;
    for (auto& resourceTask: fmrResourceTaskInfo.FmrResourceTasks) {
        auto protoResourceTask = FmrTableInputRefToProto(resourceTask);
        protoFmrResourceTaskInfo.AddFmrResourceTasks()->Swap(&protoResourceTask);
    }
    protoFmrResourceTaskInfo.SetLocalPath(fmrResourceTaskInfo.LocalPath);
    protoFmrResourceTaskInfo.SetAlias(fmrResourceTaskInfo.Alias);
    return protoFmrResourceTaskInfo;
}

TFmrResourceTaskInfo FmrResourceTaskInfoFromProto(const NProto::TFmrResourceTaskInfo& protoFmrResourceTaskInfo) {
    TFmrResourceTaskInfo resourceTaskInfo;
    for (ui64 i = 0; i < protoFmrResourceTaskInfo.FmrResourceTasksSize(); ++i) {
        resourceTaskInfo.FmrResourceTasks.emplace_back(FmrTableInputRefFromProto(protoFmrResourceTaskInfo.GetFmrResourceTasks(i)));
    }
    resourceTaskInfo.LocalPath = protoFmrResourceTaskInfo.GetLocalPath();
    resourceTaskInfo.Alias = protoFmrResourceTaskInfo.GetAlias();
    return resourceTaskInfo;
}

NProto::TTask TaskToProto(const TTask& task) {
    NProto::TTask protoTask;
    protoTask.SetTaskType(NProto::ETaskType(task.TaskType));
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
    for (auto& fileInfo: task.Files) {
        NProto::TFileInfo protoFileInfo = FileInfoToProto(fileInfo);
        protoTask.AddFiles()->Swap(&protoFileInfo);
    }
    for (auto& ytResourceInfo: task.YtResources) {
        NProto::TYtResourceInfo protoYtResourceInfo = YtResourceInfoToProto(ytResourceInfo);
        protoTask.AddYtResources()->Swap(&protoYtResourceInfo);
    }
    for (auto& fmrResourceInfo: task.FmrResources) {
        NProto::TFmrResourceTaskInfo protoFmrResourceTaskInfo = FmrResourceTaskInfoToProto(fmrResourceInfo);
        protoTask.AddFmrResources()->Swap(&protoFmrResourceTaskInfo);
    }
    if (task.JobEnvironmentDir.Defined()) {
        protoTask.SetJobEnvironmentDir(*task.JobEnvironmentDir);
    }
    return protoTask;
}

TTask TaskFromProto(const NProto::TTask& protoTask) {
    TTask task;
    task.TaskType = ETaskType(protoTask.GetTaskType());
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
    for (ui64 i = 0; i < protoTask.FilesSize(); ++i) {
        task.Files.emplace_back(FileInfoFromProto(protoTask.GetFiles(i)));
    }
    for (ui64 i = 0; i < protoTask.YtResourcesSize(); ++i) {
        task.YtResources.emplace_back(YtResourceInfoFromProto(protoTask.GetYtResources(i)));
    }
    for (ui64 i = 0; i < protoTask.FmrResourcesSize(); ++i) {
        task.FmrResources.emplace_back(FmrResourceTaskInfoFromProto(protoTask.GetFmrResources(i)));
    }
    if (protoTask.HasJobEnvironmentDir()) {
        task.JobEnvironmentDir = protoTask.GetJobEnvironmentDir();
    }
    return task;
}

NProto::TTaskUploadResult TaskUploadResultToProto(const TTaskUploadResult& taskUploadResult) {
    Y_UNUSED(taskUploadResult);
    NProto::TTaskUploadResult protoTaskUploadResult;
    return protoTaskUploadResult;
}

TTaskUploadResult TaskUploadResultFromProto(const NProto::TTaskUploadResult& protoTaskUploadResult) {
    Y_UNUSED(protoTaskUploadResult);
    return TTaskUploadResult();
}

NProto::TTaskDownloadResult TaskDownloadResultToProto(const TTaskDownloadResult& taskDownloadResult) {
    Y_UNUSED(taskDownloadResult);
    NProto::TTaskDownloadResult protoTaskDownloadResult;
    return protoTaskDownloadResult;
}

TTaskDownloadResult TaskDownloadResultFromProto(const NProto::TTaskDownloadResult& protoTaskDownloadResult) {
    Y_UNUSED(protoTaskDownloadResult);
    return TTaskDownloadResult();
}

NProto::TTaskMergeResult TaskMergeResultToProto(const TTaskMergeResult& taskMergeResult) {
    Y_UNUSED(taskMergeResult);
    NProto::TTaskMergeResult protoTaskMergeResult;
    return protoTaskMergeResult;
}

TTaskMergeResult TaskMergeResultFromProto(const NProto::TTaskMergeResult& protoTaskMergeResult) {
    Y_UNUSED(protoTaskMergeResult);
    return TTaskMergeResult();
}

NProto::TTaskMapResult TaskMapResultToProto(const TTaskMapResult& taskMapResult) {
    Y_UNUSED(taskMapResult);
    NProto::TTaskMapResult protoTaskMapResult;
    return protoTaskMapResult;
}

TTaskMapResult TaskMapResultFromProto(const NProto::TTaskMapResult& protoTaskMapResult) {
    Y_UNUSED(protoTaskMapResult);
    return TTaskMapResult();
}

NProto::TTaskSortedUploadResult TaskSortedUploadResultToProto(const TTaskSortedUploadResult& taskSortedUploadResult) {
    NProto::TTaskSortedUploadResult protoTaskSortedUploadResult;
    protoTaskSortedUploadResult.SetFragmentResult(taskSortedUploadResult.FragmentResultYson);
    protoTaskSortedUploadResult.SetFragmentOrder(taskSortedUploadResult.FragmentOrder);
    return protoTaskSortedUploadResult;
}

TTaskSortedUploadResult TaskSortedUploadResultFromProto(const NProto::TTaskSortedUploadResult& protoTaskSortedUploadResult) {
    TTaskSortedUploadResult taskSortedUploadResult;
    taskSortedUploadResult.FragmentResultYson = protoTaskSortedUploadResult.GetFragmentResult();
    taskSortedUploadResult.FragmentOrder = protoTaskSortedUploadResult.GetFragmentOrder();
    return taskSortedUploadResult;
}

TTaskResult TaskResultFromProto(const NProto::TTaskResult& protoTaskResult) {
    if (protoTaskResult.HasTaskUploadResult()) {
        return TaskUploadResultFromProto(protoTaskResult.GetTaskUploadResult());
    } else if (protoTaskResult.HasTaskDownloadResult()) {
        return TaskDownloadResultFromProto(protoTaskResult.GetTaskDownloadResult());
    } else if (protoTaskResult.HasTaskMergeResult()) {
        return TaskMergeResultFromProto(protoTaskResult.GetTaskMergeResult());
    } else if (protoTaskResult.HasTaskMapResult()) {
        return TaskMapResultFromProto(protoTaskResult.GetTaskMapResult());
    } else if (protoTaskResult.HasTaskSortedUploadResult()) {
        return TaskSortedUploadResultFromProto(protoTaskResult.GetTaskSortedUploadResult());
    }
    return TTaskUploadResult();
}

NProto::TTaskResult TaskResultToProto(const TTaskResult& taskResult) {
    NProto::TTaskResult protoTaskResult;
    if (auto* taskUploadResultPtr = std::get_if<TTaskUploadResult>(&taskResult)) {
        auto uploadTask = TaskUploadResultToProto(*taskUploadResultPtr);
        protoTaskResult.MutableTaskUploadResult()->Swap(&uploadTask);
    } else if (auto* taskDownloadResultPtr = std::get_if<TTaskDownloadResult>(&taskResult)) {
        auto downloadTask = TaskDownloadResultToProto(*taskDownloadResultPtr);
        protoTaskResult.MutableTaskDownloadResult()->Swap(&downloadTask);
    } else if (auto* taskMergeResultPtr = std::get_if<TTaskMergeResult>(&taskResult)) {
        auto mergeTask = TaskMergeResultToProto(*taskMergeResultPtr);
        protoTaskResult.MutableTaskMergeResult()->Swap(&mergeTask);
    } else if (auto* taskMapResultPtr = std::get_if<TTaskMapResult>(&taskResult)) {
        auto mapTask = TaskMapResultToProto(*taskMapResultPtr);
        protoTaskResult.MutableTaskMapResult()->Swap(&mapTask);
    } else if (auto* taskSortedUploadResultPtr = std::get_if<TTaskSortedUploadResult>(&taskResult)) {
        auto SortedUploadTask = TaskSortedUploadResultToProto(*taskSortedUploadResultPtr);
        protoTaskResult.MutableTaskSortedUploadResult()->Swap(&SortedUploadTask);
    }
    return protoTaskResult;
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

} // namespace NYql::NFmr
