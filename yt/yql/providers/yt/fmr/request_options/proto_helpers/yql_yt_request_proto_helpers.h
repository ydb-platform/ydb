#pragma once

#include <yt/yql/providers/yt/fmr/proto/request_options.pb.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

NProto::TFmrError FmrErrorToProto(const TFmrError& error);

TFmrError FmrErrorFromProto(const NProto::TFmrError& protoError);

NProto::TYtTableRef YtTableRefToProto(const TYtTableRef& ytTableRef);

TYtTableRef YtTableRefFromProto(const NProto::TYtTableRef protoYtTableRef);

NProto::TYtTableTaskRef YtTableTaskRefToProto(const TYtTableTaskRef& ytTableTaskRef);

TYtTableTaskRef YtTableTaskRefFromProto(const NProto::TYtTableTaskRef protoYtTableTaskRef);

NProto::TFmrTableId FmrTableIdToProto(const TFmrTableId& fmrTableId);

TFmrTableId FmrTableIdFromProto(const NProto::TFmrTableId& protoFmrTableId);

NProto::TFmrTableRef FmrTableRefToProto(const TFmrTableRef& fmrTableRef);

TFmrTableRef FmrTableRefFromProto(const NProto::TFmrTableRef protoFmrTableRef);

NProto::TTableRange TableRangeToProto(const TTableRange& tableRange);

TTableRange TableRangeFromProto(const NProto::TTableRange& protoTableRange);

NProto::TFmrTableInputRef FmrTableInputRefToProto(const TFmrTableInputRef& fmrTableInputRef);

TFmrTableInputRef FmrTableInputRefFromProto(const NProto::TFmrTableInputRef& protoFmrTableInputRef);

NProto::TFmrTableOutputRef FmrTableOutputRefToProto(const TFmrTableOutputRef& fmrTableOutputRef);

TFmrTableOutputRef FmrTableOutputRefFromProto(const NProto::TFmrTableOutputRef& protoFmrTableOutputRef);

NProto::TTableStats TableStatsToProto(const TTableStats& tableStats);

TTableStats TableStatsFromProto(const NProto::TTableStats& protoTableStats);

NProto::TChunkStats ChunkStatsToProto(const TChunkStats& chunkStats);

TChunkStats ChunkStatsFromProto(const NProto::TChunkStats& protoChunkStats);

NProto::TTableChunkStats TableChunkStatsToProto(const TTableChunkStats& tableChunkStats);

TTableChunkStats TableChunkStatsFromProto(const NProto::TTableChunkStats& protoTableChunkStats);

NProto::TStatistics StatisticsToProto(const TStatistics& stats);

TStatistics StatisticsFromProto(const NProto::TStatistics& protoStats);

NProto::TOperationTableRef OperationTableRefToProto(const TOperationTableRef& operationTableRef);

TOperationTableRef OperationTableRefFromProto(const NProto::TOperationTableRef& protoOperationTableRef);

NProto::TTaskTableRef TaskTableRefToProto(const TTaskTableRef& taskTableRef);

TTaskTableRef TaskTableRefFromProto(const NProto::TTaskTableRef& protoTaskTableRef);

NProto::TTaskTableInputRef TaskTableInputRefToProto(const TTaskTableInputRef& taskTableInputRef);

TTaskTableInputRef TaskTableInputRefFromProto(const NProto::TTaskTableInputRef& protoTaskTableInputRef);

NProto::TUploadOperationParams UploadOperationParamsToProto(const TUploadOperationParams& uploadOperationParams);

TUploadOperationParams UploadOperationParamsFromProto(const NProto::TUploadOperationParams& protoUploadOperationParams);

NProto::TUploadTaskParams UploadTaskParamsToProto(const TUploadTaskParams& uploadTaskParams);

TUploadTaskParams UploadTaskParamsFromProto(const NProto::TUploadTaskParams& protoUploadTaskParams);

NProto::TDownloadOperationParams DownloadOperationParamsToProto(const TDownloadOperationParams& downloadOperationParams);

TDownloadOperationParams DownloadOperationParamsFromProto(const NProto::TDownloadOperationParams& protoDownloadOperationParams);

NProto::TDownloadTaskParams DownloadTaskParamsToProto(const TDownloadTaskParams& downloadTaskParams);

TDownloadTaskParams DownloadTaskParamsFromProto(const NProto::TDownloadTaskParams& protoDownloadTaskParams);

NProto::TMergeOperationParams MergeOperationParamsToProto(const TMergeOperationParams& mergeOperationParams);

TMergeOperationParams MergeOperationParamsFromProto(const NProto::TMergeOperationParams& protoMergeOperationParams);

NProto::TMergeTaskParams MergeTaskParamsToProto(const TMergeTaskParams& mergeTaskParams);

TMergeTaskParams MergeTaskParamsFromProto(const NProto::TMergeTaskParams& protoMergeTaskParams);

NProto::TMapOperationParams MapOperationParamsToProto(const TMapOperationParams& mapOperationParams);

TMapOperationParams MapOperationParamsFromProto(const NProto::TMapOperationParams& protoMapOperationParams);

NProto::TMapTaskParams MapTaskParamsToProto(const TMapTaskParams& mapTaskParams);

TMapTaskParams MapTaskParamsFromProto(const NProto::TMapTaskParams& protoMapTaskParams);

NProto::TOperationParams OperationParamsToProto(const TOperationParams& operationParams);

TOperationParams OperationParamsFromProto(const NProto::TOperationParams& protoOperationParams);

NProto::TTaskParams TaskParamsToProto(const TTaskParams& taskParams);

TTaskParams TaskParamsFromProto(const NProto::TTaskParams& protoTaskParams);

NProto::TClusterConnection ClusterConnectionToProto(const TClusterConnection& clusterConnection);

TClusterConnection ClusterConnectionFromProto(const NProto::TClusterConnection& protoClusterConnection);

NProto::TTask TaskToProto(const TTask& task);

TTask TaskFromProto(const NProto::TTask& protoTask);

NProto::TTaskState TaskStateToProto(const TTaskState& taskState);

TTaskState TaskStateFromProto(const NProto::TTaskState& protoTaskState);

} // namespace NYql::NFmr
