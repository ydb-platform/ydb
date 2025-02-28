#pragma once

#include <yt/yql/providers/yt/fmr/proto/request_options.pb.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

NProto::TFmrError FmrErrorToProto(const TFmrError& error);

TFmrError FmrErrorFromProto(const NProto::TFmrError& protoError);

NProto::TYtTableRef YtTableRefToProto(const TYtTableRef& ytTableRef);

TYtTableRef YtTableRefFromProto(const NProto::TYtTableRef protoYtTableRef);

NProto::TFmrTableRef FmrTableRefToProto(const TFmrTableRef& fmrTableRef);

TFmrTableRef FmrTableRefFromProto(const NProto::TFmrTableRef protoFmrTableRef);

NProto::TTableRef TableRefToProto(const TTableRef& tableRef);

TTableRef TableRefFromProto(const NProto::TTableRef& protoTableRef);

NProto::TUploadTaskParams UploadTaskParamsToProto(const TUploadTaskParams& uploadTaskParams);

TUploadTaskParams UploadTaskParamsFromProto(const NProto::TUploadTaskParams& protoUploadTaskParams);

NProto::TDownloadTaskParams DownloadTaskParamsToProto(const TDownloadTaskParams& downloadTaskParams);

TDownloadTaskParams DownloadTaskParamsFromProto(const NProto::TDownloadTaskParams& protoDownloadTaskParams);

NProto::TMergeTaskParams MergeTaskParamsToProto(const TMergeTaskParams& mergeTaskParams);

TMergeTaskParams MergeTaskParamsFromProto(const NProto::TMergeTaskParams& protoMergeTaskParams);

NProto::TTaskParams TaskParamsToProto(const TTaskParams& taskParams);

TTaskParams TaskParamsFromProto(const NProto::TTaskParams& protoTaskParams);

NProto::TClusterConnection ClusterConnectionToProto(const TClusterConnection& clusterConnection);

TClusterConnection ClusterConnectionFromProto(const NProto::TClusterConnection& protoClusterConnection);

NProto::TTask TaskToProto(const TTask& task);

TTask TaskFromProto(const NProto::TTask& protoTask);

NProto::TTaskState TaskStateToProto(const TTaskState& taskState);

TTaskState TaskStateFromProto(const NProto::TTaskState& protoTaskState);

} // namespace NYql::NFmr
