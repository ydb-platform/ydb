#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/job/interface/yql_yt_job.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_sorted_merge_reader.h>

namespace NYql::NFmr {

struct TParseRecordSettings {
    ui64 MergeReadBlockCount = 1;
    ui64 MergeReadBlockSize = 1024 * 1024;
    ui64 MergeNumThreads = 3;
    ui64 UploadReadBlockCount = 1;
    ui64 UploadReadBlockSize = 1024 * 1024;
    ui64 DonwloadReadBlockCount = 1;
    ui64 DonwloadReadBlockSize = 1024 * 1024; // TODO - remove download
    ui64 MaxQueueSize = 100;
};

struct TFmrJobSettings {
    TParseRecordSettings ParseRecordSettings = TParseRecordSettings();
    TFmrReaderSettings FmrReaderSettings = TFmrReaderSettings();
    TFmrWriterSettings FmrWriterSettings = TFmrWriterSettings();
    TYtReaderSettings YtReaderSettings = TYtReaderSettings();
    TYtWriterSettings YtWriterSettings = TYtWriterSettings();
    TFmrUserJobSettings FmrUserJobSettings = TFmrUserJobSettings();
    ui64 NumThreads = 0;
};

IFmrJob::TPtr MakeFmrJob(
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    const TFmrJobSettings& settings = {},
    const TMaybe<TFmrTvmJobSettings>& tvmSettings = Nothing()
);

TJobResult RunJob(
    TTask::TPtr task,
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TFmrTvmJobSettings>& tvmSettings = Nothing()
);

TFmrJobSettings GetJobSettingsFromTask(TTask::TPtr task);

void FillMapFmrJob(
    TFmrUserJob& mapJob,
    const TMapTaskParams& mapTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    const TString& tableDataServiceDiscoveryFilePath,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
);

} // namespace NYql
