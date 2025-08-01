#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/job/interface/yql_yt_job.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

struct TParseRecordSettings {
    ui64 MergeReadBlockCount = 1;
    ui64 MergeReadBlockSize = 1024 * 1024;
    ui64 MergeNumThreads = 3;
    ui64 UploadReadBlockCount = 1;
    ui64 UploadReadBlockSize = 1024 * 1024;
    ui64 DonwloadReadBlockCount = 1;
    ui64 DonwloadReadBlockSize = 1024 * 1024; // TODO - remove download
};

struct TFmrJobSettings {
    TParseRecordSettings ParseRecordSettings = TParseRecordSettings();
    TFmrReaderSettings FmrReaderSettings = TFmrReaderSettings();
    TFmrWriterSettings FmrWriterSettings = TFmrWriterSettings();
    TYtReaderSettings YtReaderSettings = TYtReaderSettings();
    TYtWriterSettings YtWriterSettings = TYtWriterSettings();
    ui64 NumThreads = 0;
};

IFmrJob::TPtr MakeFmrJob(
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    const TFmrJobSettings& settings = {});

TJobResult RunJob(
    TTask::TPtr task,
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag
);

TFmrJobSettings GetJobSettingsFromTask(TTask::TPtr task);

void FillMapFmrJob(
    TFmrUserJob& mapJob,
    const TMapTaskParams& mapTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr jobService
);

} // namespace NYql
