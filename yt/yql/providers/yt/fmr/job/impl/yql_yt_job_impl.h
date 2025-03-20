#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/job/interface/yql_yt_job.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

struct TParseRecordSettings {
    ui64 BlockCount = 1;
    ui64 BlockSize = 1024 * 1024; // 1Mb
};

struct TFmrJobSettings {
    TParseRecordSettings ParseRecordSettings = TParseRecordSettings();
    TFmrTableDataServiceReaderSettings FmrTableDataServiceReaderSettings = TFmrTableDataServiceReaderSettings();
    TFmrTableDataServiceWriterSettings FmrTableDataServiceWriterSettings = TFmrTableDataServiceWriterSettings();
};

IFmrJob::TPtr MakeFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag, const TFmrJobSettings& settings = {});

TJobResult RunJob(TTask::TPtr task, ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag, const TFmrJobSettings& settings = {});

} // namespace NYql
