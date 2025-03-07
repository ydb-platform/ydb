#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/yql/providers/yt/fmr/job/interface/yql_yt_job.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

IFmrJob::TPtr MakeFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag);

TJobResult RunJob(TTask::TPtr task, ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag);

} // namespace NYql
