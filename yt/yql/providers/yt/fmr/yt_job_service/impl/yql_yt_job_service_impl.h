#pragma once

#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>

namespace NYql::NFmr {

IYtJobService::TPtr MakeYtJobSerivce();

} // namespace NYql::NFmr
