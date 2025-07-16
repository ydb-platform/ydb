#pragma once

#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>

namespace NYql::NFmr {

IYtJobService::TPtr MakeMockYtJobService(const std::unordered_map<TString, TString>& inputTables, std::unordered_map<TYtTableRef, TString>& outputTables);

} // namespace NYql::NFmr
