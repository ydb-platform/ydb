#pragma once

#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

namespace NYql::NFmr {

IYtService::TPtr MakeMockYtService(const std::unordered_map<TYtTableRef, TString>& inputTables, std::unordered_map<TYtTableRef, TString>& outputTables);

} // namespace NYql::NFmr
