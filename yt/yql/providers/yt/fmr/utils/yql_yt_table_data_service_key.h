#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

TString GetTableDataServiceKey(const TString& tableId, const TString& partId, ui64 chunk);

std::pair<TString, ui64> GetTableDataServiceGroupAndChunk(const TString& fullKey);

} // namespace NYql::NFmr
