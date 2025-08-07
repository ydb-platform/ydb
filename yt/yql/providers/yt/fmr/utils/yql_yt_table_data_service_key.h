#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

TString GetTableDataServiceGroup(const TString& tableId, const TString& partId);

TString GetTableDataServiceChunkId(ui64 chunkNum, const TString& columnGroupName);

} // namespace NYql::NFmr
