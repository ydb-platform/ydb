#include "yql_yt_table_data_service_key.h"
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYql::NFmr {

TString GetTableDataServiceGroup(const TString& tableId, const TString& partId) {
    return TStringBuilder() << tableId << "_" << partId;
}

TString GetTableDataServiceChunkId(ui64 chunkNum, const TString& columnGroupName) {
    if (columnGroupName.empty()) {
        return ToString(chunkNum);
    }
    return TStringBuilder() << columnGroupName << "_" << chunkNum;
}

} // namespace NYql::NFmr
